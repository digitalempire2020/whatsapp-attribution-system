/**
 * GHL Contact Update Module
 * =========================
 * Pushes UTM attribution data from matched WhatsApp clicks
 * to GoHighLevel contacts via the GHL API.
 *
 * Flow:
 * 1. WhatsApp message matched to click (has UTM data)
 * 2. Find GHL contact by phone number
 * 3. Update contact's custom fields with UTM data
 * 4. If contact not found yet, queue for retry (Stellah may create it later)
 *
 * GHL API:
 * - Search: GET /contacts/?query={phone}&locationId={locationId}
 * - Update: PUT /contacts/{contactId} with customFields array
 * - Auth: Bearer token, Version: 2021-07-28
 * - Rate limit: 100 req/10s (low volume here, ~30 matches/day)
 */

const fetch = require("node-fetch");
const config = require("./config");
const database = require("./database");

// Retry backoff intervals in minutes: 1, 5, 15, 30, 60
const RETRY_BACKOFF_MINUTES = [1, 5, 15, 30, 60];
const RETRY_INTERVAL_MS = 30 * 1000; // Check retry queue every 30 seconds
const API_DELAY_MS = 200; // Delay between GHL API calls

let retryIntervalHandle = null;

// =============================================
// PHONE NUMBER NORMALIZATION
// =============================================

/**
 * Normalize a phone number to GHL-compatible format.
 * Whapi gives "6581234567" (no +), GHL stores "+6581234567".
 *
 * @param {string} phone - Raw phone number from Whapi.
 * @returns {string} Normalized phone number with + prefix.
 */
function normalizePhone(phone) {
  if (!phone) return "";

  // Strip spaces, dashes, parentheses
  let normalized = phone.replace(/[\s\-()]/g, "");

  // Prepend + if missing
  if (!normalized.startsWith("+")) {
    normalized = "+" + normalized;
  }

  return normalized;
}

// =============================================
// GHL API CALLS
// =============================================

/**
 * Make a GHL API request with proper auth headers.
 *
 * @param {string} method - HTTP method.
 * @param {string} endpoint - API endpoint path (e.g., "/contacts/").
 * @param {Object} [body] - Request body for PUT/POST.
 * @param {Object} [queryParams] - Query string parameters.
 * @returns {Promise<Object>} Response data.
 */
async function ghlRequest(method, endpoint, body = null, queryParams = null) {
  const url = new URL(endpoint, config.ghl.apiBaseUrl);

  if (queryParams) {
    Object.entries(queryParams).forEach(([key, value]) => {
      if (value != null) url.searchParams.set(key, value);
    });
  }

  const options = {
    method,
    headers: {
      Authorization: `Bearer ${config.ghl.apiKey}`,
      Version: "2021-07-28",
      "Content-Type": "application/json",
      Accept: "application/json",
    },
  };

  if (body) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(url.toString(), options);
  const data = await response.json().catch(() => ({}));

  return {
    ok: response.ok,
    status: response.status,
    data,
  };
}

/**
 * Search for a GHL contact by phone number.
 *
 * @param {string} phone - Normalized phone number (with + prefix).
 * @returns {Promise<string|null>} GHL contact ID or null if not found.
 */
async function findContactByPhone(phone) {
  if (!phone) return null;

  try {
    const result = await ghlRequest("GET", "/contacts/", null, {
      query: phone,
      locationId: config.ghl.locationId,
    });

    if (!result.ok) {
      console.error(
        `[GHL] Contact search failed (${result.status}):`,
        JSON.stringify(result.data).substring(0, 200)
      );
      return null;
    }

    const contacts = result.data.contacts || [];

    if (contacts.length === 0) {
      return null;
    }

    // Return the first match — GHL search is exact on phone
    const contactId = contacts[0].id;
    console.log(
      `[GHL] Found contact ${contactId} for phone ${phone.substring(0, 6)}...`
    );
    return contactId;
  } catch (err) {
    console.error("[GHL] Contact search error:", err.message);
    return null;
  }
}

/**
 * Update a GHL contact's custom fields with UTM data.
 * Only writes fields that have values and whose custom field IDs are configured.
 * Skips if the contact already has UTM data populated (no overwrite).
 *
 * @param {string} contactId - GHL contact ID.
 * @param {Object} utmData - UTM data from the click record.
 * @param {number} clickId - Click record ID for audit logging.
 * @returns {Promise<boolean>} True if update succeeded.
 */
async function updateContactUTM(contactId, utmData, clickId) {
  const fieldMap = config.ghl.customFieldKeys;

  // Build the customFields array — only include fields that have both a value and a configured ID
  const customFields = [];
  const fieldsUpdated = {};

  const mappings = [
    { key: "utmCampaign", value: utmData.utm_campaign, name: "wa_utm_campaign" },
    { key: "utmContent", value: utmData.utm_content, name: "wa_utm_content" },
    { key: "utmSource", value: utmData.utm_source, name: "wa_utm_source" },
    { key: "utmMedium", value: utmData.utm_medium, name: "wa_utm_medium" },
    { key: "landingUrl", value: utmData.page_url, name: "wa_landing_url" },
  ];

  for (const { key, value, name } of mappings) {
    const fieldId = fieldMap[key];
    if (fieldId && value) {
      customFields.push({ id: fieldId, field_value: value });
      fieldsUpdated[name] = value;
    }
  }

  if (customFields.length === 0) {
    console.log("[GHL] No UTM fields to update (no values or no field IDs configured)");
    return false;
  }

  try {
    // First, check if contact already has UTM data (avoid overwriting)
    const contactResult = await ghlRequest("GET", `/contacts/${contactId}`);

    if (contactResult.ok) {
      const existingFields = contactResult.data.contact?.customFields || [];
      const existingFieldIds = new Set();

      for (const cf of existingFields) {
        const id = cf.id || cf.fieldKey;
        const val = cf.value || cf.fieldValue;
        if (id && val && String(val).trim()) {
          existingFieldIds.add(id);
        }
      }

      // Filter out fields that already have values
      const newFields = customFields.filter((f) => !existingFieldIds.has(f.id));

      if (newFields.length === 0) {
        console.log(`[GHL] Contact ${contactId} already has UTM data, skipping update`);

        database.insertGHLUpdateEvent({
          click_id: clickId,
          ghl_contact_id: contactId,
          fields_updated: { skipped: "already_populated" },
          response_status: 200,
          response_body: "Already has UTM data",
          success: true,
        });

        return true;
      }

      // Use only the new fields
      customFields.length = 0;
      customFields.push(...newFields);

      // Update fieldsUpdated to reflect only the fields actually being written
      const writtenFieldIds = new Set(newFields.map((f) => f.id));
      for (const { key, name } of mappings) {
        const fieldId = fieldMap[key];
        if (fieldId && !writtenFieldIds.has(fieldId)) {
          delete fieldsUpdated[name];
        }
      }
    }

    await delay(API_DELAY_MS);

    // Perform the update
    const updateResult = await ghlRequest("PUT", `/contacts/${contactId}`, {
      customFields,
    });

    // Log the API event
    database.insertGHLUpdateEvent({
      click_id: clickId,
      ghl_contact_id: contactId,
      fields_updated: fieldsUpdated,
      response_status: updateResult.status,
      response_body: JSON.stringify(updateResult.data).substring(0, 500),
      success: updateResult.ok,
    });

    if (updateResult.ok) {
      console.log(
        `[GHL] Updated contact ${contactId} with ${customFields.length} UTM fields`
      );
      return true;
    } else {
      console.error(
        `[GHL] Contact update failed (${updateResult.status}):`,
        JSON.stringify(updateResult.data).substring(0, 200)
      );
      return false;
    }
  } catch (err) {
    console.error("[GHL] Contact update error:", err.message);

    database.insertGHLUpdateEvent({
      click_id: clickId,
      ghl_contact_id: contactId,
      fields_updated: fieldsUpdated,
      response_status: 0,
      response_body: err.message,
      success: false,
    });

    return false;
  }
}

// =============================================
// ORCHESTRATOR — Called from webhook handler
// =============================================

/**
 * Push UTM data from a matched click to the corresponding GHL contact.
 * Called fire-and-forget from the webhook handler after a successful match.
 *
 * @param {Object} matchResult - Match result from matching engine.
 * @param {Object} messageData - Parsed message data from webhook.
 * @param {number} messageId - Saved message database ID.
 */
async function pushUTMToGHL(matchResult, messageData, messageId) {
  // Skip if no match or GHL not configured
  if (!matchResult.clickId) return;
  if (!config.ghl.apiKey || !config.ghl.locationId) {
    console.log("[GHL] Not configured, skipping UTM push");
    return;
  }

  try {
    // Get the click record (has full UTM data)
    const click = database.getClickById(matchResult.clickId);
    if (!click) {
      console.error(`[GHL] Click ${matchResult.clickId} not found in database`);
      return;
    }

    // Check if click has any UTM data worth pushing
    if (!click.utm_campaign && !click.utm_source && !click.utm_content && !click.page_url) {
      console.log("[GHL] Click has no UTM data, skipping push");
      return;
    }

    // Normalize phone number
    const phone = normalizePhone(messageData.senderPhone);
    if (!phone) {
      console.error("[GHL] No phone number available for contact search");
      return;
    }

    console.log(`[GHL] Attempting UTM push for click ${click.id}, phone ${phone.substring(0, 6)}...`);

    // Find GHL contact by phone
    const contactId = await findContactByPhone(phone);

    if (contactId) {
      // Contact found — update immediately
      const utmData = {
        utm_campaign: click.utm_campaign || "",
        utm_content: click.utm_content || "",
        utm_source: click.utm_source || "",
        utm_medium: click.utm_medium || "",
        page_url: click.page_url || "",
      };

      const success = await updateContactUTM(contactId, utmData, click.id);

      if (success) {
        console.log(`[GHL] UTM push complete for contact ${contactId}`);
      }
    } else {
      // Contact not found — queue for retry (Stellah may create it shortly)
      console.log(
        `[GHL] Contact not found for phone ${phone.substring(0, 6)}..., queuing for retry`
      );

      database.insertPendingGHLUpdate({
        click_id: click.id,
        message_id: messageId,
        sender_phone: phone,
      });
    }
  } catch (err) {
    console.error("[GHL] pushUTMToGHL unexpected error:", err.message);
  }
}

// =============================================
// RETRY QUEUE PROCESSOR
// =============================================

/**
 * Process pending GHL updates that failed or were queued because
 * the contact didn't exist yet.
 */
async function processRetryQueue() {
  if (!config.ghl.apiKey || !config.ghl.locationId) return;

  try {
    const pending = database.getPendingGHLUpdates(5);

    if (pending.length === 0) return;

    console.log(`[GHL Retry] Processing ${pending.length} pending updates`);

    for (const item of pending) {
      try {
        // Find GHL contact by phone
        const contactId = await findContactByPhone(item.sender_phone);

        if (contactId) {
          // Get click record for UTM data
          const click = database.getClickById(item.click_id);

          if (!click) {
            database.updatePendingGHLStatus(
              item.id, "failed", null, "Click record not found", null
            );
            continue;
          }

          const utmData = {
            utm_campaign: click.utm_campaign || "",
            utm_content: click.utm_content || "",
            utm_source: click.utm_source || "",
            utm_medium: click.utm_medium || "",
            page_url: click.page_url || "",
          };

          const success = await updateContactUTM(contactId, utmData, click.id);

          if (success) {
            database.updatePendingGHLStatus(
              item.id, "completed", contactId, null, null
            );
            console.log(`[GHL Retry] Successfully updated contact ${contactId}`);
          } else {
            const backoffMinutes = RETRY_BACKOFF_MINUTES[Math.min(item.attempts, RETRY_BACKOFF_MINUTES.length - 1)];
            const nextRetry = new Date(Date.now() + backoffMinutes * 60 * 1000).toISOString();

            database.updatePendingGHLStatus(
              item.id, "pending", null, "Update failed", nextRetry
            );
          }
        } else {
          // Contact still not found — schedule retry with backoff
          const backoffMinutes = RETRY_BACKOFF_MINUTES[Math.min(item.attempts, RETRY_BACKOFF_MINUTES.length - 1)];
          const nextRetry = new Date(Date.now() + backoffMinutes * 60 * 1000).toISOString();

          if (item.attempts >= 4) {
            // Last attempt — mark as failed
            database.updatePendingGHLStatus(
              item.id, "failed", null, "Contact not found after 5 attempts", null
            );
            console.log(
              `[GHL Retry] Giving up on phone ${item.sender_phone.substring(0, 6)}... after 5 attempts`
            );
          } else {
            database.updatePendingGHLStatus(
              item.id, "pending", null, "Contact not found yet", nextRetry
            );
            console.log(
              `[GHL Retry] Contact not found, retry ${item.attempts + 1}/5 in ${backoffMinutes}m`
            );
          }
        }

        // Small delay between retries to respect rate limits
        await delay(API_DELAY_MS);
      } catch (err) {
        console.error(`[GHL Retry] Error processing item ${item.id}:`, err.message);

        const backoffMinutes = RETRY_BACKOFF_MINUTES[Math.min(item.attempts, RETRY_BACKOFF_MINUTES.length - 1)];
        const nextRetry = new Date(Date.now() + backoffMinutes * 60 * 1000).toISOString();

        database.updatePendingGHLStatus(
          item.id, "pending", null, err.message, nextRetry
        );
      }
    }
  } catch (err) {
    console.error("[GHL Retry] Queue processing error:", err.message);
  }
}

/**
 * Start the background retry processor.
 * Checks the pending_ghl_updates table every 30 seconds.
 */
function startRetryProcessor() {
  if (retryIntervalHandle) {
    console.log("[GHL Retry] Already running");
    return;
  }

  console.log("[GHL] Starting retry processor (every 30s)");

  // Validate config on startup
  const missing = [];
  if (!config.ghl.apiKey) missing.push("GHL_API_KEY");
  if (!config.ghl.locationId) missing.push("GHL_LOCATION_ID");

  const fieldKeys = config.ghl.customFieldKeys;
  const configuredFields = Object.entries(fieldKeys).filter(([, v]) => v);

  if (missing.length > 0) {
    console.warn(`[GHL] Missing config: ${missing.join(", ")} — GHL push disabled`);
    return;
  }

  if (configuredFields.length === 0) {
    console.warn(
      "[GHL] No custom field IDs configured (GHL_CF_UTM_*). " +
      "Create custom fields in GHL and add their IDs to .env"
    );
    return;
  }

  console.log(`[GHL] ${configuredFields.length}/5 custom field IDs configured`);

  retryIntervalHandle = setInterval(processRetryQueue, RETRY_INTERVAL_MS);
}

/**
 * Stop the background retry processor.
 */
function stopRetryProcessor() {
  if (retryIntervalHandle) {
    clearInterval(retryIntervalHandle);
    retryIntervalHandle = null;
    console.log("[GHL] Retry processor stopped");
  }
}

// =============================================
// UTILITY
// =============================================

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// =============================================
// EXPORTS
// =============================================

module.exports = {
  normalizePhone,
  findContactByPhone,
  updateContactUTM,
  pushUTMToGHL,
  processRetryQueue,
  startRetryProcessor,
  stopRetryProcessor,
};
