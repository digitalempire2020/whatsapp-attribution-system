/**
 * Meta / Facebook Conversion API (CAPI) Integration
 * ==================================================
 * Sends server-side conversion events to Facebook via the Conversions API.
 *
 * This bypasses browser-based pixel limitations (iOS 14, ad blockers, cookie
 * deletion) by sending conversion data directly from server to Facebook.
 *
 * Uses FBCLID, FBP (_fbp cookie), FBC (_fbc cookie), hashed phone number,
 * and external_id for maximum Event Match Quality.
 *
 * SETUP REQUIRED:
 * 1. Create a Meta Pixel in Events Manager.
 * 2. Generate a CAPI access token in Events Manager > Settings.
 * 3. Fill in META_PIXEL_ID and META_ACCESS_TOKEN in your .env file.
 * 4. (Optional) Set META_TEST_EVENT_CODE for testing before going live.
 *
 * REFERENCE:
 * https://developers.facebook.com/docs/marketing-api/conversions-api
 */

const fetch = require("node-fetch");
const crypto = require("crypto");
const config = require("./config");

const META_GRAPH_API_VERSION = "v21.0";
const META_BASE_URL = `https://graph.facebook.com/${META_GRAPH_API_VERSION}`;

/**
 * Map our internal conversion types to Facebook standard events.
 */
function mapConversionTypeToFbEvent(conversionType) {
  const mapping = {
    lead: "Lead",
    appointment_scheduled: "Schedule",
    appointment_showed: "Lead",
    sale_completed: "Purchase",
    custom: "Lead",
  };
  return mapping[conversionType] || "Lead";
}

/**
 * Hash a value using SHA-256 (Facebook requires hashed user data).
 * Returns null if the input is null/undefined/empty.
 */
function hashValue(value) {
  if (!value) return null;
  return crypto
    .createHash("sha256")
    .update(value.toString().trim().toLowerCase())
    .digest("hex");
}

/**
 * Derive a two-letter country code from a phone number's international prefix.
 * Returns null if the prefix is not recognized.
 *
 * @param {string} phone - Phone number (with or without + prefix).
 * @returns {string|null} Two-letter ISO country code (lowercase).
 */
function deriveCountryCode(phone) {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, "");

  // Common prefixes for Southeast Asia + major markets
  const prefixes = {
    "65": "sg",   // Singapore
    "60": "my",   // Malaysia
    "62": "id",   // Indonesia
    "63": "ph",   // Philippines
    "66": "th",   // Thailand
    "84": "vn",   // Vietnam
    "1": "us",    // US/Canada
    "44": "gb",   // United Kingdom
    "61": "au",   // Australia
    "91": "in",   // India
    "81": "jp",   // Japan
    "82": "kr",   // South Korea
    "86": "cn",   // China
    "971": "ae",  // UAE
    "852": "hk",  // Hong Kong
    "886": "tw",  // Taiwan
  };

  // Try longest prefix first (3 digits), then 2, then 1
  for (const len of [3, 2, 1]) {
    const prefix = digits.substring(0, len);
    if (prefixes[prefix]) return prefixes[prefix];
  }

  return null;
}

/**
 * Send a conversion event to Facebook via the Conversions API.
 *
 * @param {Object} conversion - The conversion record from our database.
 * @param {Object} click - The matched WhatsApp click record with FB identifiers.
 * @param {Object|null} message - The matched WhatsApp message (has phone & sender name).
 * @returns {Object} Result with success status and response details.
 */
async function sendMetaConversion(conversion, click, message = null) {
  if (!config.meta.pixelId || !config.meta.accessToken) {
    console.log("[Meta] Missing Meta CAPI configuration, skipping");
    return { success: false, body: "Missing configuration" };
  }

  // Build user_data object for matching
  // Meta matches on multiple signals — the more we send, the higher the Event Match Quality (EMQ)
  const userData = {};

  // ---------- Browser-side identifiers ----------

  // FBP cookie (_fbp) - Facebook browser pixel ID
  if (click.fbp_cookie) {
    userData.fbp = click.fbp_cookie;
  }

  // FBC cookie (_fbc) - Facebook click ID cookie
  if (click.fbc_cookie) {
    userData.fbc = click.fbc_cookie;
  } else if (click.fbclid) {
    // Construct fbc from fbclid if _fbc cookie wasn't captured
    // Format: fb.1.{timestamp}.{fbclid}
    const clickTime = new Date(click.click_timestamp).getTime();
    userData.fbc = `fb.1.${clickTime}.${click.fbclid}`;
  }

  // Client IP address
  if (click.ip_address) {
    userData.client_ip_address = click.ip_address;
  }

  // User agent
  if (click.user_agent) {
    userData.client_user_agent = click.user_agent;
  }

  // ---------- Server-side identifiers (from WhatsApp message) ----------

  // Phone number — hashed SHA-256 in E.164 format
  // This is the WhatsApp sender's actual phone, a strong matching signal
  if (message && message.sender_phone) {
    let phone = message.sender_phone.replace(/[\s\-()]/g, "");
    if (!phone.startsWith("+")) phone = "+" + phone;
    const hashedPhone = hashValue(phone);
    if (hashedPhone) {
      userData.ph = [hashedPhone];
    }
  }

  // External ID — hashed visitor ID for cross-session/device matching
  if (click.visitor_id) {
    const hashedId = hashValue(click.visitor_id);
    if (hashedId) {
      userData.external_id = [hashedId];
    }
  }

  // Country code — derived from phone prefix, improves geographic matching
  if (message && message.sender_phone) {
    const countryCode = deriveCountryCode(message.sender_phone);
    if (countryCode) {
      userData.country = [hashValue(countryCode)];
    }
  }

  // Log match quality indicators
  const signals = [];
  if (userData.fbc || userData.fbp) signals.push("fb_cookies");
  if (userData.ph) signals.push("phone");
  if (userData.external_id) signals.push("external_id");
  if (userData.client_ip_address) signals.push("ip");
  if (userData.client_user_agent) signals.push("ua");
  console.log(`[Meta] Matching signals: ${signals.join(", ") || "none"}`);

  if (!userData.fbp && !userData.fbc && !userData.ph) {
    console.log("[Meta] Warning: No FBP, FBC, or phone — conversion may have low match quality");
  }

  // Build the event payload
  const eventTime = conversion.converted_at
    ? Math.floor(new Date(conversion.converted_at).getTime() / 1000)
    : Math.floor(Date.now() / 1000);
  const eventName = mapConversionTypeToFbEvent(conversion.conversion_type);

  const eventPayload = {
    event_name: eventName,
    event_time: eventTime,
    event_id: `conv_${conversion.id}_${eventTime}`, // Deduplication ID
    event_source_url: click.page_url || undefined,
    action_source: "website",
    user_data: userData,
  };

  // Add custom data for Purchase events
  if (conversion.conversion_type === "sale_completed" && conversion.conversion_value) {
    eventPayload.custom_data = {
      value: parseFloat(conversion.conversion_value),
      currency: conversion.currency || "SGD",
      content_type: "product",
    };
  }

  // Add lead source custom data so you can distinguish WhatsApp leads in Events Manager
  if (conversion.conversion_type === "lead") {
    eventPayload.custom_data = {
      ...(eventPayload.custom_data || {}),
      lead_event_source: "whatsapp_attribution",
    };
  }

  // Build the full API request body
  const requestBody = {
    data: [eventPayload],
  };

  // Include test event code during development
  if (config.meta.testEventCode) {
    requestBody.test_event_code = config.meta.testEventCode;
  }

  // Send to Facebook
  const url = `${META_BASE_URL}/${config.meta.pixelId}/events`;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${config.meta.accessToken}`,
    },
    body: JSON.stringify(requestBody),
    signal: controller.signal,
  }).finally(() => clearTimeout(timeout));

  const responseBody = await response.text();
  const success = response.ok;

  if (success) {
    try {
      const result = JSON.parse(responseBody);
      console.log(
        `[Meta] Conversion sent successfully. Events received: ${result.events_received || 0}`
      );
    } catch {
      console.log(`[Meta] Conversion sent successfully (non-JSON response)`);
    }
  } else {
    console.error(`[Meta] Conversion send failed: ${responseBody}`);
  }

  return {
    success,
    status: response.status,
    body: responseBody,
    payload: requestBody,
  };
}

module.exports = { sendMetaConversion };
