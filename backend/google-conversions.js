/**
 * Google Enhanced Conversions Integration
 * ========================================
 * Sends offline conversion data back to Google Ads using the
 * Google Ads API v20 Enhanced Conversions for Leads.
 *
 * This uses the GCLID (Google Click ID) captured from the visitor's
 * original ad click to attribute the WhatsApp conversion back to
 * the specific ad, keyword, and campaign that drove it.
 *
 * SETUP REQUIRED:
 * 1. Create an OAuth2 app in Google Cloud Console.
 * 2. Enable the Google Ads API.
 * 3. Generate a refresh token with the appropriate scopes.
 * 4. Create an "Offline" conversion action in Google Ads.
 * 5. Fill in the .env file with your credentials.
 *
 * REFERENCE:
 * https://developers.google.com/google-ads/api/docs/conversions/upload-clicks
 */

const fetch = require("node-fetch");
const config = require("./config");

const GOOGLE_ADS_API_VERSION = "v20";
const GOOGLE_ADS_BASE_URL = `https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}`;

/**
 * Get a fresh access token using the refresh token.
 */
async function getAccessToken() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);

  const response = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: config.googleAds.clientId,
      client_secret: config.googleAds.clientSecret,
      refresh_token: config.googleAds.refreshToken,
      grant_type: "refresh_token",
    }),
    signal: controller.signal,
  }).finally(() => clearTimeout(timeout));

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Failed to get Google access token: ${error}`);
  }

  const data = await response.json();
  return data.access_token;
}

/**
 * Map our conversion types to Google Ads conversion action names.
 */
function getConversionActionResourceName() {
  // The conversion action resource name follows this format:
  // customers/{customer_id}/conversionActions/{conversion_action_id}
  return `customers/${config.googleAds.customerId}/conversionActions/${config.googleAds.conversionActionId}`;
}

/**
 * Send a conversion event to Google Ads via Enhanced Conversions.
 *
 * @param {Object} conversion - The conversion record from our database.
 * @param {Object} click - The matched WhatsApp click record with GCLID.
 * @returns {Object} Result with success status and response details.
 */
async function sendGoogleConversion(conversion, click) {
  // Need either GCLID or GBRAID for attribution
  if (!click.gclid && !click.gbraid) {
    console.log("[Google] No GCLID or GBRAID available, skipping");
    return { success: false, body: "No GCLID or GBRAID available" };
  }

  if (!config.googleAds.customerId || !config.googleAds.conversionActionId) {
    console.log("[Google] Missing Google Ads configuration, skipping");
    return { success: false, body: "Missing configuration" };
  }

  const accessToken = await getAccessToken();

  // Build the conversion payload
  const conversionAction = getConversionActionResourceName();

  const conversionData = {
    conversionAction: conversionAction,
    // When the conversion happened — use visitor's timezone for accurate attribution
    conversionDateTime: formatGoogleDateTime(conversion.converted_at || new Date().toISOString(), click.timezone),
    // Conversion value (revenue)
    conversionValue: conversion.conversion_value || 0,
    currencyCode: conversion.currency || "SGD",
  };

  // GCLID is primary, GBRAID (iOS privacy) is fallback
  if (click.gclid) {
    conversionData.gclid = click.gclid;
  } else if (click.gbraid) {
    conversionData.gbraid = click.gbraid;
  }

  const payload = {
    conversions: [conversionData],
    // partialFailure must be true for the API to accept the request
    partialFailure: true,
  };

  const url = `${GOOGLE_ADS_BASE_URL}/customers/${config.googleAds.customerId}:uploadClickConversions`;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "developer-token": config.googleAds.developerToken,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    signal: controller.signal,
  }).finally(() => clearTimeout(timeout));

  const responseBody = await response.text();
  const success = response.ok;

  if (success) {
    console.log(`[Google] Conversion sent successfully for GCLID: ${click.gclid}`);
  } else {
    console.error(`[Google] Conversion send failed: ${responseBody}`);
  }

  return {
    success,
    status: response.status,
    body: responseBody,
    payload,
  };
}

/**
 * Format a date for Google Ads API (requires specific format).
 * Format: yyyy-mm-dd hh:mm:ss+|-hh:mm
 *
 * Uses the visitor's IANA timezone (e.g., "Asia/Singapore") captured at click time.
 * Falls back to UTC if the timezone is unavailable or invalid.
 *
 * @param {string} isoDateString - ISO 8601 date string.
 * @param {string|null} visitorTimezone - IANA timezone from the visitor's browser.
 */
function formatGoogleDateTime(isoDateString, visitorTimezone) {
  const date = new Date(isoDateString);

  // Try formatting in visitor's timezone
  if (visitorTimezone) {
    try {
      const parts = getDatePartsInTimezone(date, visitorTimezone);
      const offset = getTimezoneOffsetForZone(date, visitorTimezone);
      return `${parts.year}-${parts.month}-${parts.day} ${parts.hours}:${parts.minutes}:${parts.seconds}${offset}`;
    } catch (e) {
      console.warn(`[Google] Invalid timezone "${visitorTimezone}", falling back to UTC`);
    }
  }

  // Fallback: format as UTC
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  const seconds = String(date.getUTCSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}+00:00`;
}

/**
 * Extract date parts in a specific IANA timezone.
 */
function getDatePartsInTimezone(date, timezone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const parts = {};
  for (const { type, value } of formatter.formatToParts(date)) {
    if (type === "year") parts.year = value;
    if (type === "month") parts.month = value;
    if (type === "day") parts.day = value;
    if (type === "hour") parts.hours = value === "24" ? "00" : value;
    if (type === "minute") parts.minutes = value;
    if (type === "second") parts.seconds = value;
  }

  return parts;
}

/**
 * Calculate the UTC offset string for a given timezone at a specific date.
 * Returns format like "+08:00" or "-05:00".
 */
function getTimezoneOffsetForZone(date, timezone) {
  // Get the UTC representation and the local representation, then diff them
  const utcDate = new Date(date.toLocaleString("en-US", { timeZone: "UTC" }));
  const tzDate = new Date(date.toLocaleString("en-US", { timeZone: timezone }));
  const offsetMs = tzDate.getTime() - utcDate.getTime();
  const offsetMinutes = offsetMs / 60000;

  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absMinutes = Math.abs(offsetMinutes);
  const hours = String(Math.floor(absMinutes / 60)).padStart(2, "0");
  const minutes = String(absMinutes % 60).padStart(2, "0");
  return `${sign}${hours}:${minutes}`;
}

module.exports = { sendGoogleConversion };
