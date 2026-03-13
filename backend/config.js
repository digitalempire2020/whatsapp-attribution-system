/**
 * Configuration module
 * Loads environment variables and exports typed config object.
 */
require("dotenv").config();

const path = require("path");

const config = {
  // Database
  dbPath: process.env.DB_PATH || path.join(__dirname, "data", "attribution.db"),

  // Whapi
  whapi: {
    apiToken: process.env.WHAPI_API_TOKEN,
    webhookSecret: process.env.WHAPI_WEBHOOK_SECRET,
  },

  // Google Ads
  googleAds: {
    customerId: process.env.GOOGLE_ADS_CUSTOMER_ID?.replace(/-/g, ""),
    conversionActionId: process.env.GOOGLE_ADS_CONVERSION_ACTION_ID,
    developerToken: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    refreshToken: process.env.GOOGLE_ADS_REFRESH_TOKEN,
    clientId: process.env.GOOGLE_ADS_CLIENT_ID,
    clientSecret: process.env.GOOGLE_ADS_CLIENT_SECRET,
    loginCustomerId: process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID?.replace(/-/g, ""),
  },

  // Meta / Facebook
  meta: {
    pixelId: process.env.META_PIXEL_ID,
    accessToken: process.env.META_ACCESS_TOKEN,
    testEventCode: process.env.META_TEST_EVENT_CODE || null,
  },

  // GoHighLevel (push UTM data to contacts)
  ghl: {
    apiKey: process.env.GHL_API_KEY || null,
    locationId: process.env.GHL_LOCATION_ID || null,
    apiBaseUrl: process.env.GHL_API_BASE_URL || "https://services.leadconnectorhq.com",
    customFieldKeys: {
      utmCampaign: process.env.GHL_CF_UTM_CAMPAIGN || null,
      utmContent: process.env.GHL_CF_UTM_CONTENT || null,
      utmSource: process.env.GHL_CF_UTM_SOURCE || null,
      utmMedium: process.env.GHL_CF_UTM_MEDIUM || null,
      landingUrl: process.env.GHL_CF_LANDING_URL || null,
    },
  },

  // Server
  port: parseInt(process.env.PORT || "3000", 10),
  nodeEnv: process.env.NODE_ENV || "development",

  // Security
  apiKey: process.env.API_KEY || null,
  corsAllowedOrigins: process.env.CORS_ALLOWED_ORIGINS
    ? process.env.CORS_ALLOWED_ORIGINS.split(",").map((s) => s.trim())
    : null,
};

module.exports = config;
