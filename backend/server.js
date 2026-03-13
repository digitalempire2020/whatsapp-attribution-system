/**
 * WhatsApp Attribution System - Express Server
 * =============================================
 * Main entry point. Handles:
 * - Frontend tracking endpoints (page views, clicks)
 * - Whapi webhook for incoming WhatsApp messages
 * - Conversion recording API
 * - Conversion API dispatch (Google, Meta)
 * - Health check endpoint
 */
const express = require("express");
const path = require("path");
const crypto = require("crypto");
const cors = require("cors");
const config = require("./config");
const database = require("./database");
const { handleWhapiWebhook } = require("./whapi-webhook");
const { matchMessageToClick } = require("./matching-engine");
const { sendGoogleConversion } = require("./google-conversions");
const { sendMetaConversion } = require("./meta-conversions");
const { pushUTMToGHL, startRetryProcessor } = require("./ghl-contact-update");

// =============================================
// INPUT SANITIZATION
// =============================================
/**
 * Strip HTML tags and common XSS patterns from a string.
 * Preserves legitimate UTM values while removing injection attempts.
 */
function sanitizeString(val) {
  if (typeof val !== 'string') return val;
  return val
    .replace(/<[^>]*>/g, '')         // Strip HTML tags
    .replace(/javascript:/gi, '')     // Strip JS protocol
    .replace(/on\w+\s*=/gi, '')      // Strip event handlers (onclick=, etc.)
    .trim();
}

/**
 * Sanitize all string values in a flat object (one level deep).
 */
function sanitizeObject(obj) {
  if (!obj || typeof obj !== 'object') return obj;
  const clean = {};
  for (const [key, val] of Object.entries(obj)) {
    clean[key] = sanitizeString(val);
  }
  return clean;
}

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: "100kb" }));
// sendBeacon sends text/plain to avoid CORS preflight — parse it as JSON
app.use(express.text({ limit: "100kb", type: "text/plain" }));
app.use((req, res, next) => {
  if (typeof req.body === "string" && req.body.startsWith("{")) {
    try { req.body = JSON.parse(req.body); } catch (e) { /* leave as string */ }
  }
  next();
});
if (config.corsAllowedOrigins && config.corsAllowedOrigins.length > 0) {
  // Support wildcard subdomains: "*.bellezza.com.sg" matches any subdomain
  const wildcardPatterns = config.corsAllowedOrigins
    .filter((o) => o.startsWith("*."))
    .map((o) => o.slice(2)); // e.g. "*.bellezza.com.sg" → "bellezza.com.sg"
  const exactOrigins = config.corsAllowedOrigins.filter((o) => !o.startsWith("*."));

  app.use(
    cors({
      origin: function (origin, callback) {
        if (!origin) return callback(null, true); // allow non-browser requests
        // Check exact matches (e.g. "https://bellezza.com.sg")
        if (exactOrigins.includes(origin)) return callback(null, true);
        // Check wildcard subdomain matches (e.g. "*.bellezza.com.sg")
        for (const domain of wildcardPatterns) {
          if (
            origin.endsWith("." + domain) ||
            origin === "https://" + domain ||
            origin === "http://" + domain
          ) {
            return callback(null, true);
          }
        }
        console.warn(`[CORS] Rejected origin: ${origin}`);
        callback(new Error("CORS not allowed"));
      },
    })
  );
} else {
  // Dev mode: allow all origins but warn in production
  app.use(cors());
  if (config.nodeEnv === "production") {
    console.warn("[Server] WARNING: CORS allows all origins. Set CORS_ALLOWED_ORIGINS in production.");
  }
}

// Trust first proxy hop only (Render's reverse proxy) — prevents IP spoofing
app.set("trust proxy", 1);

// =============================================
// SIMPLE IN-MEMORY RATE LIMITER
// =============================================
// Protects public tracking endpoints from abuse (100 requests per minute per IP)
const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX = 100;

// Clean stale entries every 5 minutes to prevent memory growth
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of rateLimitMap) {
    if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS * 2) {
      rateLimitMap.delete(key);
    }
  }
}, 5 * 60 * 1000);

function rateLimit(req, res, next) {
  const ip = req.ip || "unknown";
  const now = Date.now();
  let entry = rateLimitMap.get(ip);

  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    entry = { count: 1, windowStart: now };
    rateLimitMap.set(ip, entry);
    return next();
  }

  entry.count++;
  if (entry.count > RATE_LIMIT_MAX) {
    return res.status(429).json({ error: "Too many requests" });
  }
  next();
}

// =============================================
// API KEY AUTHENTICATION MIDDLEWARE
// =============================================
function authenticateApiKey(req, res, next) {
  // If no API key configured, skip auth (dev mode)
  if (!config.apiKey) {
    return next();
  }

  // Check Authorization: Bearer <token> header first, then X-API-Key header
  let providedKey = null;
  const authHeader = req.headers["authorization"];
  if (authHeader && authHeader.startsWith("Bearer ")) {
    providedKey = authHeader.slice(7);
  } else if (req.headers["x-api-key"]) {
    providedKey = req.headers["x-api-key"];
  }

  if (providedKey !== config.apiKey) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  next();
}

// =============================================
// SERVE TRACKER SCRIPT
// =============================================
app.get("/tracker.js", (req, res) => {
  const fs = require("fs");
  const filePath = path.join(__dirname, "public", "tracker.js");
  let content = fs.readFileSync(filePath, "utf8");
  // Dynamically inject the server URL so the placeholder is never used
  const serverUrl = `${req.protocol}://${req.get("host")}`;
  content = content.replace(
    /API_URL:\s*window\.__WA_TRACKER_API_URL\s*\|\|\s*"[^"]*"/,
    `API_URL: window.__WA_TRACKER_API_URL || "${serverUrl}"`
  );
  content = content.replace(
    /API_URL:\s*"https:\/\/YOUR_SERVER_URL"/,
    `API_URL: window.__WA_TRACKER_API_URL || "${serverUrl}"`
  );
  res.setHeader("Content-Type", "application/javascript");
  res.setHeader("Cache-Control", "public, max-age=300"); // 5 min cache
  res.send(content);
});

// =============================================
// HEALTH CHECK
// =============================================
app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// =============================================
// REDIRECT - /r/:code → original landing page
// =============================================
// When the priority code appears as a URL (e.g. fatfreeze.bellezza.com.sg/r/D9C9PTD9)
// in the WhatsApp message, tapping it hits this route and redirects to the original page.
app.get("/r/:code", (req, res) => {
  const code = "WA-" + req.params.code.toUpperCase();
  try {
    const click = database.getClickByCode(code);
    if (click && click.page_url) {
      // Validate the redirect URL to prevent open redirect attacks
      try {
        const targetUrl = new URL(click.page_url);
        if (targetUrl.hostname.endsWith("bellezza.com.sg")) {
          return res.redirect(302, click.page_url);
        }
        console.warn(`[Redirect] Blocked redirect to untrusted domain: ${targetUrl.hostname}`);
      } catch (urlErr) {
        console.warn(`[Redirect] Invalid page_url stored: ${click.page_url}`);
      }
    }
  } catch (err) {
    console.error("[Redirect] Lookup error:", err.message);
  }
  // Fallback: redirect to main domain
  res.redirect(302, "https://bellezza.com.sg");
});

// =============================================
// FRONTEND TRACKING - Page Views
// =============================================
app.post("/api/track/pageview", rateLimit, (req, res) => {
  try {
    const data = req.body;
    data.ip_address = req.ip;

    if (!data.visitor_id) {
      return res.status(400).json({ error: 'visitor_id is required' });
    }

    const cleanData = sanitizeObject(data);
    const id = database.insertPageView(cleanData);
    res.status(201).json({ status: "ok", id });
  } catch (err) {
    console.error("[Track] Page view error:", err.message);
    res.status(500).json({ error: "Failed to record page view" });
  }
});

// =============================================
// FRONTEND TRACKING - WhatsApp Clicks
// =============================================
app.post("/api/track/click", rateLimit, (req, res) => {
  try {
    const data = req.body;
    data.ip_address = req.ip;

    if (!data.visitor_id || !data.priority_code) {
      return res.status(400).json({ error: 'visitor_id and priority_code are required' });
    }

    const cleanData = sanitizeObject(data);
    const id = database.insertClick(cleanData);
    res.status(201).json({ status: "ok", id });
  } catch (err) {
    // Handle duplicate priority codes
    if (err.message && err.message.includes("UNIQUE constraint failed")) {
      return res.status(200).json({ status: "duplicate" });
    }
    console.error("[Track] Click error:", err.message);
    res.status(500).json({ error: "Failed to record click" });
  }
});

// =============================================
// WHAPI WEBHOOK - Incoming WhatsApp Messages
// =============================================
app.post("/webhook/whapi", async (req, res) => {
  try {
    // Verify webhook authenticity (constant-time comparison to prevent timing attacks)
    if (config.whapi.webhookSecret) {
      const providedToken = req.query.token || req.headers["x-webhook-token"] || "";
      const expected = Buffer.from(config.whapi.webhookSecret);
      const provided = Buffer.from(providedToken);
      if (expected.length !== provided.length || !crypto.timingSafeEqual(expected, provided)) {
        console.warn("[Webhook] Invalid or missing webhook token");
        return res.status(401).json({ error: "Invalid webhook token" });
      }
    }

    console.log("[Webhook] Received Whapi event");

    const messageData = handleWhapiWebhook(req.body);

    if (!messageData) {
      return res.status(200).json({ status: "ignored" });
    }

    console.log(`[Webhook] Processing message from ${messageData.senderPhone}`);

    // Step 1: Match message to click
    const matchResult = matchMessageToClick(database, messageData);

    // Step 2: Store message
    let savedMessage;
    try {
      savedMessage = database.insertMessage({
        whapi_message_id: messageData.messageId,
        sender_phone: messageData.senderPhone,
        sender_name: messageData.senderName,
        message_text: messageData.messageText,
        priority_code: matchResult.priorityCode,
        matched_click_id: matchResult.clickId,
        match_method: matchResult.method,
        match_confidence: matchResult.confidence,
        message_timestamp: messageData.timestamp,
      });
    } catch (err) {
      if (err.message && err.message.includes("UNIQUE constraint failed")) {
        console.log("[Webhook] Duplicate message ignored:", messageData.messageId);
        return res.status(200).json({ status: "duplicate" });
      }
      throw err;
    }

    console.log(
      `[Webhook] Message saved. Match: ${matchResult.method} (confidence: ${matchResult.confidence})`
    );

    // Log time proximity matches as informational only — no action taken
    if (matchResult.method === "time_proximity") {
      console.log(
        `[Webhook] Time proximity match (click ${matchResult.clickId}) logged but NOT auto-fired — requires priority code for conversion`
      );
    }

    // Push UTM data to GHL contact ONLY for priority code matches
    // Time proximity matches risk pushing wrong UTM data to unrelated contacts
    if (matchResult.clickId && matchResult.method === "priority_code" && config.ghl && config.ghl.apiKey) {
      pushUTMToGHL(matchResult, messageData, savedMessage.id).catch((err) =>
        console.error("[GHL] Push failed:", err.message)
      );
    }

    // Auto-fire Lead conversion ONLY for high-confidence priority code matches
    // Time proximity matches are too unreliable (can match any message to any click)
    if (matchResult.clickId && matchResult.method === "priority_code") {
      try {
        const click = database.getClickById(matchResult.clickId);
        if (click) {
          const conversion = database.insertConversion({
            message_id: savedMessage.id,
            click_id: matchResult.clickId,
            conversion_type: "lead",
            conversion_value: null,
            currency: "SGD",
            notes: `Auto-fired on message match (${matchResult.method}, confidence: ${matchResult.confidence})`,
          });

          console.log(`[Webhook] Auto-fired Lead conversion (ID: ${conversion.id}) for matched message`);

          // Dispatch to Google Ads + Meta CAPI (fire-and-forget)
          // Pass message data so Meta can use hashed phone for better matching
          const message = database.getMessageById(savedMessage.id);
          dispatchConversionToAdPlatforms(conversion, click, message).catch((err) =>
            console.error("[Webhook] Ad platform dispatch failed:", err.message)
          );
        }
      } catch (convErr) {
        // Don't fail the webhook if conversion dispatch fails
        console.error("[Webhook] Auto-conversion error:", convErr.message);
      }
    }

    res.status(200).json({
      status: "processed",
      messageId: savedMessage.id,
      matchMethod: matchResult.method,
    });
  } catch (err) {
    console.error("[Webhook] Unexpected error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// =============================================
// RECORD CONVERSION - Manual or API-triggered
// =============================================
app.post("/api/conversions", authenticateApiKey, async (req, res) => {
  try {
    const {
      message_id,
      click_id,
      conversion_type,
      conversion_value,
      currency,
      notes,
    } = req.body;

    if (!conversion_type) {
      return res.status(400).json({ error: "conversion_type is required" });
    }
    if (!message_id && !click_id) {
      return res.status(400).json({ error: "Either message_id or click_id is required" });
    }

    // If only message_id, look up associated click_id
    let resolvedClickId = click_id;
    if (!resolvedClickId && message_id) {
      resolvedClickId = database.getMessageMatchedClickId(message_id);
    }

    const conversion = database.insertConversion({
      message_id: message_id || null,
      click_id: resolvedClickId || null,
      conversion_type,
      conversion_value: conversion_value || null,
      currency: currency || "SGD",
      notes: notes || null,
    });

    console.log(`[Conversion] Recorded: ${conversion_type} (ID: ${conversion.id})`);

    // Send conversion data back to ad platforms
    if (resolvedClickId) {
      const click = database.getClickById(resolvedClickId);
      if (click) {
        // Look up message for hashed phone data (improves Meta match quality)
        const message = message_id ? database.getMessageById(message_id) : null;
        await dispatchConversionToAdPlatforms(conversion, click, message);
      }
    }

    res.status(201).json({ status: "recorded", conversion });
  } catch (err) {
    console.error("[Conversion] Unexpected error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * Send conversion data back to the relevant ad platforms.
 *
 * @param {Object} conversion - The conversion record.
 * @param {Object} click - The matched click record with ad platform IDs.
 * @param {Object|null} message - The matched WhatsApp message (for phone/name data).
 */
async function dispatchConversionToAdPlatforms(conversion, click, message = null) {
  const promises = [];

  if (click.gclid && config.googleAds.customerId) {
    promises.push(
      sendGoogleConversion(conversion, click)
        .then((result) => logApiEvent(conversion.id, "google", result))
        .catch((err) => {
          console.error("[Google] Conversion send failed:", err.message);
          logApiEvent(conversion.id, "google", {
            success: false,
            status: 0,
            body: err.message,
          });
        })
    );
  }

  if ((click.fbclid || click.fbp_cookie) && config.meta.pixelId) {
    promises.push(
      sendMetaConversion(conversion, click, message)
        .then((result) => logApiEvent(conversion.id, "meta", result))
        .catch((err) => {
          console.error("[Meta] Conversion send failed:", err.message);
          logApiEvent(conversion.id, "meta", {
            success: false,
            status: 0,
            body: err.message,
          });
        })
    );
  }

  await Promise.allSettled(promises);
}

/**
 * Log the result of a conversion API call for audit trail.
 */
function logApiEvent(conversionId, platform, result) {
  try {
    database.insertApiEvent({
      conversion_id: conversionId,
      platform,
      event_name: "conversion",
      payload_sent: result.payload || null,
      response_status: result.status || null,
      response_body: result.body || null,
      success: result.success || false,
    });
  } catch (err) {
    console.error("[Audit] Failed to log API event:", err.message);
  }
}

// =============================================
// FUNNEL METRICS API
// =============================================
app.get("/api/metrics", authenticateApiKey, (req, res) => {
  try {
    const { start_date, end_date, platform } = req.query;

    const data = database.getFunnelMetrics({
      start_date: start_date || null,
      end_date: end_date || null,
      platform: platform || null,
    });

    res.json({ data });
  } catch (err) {
    console.error("[Metrics] Error:", err.message);
    res.status(500).json({ error: "Internal server error" });
  }
});

// =============================================
// 404 HANDLER — clean JSON response
// =============================================
app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

// =============================================
// GLOBAL ERROR HANDLER — suppress stack traces
// =============================================
app.use((err, req, res, next) => {
  // Log full error server-side for debugging
  console.error(`[Error] ${req.method} ${req.path}:`, err.message);

  // JSON parse errors from body-parser
  if (err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: 'Invalid JSON in request body' });
  }

  // Payload too large
  if (err.type === 'entity.too.large') {
    return res.status(413).json({ error: 'Request body too large' });
  }

  // Generic — never expose internals
  res.status(err.status || 500).json({ error: 'Internal server error' });
});

// =============================================
// START SERVER
// =============================================
app.listen(config.port, () => {
  console.log(`[Server] WhatsApp Attribution System running on port ${config.port}`);
  console.log(`[Server] Environment: ${config.nodeEnv}`);
  console.log(`[Server] Database: ${config.dbPath}`);
  console.log(`[Server] Endpoints:`);
  console.log(`  POST /api/track/pageview  - Frontend page view tracking`);
  console.log(`  POST /api/track/click     - Frontend WhatsApp click tracking`);
  console.log(`  POST /webhook/whapi       - Whapi webhook`);
  console.log(`  POST /api/conversions     - Record conversion`);
  console.log(`  GET  /api/metrics         - Funnel metrics`);

  // Security warnings
  if (!config.apiKey) {
    console.warn("[Server] WARNING: No API_KEY set. /api/conversions and /api/metrics are unprotected.");
  }
  if (!config.whapi.webhookSecret) {
    console.warn("[Server] WARNING: No WHAPI_WEBHOOK_SECRET set. Webhook endpoint accepts all requests.");
  }

  // Start GHL retry processor if configured
  if (config.ghl && config.ghl.apiKey) {
    startRetryProcessor();
  } else {
    console.log(`[Server] GHL integration not configured (set GHL_API_KEY to enable)`);
  }
});

module.exports = app;
