/**
 * Database Module (SQLite)
 * ========================
 * Initializes SQLite database, creates tables on first run,
 * and exports helper functions for all database operations.
 */
const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");
const config = require("./config");

// Ensure data directory exists
const dbDir = path.dirname(config.dbPath);
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

const db = new Database(config.dbPath);

// Enable WAL mode for better concurrent read performance
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

// =============================================
// SCHEMA INITIALIZATION
// =============================================

db.exec(`
  CREATE TABLE IF NOT EXISTS page_views (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    visitor_id              TEXT NOT NULL,
    session_id              TEXT,
    event_type              TEXT DEFAULT 'pageview',
    timestamp               TEXT NOT NULL DEFAULT (datetime('now')),

    gclid                   TEXT,
    fbclid                  TEXT,
    fbp_cookie              TEXT,
    fbc_cookie              TEXT,

    utm_source              TEXT,
    utm_medium              TEXT,
    utm_campaign            TEXT,
    utm_term                TEXT,
    utm_content             TEXT,

    origin                  TEXT,
    page_url                TEXT,
    page_path               TEXT,
    referrer                TEXT,
    session_entry_page      TEXT,

    user_agent              TEXT,
    is_mobile               INTEGER DEFAULT 0,
    fingerprint             TEXT,
    ip_address              TEXT,

    visit_count             INTEGER DEFAULT 1,
    user_status             TEXT DEFAULT 'visitor',
    consent_given           INTEGER DEFAULT 1,

    first_touch_source      TEXT,
    first_touch_medium      TEXT,
    first_touch_campaign    TEXT,
    last_touch_source       TEXT,
    last_touch_medium       TEXT,

    server_received_at      TEXT NOT NULL DEFAULT (datetime('now')),
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_pv_visitor ON page_views(visitor_id);
  CREATE INDEX IF NOT EXISTS idx_pv_timestamp ON page_views(timestamp);
  CREATE INDEX IF NOT EXISTS idx_pv_gclid ON page_views(gclid) WHERE gclid IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_pv_fbclid ON page_views(fbclid) WHERE fbclid IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_pv_session ON page_views(session_id);

  CREATE TABLE IF NOT EXISTS whatsapp_clicks (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    visitor_id              TEXT NOT NULL,
    priority_code           TEXT NOT NULL UNIQUE,
    click_timestamp         TEXT NOT NULL,
    session_id              TEXT,
    event_type              TEXT DEFAULT 'whatsapp_click',

    gclid                   TEXT,
    gbraid                  TEXT,
    wbraid                  TEXT,
    fbclid                  TEXT,
    fbp_cookie              TEXT,
    fbc_cookie              TEXT,
    ttclid                  TEXT,

    utm_source              TEXT,
    utm_medium              TEXT,
    utm_campaign            TEXT,
    utm_term                TEXT,
    utm_content             TEXT,

    first_touch_source      TEXT,
    first_touch_medium      TEXT,
    first_touch_campaign    TEXT,
    first_touch_gclid       TEXT,
    first_touch_fbclid      TEXT,

    last_touch_source       TEXT,
    last_touch_medium       TEXT,

    user_agent              TEXT,
    is_mobile               INTEGER DEFAULT 0,
    fingerprint             TEXT,
    ip_address              TEXT,
    screen_width            INTEGER,
    screen_height           INTEGER,
    language                TEXT,
    timezone                TEXT,
    pixel_ratio             REAL,

    origin                  TEXT,
    page_url                TEXT,
    page_path               TEXT,
    referrer                TEXT,
    referrer_domain         TEXT,
    session_entry_page      TEXT,

    visit_count             INTEGER DEFAULT 1,
    user_status             TEXT DEFAULT 'lead',
    consent_given           INTEGER DEFAULT 1,

    server_received_at      TEXT NOT NULL DEFAULT (datetime('now')),
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_wc_visitor ON whatsapp_clicks(visitor_id);
  CREATE INDEX IF NOT EXISTS idx_wc_code ON whatsapp_clicks(priority_code);
  CREATE INDEX IF NOT EXISTS idx_wc_timestamp ON whatsapp_clicks(click_timestamp);
  CREATE INDEX IF NOT EXISTS idx_wc_gclid ON whatsapp_clicks(gclid) WHERE gclid IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_wc_fbclid ON whatsapp_clicks(fbclid) WHERE fbclid IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_wc_session ON whatsapp_clicks(session_id);

  CREATE TABLE IF NOT EXISTS whatsapp_messages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    whapi_message_id    TEXT UNIQUE,
    sender_phone        TEXT NOT NULL,
    sender_name         TEXT,
    message_text        TEXT,
    priority_code       TEXT,
    matched_click_id    INTEGER REFERENCES whatsapp_clicks(id),
    match_method        TEXT CHECK (match_method IN ('priority_code', 'time_proximity', 'manual', 'unmatched')),
    match_confidence    REAL DEFAULT 0,
    message_timestamp   TEXT NOT NULL,
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_wm_code ON whatsapp_messages(priority_code) WHERE priority_code IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_wm_phone ON whatsapp_messages(sender_phone);
  CREATE INDEX IF NOT EXISTS idx_wm_click ON whatsapp_messages(matched_click_id) WHERE matched_click_id IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_wm_timestamp ON whatsapp_messages(message_timestamp);

  CREATE TABLE IF NOT EXISTS conversions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id          INTEGER REFERENCES whatsapp_messages(id),
    click_id            INTEGER REFERENCES whatsapp_clicks(id),
    conversion_type     TEXT NOT NULL CHECK (conversion_type IN (
                            'lead',
                            'appointment_scheduled',
                            'appointment_showed',
                            'sale_completed',
                            'custom'
                        )),
    conversion_value    REAL,
    currency            TEXT DEFAULT 'SGD',
    notes               TEXT,
    converted_at        TEXT NOT NULL DEFAULT (datetime('now')),
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_conv_message ON conversions(message_id) WHERE message_id IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_conv_click ON conversions(click_id) WHERE click_id IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_conv_type ON conversions(conversion_type);
  CREATE INDEX IF NOT EXISTS idx_conv_timestamp ON conversions(converted_at);

  CREATE TABLE IF NOT EXISTS conversion_api_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    conversion_id       INTEGER NOT NULL REFERENCES conversions(id),
    platform            TEXT NOT NULL CHECK (platform IN ('google', 'meta', 'tiktok')),
    event_name          TEXT NOT NULL,
    payload_sent        TEXT,
    response_status     INTEGER,
    response_body       TEXT,
    sent_at             TEXT NOT NULL DEFAULT (datetime('now')),
    success             INTEGER DEFAULT 0
  );

  CREATE INDEX IF NOT EXISTS idx_api_conv ON conversion_api_events(conversion_id);
  CREATE INDEX IF NOT EXISTS idx_api_platform ON conversion_api_events(platform);

  CREATE TABLE IF NOT EXISTS pending_ghl_updates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    click_id        INTEGER NOT NULL REFERENCES whatsapp_clicks(id),
    message_id      INTEGER REFERENCES whatsapp_messages(id),
    sender_phone    TEXT NOT NULL,
    status          TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed', 'skipped')),
    attempts        INTEGER DEFAULT 0,
    next_retry_at   TEXT,
    ghl_contact_id  TEXT,
    error_message   TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_pgu_status ON pending_ghl_updates(status) WHERE status = 'pending';
  CREATE INDEX IF NOT EXISTS idx_pgu_click ON pending_ghl_updates(click_id);

  CREATE TABLE IF NOT EXISTS ghl_update_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    click_id        INTEGER REFERENCES whatsapp_clicks(id),
    ghl_contact_id  TEXT NOT NULL,
    fields_updated  TEXT,
    response_status INTEGER,
    response_body   TEXT,
    success         INTEGER DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_gue_click ON ghl_update_events(click_id);
  CREATE INDEX IF NOT EXISTS idx_gue_contact ON ghl_update_events(ghl_contact_id);
`);

// =============================================
// HELPER FUNCTIONS
// =============================================

const insertPageView = db.prepare(`
  INSERT INTO page_views (
    visitor_id, session_id, event_type, timestamp,
    gclid, fbclid, fbp_cookie, fbc_cookie,
    utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    origin, page_url, page_path, referrer, session_entry_page,
    user_agent, is_mobile, fingerprint, ip_address,
    visit_count, user_status, consent_given,
    first_touch_source, first_touch_medium, first_touch_campaign,
    last_touch_source, last_touch_medium
  ) VALUES (
    @visitor_id, @session_id, @event_type, @timestamp,
    @gclid, @fbclid, @fbp_cookie, @fbc_cookie,
    @utm_source, @utm_medium, @utm_campaign, @utm_term, @utm_content,
    @origin, @page_url, @page_path, @referrer, @session_entry_page,
    @user_agent, @is_mobile, @fingerprint, @ip_address,
    @visit_count, @user_status, @consent_given,
    @first_touch_source, @first_touch_medium, @first_touch_campaign,
    @last_touch_source, @last_touch_medium
  )
`);

const insertClick = db.prepare(`
  INSERT INTO whatsapp_clicks (
    visitor_id, priority_code, click_timestamp, session_id, event_type,
    gclid, gbraid, wbraid, fbclid, fbp_cookie, fbc_cookie, ttclid,
    utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    first_touch_source, first_touch_medium, first_touch_campaign,
    first_touch_gclid, first_touch_fbclid,
    last_touch_source, last_touch_medium,
    user_agent, is_mobile, fingerprint, ip_address,
    screen_width, screen_height, language, timezone, pixel_ratio,
    origin, page_url, page_path, referrer, referrer_domain, session_entry_page,
    visit_count, user_status, consent_given
  ) VALUES (
    @visitor_id, @priority_code, @click_timestamp, @session_id, @event_type,
    @gclid, @gbraid, @wbraid, @fbclid, @fbp_cookie, @fbc_cookie, @ttclid,
    @utm_source, @utm_medium, @utm_campaign, @utm_term, @utm_content,
    @first_touch_source, @first_touch_medium, @first_touch_campaign,
    @first_touch_gclid, @first_touch_fbclid,
    @last_touch_source, @last_touch_medium,
    @user_agent, @is_mobile, @fingerprint, @ip_address,
    @screen_width, @screen_height, @language, @timezone, @pixel_ratio,
    @origin, @page_url, @page_path, @referrer, @referrer_domain, @session_entry_page,
    @visit_count, @user_status, @consent_given
  )
`);

const insertMessage = db.prepare(`
  INSERT INTO whatsapp_messages (
    whapi_message_id, sender_phone, sender_name, message_text,
    priority_code, matched_click_id, match_method, match_confidence,
    message_timestamp
  ) VALUES (
    @whapi_message_id, @sender_phone, @sender_name, @message_text,
    @priority_code, @matched_click_id, @match_method, @match_confidence,
    @message_timestamp
  )
`);

const insertConversion = db.prepare(`
  INSERT INTO conversions (
    message_id, click_id, conversion_type, conversion_value, currency, notes
  ) VALUES (
    @message_id, @click_id, @conversion_type, @conversion_value, @currency, @notes
  )
`);

const insertApiEvent = db.prepare(`
  INSERT INTO conversion_api_events (
    conversion_id, platform, event_name, payload_sent,
    response_status, response_body, success
  ) VALUES (
    @conversion_id, @platform, @event_name, @payload_sent,
    @response_status, @response_body, @success
  )
`);

// =============================================
// QUERY HELPERS
// =============================================

const getClickByCode = db.prepare(
  `SELECT * FROM whatsapp_clicks WHERE priority_code = ?`
);

const getClickById = db.prepare(
  `SELECT * FROM whatsapp_clicks WHERE id = ?`
);

const getMessageById = db.prepare(
  `SELECT * FROM whatsapp_messages WHERE id = ?`
);

const getMessageByClickId = db.prepare(
  `SELECT matched_click_id FROM whatsapp_messages WHERE matched_click_id IN (${
    // This will be called dynamically, see getMatchedClickIds below
    "?"
  })`
);

const getRecentClicks = db.prepare(`
  SELECT * FROM whatsapp_clicks
  WHERE click_timestamp >= ? AND click_timestamp <= ?
  ORDER BY click_timestamp DESC
  LIMIT 10
`);

const getMatchedClickIds = function (clickIds) {
  if (!clickIds.length) return [];
  const placeholders = clickIds.map(() => "?").join(",");
  const stmt = db.prepare(
    `SELECT matched_click_id FROM whatsapp_messages WHERE matched_click_id IN (${placeholders})`
  );
  return stmt.all(...clickIds);
};

const getConversionById = db.prepare(
  `SELECT * FROM conversions WHERE id = ?`
);

// =============================================
// GHL UPDATE HELPERS
// =============================================

const insertPendingGHLUpdate = db.prepare(`
  INSERT INTO pending_ghl_updates (
    click_id, message_id, sender_phone, status
  ) VALUES (
    @click_id, @message_id, @sender_phone, 'pending'
  )
`);

const getPendingGHLUpdates = db.prepare(`
  SELECT * FROM pending_ghl_updates
  WHERE status = 'pending'
    AND attempts < 5
    AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))
  ORDER BY created_at ASC
  LIMIT ?
`);

const updatePendingGHLStatus = db.prepare(`
  UPDATE pending_ghl_updates
  SET status = @status,
      ghl_contact_id = @ghl_contact_id,
      error_message = @error_message,
      attempts = attempts + 1,
      next_retry_at = @next_retry_at,
      updated_at = datetime('now')
  WHERE id = @id
`);

const insertGHLUpdateEvent = db.prepare(`
  INSERT INTO ghl_update_events (
    click_id, ghl_contact_id, fields_updated,
    response_status, response_body, success
  ) VALUES (
    @click_id, @ghl_contact_id, @fields_updated,
    @response_status, @response_body, @success
  )
`);

const getMessageMatchedClickId = db.prepare(
  `SELECT matched_click_id FROM whatsapp_messages WHERE id = ?`
);

const getFunnelMetrics = function (filters = {}) {
  let sql = `
    SELECT
      date(wc.click_timestamp) AS date,
      wc.utm_source,
      wc.utm_medium,
      wc.utm_campaign,
      CASE
        WHEN wc.gclid IS NOT NULL THEN 'google'
        WHEN wc.fbclid IS NOT NULL THEN 'meta'
        WHEN wc.ttclid IS NOT NULL THEN 'tiktok'
        ELSE 'organic_or_other'
      END AS ad_platform,
      COUNT(DISTINCT wc.id) AS whatsapp_clicks,
      COUNT(DISTINCT wm.id) AS whatsapp_messages_sent,
      COUNT(DISTINCT CASE WHEN c.conversion_type = 'appointment_scheduled' THEN c.id END) AS appointments_scheduled,
      COUNT(DISTINCT CASE WHEN c.conversion_type = 'appointment_showed' THEN c.id END) AS appointments_showed,
      COUNT(DISTINCT CASE WHEN c.conversion_type = 'sale_completed' THEN c.id END) AS sales_completed,
      COALESCE(SUM(CASE WHEN c.conversion_type = 'sale_completed' THEN c.conversion_value ELSE 0 END), 0) AS total_revenue
    FROM whatsapp_clicks wc
    LEFT JOIN whatsapp_messages wm ON wm.matched_click_id = wc.id
    LEFT JOIN conversions c ON c.click_id = wc.id
  `;

  const conditions = [];
  const params = [];

  if (filters.start_date) {
    conditions.push("date(wc.click_timestamp) >= ?");
    params.push(filters.start_date);
  }
  if (filters.end_date) {
    conditions.push("date(wc.click_timestamp) <= ?");
    params.push(filters.end_date);
  }

  if (conditions.length) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  sql += " GROUP BY 1, 2, 3, 4, 5";

  if (filters.platform) {
    sql = `SELECT * FROM (${sql}) sub WHERE ad_platform = ?`;
    params.push(filters.platform);
  }

  const stmt = db.prepare(sql);
  return stmt.all(...params);
};

// =============================================
// EXPORTS
// =============================================

module.exports = {
  db,
  insertPageView(data) {
    const info = insertPageView.run({
      visitor_id: data.visitor_id,
      session_id: data.session_id || null,
      event_type: data.event_type || "pageview",
      timestamp: data.timestamp || new Date().toISOString(),
      gclid: data.gclid || null,
      fbclid: data.fbclid || null,
      fbp_cookie: data.fbp_cookie || null,
      fbc_cookie: data.fbc_cookie || null,
      utm_source: data.utm_source || null,
      utm_medium: data.utm_medium || null,
      utm_campaign: data.utm_campaign || null,
      utm_term: data.utm_term || null,
      utm_content: data.utm_content || null,
      origin: data.origin || null,
      page_url: data.page_url || null,
      page_path: data.page_path || null,
      referrer: data.referrer || null,
      session_entry_page: data.session_entry_page || null,
      user_agent: data.user_agent || null,
      is_mobile: data.is_mobile ? 1 : 0,
      fingerprint: data.fingerprint || null,
      ip_address: data.ip_address || null,
      visit_count: data.visit_count || 1,
      user_status: data.user_status || "visitor",
      consent_given: data.consent_given !== undefined ? (data.consent_given ? 1 : 0) : 1,
      first_touch_source: data.first_touch_source || null,
      first_touch_medium: data.first_touch_medium || null,
      first_touch_campaign: data.first_touch_campaign || null,
      last_touch_source: data.last_touch_source || null,
      last_touch_medium: data.last_touch_medium || null,
    });
    return info.lastInsertRowid;
  },

  insertClick(data) {
    const info = insertClick.run({
      visitor_id: data.visitor_id,
      priority_code: data.priority_code,
      click_timestamp: data.click_timestamp || new Date().toISOString(),
      session_id: data.session_id || null,
      event_type: data.event_type || "whatsapp_click",
      gclid: data.gclid || null,
      gbraid: data.gbraid || null,
      wbraid: data.wbraid || null,
      fbclid: data.fbclid || null,
      fbp_cookie: data.fbp_cookie || null,
      fbc_cookie: data.fbc_cookie || null,
      ttclid: data.ttclid || null,
      utm_source: data.utm_source || null,
      utm_medium: data.utm_medium || null,
      utm_campaign: data.utm_campaign || null,
      utm_term: data.utm_term || null,
      utm_content: data.utm_content || null,
      first_touch_source: data.first_touch_source || null,
      first_touch_medium: data.first_touch_medium || null,
      first_touch_campaign: data.first_touch_campaign || null,
      first_touch_gclid: data.first_touch_gclid || null,
      first_touch_fbclid: data.first_touch_fbclid || null,
      last_touch_source: data.last_touch_source || null,
      last_touch_medium: data.last_touch_medium || null,
      user_agent: data.user_agent || null,
      is_mobile: data.is_mobile ? 1 : 0,
      fingerprint: data.fingerprint || null,
      ip_address: data.ip_address || null,
      screen_width: data.screen_width || null,
      screen_height: data.screen_height || null,
      language: data.language || null,
      timezone: data.timezone || null,
      pixel_ratio: data.pixel_ratio || null,
      origin: data.origin || null,
      page_url: data.page_url || null,
      page_path: data.page_path || null,
      referrer: data.referrer || null,
      referrer_domain: data.referrer_domain || null,
      session_entry_page: data.session_entry_page || null,
      visit_count: data.visit_count || 1,
      user_status: data.user_status || "lead",
      consent_given: data.consent_given !== undefined ? (data.consent_given ? 1 : 0) : 1,
    });
    return info.lastInsertRowid;
  },

  insertMessage(data) {
    const info = insertMessage.run({
      whapi_message_id: data.whapi_message_id,
      sender_phone: data.sender_phone,
      sender_name: data.sender_name || null,
      message_text: data.message_text || null,
      priority_code: data.priority_code || null,
      matched_click_id: data.matched_click_id || null,
      match_method: data.match_method || "unmatched",
      match_confidence: data.match_confidence || 0,
      message_timestamp: data.message_timestamp,
    });
    return { id: info.lastInsertRowid, ...data };
  },

  insertConversion(data) {
    const info = insertConversion.run({
      message_id: data.message_id || null,
      click_id: data.click_id || null,
      conversion_type: data.conversion_type,
      conversion_value: data.conversion_value || null,
      currency: data.currency || "SGD",
      notes: data.notes || null,
    });
    return { id: info.lastInsertRowid, ...data };
  },

  insertApiEvent(data) {
    insertApiEvent.run({
      conversion_id: data.conversion_id,
      platform: data.platform,
      event_name: data.event_name,
      payload_sent: data.payload_sent ? JSON.stringify(data.payload_sent) : null,
      response_status: data.response_status || null,
      response_body: data.response_body || null,
      success: data.success ? 1 : 0,
    });
  },

  getClickByCode(code) {
    return getClickByCode.get(code);
  },

  getClickById(id) {
    return getClickById.get(id);
  },

  getMessageById(id) {
    return getMessageById.get(id);
  },

  getMessageMatchedClickId(messageId) {
    const row = getMessageMatchedClickId.get(messageId);
    return row ? row.matched_click_id : null;
  },

  getRecentClicks(windowStart, messageTime) {
    return getRecentClicks.all(windowStart, messageTime);
  },

  getMatchedClickIds,

  getFunnelMetrics,

  // GHL update helpers
  insertPendingGHLUpdate(data) {
    const info = insertPendingGHLUpdate.run({
      click_id: data.click_id,
      message_id: data.message_id || null,
      sender_phone: data.sender_phone,
    });
    return { id: info.lastInsertRowid, ...data };
  },

  getPendingGHLUpdates(limit = 10) {
    return getPendingGHLUpdates.all(limit);
  },

  updatePendingGHLStatus(id, status, ghlContactId, errorMessage, nextRetryAt) {
    updatePendingGHLStatus.run({
      id,
      status,
      ghl_contact_id: ghlContactId || null,
      error_message: errorMessage || null,
      next_retry_at: nextRetryAt || null,
    });
  },

  insertGHLUpdateEvent(data) {
    insertGHLUpdateEvent.run({
      click_id: data.click_id || null,
      ghl_contact_id: data.ghl_contact_id,
      fields_updated: data.fields_updated ? JSON.stringify(data.fields_updated) : null,
      response_status: data.response_status || null,
      response_body: data.response_body || null,
      success: data.success ? 1 : 0,
    });
  },
};
