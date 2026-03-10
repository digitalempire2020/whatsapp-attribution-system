/**
 * WhatsApp Attribution Tracker v2.0
 * ===================================
 * Embed this script on client websites to capture visitor data,
 * generate unique priority codes, and inject them into WhatsApp
 * click-to-chat URLs for full-funnel server-side attribution.
 *
 * SETUP:
 * 1. Set API_URL to your backend server URL.
 * 2. Add your WhatsApp button selectors to WHATSAPP_SELECTORS.
 * 3. Include this script before </body> on every page.
 *
 * <script src="whatsapp-tracker.js"></script>
 */

(function () {
  "use strict";

  // =============================================
  // CONFIGURATION - Update these for each client
  // =============================================
  const CONFIG = {
    // Your backend server URL (Express)
    API_URL: "https://YOUR_SERVER_URL",

    // CSS selectors that match WhatsApp buttons/links on the site
    WHATSAPP_SELECTORS: [
      'a[href*="wa.me"]',
      'a[href*="whatsapp.com"]',
      'a[href*="api.whatsapp.com"]',
      '[data-whatsapp]',
      '.whatsapp-button',
      '.wa-button',
    ],

    // Cookie/localStorage key prefix
    STORAGE_PREFIX: "wa_attr_",

    // How long to keep the visitor cookie (days)
    COOKIE_EXPIRY_DAYS: 30,

    // Priority code format prefix
    CODE_PREFIX: "WA",

    // Enable console logging in development
    DEBUG: false,
  };

  // =============================================
  // UTILITY FUNCTIONS
  // =============================================

  function log(...args) {
    if (CONFIG.DEBUG) {
      console.log("[WA-Tracker]", ...args);
    }
  }

  /**
   * Generate a cryptographically random priority code.
   * Format: WA-XXXXXXXX (8 alphanumeric chars)
   */
  function generatePriorityCode() {
    const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let code = "";
    const array = new Uint8Array(8);
    crypto.getRandomValues(array);
    for (let i = 0; i < 8; i++) {
      code += chars[array[i] % chars.length];
    }
    return `${CONFIG.CODE_PREFIX}-${code}`;
  }

  /**
   * Get or create a persistent visitor ID (stored in cookie + localStorage).
   */
  function getVisitorId() {
    const key = CONFIG.STORAGE_PREFIX + "visitor_id";

    let visitorId = null;
    try {
      visitorId = localStorage.getItem(key);
    } catch (e) {}

    if (!visitorId) {
      visitorId = getCookie(key);
    }

    if (!visitorId) {
      visitorId = "v_" + Date.now() + "_" + Math.random().toString(36).substr(2, 9);
    }

    try {
      localStorage.setItem(key, visitorId);
    } catch (e) {}
    setCookie(key, visitorId, CONFIG.COOKIE_EXPIRY_DAYS);

    return visitorId;
  }

  /**
   * Get or generate a priority code for this session.
   */
  function getPriorityCode() {
    const key = CONFIG.STORAGE_PREFIX + "priority_code";

    let code = null;
    try {
      code = sessionStorage.getItem(key);
    } catch (e) {}

    if (!code) {
      code = generatePriorityCode();
      try {
        sessionStorage.setItem(key, code);
      } catch (e) {}
    }

    return code;
  }

  // =============================================
  // COOKIE HELPERS
  // =============================================

  function setCookie(name, value, days) {
    const expires = new Date(Date.now() + days * 864e5).toUTCString();
    document.cookie = `${name}=${encodeURIComponent(value)};expires=${expires};path=/;SameSite=Lax`;
  }

  function getCookie(name) {
    const match = document.cookie.match(
      new RegExp("(?:^|; )" + name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "=([^;]*)")
    );
    return match ? decodeURIComponent(match[1]) : null;
  }

  // =============================================
  // DATA CAPTURE
  // =============================================

  function getUrlParams() {
    const params = {};
    const searchParams = new URLSearchParams(window.location.search);
    for (const [key, value] of searchParams.entries()) {
      params[key] = value;
    }
    return params;
  }

  function captureClickIds() {
    const params = getUrlParams();

    return {
      gclid: params.gclid || getCookie("_gcl_aw") || null,
      gbraid: params.gbraid || null,
      wbraid: params.wbraid || null,
      gclsrc: params.gclsrc || null,
      fbclid: params.fbclid || null,
      fbp_cookie: getCookie("_fbp") || null,
      fbc_cookie: getCookie("_fbc") || null,
      ttclid: params.ttclid || null,
      msclkid: params.msclkid || null,
    };
  }

  function captureUtms() {
    const params = getUrlParams();
    return {
      utm_source: params.utm_source || null,
      utm_medium: params.utm_medium || null,
      utm_campaign: params.utm_campaign || null,
      utm_term: params.utm_term || null,
      utm_content: params.utm_content || null,
    };
  }

  function detectIsMobile() {
    const ua = navigator.userAgent || "";
    return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua) ||
      (navigator.maxTouchPoints > 0 && window.innerWidth < 768);
  }

  async function generateFingerprint() {
    const components = [
      navigator.userAgent,
      navigator.language,
      screen.width + "x" + screen.height,
      screen.colorDepth,
      new Date().getTimezoneOffset(),
      navigator.platform || "",
      window.devicePixelRatio || 1,
      navigator.hardwareConcurrency || "",
      "ontouchstart" in window,
      navigator.cookieEnabled,
      getCanvasFingerprint(),
    ];

    const raw = components.join("|");

    try {
      const encoder = new TextEncoder();
      const data = encoder.encode(raw);
      const hashBuffer = await crypto.subtle.digest("SHA-256", data);
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      return hashArray.slice(0, 16).map(b => b.toString(16).padStart(2, "0")).join("");
    } catch (e) {
      let hash = 0;
      for (let i = 0; i < raw.length; i++) {
        const char = raw.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
      }
      return Math.abs(hash).toString(16).padStart(32, "0");
    }
  }

  function getCanvasFingerprint() {
    try {
      const canvas = document.createElement("canvas");
      canvas.width = 200;
      canvas.height = 50;
      const ctx = canvas.getContext("2d");
      ctx.textBaseline = "top";
      ctx.font = "14px Arial";
      ctx.fillStyle = "#f60";
      ctx.fillRect(0, 0, 100, 50);
      ctx.fillStyle = "#069";
      ctx.fillText("WA-Track", 2, 15);
      return canvas.toDataURL().slice(-50);
    } catch (e) {
      return "no-canvas";
    }
  }

  function checkConsentGiven() {
    const consentCookies = [
      "cookie_consent",
      "cookieconsent_status",
      "CookieConsent",
      "gdpr_consent",
      "cc_cookie",
    ];

    for (const name of consentCookies) {
      const val = getCookie(name);
      if (val) {
        return val === "true" || val === "allow" || val === "accepted" || val === "yes" || val === "1";
      }
    }

    try {
      const lsConsent = localStorage.getItem("cookie_consent") || localStorage.getItem("tracking_consent");
      if (lsConsent) {
        return lsConsent === "true" || lsConsent === "accepted" || lsConsent === "1";
      }
    } catch (e) {}

    return true;
  }

  function getSessionEntryPage() {
    const key = CONFIG.STORAGE_PREFIX + "session_entry_page";
    let entryPage = null;
    try {
      entryPage = sessionStorage.getItem(key);
    } catch (e) {}

    if (!entryPage) {
      entryPage = window.location.pathname;
      try {
        sessionStorage.setItem(key, entryPage);
      } catch (e) {}
    }

    return entryPage;
  }

  function capturePageData() {
    return {
      origin: window.location.origin,
      page_url: window.location.href,
      page_path: window.location.pathname,
      page_title: document.title,
      referrer: document.referrer || null,
      referrer_domain: document.referrer
        ? new URL(document.referrer).hostname
        : null,
    };
  }

  function getVisitCount() {
    const key = CONFIG.STORAGE_PREFIX + "visit_count";
    let count = 0;
    try {
      count = parseInt(localStorage.getItem(key) || "0", 10);
    } catch (e) {}
    count++;
    try {
      localStorage.setItem(key, count.toString());
    } catch (e) {}
    return count;
  }

  function persistFirstTouch(clickIds, utms) {
    const key = CONFIG.STORAGE_PREFIX + "first_touch";
    try {
      if (!localStorage.getItem(key)) {
        const data = { clickIds, utms, timestamp: new Date().toISOString() };
        localStorage.setItem(key, JSON.stringify(data));
      }
    } catch (e) {}
  }

  function getFirstTouch() {
    const key = CONFIG.STORAGE_PREFIX + "first_touch";
    try {
      const raw = localStorage.getItem(key);
      if (raw) return JSON.parse(raw);
    } catch (e) {}
    return { clickIds: {}, utms: {} };
  }

  function persistLastTouch(clickIds, utms) {
    const key = CONFIG.STORAGE_PREFIX + "last_touch";
    if (utms.utm_source || utms.utm_medium || clickIds.gclid || clickIds.fbclid) {
      try {
        const data = { clickIds, utms, timestamp: new Date().toISOString() };
        localStorage.setItem(key, JSON.stringify(data));
      } catch (e) {}
    }
  }

  function getLastTouch() {
    const key = CONFIG.STORAGE_PREFIX + "last_touch";
    try {
      const raw = localStorage.getItem(key);
      if (raw) return JSON.parse(raw);
    } catch (e) {}
    return { clickIds: {}, utms: {} };
  }

  function getSessionId() {
    const key = CONFIG.STORAGE_PREFIX + "session_id";
    let sid = null;
    try {
      sid = sessionStorage.getItem(key);
    } catch (e) {}
    if (!sid) {
      sid = "s_" + Date.now() + "_" + Math.random().toString(36).substr(2, 6);
      try {
        sessionStorage.setItem(key, sid);
      } catch (e) {}
    }
    return sid;
  }

  // =============================================
  // SERVER API
  // =============================================

  /**
   * Send data to the Express backend.
   */
  async function sendToServer(endpoint, data) {
    const url = `${CONFIG.API_URL}${endpoint}`;
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(data),
      });

      if (!response.ok) {
        const errorText = await response.text();
        log("Server error:", response.status, errorText);
        return null;
      }

      const result = await response.json();
      log("Server success:", endpoint, result);
      return result;
    } catch (err) {
      log("Server request failed:", err);
      return null;
    }
  }

  /**
   * Send data using sendBeacon for reliability when navigating away.
   */
  function sendBeaconToServer(endpoint, data) {
    const url = `${CONFIG.API_URL}${endpoint}`;
    const blob = new Blob([JSON.stringify(data)], { type: "application/json" });

    const beaconSent = navigator.sendBeacon(url, blob);

    if (!beaconSent) {
      sendToServer(endpoint, data);
    }

    return beaconSent;
  }

  // =============================================
  // WHATSAPP BUTTON INTERCEPTION
  // =============================================

  function injectCodeIntoWhatsAppUrl(href, priorityCode) {
    try {
      const url = new URL(href);
      const existingText = url.searchParams.get("text") || "";
      const codeSnippet = `\n\n[Code: ${priorityCode}]`;
      url.searchParams.set("text", existingText + codeSnippet);
      return url.toString();
    } catch (e) {
      const separator = href.includes("?") ? "&" : "?";
      return href + separator + "text=" + encodeURIComponent(`[Code: ${priorityCode}]`);
    }
  }

  async function handleWhatsAppClick(event, element) {
    const priorityCode = getPriorityCode();
    const clickIds = captureClickIds();
    const utms = captureUtms();
    const pageData = capturePageData();
    const firstTouch = getFirstTouch();
    const lastTouch = getLastTouch();
    const visitCount = getVisitCount();
    const sessionEntryPage = getSessionEntryPage();
    const isMobile = detectIsMobile();
    const consentGiven = checkConsentGiven();

    let fingerprint = null;
    try {
      fingerprint = await generateFingerprint();
    } catch (e) {
      log("Fingerprint generation failed:", e);
    }

    log("WhatsApp click captured:", priorityCode);

    // Modify the WhatsApp URL to include the priority code
    const originalHref = element.getAttribute("href");
    if (originalHref) {
      const modifiedHref = injectCodeIntoWhatsAppUrl(originalHref, priorityCode);
      element.setAttribute("href", modifiedHref);
      log("Modified WhatsApp URL:", modifiedHref);
    }

    const payload = {
      visitor_id: getVisitorId(),
      priority_code: priorityCode,
      click_timestamp: new Date().toISOString(),
      session_id: getSessionId(),
      event_type: "whatsapp_click",

      gclid: clickIds.gclid,
      gbraid: clickIds.gbraid,
      wbraid: clickIds.wbraid,
      fbclid: clickIds.fbclid,
      fbp_cookie: clickIds.fbp_cookie,
      fbc_cookie: clickIds.fbc_cookie,
      ttclid: clickIds.ttclid,

      utm_source: utms.utm_source,
      utm_medium: utms.utm_medium,
      utm_campaign: utms.utm_campaign,
      utm_term: utms.utm_term,
      utm_content: utms.utm_content,

      first_touch_source: firstTouch.utms?.utm_source || null,
      first_touch_medium: firstTouch.utms?.utm_medium || null,
      first_touch_campaign: firstTouch.utms?.utm_campaign || null,
      first_touch_gclid: firstTouch.clickIds?.gclid || null,
      first_touch_fbclid: firstTouch.clickIds?.fbclid || null,

      last_touch_source: lastTouch.utms?.utm_source || utms.utm_source || null,
      last_touch_medium: lastTouch.utms?.utm_medium || utms.utm_medium || null,

      user_agent: navigator.userAgent,
      is_mobile: isMobile,
      fingerprint: fingerprint,
      screen_width: screen.width,
      screen_height: screen.height,
      language: navigator.language,
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      pixel_ratio: window.devicePixelRatio || 1,

      origin: pageData.origin,
      page_url: pageData.page_url,
      page_path: pageData.page_path,
      referrer: pageData.referrer,
      referrer_domain: pageData.referrer_domain,
      session_entry_page: sessionEntryPage,

      visit_count: visitCount,
      user_status: "lead",
      consent_given: consentGiven,

      // ip_address is captured server-side by Express (req.ip)
    };

    // Send via sendBeacon for reliability (page is navigating to WhatsApp)
    sendBeaconToServer("/api/track/click", payload);

    log("Click data sent to server");
  }

  function attachWhatsAppListeners() {
    const selectorString = CONFIG.WHATSAPP_SELECTORS.join(", ");

    function attachToElement(element) {
      if (element._waTrackerAttached) return;
      element._waTrackerAttached = true;

      element.addEventListener("click", function (event) {
        handleWhatsAppClick(event, element);
      });

      log("Attached tracker to:", element.tagName, element.href || element.className);
    }

    document.querySelectorAll(selectorString).forEach(attachToElement);

    const observer = new MutationObserver(function (mutations) {
      for (const mutation of mutations) {
        for (const node of mutation.addedNodes) {
          if (node.nodeType !== 1) continue;
          if (node.matches && node.matches(selectorString)) {
            attachToElement(node);
          }
          if (node.querySelectorAll) {
            node.querySelectorAll(selectorString).forEach(attachToElement);
          }
        }
      }
    });

    observer.observe(document.body, { childList: true, subtree: true });
    log("WhatsApp listeners attached, MutationObserver active");
  }

  // =============================================
  // PAGE VIEW TRACKING
  // =============================================

  async function trackPageView() {
    const visitorId = getVisitorId();
    const clickIds = captureClickIds();
    const utms = captureUtms();
    const pageData = capturePageData();
    const sessionEntryPage = getSessionEntryPage();
    const isMobile = detectIsMobile();
    const consentGiven = checkConsentGiven();

    let fingerprint = null;
    try {
      fingerprint = await generateFingerprint();
    } catch (e) {}

    const payload = {
      visitor_id: visitorId,
      session_id: getSessionId(),
      event_type: "pageview",
      timestamp: new Date().toISOString(),

      gclid: clickIds.gclid,
      fbclid: clickIds.fbclid,
      fbp_cookie: clickIds.fbp_cookie,
      fbc_cookie: clickIds.fbc_cookie,

      utm_source: utms.utm_source,
      utm_medium: utms.utm_medium,
      utm_campaign: utms.utm_campaign,
      utm_term: utms.utm_term,
      utm_content: utms.utm_content,

      origin: pageData.origin,
      page_url: pageData.page_url,
      page_path: pageData.page_path,
      referrer: pageData.referrer,
      session_entry_page: sessionEntryPage,

      user_agent: navigator.userAgent,
      is_mobile: isMobile,
      fingerprint: fingerprint,

      visit_count: getVisitCount(),
      user_status: "visitor",
      consent_given: consentGiven,

      first_touch_source: getFirstTouch().utms?.utm_source || null,
      first_touch_medium: getFirstTouch().utms?.utm_medium || null,
      first_touch_campaign: getFirstTouch().utms?.utm_campaign || null,
      last_touch_source: getLastTouch().utms?.utm_source || utms.utm_source || null,
      last_touch_medium: getLastTouch().utms?.utm_medium || utms.utm_medium || null,

      // ip_address captured server-side by Express (req.ip)
    };

    sendToServer("/api/track/pageview", payload);
  }

  // =============================================
  // INITIALIZATION
  // =============================================

  function init() {
    log("Initializing WhatsApp Attribution Tracker v2.0");

    getVisitorId();
    getSessionEntryPage();

    const clickIds = captureClickIds();
    const utms = captureUtms();
    persistFirstTouch(clickIds, utms);
    persistLastTouch(clickIds, utms);

    trackPageView();

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", attachWhatsAppListeners);
    } else {
      attachWhatsAppListeners();
    }

    log("Tracker initialized. Priority code:", getPriorityCode());
  }

  init();
})();
