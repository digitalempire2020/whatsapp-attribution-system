/**
 * Matching Engine
 * ===============
 * Matches incoming WhatsApp messages to WhatsApp button clicks.
 *
 * Two matching strategies:
 * 1. PRIORITY CODE MATCH (high confidence):
 *    Extract the priority code from the message text and look it up
 *    in the whatsapp_clicks table.
 *
 * 2. TIME PROXIMITY MATCH (medium confidence):
 *    If no priority code is found (user deleted it), match by finding
 *    the most recent unmatched click within a configurable time window.
 */

// Time window for proximity matching (in minutes)
const TIME_PROXIMITY_WINDOW_MINUTES = 5;

/**
 * Extract a priority code from message text.
 * Looks for patterns like [Code: WA-XXXXXXXX] in the message.
 *
 * @param {string} text - The WhatsApp message text.
 * @returns {string|null} The extracted priority code or null.
 */
function extractPriorityCode(text) {
  if (!text) return null;

  // Pattern 1: [Code: WA-XXXXXXXX]
  const bracketMatch = text.match(/\[Code:\s*(WA-[A-Z0-9]{6,12})\]/i);
  if (bracketMatch) return bracketMatch[1].toUpperCase();

  // Pattern 2: Code: WA-XXXXXXXX (without brackets)
  const plainMatch = text.match(/Code:\s*(WA-[A-Z0-9]{6,12})/i);
  if (plainMatch) return plainMatch[1].toUpperCase();

  // Pattern 3: Just the code itself (WA-XXXXXXXX)
  const standaloneMatch = text.match(/\b(WA-[A-Z0-9]{6,12})\b/i);
  if (standaloneMatch) return standaloneMatch[1].toUpperCase();

  return null;
}

/**
 * Match a message to a click using the priority code.
 *
 * @param {Object} database - Database module instance.
 * @param {string} priorityCode - The extracted priority code.
 * @returns {Object|null} The matched click record or null.
 */
function matchByPriorityCode(database, priorityCode) {
  const click = database.getClickByCode(priorityCode);

  if (!click) {
    console.log(`[Matching] No click found for code: ${priorityCode}`);
    return null;
  }

  console.log(`[Matching] Priority code match found: click ID ${click.id}`);
  return click;
}

/**
 * Match a message to a click using time proximity.
 * Finds the most recent unmatched click within the time window.
 *
 * @param {Object} database - Database module instance.
 * @param {string} messageTimestamp - ISO timestamp of the incoming message.
 * @returns {Object|null} The matched click record or null.
 */
function matchByTimeProximity(database, messageTimestamp) {
  const msgTime = new Date(messageTimestamp);
  const windowStart = new Date(
    msgTime.getTime() - TIME_PROXIMITY_WINDOW_MINUTES * 60 * 1000
  );

  const recentClicks = database.getRecentClicks(
    windowStart.toISOString(),
    msgTime.toISOString()
  );

  if (!recentClicks || recentClicks.length === 0) {
    console.log("[Matching] No recent clicks found for time proximity match");
    return null;
  }

  // Check which clicks already have a matched message
  const clickIds = recentClicks.map((c) => c.id);
  const existingMatches = database.getMatchedClickIds(clickIds);
  const matchedClickIds = new Set(
    existingMatches.map((m) => m.matched_click_id)
  );

  // Find the first unmatched click (most recent)
  const unmatchedClick = recentClicks.find((c) => !matchedClickIds.has(c.id));

  if (!unmatchedClick) {
    console.log("[Matching] All recent clicks already matched");
    return null;
  }

  const timeDiffMs = msgTime.getTime() - new Date(unmatchedClick.click_timestamp).getTime();
  const timeDiffMinutes = timeDiffMs / 60000;

  console.log(
    `[Matching] Time proximity match: click ID ${unmatchedClick.id} ` +
    `(${timeDiffMinutes.toFixed(1)} minutes apart)`
  );

  return { click: unmatchedClick, timeDiffMinutes };
}

/**
 * Main matching function. Attempts to match an incoming message to a click.
 *
 * @param {Object} database - Database module instance.
 * @param {Object} messageData - Parsed message data from the webhook handler.
 * @returns {Object} Match result with clickId, method, confidence, and priorityCode.
 */
function matchMessageToClick(database, messageData) {
  const result = {
    clickId: null,
    method: "unmatched",
    confidence: 0,
    priorityCode: null,
  };

  // Strategy 1: Try priority code matching
  const priorityCode = extractPriorityCode(messageData.messageText);

  if (priorityCode) {
    result.priorityCode = priorityCode;
    const click = matchByPriorityCode(database, priorityCode);

    if (click) {
      result.clickId = click.id;
      result.method = "priority_code";
      result.confidence = 1.0;
      return result;
    }
  }

  // Strategy 2: Fallback to time proximity matching
  const proximityResult = matchByTimeProximity(
    database,
    messageData.timestamp
  );

  if (proximityResult) {
    result.clickId = proximityResult.click.id;
    result.method = "time_proximity";
    result.confidence = Math.max(
      0.5,
      1.0 - proximityResult.timeDiffMinutes * 0.1
    );
    return result;
  }

  console.log(
    `[Matching] No match found for message from ${messageData.senderPhone}`
  );
  return result;
}

module.exports = { matchMessageToClick, extractPriorityCode };
