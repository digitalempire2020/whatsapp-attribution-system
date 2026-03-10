/**
 * Whapi Webhook Handler
 * =====================
 * Parses incoming webhook events from Whapi (non-official WhatsApp API).
 * Extracts message data for the matching engine.
 *
 * Whapi webhook payload structure (incoming message):
 * {
 *   "event": "messages",
 *   "data": {
 *     "messages": [{
 *       "id": "message_id",
 *       "from": "phone_number@s.whatsapp.net",
 *       "pushName": "Contact Name",
 *       "body": "Message text",
 *       "timestamp": 1234567890,
 *       "type": "text",
 *       ...
 *     }]
 *   }
 * }
 *
 * NOTE: Whapi's exact payload format may vary by version.
 * Adjust the parsing logic below to match your Whapi setup.
 */

/**
 * Parse a Whapi webhook payload and extract the relevant message data.
 * Returns null if the event is not a relevant incoming text message.
 *
 * @param {Object} webhookBody - The raw webhook request body from Whapi.
 * @returns {Object|null} Parsed message data or null if not relevant.
 */
function handleWhapiWebhook(webhookBody) {
  // Whapi can send different event types. We only care about incoming messages.
  // The exact structure depends on your Whapi version and configuration.

  // Handle Whapi v2+ format
  if (webhookBody?.messages && Array.isArray(webhookBody.messages)) {
    return parseWhapiMessage(webhookBody.messages[0]);
  }

  // Handle event-wrapped format
  if (webhookBody?.event === "messages" && webhookBody?.data?.messages) {
    return parseWhapiMessage(webhookBody.data.messages[0]);
  }

  // Handle single message format
  if (webhookBody?.id && webhookBody?.from && webhookBody?.body !== undefined) {
    return parseWhapiMessage(webhookBody);
  }

  // Handle Whapi "message" event (singular)
  if (webhookBody?.event === "message" && webhookBody?.data) {
    return parseWhapiMessage(webhookBody.data);
  }

  console.log("[Whapi] Unrecognized webhook format, ignoring:", JSON.stringify(webhookBody).substring(0, 200));
  return null;
}

/**
 * Parse a single Whapi message object into our standard format.
 */
function parseWhapiMessage(msg) {
  if (!msg) return null;

  // Skip non-text messages (images, videos, etc.)
  const messageType = msg.type || "text";
  if (messageType !== "text" && messageType !== "chat") {
    console.log(`[Whapi] Skipping non-text message type: ${messageType}`);
    return null;
  }

  // Skip outgoing messages (fromMe = true)
  if (msg.fromMe === true || msg.from_me === true) {
    return null;
  }

  // Extract phone number (remove @s.whatsapp.net or @c.us suffix)
  const rawFrom = msg.from || msg.chat_id || "";
  const senderPhone = rawFrom.replace(/@(s\.whatsapp\.net|c\.us)$/i, "");

  if (!senderPhone) {
    console.log("[Whapi] No sender phone found in message");
    return null;
  }

  // Extract message text
  // Check msg.text?.body before msg.text because msg.text can be an object {body: "..."}
  const messageText = msg.body || msg.text?.body || (typeof msg.text === "string" ? msg.text : "") || "";

  // Parse timestamp
  let timestamp;
  if (msg.timestamp) {
    // Whapi often sends Unix timestamps in seconds
    const ts = typeof msg.timestamp === "number"
      ? (msg.timestamp > 1e12 ? msg.timestamp : msg.timestamp * 1000)
      : Date.parse(msg.timestamp);
    timestamp = new Date(ts).toISOString();
  } else {
    timestamp = new Date().toISOString();
  }

  return {
    messageId: msg.id || msg.message_id || `whapi_${Date.now()}`,
    senderPhone,
    senderName: msg.pushName || msg.push_name || msg.notify || null,
    messageText,
    messageType,
    timestamp,
    rawPayload: msg,
  };
}

module.exports = { handleWhapiWebhook };
