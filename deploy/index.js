var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/durable/notifications-hub.ts
import { DurableObject, waitUntil } from "cloudflare:workers";
var SIGNALR_RECORD_SEPARATOR = 30;
var SIGNALR_HANDSHAKE_ACK = new Uint8Array([123, 125, SIGNALR_RECORD_SEPARATOR]);
var SIGNALR_UPDATE_TYPE_SYNC_VAULT = 5;
var SIGNALR_UPDATE_TYPE_LOG_OUT = 11;
var SIGNALR_UPDATE_TYPE_DEVICE_STATUS = 12;
var SIGNALR_UPDATE_TYPE_BACKUP_RESTORE_PROGRESS = 13;
function concatBytes(chunks) {
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
__name(concatBytes, "concatBytes");
function encodeUtf8(value) {
  return new TextEncoder().encode(value);
}
__name(encodeUtf8, "encodeUtf8");
function decodeIncomingMessage(data) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(data));
  return new TextDecoder().decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
}
__name(decodeIncomingMessage, "decodeIncomingMessage");
function encodeMsgPackInteger(value) {
  const normalized = Math.trunc(value);
  if (normalized >= 0 && normalized <= 127) {
    return new Uint8Array([normalized]);
  }
  if (normalized >= 0 && normalized <= 255) {
    return new Uint8Array([204, normalized]);
  }
  if (normalized >= 0 && normalized <= 65535) {
    return new Uint8Array([205, normalized >> 8, normalized & 255]);
  }
  const safe = normalized >>> 0;
  return new Uint8Array([
    206,
    safe >>> 24 & 255,
    safe >>> 16 & 255,
    safe >>> 8 & 255,
    safe & 255
  ]);
}
__name(encodeMsgPackInteger, "encodeMsgPackInteger");
function encodeMsgPackString(value) {
  const bytes = encodeUtf8(value);
  const len = bytes.length;
  if (len < 32) {
    return concatBytes([new Uint8Array([160 | len]), bytes]);
  }
  if (len <= 255) {
    return concatBytes([new Uint8Array([217, len]), bytes]);
  }
  return concatBytes([new Uint8Array([218, len >> 8 & 255, len & 255]), bytes]);
}
__name(encodeMsgPackString, "encodeMsgPackString");
function encodeMsgPackTimestamp(date) {
  const seconds = BigInt(Math.floor(date.getTime() / 1e3));
  const nanos = BigInt(date.getMilliseconds()) * 1000000n;
  const timestamp = nanos << 34n | seconds;
  const payload = new Uint8Array(8);
  for (let i = 7; i >= 0; i--) {
    payload[i] = Number(timestamp >> BigInt((7 - i) * 8) & 0xffn);
  }
  return concatBytes([new Uint8Array([199, 8, 255]), payload]);
}
__name(encodeMsgPackTimestamp, "encodeMsgPackTimestamp");
function encodeMsgPackArray(values) {
  const items = values.map(encodeMsgPack);
  const len = items.length;
  const header = len < 16 ? new Uint8Array([144 | len]) : new Uint8Array([220, len >> 8 & 255, len & 255]);
  return concatBytes([header, ...items]);
}
__name(encodeMsgPackArray, "encodeMsgPackArray");
function encodeMsgPackMap(value) {
  const entries = Object.entries(value);
  const len = entries.length;
  const header = len < 16 ? new Uint8Array([128 | len]) : new Uint8Array([222, len >> 8 & 255, len & 255]);
  const chunks = [header];
  for (const [key, entryValue] of entries) {
    chunks.push(encodeMsgPackString(key), encodeMsgPack(entryValue));
  }
  return concatBytes(chunks);
}
__name(encodeMsgPackMap, "encodeMsgPackMap");
function encodeMsgPack(value) {
  if (value === null || value === void 0) return new Uint8Array([192]);
  if (value instanceof Date) return encodeMsgPackTimestamp(value);
  if (typeof value === "string") return encodeMsgPackString(value);
  if (typeof value === "number") return encodeMsgPackInteger(value);
  if (typeof value === "boolean") return new Uint8Array([value ? 195 : 194]);
  if (Array.isArray(value)) return encodeMsgPackArray(value);
  if (value instanceof Uint8Array) {
    const len = value.length;
    if (len <= 255) return concatBytes([new Uint8Array([196, len]), value]);
    return concatBytes([new Uint8Array([197, len >> 8 & 255, len & 255]), value]);
  }
  return encodeMsgPackMap(value);
}
__name(encodeMsgPack, "encodeMsgPack");
function frameSignalRBinary(payload) {
  const len = payload.length;
  const prefix = [];
  let value = len;
  do {
    let current = value & 127;
    value >>>= 7;
    if (value > 0) current |= 128;
    prefix.push(current);
  } while (value > 0);
  return concatBytes([new Uint8Array(prefix), payload]);
}
__name(frameSignalRBinary, "frameSignalRBinary");
function buildSignalRJsonInvocation(updateType, payload, contextId) {
  return JSON.stringify({
    type: 1,
    target: "ReceiveMessage",
    arguments: [
      {
        ContextId: contextId,
        Type: updateType,
        Payload: payload
      }
    ]
  }) + String.fromCharCode(SIGNALR_RECORD_SEPARATOR);
}
__name(buildSignalRJsonInvocation, "buildSignalRJsonInvocation");
function buildSignalRMessagePackInvocation(updateType, messagePayload, contextId) {
  const encodedPayload = encodeMsgPack([
    1,
    {},
    null,
    "ReceiveMessage",
    [
      {
        ContextId: contextId,
        Type: updateType,
        Payload: messagePayload
      }
    ]
  ]);
  return frameSignalRBinary(encodedPayload);
}
__name(buildSignalRMessagePackInvocation, "buildSignalRMessagePackInvocation");
var NotificationsHub = class extends DurableObject {
  static {
    __name(this, "NotificationsHub");
  }
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair(
        JSON.stringify({ type: 6 }) + String.fromCharCode(SIGNALR_RECORD_SEPARATOR),
        JSON.stringify({ type: 6 }) + String.fromCharCode(SIGNALR_RECORD_SEPARATOR)
      )
    );
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/internal/notify" && request.method === "POST") {
      const body = await request.json().catch(() => null);
      const revisionDate = String(body?.revisionDate || "").trim() || (/* @__PURE__ */ new Date()).toISOString();
      const userId = String(request.headers.get("X-NodeWarden-UserId") || body?.userId || "").trim();
      const contextId = String(body?.contextId || "").trim() || null;
      const updateType = Number(body?.updateType || SIGNALR_UPDATE_TYPE_SYNC_VAULT) || SIGNALR_UPDATE_TYPE_SYNC_VAULT;
      const targetDeviceIdentifier = String(body?.targetDeviceIdentifier || "").trim() || null;
      const payload = body?.payload && typeof body.payload === "object" ? body.payload : {
        UserId: userId,
        Date: revisionDate
      };
      this.broadcastMessage(updateType, payload, contextId, targetDeviceIdentifier);
      return new Response(null, { status: 204 });
    }
    if (url.pathname === "/internal/online" && request.method === "GET") {
      return new Response(JSON.stringify({ deviceIdentifiers: this.getOnlineDeviceIdentifiers() }), {
        status: 200,
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    if (url.pathname !== "/notifications/hub") {
      return new Response("Not found", { status: 404 });
    }
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected websocket", { status: 426 });
    }
    const requestUserId = String(url.searchParams.get("nw_uid") || "").trim();
    const requestDeviceIdentifier = String(url.searchParams.get("nw_did") || "").trim() || null;
    if (!requestUserId) {
      return new Response("Unauthorized", { status: 401 });
    }
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    const tags = [];
    if (requestDeviceIdentifier) {
      tags.push(`device:${requestDeviceIdentifier}`);
    }
    this.ctx.acceptWebSocket(server, tags);
    server.serializeAttachment({
      userId: requestUserId,
      handshakeComplete: false,
      protocol: "messagepack",
      deviceIdentifier: requestDeviceIdentifier
    });
    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
  async webSocketMessage(ws, message) {
    const attachment = ws.deserializeAttachment();
    if (!attachment) return;
    if (!attachment.handshakeComplete) {
      const text = decodeIncomingMessage(message);
      const frames = text.split(String.fromCharCode(SIGNALR_RECORD_SEPARATOR)).filter(Boolean);
      for (const frame of frames) {
        try {
          const handshake = JSON.parse(frame);
          attachment.protocol = handshake.protocol === "json" ? "json" : "messagepack";
          attachment.handshakeComplete = true;
          ws.serializeAttachment(attachment);
          ws.send(SIGNALR_HANDSHAKE_ACK);
          this.broadcastDeviceStatus(attachment.userId);
          return;
        } catch {
        }
      }
      return;
    }
    if (typeof message !== "string") {
      try {
        ws.send(message);
      } catch {
      }
    }
  }
  async webSocketClose(ws, code, reason, wasClean) {
    const attachment = ws.deserializeAttachment();
    const shouldBroadcast = !!attachment?.handshakeComplete;
    if (shouldBroadcast && attachment?.userId) {
      this.broadcastDeviceStatus(attachment.userId);
    }
  }
  async webSocketError(ws, error) {
    const attachment = ws.deserializeAttachment();
    const shouldBroadcast = !!attachment?.handshakeComplete;
    if (shouldBroadcast && attachment?.userId) {
      this.broadcastDeviceStatus(attachment.userId);
    }
  }
  getOnlineDeviceIdentifiers() {
    const out = /* @__PURE__ */ new Set();
    for (const ws of this.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment();
      if (!attachment?.handshakeComplete || !attachment.deviceIdentifier) continue;
      out.add(attachment.deviceIdentifier);
    }
    return Array.from(out);
  }
  broadcastMessage(updateType, payload, contextId, targetDeviceIdentifier) {
    const sockets = targetDeviceIdentifier ? this.ctx.getWebSockets(`device:${targetDeviceIdentifier}`) : this.ctx.getWebSockets();
    if (sockets.length === 0) return;
    for (const ws of sockets) {
      const attachment = ws.deserializeAttachment();
      if (!attachment?.handshakeComplete) continue;
      try {
        if (attachment.protocol === "json") {
          ws.send(buildSignalRJsonInvocation(updateType, payload, contextId));
        } else {
          ws.send(buildSignalRMessagePackInvocation(updateType, payload, contextId));
        }
      } catch {
        try {
          ws.close(1011, "Notification send failed");
        } catch {
        }
      }
    }
  }
  broadcastDeviceStatus(userId) {
    this.broadcastMessage(
      SIGNALR_UPDATE_TYPE_DEVICE_STATUS,
      {
        UserId: userId,
        Date: (/* @__PURE__ */ new Date()).toISOString()
      },
      null,
      null
    );
  }
};
function notifyUserVaultSync(env, userId, revisionDate, contextId) {
  waitUntil(notifyUserUpdate(env, userId, SIGNALR_UPDATE_TYPE_SYNC_VAULT, revisionDate, contextId ?? null, null));
}
__name(notifyUserVaultSync, "notifyUserVaultSync");
function notifyUserLogout(env, userId, targetDeviceIdentifier) {
  waitUntil(notifyUserUpdate(env, userId, SIGNALR_UPDATE_TYPE_LOG_OUT, (/* @__PURE__ */ new Date()).toISOString(), null, targetDeviceIdentifier ?? null));
}
__name(notifyUserLogout, "notifyUserLogout");
async function getOnlineUserDevices(env, userId) {
  try {
    const id = env.NOTIFICATIONS_HUB.idFromName(userId);
    const stub = env.NOTIFICATIONS_HUB.get(id);
    const response = await stub.fetch("https://notifications/internal/online");
    if (!response.ok) return [];
    const body = await response.json().catch(() => null);
    return Array.isArray(body?.deviceIdentifiers) ? body.deviceIdentifiers.filter((value) => !!String(value || "").trim()) : [];
  } catch {
    return [];
  }
}
__name(getOnlineUserDevices, "getOnlineUserDevices");
async function notifyUserUpdate(env, userId, updateType, revisionDate, contextId, targetDeviceIdentifier) {
  try {
    const id = env.NOTIFICATIONS_HUB.idFromName(userId);
    const stub = env.NOTIFICATIONS_HUB.get(id);
    await stub.fetch("https://notifications/internal/notify", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-NodeWarden-UserId": userId
      },
      body: JSON.stringify({
        revisionDate,
        contextId: contextId || null,
        updateType,
        targetDeviceIdentifier: targetDeviceIdentifier || null,
        payload: {
          UserId: userId,
          Date: revisionDate
        }
      })
    });
  } catch (error) {
    console.error("Failed to broadcast realtime notification:", error);
  }
}
__name(notifyUserUpdate, "notifyUserUpdate");
async function notifyUserBackupProgress(env, userId, progress, targetDeviceIdentifier) {
  const revisionDate = progress.timestamp || (/* @__PURE__ */ new Date()).toISOString();
  try {
    const id = env.NOTIFICATIONS_HUB.idFromName(userId);
    const stub = env.NOTIFICATIONS_HUB.get(id);
    await stub.fetch("https://notifications/internal/notify", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-NodeWarden-UserId": userId
      },
      body: JSON.stringify({
        revisionDate,
        contextId: null,
        updateType: SIGNALR_UPDATE_TYPE_BACKUP_RESTORE_PROGRESS,
        targetDeviceIdentifier: targetDeviceIdentifier || null,
        payload: {
          UserId: userId,
          Date: revisionDate,
          ...progress
        }
      })
    });
  } catch (error) {
    console.error("Failed to broadcast backup progress:", error);
  }
}
__name(notifyUserBackupProgress, "notifyUserBackupProgress");
async function notifyUserBackupRestoreProgress(env, userId, progress, targetDeviceIdentifier) {
  return notifyUserBackupProgress(env, userId, progress, targetDeviceIdentifier);
}
__name(notifyUserBackupRestoreProgress, "notifyUserBackupRestoreProgress");

// src/types/index.ts
var DEFAULT_DEV_SECRET = "Enter-your-JWT-key-here-at-least-32-characters";

// src/config/limits.ts
var LIMITS = {
  auth: {
    // Access token lifetime in seconds.
    // 璁块棶浠ょ墝鏈夋晥鏈燂紙绉掞級銆?    accessTokenTtlSeconds: 7200,
    // Refresh token lifetime in milliseconds.
    // 鍒锋柊浠ょ墝鏈夋晥鏈燂紙姣锛夈€?    refreshTokenTtlMs: 365 * 24 * 60 * 60 * 1e3,
    // Grace window for previous refresh token after rotation (ms).
    // 鍒锋柊浠ょ墝杞崲鍚庣殑鏃т护鐗屽闄愮獥鍙ｏ紙姣锛夈€?    refreshTokenOverlapGraceMs: 30 * 60 * 1e3,
    // Refresh token random byte length.
    // 鍒锋柊浠ょ墝闅忔満瀛楄妭闀垮害銆?    refreshTokenRandomBytes: 32,
    // Attachment download token lifetime in seconds.
    // 闄勪欢涓嬭浇浠ょ墝鏈夋晥鏈燂紙绉掞級銆?    fileDownloadTokenTtlSeconds: 300,
    // Send access token lifetime in seconds.
    // Send 璁块棶浠ょ墝鏈夋晥鏈燂紙绉掞級銆?    sendAccessTokenTtlSeconds: 300,
    // Minimum required JWT secret length.
    // JWT 瀵嗛挜鏈€灏忛暱搴﹁姹傘€?    jwtSecretMinLength: 32,
    // Default PBKDF2 iterations for account creation/prelogin fallback.
    // 璐︽埛鍒涘缓涓庨鐧诲綍鍥為€€浣跨敤鐨勯粯璁?PBKDF2 杩唬娆℃暟銆?    defaultKdfIterations: 6e5,
    // clientSecret length
    // clientSecret 闀垮害
    clientSecretLength: 30
  },
  rateLimit: {
    // Max failed login attempts before temporary lock.
    // 瑙﹀彂涓存椂閿佸畾鍓嶅厑璁哥殑鏈€澶х櫥褰曞け璐ユ鏁般€?    loginMaxAttempts: 10,
    // Login lock duration in minutes.
    // 鐧诲綍閿佸畾鏃堕暱锛堝垎閽燂級銆?    loginLockoutMinutes: 2,
    // Authenticated API request budget per user per minute (all reads & writes combined).
    // 璁よ瘉 API 姣忕敤鎴锋瘡鍒嗛挓璇锋眰閰嶉锛堣鍐欏悎璁★級銆?    apiRequestsPerMinute: 200,
    // Public (unauthenticated) request budget per IP per minute.
    // 鍏紑锛堟湭璁よ瘉锛夋帴鍙ｆ瘡 IP 姣忓垎閽熻姹傞厤棰濄€?    publicRequestsPerMinute: 60,
    // Public read-only request budget per IP per minute.
    // 鍏紑鍙鎺ュ彛姣?IP 姣忓垎閽熻姹傞厤棰濄€?    publicReadRequestsPerMinute: 120,
    // Sensitive public/auth request budget per IP per minute.
    // 鏁忔劅鍏紑/璁よ瘉鎺ュ彛姣?IP 姣忓垎閽熻姹傞厤棰濄€?    sensitivePublicRequestsPerMinute: 30,
    // Password hint lookup budget per IP per minute.
    // 瀵嗙爜鎻愮ず鏌ヨ鎺ュ彛姣?IP 姣忓垎閽熻姹傞厤棰濄€?    passwordHintRequestsPerMinute: 1,
    // Password hint lookup budget per IP per hour.
    // 瀵嗙爜鎻愮ず鏌ヨ鎺ュ彛姣?IP 姣忓皬鏃惰姹傞厤棰濄€?    passwordHintRequestsPerHour: 3,
    // Register endpoint budget per IP per minute.
    // 娉ㄥ唽鎺ュ彛姣?IP 姣忓垎閽熻姹傞厤棰濄€?    registerRequestsPerMinute: 5,
    // Refresh-token grant budget per IP per minute.
    // refresh_token 鎺堟潈姣?IP 姣忓垎閽熻姹傞厤棰濄€?    refreshTokenRequestsPerMinute: 30,
    // Fixed window size for API rate limiting in seconds.
    // API 闄愭祦鍥哄畾绐楀彛澶у皬锛堢锛夈€?    apiWindowSeconds: 60,
    // Probability to run low-frequency cleanup on request path.
    // 鍦ㄨ姹傝矾寰勪腑瑙﹀彂浣庨娓呯悊鐨勬鐜囥€?    cleanupProbability: 0.05,
    // Minimum interval between login-attempt cleanup runs.
    // 鐧诲綍灏濊瘯琛ㄦ竻鐞嗙殑鏈€灏忛棿闅斻€?    loginIpCleanupIntervalMs: 10 * 60 * 1e3,
    // Retention window for login IP records.
    // 鐧诲綍 IP 璁板綍淇濈暀鏃堕暱銆?    loginIpRetentionMs: 30 * 24 * 60 * 60 * 1e3
  },
  cleanup: {
    // Minimum interval between refresh-token cleanup runs.
    // refresh_token 琛ㄦ竻鐞嗘渶灏忛棿闅斻€?    refreshTokenCleanupIntervalMs: 30 * 60 * 1e3,
    // Minimum interval between used attachment token cleanup runs.
    // 宸蹭娇鐢ㄩ檮浠朵护鐗岃〃娓呯悊鏈€灏忛棿闅斻€?    attachmentTokenCleanupIntervalMs: 10 * 60 * 1e3,
    // Probability to trigger cleanup during requests.
    // 璇锋眰杩囩▼涓Е鍙戞竻鐞嗙殑姒傜巼銆?    cleanupProbability: 0.05
  },
  attachment: {
    // Max attachment upload size in bytes.
    // 闄勪欢涓婁紶澶у皬涓婇檺锛堝瓧鑺傦級銆?    maxFileSizeBytes: 100 * 1024 * 1024
  },
  send: {
    // Max file size allowed for Send file uploads.
    // Send 鏂囦欢涓婁紶澶у皬涓婇檺銆?    maxFileSizeBytes: 100 * 1024 * 1024,
    // Max days allowed between now and deletion date.
    // 鍏佽鐨勬渶杩滃垹闄ゆ棩鏈燂紙璺濆綋鍓嶅ぉ鏁帮級銆?    maxDeletionDays: 31
  },
  pagination: {
    // Default page size when client does not specify pageSize.
    // 瀹㈡埛绔湭浼?pageSize 鏃剁殑榛樿鍒嗛〉澶у皬銆?    defaultPageSize: 100,
    // Hard maximum page size accepted by server.
    // 鏈嶅姟绔厑璁哥殑鏈€澶у垎椤靛ぇ灏忋€?    maxPageSize: 500
  },
  cors: {
    // Browser preflight cache max age in seconds.
    // 娴忚鍣ㄩ妫€璇锋眰缂撳瓨鏃堕暱锛堢锛夈€?    preflightMaxAgeSeconds: 86400
  },
  cache: {
    // Icon proxy cache TTL in seconds.
    // 鍥炬爣浠ｇ悊缂撳瓨鏃堕暱锛堢锛夈€?    iconTtlSeconds: 604800,
    // In-memory /api/sync response cache TTL (milliseconds).
    // /api/sync 鍐呭瓨缂撳瓨鏈夋晥鏈燂紙姣锛夈€?    syncResponseTtlMs: 30 * 1e3,
    // Max size of a single cached /api/sync body in bytes.
    // 鍗曚釜 /api/sync 缂撳瓨鍝嶅簲鍏佽鐨勬渶澶у瓧鑺傛暟銆?    syncResponseMaxBodyBytes: 512 * 1024,
    // Max total in-memory bytes used by /api/sync cache per isolate.
    // 姣忎釜 isolate 涓?/api/sync 缂撳瓨鍏佽鍗犵敤鐨勬渶澶ф€诲瓧鑺傛暟銆?    syncResponseMaxTotalBytes: 2 * 1024 * 1024,
    // Max in-memory /api/sync cache entries per isolate.
    // 姣忎釜 isolate 鐨?/api/sync 鏈€澶х紦瀛樻潯鐩暟銆?    syncResponseMaxEntries: 64
  },
  performance: {
    // Max IDs per SQL batch when moving ciphers in bulk.
    // 鎵归噺绉诲姩瀵嗙爜椤规椂姣忔壒 SQL 鐨勬渶澶?ID 鏁伴噺銆?    bulkMoveChunkSize: 200,
    // Max total items (folders + ciphers) allowed in a single import.
    // 鍗曟瀵煎叆鍏佽鐨勬渶澶ф潯鐩暟锛堟枃浠跺す + 瀵嗙爜椤瑰悎璁★級銆?    importItemLimit: 5e3,
    // Small fixed concurrency for blob/attachment batch cleanup work.
    // 闄勪欢 / blob 鎵归噺娓呯悊鏃剁殑淇濆畧骞跺彂鏁般€?    attachmentDeleteConcurrency: 4
  },
  request: {
    // Hard body size limit for JSON API endpoints (bytes). File upload paths are exempt.
    // JSON 鎺ュ彛璇锋眰 body 澶у皬涓婇檺锛堝瓧鑺傦級锛屾枃浠朵笂浼犳帴鍙ｉ櫎澶栥€?    maxBodyBytes: 25 * 1024 * 1024
  },
  compatibility: {
    // Single source of truth for /config.version and /api/version.
    // /config.version 涓?/api/version 鐨勭粺涓€鐗堟湰鍙锋潵婧愩€?    bitwardenServerVersion: "2026.1.0"
  }
};

// src/utils/jwt.ts
var hmacKeyCache = /* @__PURE__ */ new Map();
function base64UrlEncode(data) {
  const base64 = btoa(String.fromCharCode(...data));
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
__name(base64UrlEncode, "base64UrlEncode");
function base64UrlDecode(str) {
  str = str.replace(/-/g, "+").replace(/_/g, "/");
  while (str.length % 4) str += "=";
  const binary = atob(str);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}
__name(base64UrlDecode, "base64UrlDecode");
function getHmacKey(secret) {
  const cacheKey = secret;
  let cached = hmacKeyCache.get(cacheKey);
  if (cached) return cached;
  const encoder = new TextEncoder();
  cached = crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
  hmacKeyCache.set(cacheKey, cached);
  return cached;
}
__name(getHmacKey, "getHmacKey");
async function createJWT(payload, secret, expiresIn = LIMITS.auth.accessTokenTtlSeconds) {
  const header = { alg: "HS256", typ: "JWT" };
  const now = Math.floor(Date.now() / 1e3);
  const fullPayload = {
    ...payload,
    email_verified: true,
    // required by mobile client
    amr: ["Application"],
    // authentication methods reference - required by mobile client
    iat: now,
    exp: now + expiresIn,
    iss: "nodewarden",
    premium: true
  };
  const encoder = new TextEncoder();
  const headerB64 = base64UrlEncode(encoder.encode(JSON.stringify(header)));
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(fullPayload)));
  const data = `${headerB64}.${payloadB64}`;
  const key = await getHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  const signatureB64 = base64UrlEncode(new Uint8Array(signature));
  return `${data}.${signatureB64}`;
}
__name(createJWT, "createJWT");
async function verifyJWT(token, secret) {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [headerB64, payloadB64, signatureB64] = parts;
    const encoder = new TextEncoder();
    const key = await getHmacKey(secret);
    const data = `${headerB64}.${payloadB64}`;
    const signature = base64UrlDecode(signatureB64);
    const valid = await crypto.subtle.verify("HMAC", key, signature, encoder.encode(data));
    if (!valid) return null;
    const payload = JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadB64)));
    const now = Math.floor(Date.now() / 1e3);
    if (payload.exp < now) return null;
    return payload;
  } catch {
    return null;
  }
}
__name(verifyJWT, "verifyJWT");
function createRefreshToken() {
  const bytes = new Uint8Array(LIMITS.auth.refreshTokenRandomBytes);
  crypto.getRandomValues(bytes);
  return base64UrlEncode(bytes);
}
__name(createRefreshToken, "createRefreshToken");
async function createFileDownloadToken(cipherId, attachmentId, secret) {
  const header = { alg: "HS256", typ: "JWT" };
  const now = Math.floor(Date.now() / 1e3);
  const payload = {
    cipherId,
    attachmentId,
    jti: createRefreshToken(),
    exp: now + LIMITS.auth.fileDownloadTokenTtlSeconds
    // 5 minutes
  };
  const encoder = new TextEncoder();
  const headerB64 = base64UrlEncode(encoder.encode(JSON.stringify(header)));
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(payload)));
  const data = `${headerB64}.${payloadB64}`;
  const key = await getHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  const signatureB64 = base64UrlEncode(new Uint8Array(signature));
  return `${data}.${signatureB64}`;
}
__name(createFileDownloadToken, "createFileDownloadToken");
async function verifyFileDownloadToken(token, secret) {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [headerB64, payloadB64, signatureB64] = parts;
    const encoder = new TextEncoder();
    const key = await getHmacKey(secret);
    const data = `${headerB64}.${payloadB64}`;
    const signature = base64UrlDecode(signatureB64);
    const valid = await crypto.subtle.verify("HMAC", key, signature, encoder.encode(data));
    if (!valid) return null;
    const payload = JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadB64)));
    const now = Math.floor(Date.now() / 1e3);
    if (payload.exp < now) return null;
    return payload;
  } catch {
    return null;
  }
}
__name(verifyFileDownloadToken, "verifyFileDownloadToken");
async function createAttachmentUploadToken(userId, cipherId, attachmentId, secret) {
  const header = { alg: "HS256", typ: "JWT" };
  const now = Math.floor(Date.now() / 1e3);
  const payload = {
    userId,
    cipherId,
    attachmentId,
    exp: now + LIMITS.auth.fileDownloadTokenTtlSeconds
  };
  const encoder = new TextEncoder();
  const headerB64 = base64UrlEncode(encoder.encode(JSON.stringify(header)));
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(payload)));
  const data = `${headerB64}.${payloadB64}`;
  const key = await getHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  const signatureB64 = base64UrlEncode(new Uint8Array(signature));
  return `${data}.${signatureB64}`;
}
__name(createAttachmentUploadToken, "createAttachmentUploadToken");
async function verifyAttachmentUploadToken(token, secret) {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [headerB64, payloadB64, signatureB64] = parts;
    const encoder = new TextEncoder();
    const key = await getHmacKey(secret);
    const data = `${headerB64}.${payloadB64}`;
    const signature = base64UrlDecode(signatureB64);
    const valid = await crypto.subtle.verify("HMAC", key, signature, encoder.encode(data));
    if (!valid) return null;
    const payload = JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadB64)));
    const now = Math.floor(Date.now() / 1e3);
    if (payload.exp < now) return null;
    if (!payload.userId || !payload.cipherId || !payload.attachmentId) return null;
    return payload;
  } catch {
    return null;
  }
}
__name(verifyAttachmentUploadToken, "verifyAttachmentUploadToken");
async function createSendFileDownloadToken(sendId, fileId, secret) {
  const header = { alg: "HS256", typ: "JWT" };
  const now = Math.floor(Date.now() / 1e3);
  const payload = {
    sendId,
    fileId,
    jti: createRefreshToken(),
    exp: now + LIMITS.auth.fileDownloadTokenTtlSeconds
  };
  const encoder = new TextEncoder();
  const headerB64 = base64UrlEncode(encoder.encode(JSON.stringify(header)));
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(payload)));
  const data = `${headerB64}.${payloadB64}`;
  const key = await getHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  const signatureB64 = base64UrlEncode(new Uint8Array(signature));
  return `${data}.${signatureB64}`;
}
__name(createSendFileDownloadToken, "createSendFileDownloadToken");
async function verifySendFileDownloadToken(token, secret) {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [headerB64, payloadB64, signatureB64] = parts;
    const encoder = new TextEncoder();
    const key = await getHmacKey(secret);
    const data = `${headerB64}.${payloadB64}`;
    const signature = base64UrlDecode(signatureB64);
    const valid = await crypto.subtle.verify("HMAC", key, signature, encoder.encode(data));
    if (!valid) return null;
    const payload = JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadB64)));
    if (typeof payload.sendId !== "string" || typeof payload.fileId !== "string" || typeof payload.jti !== "string" || !payload.jti || typeof payload.exp !== "number") {
      return null;
    }
    const now = Math.floor(Date.now() / 1e3);
    if (payload.exp < now) return null;
    return payload;
  } catch {
    return null;
  }
}
__name(verifySendFileDownloadToken, "verifySendFileDownloadToken");
async function createSendFileUploadToken(userId, sendId, fileId, secret) {
  const header = { alg: "HS256", typ: "JWT" };
  const now = Math.floor(Date.now() / 1e3);
  const payload = {
    userId,
    sendId,
    fileId,
    exp: now + LIMITS.auth.fileDownloadTokenTtlSeconds
  };
  const encoder = new TextEncoder();
  const headerB64 = base64UrlEncode(encoder.encode(JSON.stringify(header)));
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(payload)));
  const data = `${headerB64}.${payloadB64}`;
  const key = await getHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  const signatureB64 = base64UrlEncode(new Uint8Array(signature));
  return `${data}.${signatureB64}`;
}
__name(createSendFileUploadToken, "createSendFileUploadToken");
async function verifySendFileUploadToken(token, secret) {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [headerB64, payloadB64, signatureB64] = parts;
    const encoder = new TextEncoder();
    const key = await getHmacKey(secret);
    const data = `${headerB64}.${payloadB64}`;
    const signature = base64UrlDecode(signatureB64);
    const valid = await crypto.subtle.verify("HMAC", key, signature, encoder.encode(data));
    if (!valid) return null;
    const payload = JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadB64)));
    const now = Math.floor(Date.now() / 1e3);
    if (payload.exp < now) return null;
    if (!payload.userId || !payload.sendId || !payload.fileId) return null;
    return payload;
  } catch {
    return null;
  }
}
__name(verifySendFileUploadToken, "verifySendFileUploadToken");
async function createSendAccessToken(sendId, secret) {
  const header = { alg: "HS256", typ: "JWT" };
  const now = Math.floor(Date.now() / 1e3);
  const payload = {
    sub: sendId,
    typ: "send_access",
    iat: now,
    exp: now + LIMITS.auth.sendAccessTokenTtlSeconds
  };
  const encoder = new TextEncoder();
  const headerB64 = base64UrlEncode(encoder.encode(JSON.stringify(header)));
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(payload)));
  const data = `${headerB64}.${payloadB64}`;
  const key = await getHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  const signatureB64 = base64UrlEncode(new Uint8Array(signature));
  return `${data}.${signatureB64}`;
}
__name(createSendAccessToken, "createSendAccessToken");
async function verifySendAccessToken(token, secret) {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [headerB64, payloadB64, signatureB64] = parts;
    const encoder = new TextEncoder();
    const key = await getHmacKey(secret);
    const data = `${headerB64}.${payloadB64}`;
    const signature = base64UrlDecode(signatureB64);
    const valid = await crypto.subtle.verify("HMAC", key, signature, encoder.encode(data));
    if (!valid) return null;
    const payload = JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadB64)));
    const now = Math.floor(Date.now() / 1e3);
    if (payload.exp < now) return null;
    if (payload.typ !== "send_access") return null;
    if (!payload.sub) return null;
    return payload;
  } catch {
    return null;
  }
}
__name(verifySendAccessToken, "verifySendAccessToken");

// src/services/storage-schema.ts
var SCHEMA_STATEMENTS = [
  "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT, master_password_hint TEXT, master_password_hash TEXT NOT NULL, key TEXT NOT NULL, private_key TEXT, public_key TEXT, kdf_type INTEGER NOT NULL, kdf_iterations INTEGER NOT NULL, kdf_memory INTEGER, kdf_parallelism INTEGER, security_stamp TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'user', status TEXT NOT NULL DEFAULT 'active', verify_devices INTEGER NOT NULL DEFAULT 1, totp_secret TEXT, totp_recovery_code TEXT, api_key TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
  "ALTER TABLE users ADD COLUMN master_password_hint TEXT",
  "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'",
  "ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'",
  "ALTER TABLE users ADD COLUMN verify_devices INTEGER NOT NULL DEFAULT 1",
  "ALTER TABLE users ADD COLUMN totp_secret TEXT",
  "ALTER TABLE users ADD COLUMN totp_recovery_code TEXT",
  "ALTER TABLE users ADD COLUMN api_key TEXT",
  "CREATE TABLE IF NOT EXISTS domain_settings (user_id TEXT PRIMARY KEY, equivalent_domains TEXT NOT NULL DEFAULT '[]', custom_equivalent_domains TEXT NOT NULL DEFAULT '[]', excluded_global_equivalent_domains TEXT NOT NULL DEFAULT '[]', updated_at TEXT NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
  "ALTER TABLE domain_settings ADD COLUMN custom_equivalent_domains TEXT NOT NULL DEFAULT '[]'",
  "CREATE TABLE IF NOT EXISTS user_revisions (user_id TEXT PRIMARY KEY, revision_date TEXT NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
  "CREATE TABLE IF NOT EXISTS ciphers (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, type INTEGER NOT NULL, folder_id TEXT, name TEXT, notes TEXT, favorite INTEGER NOT NULL DEFAULT 0, data TEXT NOT NULL, reprompt INTEGER, key TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, archived_at TEXT, deleted_at TEXT, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
  "ALTER TABLE ciphers ADD COLUMN archived_at TEXT",
  "CREATE INDEX IF NOT EXISTS idx_ciphers_user_updated ON ciphers(user_id, updated_at)",
  "CREATE INDEX IF NOT EXISTS idx_ciphers_user_archived ON ciphers(user_id, archived_at)",
  "CREATE INDEX IF NOT EXISTS idx_ciphers_user_deleted ON ciphers(user_id, deleted_at)",
  "CREATE INDEX IF NOT EXISTS idx_ciphers_user_deleted_updated ON ciphers(user_id, deleted_at, updated_at)",
  "CREATE INDEX IF NOT EXISTS idx_ciphers_user_folder ON ciphers(user_id, folder_id)",
  "CREATE TABLE IF NOT EXISTS folders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, name TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
  "CREATE INDEX IF NOT EXISTS idx_folders_user_updated ON folders(user_id, updated_at)",
  "CREATE TABLE IF NOT EXISTS attachments (id TEXT PRIMARY KEY, cipher_id TEXT NOT NULL, file_name TEXT NOT NULL, size INTEGER NOT NULL, size_name TEXT NOT NULL, key TEXT, FOREIGN KEY (cipher_id) REFERENCES ciphers(id) ON DELETE CASCADE)",
  "CREATE INDEX IF NOT EXISTS idx_attachments_cipher ON attachments(cipher_id)",
  "CREATE TABLE IF NOT EXISTS sends (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, type INTEGER NOT NULL, name TEXT NOT NULL, notes TEXT, data TEXT NOT NULL, key TEXT NOT NULL, password_hash TEXT, password_salt TEXT, password_iterations INTEGER, auth_type INTEGER NOT NULL DEFAULT 2, emails TEXT, max_access_count INTEGER, access_count INTEGER NOT NULL DEFAULT 0, disabled INTEGER NOT NULL DEFAULT 0, hide_email INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, expiration_date TEXT, deletion_date TEXT NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
  "CREATE INDEX IF NOT EXISTS idx_sends_user_updated ON sends(user_id, updated_at)",
  "CREATE INDEX IF NOT EXISTS idx_sends_user_deletion ON sends(user_id, deletion_date)",
  "CREATE INDEX IF NOT EXISTS idx_sends_user_updated_id ON sends(user_id, updated_at, id)",
  "ALTER TABLE sends ADD COLUMN auth_type INTEGER NOT NULL DEFAULT 2",
  "ALTER TABLE sends ADD COLUMN emails TEXT",
  "CREATE TABLE IF NOT EXISTS refresh_tokens (token TEXT PRIMARY KEY, user_id TEXT NOT NULL, expires_at INTEGER NOT NULL, device_identifier TEXT, device_session_stamp TEXT, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
  "CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user ON refresh_tokens(user_id)",
  "ALTER TABLE refresh_tokens ADD COLUMN device_identifier TEXT",
  "ALTER TABLE refresh_tokens ADD COLUMN device_session_stamp TEXT",
  "CREATE TABLE IF NOT EXISTS invites (code TEXT PRIMARY KEY, created_by TEXT NOT NULL, used_by TEXT, expires_at TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY (used_by) REFERENCES users(id) ON DELETE SET NULL)",
  "CREATE INDEX IF NOT EXISTS idx_invites_status_expires ON invites(status, expires_at)",
  "CREATE INDEX IF NOT EXISTS idx_invites_created_by ON invites(created_by, created_at)",
  "CREATE TABLE IF NOT EXISTS audit_logs (id TEXT PRIMARY KEY, actor_user_id TEXT, action TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'system', level TEXT NOT NULL DEFAULT 'info', target_type TEXT, target_id TEXT, metadata TEXT, created_at TEXT NOT NULL, FOREIGN KEY (actor_user_id) REFERENCES users(id) ON DELETE SET NULL)",
  "ALTER TABLE audit_logs ADD COLUMN category TEXT NOT NULL DEFAULT 'system'",
  "ALTER TABLE audit_logs ADD COLUMN level TEXT NOT NULL DEFAULT 'info'",
  "UPDATE audit_logs SET category = json_extract(metadata, '$.category') WHERE json_valid(metadata) AND json_extract(metadata, '$.category') IN ('auth', 'security', 'device', 'data', 'system')",
  "UPDATE audit_logs SET level = json_extract(metadata, '$.level') WHERE json_valid(metadata) AND json_extract(metadata, '$.level') IN ('info', 'warn', 'error', 'security')",
  "CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at)",
  "CREATE INDEX IF NOT EXISTS idx_audit_logs_actor_created ON audit_logs(actor_user_id, created_at)",
  "CREATE INDEX IF NOT EXISTS idx_audit_logs_category_created ON audit_logs(category, created_at)",
  "CREATE INDEX IF NOT EXISTS idx_audit_logs_level_created ON audit_logs(level, created_at)",
  "CREATE TABLE IF NOT EXISTS devices (user_id TEXT NOT NULL, device_identifier TEXT NOT NULL, name TEXT NOT NULL, type INTEGER NOT NULL, session_stamp TEXT, encrypted_user_key TEXT, encrypted_public_key TEXT, encrypted_private_key TEXT, banned INTEGER NOT NULL DEFAULT 0, banned_at TEXT, device_note TEXT, last_seen_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (user_id, device_identifier), FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
  "CREATE INDEX IF NOT EXISTS idx_devices_user_updated ON devices(user_id, updated_at)",
  "ALTER TABLE devices ADD COLUMN session_stamp TEXT",
  "ALTER TABLE devices ADD COLUMN encrypted_user_key TEXT",
  "ALTER TABLE devices ADD COLUMN encrypted_public_key TEXT",
  "ALTER TABLE devices ADD COLUMN encrypted_private_key TEXT",
  "ALTER TABLE devices ADD COLUMN banned INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE devices ADD COLUMN banned_at TEXT",
  "ALTER TABLE devices ADD COLUMN device_note TEXT",
  "ALTER TABLE devices ADD COLUMN last_seen_at TEXT",
  "CREATE INDEX IF NOT EXISTS idx_devices_user_last_seen ON devices(user_id, last_seen_at)",
  "CREATE TABLE IF NOT EXISTS trusted_two_factor_device_tokens (token TEXT PRIMARY KEY, user_id TEXT NOT NULL, device_identifier TEXT NOT NULL, expires_at INTEGER NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
  "CREATE INDEX IF NOT EXISTS idx_trusted_two_factor_device_tokens_user_device ON trusted_two_factor_device_tokens(user_id, device_identifier)",
  "CREATE TABLE IF NOT EXISTS login_attempts_ip (ip TEXT PRIMARY KEY, attempts INTEGER NOT NULL, locked_until INTEGER, updated_at INTEGER NOT NULL)",
  "CREATE TABLE IF NOT EXISTS used_attachment_download_tokens (jti TEXT PRIMARY KEY, expires_at INTEGER NOT NULL)"
];
async function executeSchemaStatement(db, statement) {
  try {
    await db.prepare(statement).run();
  } catch (error) {
    const msg = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
    if (msg.includes("already exists") || msg.includes("duplicate column name")) {
      return;
    }
    throw error;
  }
}
__name(executeSchemaStatement, "executeSchemaStatement");
async function ensureAdminUserExists(db) {
  const admin = await db.prepare("SELECT id FROM users WHERE role = 'admin' LIMIT 1").first();
  if (admin?.id) return;
  const firstUser = await db.prepare("SELECT id FROM users ORDER BY created_at ASC LIMIT 1").first();
  if (!firstUser?.id) return;
  await db.prepare("UPDATE users SET role = 'admin', updated_at = ? WHERE id = ?").bind((/* @__PURE__ */ new Date()).toISOString(), firstUser.id).run();
}
__name(ensureAdminUserExists, "ensureAdminUserExists");
async function ensureStorageSchema(db) {
  await db.prepare("PRAGMA foreign_keys = ON").run();
  await db.prepare("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL)").run();
  for (const stmt of SCHEMA_STATEMENTS) {
    await executeSchemaStatement(db, stmt);
  }
  await ensureAdminUserExists(db);
}
__name(ensureStorageSchema, "ensureStorageSchema");

// src/services/storage-config-repo.ts
async function isRegistered(db) {
  const row = await db.prepare("SELECT value FROM config WHERE key = ?").bind("registered").first();
  return row?.value === "true";
}
__name(isRegistered, "isRegistered");
async function getConfigValue(db, key) {
  const row = await db.prepare("SELECT value FROM config WHERE key = ?").bind(key).first();
  return typeof row?.value === "string" ? row.value : null;
}
__name(getConfigValue, "getConfigValue");
async function setConfigValue(db, key, value) {
  await db.prepare("INSERT INTO config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").bind(key, value).run();
}
__name(setConfigValue, "setConfigValue");
async function setRegistered(db) {
  await db.prepare("INSERT INTO config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").bind("registered", "true").run();
}
__name(setRegistered, "setRegistered");

// src/services/storage-user-repo.ts
var USER_SELECT_COLUMNS = "id, email, name, master_password_hint, master_password_hash, key, private_key, public_key, kdf_type, kdf_iterations, kdf_memory, kdf_parallelism, security_stamp, role, status, verify_devices, totp_secret, totp_recovery_code, api_key, created_at, updated_at";
function mapUserRow(row) {
  return {
    id: row.id,
    email: row.email,
    name: row.name,
    masterPasswordHint: row.master_password_hint ?? null,
    masterPasswordHash: row.master_password_hash,
    key: row.key,
    privateKey: row.private_key,
    publicKey: row.public_key,
    kdfType: row.kdf_type,
    kdfIterations: row.kdf_iterations,
    kdfMemory: row.kdf_memory ?? void 0,
    kdfParallelism: row.kdf_parallelism ?? void 0,
    securityStamp: row.security_stamp,
    role: row.role === "admin" ? "admin" : "user",
    status: row.status === "banned" ? "banned" : "active",
    verifyDevices: row.verify_devices == null ? true : !!row.verify_devices,
    totpSecret: row.totp_secret ?? null,
    totpRecoveryCode: row.totp_recovery_code ?? null,
    apiKey: row.api_key ?? null,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}
__name(mapUserRow, "mapUserRow");
async function getUser(db, email) {
  const row = await db.prepare(`SELECT ${USER_SELECT_COLUMNS} FROM users WHERE email = ?`).bind(email.toLowerCase()).first();
  if (!row) return null;
  return mapUserRow(row);
}
__name(getUser, "getUser");
async function getUserById(db, id) {
  const row = await db.prepare(`SELECT ${USER_SELECT_COLUMNS} FROM users WHERE id = ?`).bind(id).first();
  if (!row) return null;
  return mapUserRow(row);
}
__name(getUserById, "getUserById");
async function getUserCount(db) {
  const row = await db.prepare("SELECT COUNT(*) AS count FROM users").first();
  return Number(row?.count || 0);
}
__name(getUserCount, "getUserCount");
async function getAllUsers(db) {
  const res = await db.prepare(`SELECT ${USER_SELECT_COLUMNS} FROM users ORDER BY created_at ASC`).all();
  return (res.results || []).map((row) => mapUserRow(row));
}
__name(getAllUsers, "getAllUsers");
async function saveUser(db, safeBind, user) {
  const email = user.email.toLowerCase();
  const stmt = db.prepare(
    "INSERT INTO users(id, email, name, master_password_hint, master_password_hash, key, private_key, public_key, kdf_type, kdf_iterations, kdf_memory, kdf_parallelism, security_stamp, role, status, verify_devices, totp_secret, totp_recovery_code, api_key, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET email=excluded.email, name=excluded.name, master_password_hint=excluded.master_password_hint, master_password_hash=excluded.master_password_hash, key=excluded.key, private_key=excluded.private_key, public_key=excluded.public_key, kdf_type=excluded.kdf_type, kdf_iterations=excluded.kdf_iterations, kdf_memory=excluded.kdf_memory, kdf_parallelism=excluded.kdf_parallelism, security_stamp=excluded.security_stamp, role=excluded.role, status=excluded.status, verify_devices=excluded.verify_devices, totp_secret=excluded.totp_secret, totp_recovery_code=excluded.totp_recovery_code, api_key=excluded.api_key, updated_at=excluded.updated_at"
  );
  await safeBind(
    stmt,
    user.id,
    email,
    user.name,
    user.masterPasswordHint,
    user.masterPasswordHash,
    user.key,
    user.privateKey,
    user.publicKey,
    user.kdfType,
    user.kdfIterations,
    user.kdfMemory,
    user.kdfParallelism,
    user.securityStamp,
    user.role,
    user.status,
    user.verifyDevices ? 1 : 0,
    user.totpSecret,
    user.totpRecoveryCode,
    user.apiKey,
    user.createdAt,
    user.updatedAt
  ).run();
}
__name(saveUser, "saveUser");
async function createUser(db, safeBind, user) {
  await saveUser(db, safeBind, user);
}
__name(createUser, "createUser");
async function createFirstUser(db, safeBind, user) {
  const email = user.email.toLowerCase();
  const stmt = db.prepare(
    "INSERT INTO users(id, email, name, master_password_hint, master_password_hash, key, private_key, public_key, kdf_type, kdf_iterations, kdf_memory, kdf_parallelism, security_stamp, role, status, verify_devices, totp_secret, totp_recovery_code, api_key, created_at, updated_at) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM users LIMIT 1)"
  );
  const result = await safeBind(
    stmt,
    user.id,
    email,
    user.name,
    user.masterPasswordHint,
    user.masterPasswordHash,
    user.key,
    user.privateKey,
    user.publicKey,
    user.kdfType,
    user.kdfIterations,
    user.kdfMemory,
    user.kdfParallelism,
    user.securityStamp,
    user.role,
    user.status,
    user.verifyDevices ? 1 : 0,
    user.totpSecret,
    user.totpRecoveryCode,
    user.apiKey,
    user.createdAt,
    user.updatedAt
  ).run();
  return (result.meta.changes ?? 0) > 0;
}
__name(createFirstUser, "createFirstUser");
async function deleteUserById(db, id) {
  const result = await db.prepare("DELETE FROM users WHERE id = ?").bind(id).run();
  return (result.meta.changes ?? 0) > 0;
}
__name(deleteUserById, "deleteUserById");

// src/services/storage-admin-repo.ts
function auditLogFromRow(row) {
  return {
    id: row.id,
    actorUserId: row.actor_user_id ?? null,
    actorEmail: row.actor_email ?? null,
    action: row.action,
    category: row.category || "system",
    level: row.level || "info",
    targetType: row.target_type ?? null,
    targetId: row.target_id ?? null,
    targetUserEmail: row.target_user_email ?? null,
    metadata: row.metadata ?? null,
    createdAt: row.created_at
  };
}
__name(auditLogFromRow, "auditLogFromRow");
function buildAuditWhere(options) {
  const conditions = [];
  const params = [];
  if (options.from) {
    conditions.push("l.created_at >= ?");
    params.push(options.from);
  }
  if (options.to) {
    conditions.push("l.created_at <= ?");
    params.push(options.to);
  }
  if (options.category) {
    conditions.push("l.category = ?");
    params.push(options.category);
  }
  if (options.level) {
    conditions.push("l.level = ?");
    params.push(options.level);
  }
  if (options.q) {
    const q = options.q.toLowerCase().slice(0, 48);
    const like = `%${q}%`;
    conditions.push(
      "(LOWER(l.action) LIKE ? OR LOWER(COALESCE(l.actor_user_id, '')) LIKE ? OR LOWER(COALESCE(l.target_type, '')) LIKE ? OR LOWER(COALESCE(l.target_id, '')) LIKE ? OR LOWER(COALESCE(actor.email, '')) LIKE ? OR LOWER(COALESCE(target.email, '')) LIKE ?)"
    );
    params.push(like, like, like, like, like, like);
  }
  return {
    where: conditions.length ? `WHERE ${conditions.join(" AND ")}` : "",
    params
  };
}
__name(buildAuditWhere, "buildAuditWhere");
async function createInvite(db, invite) {
  await db.prepare(
    "INSERT INTO invites(code, created_by, used_by, expires_at, status, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)"
  ).bind(invite.code, invite.createdBy, invite.usedBy, invite.expiresAt, invite.status, invite.createdAt, invite.updatedAt).run();
}
__name(createInvite, "createInvite");
async function getInvite(db, code) {
  const row = await db.prepare("SELECT code, created_by, used_by, expires_at, status, created_at, updated_at FROM invites WHERE code = ?").bind(code).first();
  if (!row) return null;
  return {
    code: row.code,
    createdBy: row.created_by,
    usedBy: row.used_by ?? null,
    expiresAt: row.expires_at,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}
__name(getInvite, "getInvite");
async function listInvites(db, includeInactive = false) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const predicate = includeInactive ? "1 = 1" : "(status = 'active' AND expires_at > ?)";
  const query = `SELECT code, created_by, used_by, expires_at, status, created_at, updated_at FROM invites WHERE ${predicate} ORDER BY created_at DESC`;
  const res = includeInactive ? await db.prepare(query).all() : await db.prepare(query).bind(now).all();
  return (res.results || []).map((row) => ({
    code: row.code,
    createdBy: row.created_by,
    usedBy: row.used_by ?? null,
    expiresAt: row.expires_at,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  }));
}
__name(listInvites, "listInvites");
async function markInviteUsed(db, code, userId) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const result = await db.prepare(
    "UPDATE invites SET status = 'used', used_by = ?, updated_at = ? WHERE code = ? AND status = 'active' AND expires_at > ?"
  ).bind(userId, now, code, now).run();
  return (result.meta.changes ?? 0) > 0;
}
__name(markInviteUsed, "markInviteUsed");
async function revokeInvite(db, code) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const result = await db.prepare("UPDATE invites SET status = 'revoked', updated_at = ? WHERE code = ? AND status = 'active'").bind(now, code).run();
  return (result.meta.changes ?? 0) > 0;
}
__name(revokeInvite, "revokeInvite");
async function deleteAllInvites(db) {
  const result = await db.prepare("DELETE FROM invites").run();
  return Number(result.meta.changes ?? 0);
}
__name(deleteAllInvites, "deleteAllInvites");
async function createAuditLog(db, log) {
  await db.prepare(
    "INSERT INTO audit_logs(id, actor_user_id, action, category, level, target_type, target_id, metadata, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
  ).bind(log.id, log.actorUserId, log.action, log.category, log.level, log.targetType, log.targetId, log.metadata, log.createdAt).run();
}
__name(createAuditLog, "createAuditLog");
async function pruneAuditLogs(db, beforeIso) {
  const result = await db.prepare("DELETE FROM audit_logs WHERE created_at < ?").bind(beforeIso).run();
  return Number(result.meta.changes ?? 0);
}
__name(pruneAuditLogs, "pruneAuditLogs");
async function pruneAuditLogsToMax(db, maxEntries) {
  const limit = Math.max(1, Math.floor(maxEntries));
  const result = await db.prepare(
    "DELETE FROM audit_logs WHERE id IN (SELECT id FROM audit_logs ORDER BY created_at DESC LIMIT -1 OFFSET ?)"
  ).bind(limit).run();
  return Number(result.meta.changes ?? 0);
}
__name(pruneAuditLogsToMax, "pruneAuditLogsToMax");
async function clearAuditLogs(db) {
  const result = await db.prepare("DELETE FROM audit_logs").run();
  return Number(result.meta.changes ?? 0);
}
__name(clearAuditLogs, "clearAuditLogs");
async function listAuditLogs(db, options) {
  const limit = Math.max(1, Math.min(200, Math.floor(options.limit || 50)));
  const offset = Math.max(0, Math.floor(options.offset || 0));
  const { where, params } = buildAuditWhere(options);
  const rows = await db.prepare(
    `SELECT l.id, l.actor_user_id, actor.email AS actor_email, l.action, l.category, l.level, l.target_type, l.target_id, target.email AS target_user_email, l.metadata, l.created_at FROM audit_logs l LEFT JOIN users actor ON actor.id = l.actor_user_id LEFT JOIN users target ON l.target_type = 'user' AND target.id = l.target_id ${where} ORDER BY l.created_at DESC LIMIT ? OFFSET ?`
  ).bind(...params, limit + 1, offset).all();
  const results = rows.results || [];
  const logs = results.slice(0, limit).map(auditLogFromRow);
  const hasMore = results.length > limit;
  return {
    logs,
    total: offset + logs.length + (hasMore ? 1 : 0),
    hasMore
  };
}
__name(listAuditLogs, "listAuditLogs");

// src/services/storage-folder-repo.ts
function mapFolderRow(row) {
  return {
    id: row.id,
    userId: row.user_id,
    name: row.name,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}
__name(mapFolderRow, "mapFolderRow");
async function getFolder(db, id) {
  const row = await db.prepare("SELECT id, user_id, name, created_at, updated_at FROM folders WHERE id = ?").bind(id).first();
  if (!row) return null;
  return mapFolderRow(row);
}
__name(getFolder, "getFolder");
async function saveFolder(db, folder) {
  await db.prepare(
    "INSERT INTO folders(id, user_id, name, created_at, updated_at) VALUES(?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET user_id=excluded.user_id, name=excluded.name, updated_at=excluded.updated_at"
  ).bind(folder.id, folder.userId, folder.name, folder.createdAt, folder.updatedAt).run();
}
__name(saveFolder, "saveFolder");
async function deleteFolder(db, id, userId) {
  await db.prepare("DELETE FROM folders WHERE id = ? AND user_id = ?").bind(id, userId).run();
}
__name(deleteFolder, "deleteFolder");
async function clearFolderFromCiphers(db, userId, folderId) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  await db.prepare(
    `UPDATE ciphers
       SET folder_id = NULL, updated_at = ?,
           data = json_remove(data, '$.folderId', '$.folder_id', '$.updatedAt', '$.revisionDate')
       WHERE user_id = ? AND folder_id = ?`
  ).bind(now, userId, folderId).run();
}
__name(clearFolderFromCiphers, "clearFolderFromCiphers");
async function bulkDeleteFolders(db, userId, ids, sqlChunkSize, updateRevisionDate2) {
  const uniqueIds = Array.from(new Set(ids.map((id) => String(id || "").trim()).filter(Boolean)));
  if (!uniqueIds.length) return null;
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const chunkSize = sqlChunkSize(2);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(
      `UPDATE ciphers
         SET folder_id = NULL, updated_at = ?,
             data = json_remove(data, '$.folderId', '$.folder_id', '$.updatedAt', '$.revisionDate')
         WHERE user_id = ? AND folder_id IN (${placeholders})`
    ).bind(now, userId, ...chunk).run();
    await db.prepare(`DELETE FROM folders WHERE user_id = ? AND id IN (${placeholders})`).bind(userId, ...chunk).run();
  }
  return updateRevisionDate2(userId);
}
__name(bulkDeleteFolders, "bulkDeleteFolders");
async function getAllFolders(db, userId) {
  const res = await db.prepare("SELECT id, user_id, name, created_at, updated_at FROM folders WHERE user_id = ? ORDER BY updated_at DESC").bind(userId).all();
  return (res.results || []).map((row) => mapFolderRow(row));
}
__name(getAllFolders, "getAllFolders");
async function getFoldersPage(db, userId, limit, offset) {
  const res = await db.prepare(
    "SELECT id, user_id, name, created_at, updated_at FROM folders WHERE user_id = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?"
  ).bind(userId, limit, offset).all();
  return (res.results || []).map((row) => mapFolderRow(row));
}
__name(getFoldersPage, "getFoldersPage");

// src/services/storage-cipher-repo.ts
function normalizeOptionalId(value) {
  if (value == null) return null;
  const normalized = String(value).trim();
  return normalized ? normalized : null;
}
__name(normalizeOptionalId, "normalizeOptionalId");
var CIPHER_SCALAR_DATA_KEYS = /* @__PURE__ */ new Set([
  "id",
  "userId",
  "user_id",
  "type",
  "folderId",
  "folder_id",
  "name",
  "notes",
  "favorite",
  "reprompt",
  "key",
  "attachments",
  "Attachments",
  "attachments2",
  "Attachments2",
  "createdAt",
  "created_at",
  "creationDate",
  "updatedAt",
  "updated_at",
  "revisionDate",
  "archivedAt",
  "archived_at",
  "archivedDate",
  "deletedAt",
  "deleted_at",
  "deletedDate"
]);
function buildCipherData(cipher, folderId) {
  const payload = {
    ...cipher,
    folderId
  };
  for (const key of CIPHER_SCALAR_DATA_KEYS) {
    delete payload[key];
  }
  return JSON.stringify(payload);
}
__name(buildCipherData, "buildCipherData");
function parseCipherRow(row) {
  if (!row?.data) return null;
  try {
    const parsed = JSON.parse(row.data);
    const folderId = normalizeOptionalId(row.folder_id ?? parsed.folderId ?? null);
    return {
      ...parsed,
      id: row.id,
      userId: row.user_id,
      type: Number(row.type) || Number(parsed.type) || 1,
      folderId,
      name: row.name ?? parsed.name ?? null,
      notes: row.notes ?? parsed.notes ?? null,
      favorite: row.favorite != null ? !!row.favorite : !!parsed.favorite,
      reprompt: row.reprompt ?? parsed.reprompt ?? 0,
      key: row.key ?? parsed.key ?? null,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      archivedAt: row.archived_at ?? parsed.archivedAt ?? parsed.archivedDate ?? null,
      deletedAt: row.deleted_at ?? null
    };
  } catch {
    console.error("Corrupted cipher data, id:", row.id);
    return null;
  }
}
__name(parseCipherRow, "parseCipherRow");
function selectCipherColumns() {
  return "id, user_id, type, folder_id, name, notes, favorite, data, reprompt, key, created_at, updated_at, archived_at, deleted_at";
}
__name(selectCipherColumns, "selectCipherColumns");
async function getCipher(db, id) {
  const row = await db.prepare(`SELECT ${selectCipherColumns()} FROM ciphers WHERE id = ?`).bind(id).first();
  return parseCipherRow(row);
}
__name(getCipher, "getCipher");
async function saveCipher(db, safeBind, cipher) {
  const folderId = normalizeOptionalId(cipher.folderId);
  const data = buildCipherData(cipher, folderId);
  const stmt = db.prepare(
    "INSERT INTO ciphers(id, user_id, type, folder_id, name, notes, favorite, data, reprompt, key, created_at, updated_at, archived_at, deleted_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET user_id=excluded.user_id, type=excluded.type, folder_id=excluded.folder_id, name=excluded.name, notes=excluded.notes, favorite=excluded.favorite, data=excluded.data, reprompt=excluded.reprompt, key=excluded.key, updated_at=excluded.updated_at, archived_at=excluded.archived_at, deleted_at=excluded.deleted_at"
  );
  await safeBind(
    stmt,
    cipher.id,
    cipher.userId,
    Number(cipher.type) || 1,
    folderId,
    cipher.name,
    cipher.notes,
    cipher.favorite ? 1 : 0,
    data,
    cipher.reprompt ?? 0,
    cipher.key,
    cipher.createdAt,
    cipher.updatedAt,
    cipher.archivedAt ?? null,
    cipher.deletedAt
  ).run();
}
__name(saveCipher, "saveCipher");
function sanitizeIds(ids) {
  return Array.from(new Set(ids.map((id) => String(id || "").trim()).filter(Boolean)));
}
__name(sanitizeIds, "sanitizeIds");
async function deleteCipher(db, id, userId) {
  await db.prepare("DELETE FROM ciphers WHERE id = ? AND user_id = ?").bind(id, userId).run();
}
__name(deleteCipher, "deleteCipher");
async function bulkSoftDeleteCiphers(db, sqlChunkSize, updateRevisionDate2, ids, userId) {
  if (ids.length === 0) return null;
  const uniqueIds = sanitizeIds(ids);
  if (!uniqueIds.length) return null;
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const chunkSize = sqlChunkSize(3);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(
      `UPDATE ciphers
         SET deleted_at = ?, updated_at = ?,
             data = json_remove(data, '$.deletedAt', '$.deletedDate', '$.updatedAt', '$.revisionDate')
         WHERE user_id = ? AND id IN (${placeholders})`
    ).bind(now, now, userId, ...chunk).run();
  }
  return updateRevisionDate2(userId);
}
__name(bulkSoftDeleteCiphers, "bulkSoftDeleteCiphers");
async function bulkRestoreCiphers(db, sqlChunkSize, updateRevisionDate2, ids, userId) {
  if (ids.length === 0) return null;
  const uniqueIds = sanitizeIds(ids);
  if (!uniqueIds.length) return null;
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const chunkSize = sqlChunkSize(2);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(
      `UPDATE ciphers
         SET deleted_at = NULL, updated_at = ?,
             data = json_remove(data, '$.deletedAt', '$.deletedDate', '$.updatedAt', '$.revisionDate')
         WHERE user_id = ? AND id IN (${placeholders})`
    ).bind(now, userId, ...chunk).run();
  }
  return updateRevisionDate2(userId);
}
__name(bulkRestoreCiphers, "bulkRestoreCiphers");
async function bulkDeleteCiphers(db, sqlChunkSize, updateRevisionDate2, ids, userId) {
  if (ids.length === 0) return null;
  const uniqueIds = sanitizeIds(ids);
  if (!uniqueIds.length) return null;
  const chunkSize = sqlChunkSize(1);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(`DELETE FROM ciphers WHERE user_id = ? AND id IN (${placeholders})`).bind(userId, ...chunk).run();
  }
  return updateRevisionDate2(userId);
}
__name(bulkDeleteCiphers, "bulkDeleteCiphers");
async function getAllCiphers(db, userId) {
  const res = await db.prepare(`SELECT ${selectCipherColumns()} FROM ciphers WHERE user_id = ? ORDER BY updated_at DESC`).bind(userId).all();
  return (res.results || []).flatMap((row) => {
    const cipher = parseCipherRow(row);
    return cipher ? [cipher] : [];
  });
}
__name(getAllCiphers, "getAllCiphers");
async function getCiphersPage(db, userId, includeDeleted, limit, offset) {
  const whereDeleted = includeDeleted ? "" : "AND deleted_at IS NULL";
  const res = await db.prepare(
    `SELECT ${selectCipherColumns()} FROM ciphers
       WHERE user_id = ?
       ${whereDeleted}
       ORDER BY updated_at DESC
       LIMIT ? OFFSET ?`
  ).bind(userId, limit, offset).all();
  return (res.results || []).flatMap((row) => {
    const cipher = parseCipherRow(row);
    return cipher ? [cipher] : [];
  });
}
__name(getCiphersPage, "getCiphersPage");
async function getCiphersByIds(db, sqlChunkSize, ids, userId) {
  if (ids.length === 0) return [];
  const uniqueIds = sanitizeIds(ids);
  if (!uniqueIds.length) return [];
  const chunkSize = sqlChunkSize(1);
  const out = [];
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    const stmt = db.prepare(`SELECT ${selectCipherColumns()} FROM ciphers WHERE user_id = ? AND id IN (${placeholders})`);
    const res = await stmt.bind(userId, ...chunk).all();
    out.push(
      ...(res.results || []).flatMap((row) => {
        const cipher = parseCipherRow(row);
        return cipher ? [cipher] : [];
      })
    );
  }
  return out;
}
__name(getCiphersByIds, "getCiphersByIds");
async function bulkMoveCiphers(db, sqlChunkSize, updateRevisionDate2, ids, folderId, userId) {
  if (ids.length === 0) return null;
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const normalizedFolderId = normalizeOptionalId(folderId);
  const uniqueIds = sanitizeIds(ids);
  const chunkSize = sqlChunkSize(3);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(
      `UPDATE ciphers
         SET folder_id = ?, updated_at = ?,
             data = json_remove(data, '$.folderId', '$.folder_id', '$.updatedAt', '$.revisionDate')
         WHERE user_id = ? AND id IN (${placeholders})`
    ).bind(normalizedFolderId, now, userId, ...chunk).run();
  }
  return updateRevisionDate2(userId);
}
__name(bulkMoveCiphers, "bulkMoveCiphers");
async function bulkArchiveCiphers(db, sqlChunkSize, updateRevisionDate2, ids, userId) {
  if (ids.length === 0) return null;
  const uniqueIds = sanitizeIds(ids);
  if (!uniqueIds.length) return null;
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const chunkSize = sqlChunkSize(3);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(
      `UPDATE ciphers
         SET archived_at = ?, updated_at = ?,
             data = json_remove(data, '$.archivedAt', '$.archivedDate', '$.updatedAt', '$.revisionDate')
         WHERE user_id = ? AND id IN (${placeholders}) AND deleted_at IS NULL`
    ).bind(now, now, userId, ...chunk).run();
  }
  return updateRevisionDate2(userId);
}
__name(bulkArchiveCiphers, "bulkArchiveCiphers");
async function bulkUnarchiveCiphers(db, sqlChunkSize, updateRevisionDate2, ids, userId) {
  if (ids.length === 0) return null;
  const uniqueIds = sanitizeIds(ids);
  if (!uniqueIds.length) return null;
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const chunkSize = sqlChunkSize(2);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(
      `UPDATE ciphers
         SET archived_at = NULL, updated_at = ?,
             data = json_remove(data, '$.archivedAt', '$.archivedDate', '$.updatedAt', '$.revisionDate')
         WHERE user_id = ? AND id IN (${placeholders})`
    ).bind(now, userId, ...chunk).run();
  }
  return updateRevisionDate2(userId);
}
__name(bulkUnarchiveCiphers, "bulkUnarchiveCiphers");

// src/services/storage-attachment-repo.ts
async function getAttachment(db, id) {
  const row = await db.prepare("SELECT id, cipher_id, file_name, size, size_name, key FROM attachments WHERE id = ?").bind(id).first();
  if (!row) return null;
  return {
    id: row.id,
    cipherId: row.cipher_id,
    fileName: row.file_name,
    size: row.size,
    sizeName: row.size_name,
    key: row.key
  };
}
__name(getAttachment, "getAttachment");
async function saveAttachment(db, safeBind, attachment) {
  const stmt = db.prepare(
    "INSERT INTO attachments(id, cipher_id, file_name, size, size_name, key) VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET cipher_id=excluded.cipher_id, file_name=excluded.file_name, size=excluded.size, size_name=excluded.size_name, key=excluded.key"
  );
  await safeBind(stmt, attachment.id, attachment.cipherId, attachment.fileName, attachment.size, attachment.sizeName, attachment.key).run();
}
__name(saveAttachment, "saveAttachment");
async function deleteAttachment(db, id) {
  await db.prepare("DELETE FROM attachments WHERE id = ?").bind(id).run();
}
__name(deleteAttachment, "deleteAttachment");
async function bulkDeleteAttachmentsByIds(db, sqlChunkSize, attachmentIds) {
  const uniqueIds = [...new Set(attachmentIds.map((id) => String(id || "").trim()).filter(Boolean))];
  if (!uniqueIds.length) return;
  const chunkSize = sqlChunkSize(0);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(`DELETE FROM attachments WHERE id IN (${placeholders})`).bind(...chunk).run();
  }
}
__name(bulkDeleteAttachmentsByIds, "bulkDeleteAttachmentsByIds");
async function getAttachmentsByCipher(db, cipherId) {
  const res = await db.prepare("SELECT id, cipher_id, file_name, size, size_name, key FROM attachments WHERE cipher_id = ?").bind(cipherId).all();
  return (res.results || []).map((r) => ({
    id: r.id,
    cipherId: r.cipher_id,
    fileName: r.file_name,
    size: r.size,
    sizeName: r.size_name,
    key: r.key
  }));
}
__name(getAttachmentsByCipher, "getAttachmentsByCipher");
async function getAttachmentsByCipherIds(db, sqlChunkSize, cipherIds) {
  const grouped = /* @__PURE__ */ new Map();
  if (cipherIds.length === 0) return grouped;
  const uniqueCipherIds = [...new Set(cipherIds)];
  const chunkSize = sqlChunkSize(0);
  for (let i = 0; i < uniqueCipherIds.length; i += chunkSize) {
    const chunk = uniqueCipherIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    const res = await db.prepare(`SELECT id, cipher_id, file_name, size, size_name, key FROM attachments WHERE cipher_id IN (${placeholders})`).bind(...chunk).all();
    for (const row of res.results || []) {
      const item = {
        id: row.id,
        cipherId: row.cipher_id,
        fileName: row.file_name,
        size: row.size,
        sizeName: row.size_name,
        key: row.key
      };
      const list = grouped.get(item.cipherId);
      if (list) list.push(item);
      else grouped.set(item.cipherId, [item]);
    }
  }
  return grouped;
}
__name(getAttachmentsByCipherIds, "getAttachmentsByCipherIds");
async function getAttachmentsByUserId(db, userId) {
  const grouped = /* @__PURE__ */ new Map();
  const res = await db.prepare(
    `SELECT a.id, a.cipher_id, a.file_name, a.size, a.size_name, a.key
       FROM attachments a
       INNER JOIN ciphers c ON c.id = a.cipher_id
       WHERE c.user_id = ?`
  ).bind(userId).all();
  for (const row of res.results || []) {
    const item = {
      id: row.id,
      cipherId: row.cipher_id,
      fileName: row.file_name,
      size: row.size,
      sizeName: row.size_name,
      key: row.key
    };
    const list = grouped.get(item.cipherId);
    if (list) list.push(item);
    else grouped.set(item.cipherId, [item]);
  }
  return grouped;
}
__name(getAttachmentsByUserId, "getAttachmentsByUserId");
async function addAttachmentToCipher(db, cipherId, attachmentId) {
  await db.prepare("UPDATE attachments SET cipher_id = ? WHERE id = ?").bind(cipherId, attachmentId).run();
}
__name(addAttachmentToCipher, "addAttachmentToCipher");
async function deleteAllAttachmentsByCipher(db, cipherId) {
  await db.prepare("DELETE FROM attachments WHERE cipher_id = ?").bind(cipherId).run();
}
__name(deleteAllAttachmentsByCipher, "deleteAllAttachmentsByCipher");
async function updateCipherRevisionDate(getCipherById, saveCipherRecord, updateRevisionDate2, cipherId) {
  const cipher = await getCipherById(cipherId);
  if (!cipher) return null;
  cipher.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await saveCipherRecord(cipher);
  const revisionDate = await updateRevisionDate2(cipher.userId);
  return { userId: cipher.userId, revisionDate };
}
__name(updateCipherRevisionDate, "updateCipherRevisionDate");

// src/services/storage-send-repo.ts
function mapSendRow(row) {
  return {
    id: row.id,
    userId: row.user_id,
    type: row.type,
    name: row.name,
    notes: row.notes,
    data: row.data,
    key: row.key,
    passwordHash: row.password_hash,
    passwordSalt: row.password_salt,
    passwordIterations: row.password_iterations,
    authType: row.auth_type ?? 0,
    emails: row.emails ?? null,
    maxAccessCount: row.max_access_count,
    accessCount: row.access_count,
    disabled: !!row.disabled,
    hideEmail: row.hide_email === null || row.hide_email === void 0 ? null : !!row.hide_email,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    expirationDate: row.expiration_date,
    deletionDate: row.deletion_date
  };
}
__name(mapSendRow, "mapSendRow");
async function getSend(db, id) {
  const row = await db.prepare(
    "SELECT id, user_id, type, name, notes, data, key, password_hash, password_salt, password_iterations, auth_type, emails, max_access_count, access_count, disabled, hide_email, created_at, updated_at, expiration_date, deletion_date FROM sends WHERE id = ?"
  ).bind(id).first();
  if (!row) return null;
  return mapSendRow(row);
}
__name(getSend, "getSend");
async function saveSend(db, safeBind, send) {
  const stmt = db.prepare(
    "INSERT INTO sends(id, user_id, type, name, notes, data, key, password_hash, password_salt, password_iterations, auth_type, emails, max_access_count, access_count, disabled, hide_email, created_at, updated_at, expiration_date, deletion_date) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET user_id=excluded.user_id, type=excluded.type, name=excluded.name, notes=excluded.notes, data=excluded.data, key=excluded.key, password_hash=excluded.password_hash, password_salt=excluded.password_salt, password_iterations=excluded.password_iterations, auth_type=excluded.auth_type, emails=excluded.emails, max_access_count=excluded.max_access_count, access_count=excluded.access_count, disabled=excluded.disabled, hide_email=excluded.hide_email, updated_at=excluded.updated_at, expiration_date=excluded.expiration_date, deletion_date=excluded.deletion_date"
  );
  await safeBind(
    stmt,
    send.id,
    send.userId,
    Number(send.type) || 0,
    send.name,
    send.notes,
    send.data,
    send.key,
    send.passwordHash,
    send.passwordSalt,
    send.passwordIterations,
    send.authType,
    send.emails,
    send.maxAccessCount,
    send.accessCount,
    send.disabled ? 1 : 0,
    send.hideEmail === null || send.hideEmail === void 0 ? null : send.hideEmail ? 1 : 0,
    send.createdAt,
    send.updatedAt,
    send.expirationDate,
    send.deletionDate
  ).run();
}
__name(saveSend, "saveSend");
async function incrementSendAccessCount(db, sendId) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const result = await db.prepare(
    "UPDATE sends SET access_count = access_count + 1, updated_at = ? WHERE id = ? AND (max_access_count IS NULL OR access_count < max_access_count)"
  ).bind(now, sendId).run();
  return (result.meta.changes ?? 0) > 0;
}
__name(incrementSendAccessCount, "incrementSendAccessCount");
async function deleteSend(db, id, userId) {
  await db.prepare("DELETE FROM sends WHERE id = ? AND user_id = ?").bind(id, userId).run();
}
__name(deleteSend, "deleteSend");
async function getSendsByIds(db, sqlChunkSize, ids, userId) {
  if (ids.length === 0) return [];
  const uniqueIds = Array.from(new Set(ids.map((id) => String(id || "").trim()).filter(Boolean)));
  if (!uniqueIds.length) return [];
  const chunkSize = sqlChunkSize(1);
  const out = [];
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    const res = await db.prepare(
      `SELECT id, user_id, type, name, notes, data, key, password_hash, password_salt, password_iterations, auth_type, emails, max_access_count, access_count, disabled, hide_email, created_at, updated_at, expiration_date, deletion_date
         FROM sends
         WHERE user_id = ? AND id IN (${placeholders})`
    ).bind(userId, ...chunk).all();
    out.push(...(res.results || []).map((row) => mapSendRow(row)));
  }
  return out;
}
__name(getSendsByIds, "getSendsByIds");
async function bulkDeleteSends(db, sqlChunkSize, updateRevisionDate2, ids, userId) {
  if (ids.length === 0) return null;
  const uniqueIds = Array.from(new Set(ids.map((id) => String(id || "").trim()).filter(Boolean)));
  if (!uniqueIds.length) return null;
  const chunkSize = sqlChunkSize(1);
  for (let i = 0; i < uniqueIds.length; i += chunkSize) {
    const chunk = uniqueIds.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(",");
    await db.prepare(`DELETE FROM sends WHERE user_id = ? AND id IN (${placeholders})`).bind(userId, ...chunk).run();
  }
  return updateRevisionDate2(userId);
}
__name(bulkDeleteSends, "bulkDeleteSends");
async function getAllSends(db, userId) {
  const res = await db.prepare(
    "SELECT id, user_id, type, name, notes, data, key, password_hash, password_salt, password_iterations, auth_type, emails, max_access_count, access_count, disabled, hide_email, created_at, updated_at, expiration_date, deletion_date FROM sends WHERE user_id = ? ORDER BY updated_at DESC"
  ).bind(userId).all();
  return (res.results || []).map((row) => mapSendRow(row));
}
__name(getAllSends, "getAllSends");
async function getSendsPage(db, userId, limit, offset) {
  const res = await db.prepare(
    "SELECT id, user_id, type, name, notes, data, key, password_hash, password_salt, password_iterations, auth_type, emails, max_access_count, access_count, disabled, hide_email, created_at, updated_at, expiration_date, deletion_date FROM sends WHERE user_id = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?"
  ).bind(userId, limit, offset).all();
  return (res.results || []).map((row) => mapSendRow(row));
}
__name(getSendsPage, "getSendsPage");

// src/services/storage-refresh-token-repo.ts
async function saveRefreshToken(db, refreshTokenKey, maybeCleanupExpiredRefreshTokens, token, userId, expiresAtMs, deviceIdentifier, deviceSessionStamp) {
  await maybeCleanupExpiredRefreshTokens(Date.now());
  const tokenKey = await refreshTokenKey(token);
  await db.prepare(
    "INSERT INTO refresh_tokens(token, user_id, expires_at, device_identifier, device_session_stamp) VALUES(?, ?, ?, ?, ?) ON CONFLICT(token) DO UPDATE SET user_id=excluded.user_id, expires_at=excluded.expires_at, device_identifier=excluded.device_identifier, device_session_stamp=excluded.device_session_stamp"
  ).bind(tokenKey, userId, expiresAtMs, deviceIdentifier ?? null, deviceSessionStamp ?? null).run();
}
__name(saveRefreshToken, "saveRefreshToken");
async function getRefreshTokenRecord(db, refreshTokenKey, maybeCleanupExpiredRefreshTokens, saveRefreshTokenRecord, deleteRefreshTokenRecord, token) {
  const now = Date.now();
  await maybeCleanupExpiredRefreshTokens(now);
  const tokenKey = await refreshTokenKey(token);
  let row = await db.prepare("SELECT user_id, expires_at, device_identifier, device_session_stamp FROM refresh_tokens WHERE token = ?").bind(tokenKey).first();
  if (!row) {
    const legacyRow = await db.prepare("SELECT user_id, expires_at, device_identifier, device_session_stamp FROM refresh_tokens WHERE token = ?").bind(token).first();
    if (legacyRow) {
      if (legacyRow.expires_at && legacyRow.expires_at < now) {
        await deleteRefreshTokenRecord(token);
        return null;
      }
      await saveRefreshTokenRecord(
        token,
        legacyRow.user_id,
        legacyRow.expires_at,
        legacyRow.device_identifier ?? null,
        legacyRow.device_session_stamp ?? null
      );
      await db.prepare("DELETE FROM refresh_tokens WHERE token = ?").bind(token).run();
      return {
        userId: legacyRow.user_id,
        expiresAt: legacyRow.expires_at,
        deviceIdentifier: legacyRow.device_identifier ?? null,
        deviceSessionStamp: legacyRow.device_session_stamp ?? null
      };
    }
  }
  if (!row) return null;
  if (row.expires_at && row.expires_at < now) {
    await deleteRefreshTokenRecord(token);
    return null;
  }
  return {
    userId: row.user_id,
    expiresAt: row.expires_at,
    deviceIdentifier: row.device_identifier ?? null,
    deviceSessionStamp: row.device_session_stamp ?? null
  };
}
__name(getRefreshTokenRecord, "getRefreshTokenRecord");
async function deleteRefreshToken(db, refreshTokenKey, token) {
  const tokenKey = await refreshTokenKey(token);
  await db.prepare("DELETE FROM refresh_tokens WHERE token = ?").bind(token).run();
  await db.prepare("DELETE FROM refresh_tokens WHERE token = ?").bind(tokenKey).run();
}
__name(deleteRefreshToken, "deleteRefreshToken");
async function deleteRefreshTokensByUserId(db, userId) {
  const result = await db.prepare("DELETE FROM refresh_tokens WHERE user_id = ?").bind(userId).run();
  return Number(result.meta.changes ?? 0);
}
__name(deleteRefreshTokensByUserId, "deleteRefreshTokensByUserId");
async function deleteRefreshTokensByDevice(db, userId, deviceIdentifier) {
  const result = await db.prepare("DELETE FROM refresh_tokens WHERE user_id = ? AND device_identifier = ?").bind(userId, deviceIdentifier).run();
  return Number(result.meta.changes ?? 0);
}
__name(deleteRefreshTokensByDevice, "deleteRefreshTokensByDevice");
async function constrainRefreshTokenExpiry(db, refreshTokenKey, token, maxExpiresAtMs) {
  const tokenKey = await refreshTokenKey(token);
  await db.prepare(
    "UPDATE refresh_tokens SET expires_at = CASE WHEN expires_at > ? THEN ? ELSE expires_at END WHERE token = ?"
  ).bind(maxExpiresAtMs, maxExpiresAtMs, tokenKey).run();
  await db.prepare(
    "UPDATE refresh_tokens SET expires_at = CASE WHEN expires_at > ? THEN ? ELSE expires_at END WHERE token = ?"
  ).bind(maxExpiresAtMs, maxExpiresAtMs, token).run();
}
__name(constrainRefreshTokenExpiry, "constrainRefreshTokenExpiry");

// src/services/storage-device-repo.ts
function mapDeviceRow(row) {
  return {
    userId: row.user_id,
    deviceIdentifier: row.device_identifier,
    name: row.name,
    deviceNote: row.device_note ?? null,
    type: row.type,
    sessionStamp: row.session_stamp || "",
    encryptedUserKey: row.encrypted_user_key ?? null,
    encryptedPublicKey: row.encrypted_public_key ?? null,
    encryptedPrivateKey: row.encrypted_private_key ?? null,
    lastSeenAt: row.last_seen_at ?? null,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}
__name(mapDeviceRow, "mapDeviceRow");
async function upsertDevice(db, getDeviceById, userId, deviceIdentifier, name, type, sessionStamp, keys) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const existingDevice = await getDeviceById(userId, deviceIdentifier);
  const effectiveSessionStamp = String(sessionStamp || "").trim() || existingDevice?.sessionStamp || "";
  const effectiveName = String(name || "").trim() || String(existingDevice?.name || "").trim();
  await db.prepare(
    "INSERT INTO devices(user_id, device_identifier, name, type, session_stamp, encrypted_user_key, encrypted_public_key, encrypted_private_key, banned, banned_at, device_note, last_seen_at, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, ?) ON CONFLICT(user_id, device_identifier) DO UPDATE SET name=excluded.name, type=excluded.type, session_stamp=excluded.session_stamp, encrypted_user_key=COALESCE(excluded.encrypted_user_key, encrypted_user_key), encrypted_public_key=COALESCE(excluded.encrypted_public_key, encrypted_public_key), encrypted_private_key=COALESCE(excluded.encrypted_private_key, encrypted_private_key), last_seen_at=excluded.last_seen_at, updated_at=excluded.updated_at"
  ).bind(
    userId,
    deviceIdentifier,
    effectiveName,
    type,
    effectiveSessionStamp,
    keys?.encryptedUserKey ?? null,
    keys?.encryptedPublicKey ?? null,
    keys?.encryptedPrivateKey ?? null,
    existingDevice?.deviceNote ?? null,
    now,
    now,
    now
  ).run();
}
__name(upsertDevice, "upsertDevice");
async function updateDeviceName(db, userId, deviceIdentifier, name) {
  const result = await db.prepare("UPDATE devices SET device_note = ? WHERE user_id = ? AND device_identifier = ?").bind(String(name || "").trim(), userId, deviceIdentifier).run();
  return Number(result.meta.changes ?? 0) > 0;
}
__name(updateDeviceName, "updateDeviceName");
async function touchDeviceLastSeen(db, userId, deviceIdentifier) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const result = await db.prepare("UPDATE devices SET last_seen_at = ? WHERE user_id = ? AND device_identifier = ?").bind(now, userId, deviceIdentifier).run();
  return Number(result.meta.changes ?? 0) > 0;
}
__name(touchDeviceLastSeen, "touchDeviceLastSeen");
async function updateDeviceKeys(db, userId, deviceIdentifier, keys) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const result = await db.prepare(
    "UPDATE devices SET encrypted_user_key = ?, encrypted_public_key = ?, encrypted_private_key = ?, updated_at = ? WHERE user_id = ? AND device_identifier = ?"
  ).bind(
    keys.encryptedUserKey ?? null,
    keys.encryptedPublicKey ?? null,
    keys.encryptedPrivateKey ?? null,
    now,
    userId,
    deviceIdentifier
  ).run();
  return Number(result.meta.changes ?? 0) > 0;
}
__name(updateDeviceKeys, "updateDeviceKeys");
async function clearDeviceKeys(db, userId, deviceIdentifiers) {
  const uniqueIds = Array.from(
    new Set(deviceIdentifiers.map((id) => String(id || "").trim()).filter(Boolean))
  );
  if (!uniqueIds.length) return 0;
  const placeholders = uniqueIds.map(() => "?").join(",");
  const result = await db.prepare(
    `UPDATE devices
       SET encrypted_user_key = NULL,
           encrypted_public_key = NULL,
           encrypted_private_key = NULL,
           updated_at = ?
       WHERE user_id = ? AND device_identifier IN (${placeholders})`
  ).bind((/* @__PURE__ */ new Date()).toISOString(), userId, ...uniqueIds).run();
  return Number(result.meta.changes ?? 0);
}
__name(clearDeviceKeys, "clearDeviceKeys");
async function isKnownDevice(db, userId, deviceIdentifier) {
  const row = await db.prepare("SELECT 1 FROM devices WHERE user_id = ? AND device_identifier = ? LIMIT 1").bind(userId, deviceIdentifier).first();
  return !!row;
}
__name(isKnownDevice, "isKnownDevice");
async function isKnownDeviceByEmail(getUserByEmail, isKnownDeviceForUser, email, deviceIdentifier) {
  const user = await getUserByEmail(email);
  if (!user) return false;
  return isKnownDeviceForUser(user.id, deviceIdentifier);
}
__name(isKnownDeviceByEmail, "isKnownDeviceByEmail");
async function getDevicesByUserId(db, userId) {
  const res = await db.prepare(
    "SELECT user_id, device_identifier, name, type, session_stamp, encrypted_user_key, encrypted_public_key, encrypted_private_key, banned, banned_at, device_note, last_seen_at, created_at, updated_at FROM devices WHERE user_id = ? ORDER BY COALESCE(last_seen_at, created_at) DESC, updated_at DESC"
  ).bind(userId).all();
  return (res.results || []).map(mapDeviceRow);
}
__name(getDevicesByUserId, "getDevicesByUserId");
async function getDevice(db, userId, deviceIdentifier) {
  const row = await db.prepare(
    "SELECT user_id, device_identifier, name, type, session_stamp, encrypted_user_key, encrypted_public_key, encrypted_private_key, banned, banned_at, device_note, last_seen_at, created_at, updated_at FROM devices WHERE user_id = ? AND device_identifier = ? LIMIT 1"
  ).bind(userId, deviceIdentifier).first();
  return row ? mapDeviceRow(row) : null;
}
__name(getDevice, "getDevice");
async function deleteDevice(db, userId, deviceIdentifier) {
  const result = await db.prepare("DELETE FROM devices WHERE user_id = ? AND device_identifier = ?").bind(userId, deviceIdentifier).run();
  return Number(result.meta.changes ?? 0) > 0;
}
__name(deleteDevice, "deleteDevice");
async function deleteDevicesByUserId(db, userId) {
  const result = await db.prepare("DELETE FROM devices WHERE user_id = ?").bind(userId).run();
  return Number(result.meta.changes ?? 0);
}
__name(deleteDevicesByUserId, "deleteDevicesByUserId");
async function getTrustedDeviceTokenSummariesByUserId(db, userId) {
  const now = Date.now();
  await db.prepare("DELETE FROM trusted_two_factor_device_tokens WHERE expires_at < ?").bind(now).run();
  const res = await db.prepare(
    "SELECT device_identifier, MAX(expires_at) AS expires_at, COUNT(*) AS token_count FROM trusted_two_factor_device_tokens WHERE user_id = ? GROUP BY device_identifier ORDER BY expires_at DESC"
  ).bind(userId).all();
  return (res.results || []).map((row) => ({
    deviceIdentifier: row.device_identifier,
    expiresAt: Number(row.expires_at || 0),
    tokenCount: Number(row.token_count || 0)
  }));
}
__name(getTrustedDeviceTokenSummariesByUserId, "getTrustedDeviceTokenSummariesByUserId");
async function deleteTrustedTwoFactorTokensByDevice(db, userId, deviceIdentifier) {
  const result = await db.prepare("DELETE FROM trusted_two_factor_device_tokens WHERE user_id = ? AND device_identifier = ?").bind(userId, deviceIdentifier).run();
  return Number(result.meta.changes ?? 0);
}
__name(deleteTrustedTwoFactorTokensByDevice, "deleteTrustedTwoFactorTokensByDevice");
async function deleteTrustedTwoFactorTokensByUserId(db, userId) {
  const result = await db.prepare("DELETE FROM trusted_two_factor_device_tokens WHERE user_id = ?").bind(userId).run();
  return Number(result.meta.changes ?? 0);
}
__name(deleteTrustedTwoFactorTokensByUserId, "deleteTrustedTwoFactorTokensByUserId");
async function updateTrustedTwoFactorTokensExpiryByDevice(db, userId, deviceIdentifier, expiresAtMs) {
  const now = Date.now();
  await db.prepare("DELETE FROM trusted_two_factor_device_tokens WHERE expires_at < ?").bind(now).run();
  const result = await db.prepare("UPDATE trusted_two_factor_device_tokens SET expires_at = ? WHERE user_id = ? AND device_identifier = ? AND expires_at >= ?").bind(expiresAtMs, userId, deviceIdentifier, now).run();
  return Number(result.meta.changes ?? 0);
}
__name(updateTrustedTwoFactorTokensExpiryByDevice, "updateTrustedTwoFactorTokensExpiryByDevice");
async function saveTrustedTwoFactorDeviceToken(db, trustedTokenKey, token, userId, deviceIdentifier, expiresAtMs) {
  const tokenKey = await trustedTokenKey(token);
  await db.prepare("DELETE FROM trusted_two_factor_device_tokens WHERE expires_at < ?").bind(Date.now()).run();
  await db.prepare(
    "INSERT INTO trusted_two_factor_device_tokens(token, user_id, device_identifier, expires_at) VALUES(?, ?, ?, ?) ON CONFLICT(token) DO UPDATE SET user_id=excluded.user_id, device_identifier=excluded.device_identifier, expires_at=excluded.expires_at"
  ).bind(tokenKey, userId, deviceIdentifier, expiresAtMs).run();
}
__name(saveTrustedTwoFactorDeviceToken, "saveTrustedTwoFactorDeviceToken");
async function getTrustedTwoFactorDeviceTokenUserId(db, trustedTokenKey, token, deviceIdentifier) {
  const now = Date.now();
  const tokenKey = await trustedTokenKey(token);
  const row = await db.prepare("SELECT user_id, expires_at FROM trusted_two_factor_device_tokens WHERE token = ? AND device_identifier = ?").bind(tokenKey, deviceIdentifier).first();
  if (!row) return null;
  if (row.expires_at && row.expires_at < now) {
    await db.prepare("DELETE FROM trusted_two_factor_device_tokens WHERE token = ?").bind(tokenKey).run();
    return null;
  }
  return row.user_id;
}
__name(getTrustedTwoFactorDeviceTokenUserId, "getTrustedTwoFactorDeviceTokenUserId");

// src/services/storage-attachment-token-repo.ts
async function ensureUsedAttachmentDownloadTokenTable(db) {
  await db.prepare(
    "CREATE TABLE IF NOT EXISTS used_attachment_download_tokens (jti TEXT PRIMARY KEY, expires_at INTEGER NOT NULL)"
  ).run();
}
__name(ensureUsedAttachmentDownloadTokenTable, "ensureUsedAttachmentDownloadTokenTable");
async function consumeAttachmentDownloadToken(db, shouldRunPeriodicCleanup, lastCleanupAt, cleanupIntervalMs, jti, expUnixSeconds) {
  const nowMs = Date.now();
  let cleanedUpAt = null;
  if (shouldRunPeriodicCleanup(lastCleanupAt, cleanupIntervalMs)) {
    await db.prepare("DELETE FROM used_attachment_download_tokens WHERE expires_at < ?").bind(nowMs).run();
    cleanedUpAt = nowMs;
  }
  const expiresAtMs = expUnixSeconds * 1e3;
  const result = await db.prepare(
    "INSERT INTO used_attachment_download_tokens(jti, expires_at) VALUES(?, ?) ON CONFLICT(jti) DO NOTHING"
  ).bind(jti, expiresAtMs).run();
  return {
    consumed: (result.meta.changes ?? 0) > 0,
    cleanedUpAt
  };
}
__name(consumeAttachmentDownloadToken, "consumeAttachmentDownloadToken");

// src/services/storage-revision-repo.ts
async function getRevisionDate(db, userId) {
  const row = await db.prepare("SELECT revision_date FROM user_revisions WHERE user_id = ?").bind(userId).first();
  if (row?.revision_date) return row.revision_date;
  const date = (/* @__PURE__ */ new Date()).toISOString();
  await db.prepare(
    "INSERT INTO user_revisions(user_id, revision_date) VALUES(?, ?) ON CONFLICT(user_id) DO NOTHING"
  ).bind(userId, date).run();
  return date;
}
__name(getRevisionDate, "getRevisionDate");
async function updateRevisionDate(db, userId) {
  const date = (/* @__PURE__ */ new Date()).toISOString();
  await db.prepare(
    "INSERT INTO user_revisions(user_id, revision_date) VALUES(?, ?) ON CONFLICT(user_id) DO UPDATE SET revision_date = excluded.revision_date"
  ).bind(userId, date).run();
  return date;
}
__name(updateRevisionDate, "updateRevisionDate");

// src/static/global_domains.bitwarden.json
var global_domains_bitwarden_default = [
  { type: 2, domains: ["ameritrade.com", "tdameritrade.com"], excluded: false },
  { type: 3, domains: ["bankofamerica.com", "bofa.com", "mbna.com", "usecfo.com"], excluded: false },
  { type: 4, domains: ["sprint.com", "sprintpcs.com", "nextel.com"], excluded: false },
  { type: 0, domains: ["youtube.com", "google.com", "gmail.com"], excluded: false },
  { type: 1, domains: ["apple.com", "icloud.com"], excluded: false },
  { type: 5, domains: ["wellsfargo.com", "wf.com", "wellsfargoadvisors.com"], excluded: false },
  { type: 6, domains: ["mymerrill.com", "ml.com", "merrilledge.com"], excluded: false },
  { type: 7, domains: ["accountonline.com", "citi.com", "citibank.com", "citicards.com", "citibankonline.com"], excluded: false },
  { type: 8, domains: ["cnet.com", "cnettv.com", "com.com", "download.com", "news.com", "search.com", "upload.com"], excluded: false },
  { type: 9, domains: ["bananarepublic.com", "gap.com", "oldnavy.com", "piperlime.com"], excluded: false },
  { type: 10, domains: ["bing.com", "hotmail.com", "live.com", "microsoft.com", "msn.com", "passport.net", "windows.com", "microsoftonline.com", "office.com", "office365.com", "microsoftstore.com", "xbox.com", "azure.com", "windowsazure.com", "cloud.microsoft"], excluded: false },
  { type: 11, domains: ["ua2go.com", "ual.com", "united.com", "unitedwifi.com"], excluded: false },
  { type: 12, domains: ["overture.com", "yahoo.com"], excluded: false },
  { type: 13, domains: ["zonealarm.com", "zonelabs.com"], excluded: false },
  { type: 14, domains: ["paypal.com", "paypal-search.com"], excluded: false },
  { type: 15, domains: ["avon.com", "youravon.com"], excluded: false },
  { type: 16, domains: ["diapers.com", "soap.com", "wag.com", "yoyo.com", "beautybar.com", "casa.com", "afterschool.com", "vine.com", "bookworm.com", "look.com", "vinemarket.com"], excluded: false },
  { type: 17, domains: ["1800contacts.com", "800contacts.com"], excluded: false },
  { type: 18, domains: ["amazon.com", "amazon.com.be", "amazon.ae", "amazon.ca", "amazon.co.uk", "amazon.com.au", "amazon.com.br", "amazon.com.mx", "amazon.com.tr", "amazon.de", "amazon.es", "amazon.fr", "amazon.in", "amazon.it", "amazon.nl", "amazon.pl", "amazon.sa", "amazon.se", "amazon.sg"], excluded: false },
  { type: 19, domains: ["cox.com", "cox.net", "coxbusiness.com"], excluded: false },
  { type: 20, domains: ["mynortonaccount.com", "norton.com"], excluded: false },
  { type: 21, domains: ["verizon.com", "verizon.net"], excluded: false },
  { type: 22, domains: ["rakuten.com", "buy.com"], excluded: false },
  { type: 23, domains: ["siriusxm.com", "sirius.com"], excluded: false },
  { type: 24, domains: ["ea.com", "origin.com", "play4free.com", "tiberiumalliance.com"], excluded: false },
  { type: 25, domains: ["37signals.com", "basecamp.com", "basecamphq.com", "highrisehq.com"], excluded: false },
  { type: 26, domains: ["steampowered.com", "steamcommunity.com", "steamgames.com"], excluded: false },
  { type: 27, domains: ["chart.io", "chartio.com"], excluded: false },
  { type: 28, domains: ["gotomeeting.com", "citrixonline.com"], excluded: false },
  { type: 29, domains: ["gogoair.com", "gogoinflight.com"], excluded: false },
  { type: 30, domains: ["mysql.com", "oracle.com"], excluded: false },
  { type: 31, domains: ["discover.com", "discovercard.com"], excluded: false },
  { type: 32, domains: ["dcu.org", "dcu-online.org"], excluded: false },
  { type: 33, domains: ["healthcare.gov", "cuidadodesalud.gov", "cms.gov"], excluded: false },
  { type: 34, domains: ["pepco.com", "pepcoholdings.com"], excluded: false },
  { type: 35, domains: ["century21.com", "21online.com"], excluded: false },
  { type: 36, domains: ["comcast.com", "comcast.net", "xfinity.com"], excluded: false },
  { type: 37, domains: ["cricketwireless.com", "aiowireless.com"], excluded: false },
  { type: 38, domains: ["mandtbank.com", "mtb.com"], excluded: false },
  { type: 39, domains: ["dropbox.com", "getdropbox.com"], excluded: false },
  { type: 40, domains: ["snapfish.com", "snapfish.ca"], excluded: false },
  { type: 41, domains: ["alibaba.com", "aliexpress.com", "aliyun.com", "net.cn"], excluded: false },
  { type: 42, domains: ["playstation.com", "sonyentertainmentnetwork.com"], excluded: false },
  { type: 43, domains: ["mercadolivre.com", "mercadolivre.com.br", "mercadolibre.com", "mercadolibre.com.ar", "mercadolibre.com.mx"], excluded: false },
  { type: 44, domains: ["zendesk.com", "zopim.com"], excluded: false },
  { type: 45, domains: ["autodesk.com", "tinkercad.com"], excluded: false },
  { type: 46, domains: ["railnation.ru", "railnation.de", "rail-nation.com", "railnation.gr", "railnation.us", "trucknation.de", "traviangames.com"], excluded: false },
  { type: 47, domains: ["wpcu.coop", "wpcuonline.com"], excluded: false },
  { type: 48, domains: ["mathletics.com", "mathletics.com.au", "mathletics.co.uk"], excluded: false },
  { type: 49, domains: ["discountbank.co.il", "telebank.co.il"], excluded: false },
  { type: 50, domains: ["mi.com", "xiaomi.com"], excluded: false },
  { type: 52, domains: ["postepay.it", "poste.it"], excluded: false },
  { type: 51, domains: ["facebook.com", "messenger.com"], excluded: false },
  { type: 53, domains: ["skysports.com", "skybet.com", "skyvegas.com"], excluded: false },
  { type: 54, domains: ["disneymoviesanywhere.com", "go.com", "disney.com", "dadt.com", "disneyplus.com"], excluded: false },
  { type: 55, domains: ["pokemon-gl.com", "pokemon.com"], excluded: false },
  { type: 56, domains: ["myuv.com", "uvvu.com"], excluded: false },
  { type: 58, domains: ["mdsol.com", "imedidata.com"], excluded: false },
  { type: 57, domains: ["bank-yahav.co.il", "bankhapoalim.co.il"], excluded: false },
  { type: 59, domains: ["sears.com", "shld.net"], excluded: false },
  { type: 60, domains: ["xiami.com", "alipay.com"], excluded: false },
  { type: 61, domains: ["belkin.com", "seedonk.com"], excluded: false },
  { type: 62, domains: ["turbotax.com", "intuit.com"], excluded: false },
  { type: 63, domains: ["shopify.com", "myshopify.com"], excluded: false },
  { type: 64, domains: ["ebay.com", "ebay.at", "ebay.be", "ebay.ca", "ebay.ch", "ebay.cn", "ebay.co.jp", "ebay.co.th", "ebay.co.uk", "ebay.com.au", "ebay.com.hk", "ebay.com.my", "ebay.com.sg", "ebay.com.tw", "ebay.de", "ebay.es", "ebay.fr", "ebay.ie", "ebay.in", "ebay.it", "ebay.nl", "ebay.ph", "ebay.pl"], excluded: false },
  { type: 65, domains: ["techdata.com", "techdata.ch"], excluded: false },
  { type: 66, domains: ["schwab.com", "schwabplan.com"], excluded: false },
  { type: 68, domains: ["tesla.com", "teslamotors.com"], excluded: false },
  { type: 69, domains: ["morganstanley.com", "morganstanleyclientserv.com", "stockplanconnect.com", "ms.com"], excluded: false },
  { type: 70, domains: ["taxact.com", "taxactonline.com"], excluded: false },
  { type: 71, domains: ["mediawiki.org", "wikibooks.org", "wikidata.org", "wikimedia.org", "wikinews.org", "wikipedia.org", "wikiquote.org", "wikisource.org", "wikiversity.org", "wikivoyage.org", "wiktionary.org"], excluded: false },
  { type: 72, domains: ["airbnb.at", "airbnb.be", "airbnb.ca", "airbnb.ch", "airbnb.cl", "airbnb.co.cr", "airbnb.co.id", "airbnb.co.in", "airbnb.co.kr", "airbnb.co.nz", "airbnb.co.uk", "airbnb.co.ve", "airbnb.com", "airbnb.com.ar", "airbnb.com.au", "airbnb.com.bo", "airbnb.com.br", "airbnb.com.bz", "airbnb.com.co", "airbnb.com.ec", "airbnb.com.gt", "airbnb.com.hk", "airbnb.com.hn", "airbnb.com.mt", "airbnb.com.my", "airbnb.com.ni", "airbnb.com.pa", "airbnb.com.pe", "airbnb.com.py", "airbnb.com.sg", "airbnb.com.sv", "airbnb.com.tr", "airbnb.com.tw", "airbnb.cz", "airbnb.de", "airbnb.dk", "airbnb.es", "airbnb.fi", "airbnb.fr", "airbnb.gr", "airbnb.gy", "airbnb.hu", "airbnb.ie", "airbnb.is", "airbnb.it", "airbnb.jp", "airbnb.mx", "airbnb.nl", "airbnb.no", "airbnb.pl", "airbnb.pt", "airbnb.ru", "airbnb.se"], excluded: false },
  { type: 73, domains: ["eventbrite.at", "eventbrite.be", "eventbrite.ca", "eventbrite.ch", "eventbrite.cl", "eventbrite.co", "eventbrite.co.nz", "eventbrite.co.uk", "eventbrite.com", "eventbrite.com.ar", "eventbrite.com.au", "eventbrite.com.br", "eventbrite.com.mx", "eventbrite.com.pe", "eventbrite.de", "eventbrite.dk", "eventbrite.es", "eventbrite.fi", "eventbrite.fr", "eventbrite.hk", "eventbrite.ie", "eventbrite.it", "eventbrite.nl", "eventbrite.pt", "eventbrite.se", "eventbrite.sg"], excluded: false },
  { type: 74, domains: ["stackexchange.com", "superuser.com", "stackoverflow.com", "serverfault.com", "mathoverflow.net", "askubuntu.com", "stackapps.com"], excluded: false },
  { type: 75, domains: ["docusign.com", "docusign.net"], excluded: false },
  { type: 76, domains: ["envato.com", "themeforest.net", "codecanyon.net", "videohive.net", "audiojungle.net", "graphicriver.net", "photodune.net", "3docean.net"], excluded: false },
  { type: 77, domains: ["x10hosting.com", "x10premium.com"], excluded: false },
  { type: 78, domains: ["dnsomatic.com", "opendns.com", "umbrella.com"], excluded: false },
  { type: 79, domains: ["cagreatamerica.com", "canadaswonderland.com", "carowinds.com", "cedarfair.com", "cedarpoint.com", "dorneypark.com", "kingsdominion.com", "knotts.com", "miadventure.com", "schlitterbahn.com", "valleyfair.com", "visitkingsisland.com", "worldsoffun.com"], excluded: false },
  { type: 80, domains: ["ubnt.com", "ui.com"], excluded: false },
  { type: 81, domains: ["discordapp.com", "discord.com"], excluded: false },
  { type: 82, domains: ["netcup.de", "netcup.eu", "customercontrolpanel.de"], excluded: false },
  { type: 83, domains: ["yandex.com", "ya.ru", "yandex.az", "yandex.by", "yandex.co.il", "yandex.com.am", "yandex.com.ge", "yandex.com.tr", "yandex.ee", "yandex.fi", "yandex.fr", "yandex.kg", "yandex.kz", "yandex.lt", "yandex.lv", "yandex.md", "yandex.pl", "yandex.ru", "yandex.tj", "yandex.tm", "yandex.ua", "yandex.uz"], excluded: false },
  { type: 84, domains: ["sonyentertainmentnetwork.com", "sony.com"], excluded: false },
  { type: 85, domains: ["proton.me", "protonmail.com", "protonvpn.com"], excluded: false },
  { type: 86, domains: ["ubisoft.com", "ubi.com"], excluded: false },
  { type: 87, domains: ["transferwise.com", "wise.com"], excluded: false },
  { type: 88, domains: ["takeaway.com", "just-eat.dk", "just-eat.no", "just-eat.fr", "just-eat.ch", "lieferando.de", "lieferando.at", "thuisbezorgd.nl", "pyszne.pl"], excluded: false },
  { type: 89, domains: ["atlassian.com", "bitbucket.org", "trello.com", "statuspage.io", "atlassian.net", "jira.com"], excluded: false },
  { type: 90, domains: ["pinterest.com", "pinterest.com.au", "pinterest.cl", "pinterest.de", "pinterest.dk", "pinterest.es", "pinterest.fr", "pinterest.co.uk", "pinterest.jp", "pinterest.co.kr", "pinterest.nz", "pinterest.pt", "pinterest.se"], excluded: false },
  { type: 91, domains: ["twitter.com", "x.com"], excluded: false }
];

// src/static/global_domains.custom.json
var global_domains_custom_default = [
  { type: -10001, domains: ["nodewarden.example", "nw.example"], excluded: false, source: "nodewarden" }
];

// shared/domain-normalize.ts
var MULTI_LABEL_PUBLIC_SUFFIXES = /* @__PURE__ */ new Set([
  "ac.cn",
  "com.cn",
  "edu.cn",
  "gov.cn",
  "net.cn",
  "org.cn",
  "ah.cn",
  "bj.cn",
  "cq.cn",
  "fj.cn",
  "gd.cn",
  "gs.cn",
  "gx.cn",
  "gz.cn",
  "ha.cn",
  "hb.cn",
  "he.cn",
  "hi.cn",
  "hk.cn",
  "hl.cn",
  "hn.cn",
  "jl.cn",
  "js.cn",
  "jx.cn",
  "ln.cn",
  "mo.cn",
  "nm.cn",
  "nx.cn",
  "qh.cn",
  "sc.cn",
  "sd.cn",
  "sh.cn",
  "sn.cn",
  "sx.cn",
  "tj.cn",
  "tw.cn",
  "xj.cn",
  "xz.cn",
  "yn.cn",
  "zj.cn",
  "co.uk",
  "org.uk",
  "net.uk",
  "ac.uk",
  "gov.uk",
  "com.au",
  "net.au",
  "org.au",
  "edu.au",
  "gov.au",
  "co.nz",
  "org.nz",
  "net.nz",
  "com.br",
  "com.mx",
  "com.ar",
  "com.tr",
  "com.sg",
  "com.my",
  "com.hk",
  "com.tw",
  "co.jp",
  "ne.jp",
  "or.jp",
  "co.kr",
  "or.kr",
  "co.in",
  "firm.in",
  "net.in",
  "org.in",
  "co.id",
  "or.id",
  "web.id",
  "co.il",
  "org.il",
  "co.za",
  "com.sa",
  "com.ph",
  "com.vn",
  "com.pk",
  "com.bd",
  "com.ng",
  "github.io",
  "pages.dev",
  "workers.dev",
  "cloudflareaccess.com",
  "vercel.app",
  "netlify.app",
  "web.app",
  "firebaseapp.com",
  "herokuapp.com",
  "fly.dev",
  "railway.app",
  "render.com",
  "onrender.com"
]);
function extractHost(input) {
  let raw = input.trim().toLowerCase();
  if (!raw) return "";
  raw = raw.replace(/\\/g, "/");
  try {
    const candidate = /^[a-z][a-z0-9+.-]*:\/\//i.test(raw) ? raw : `https://${raw}`;
    const parsed = new URL(candidate);
    raw = parsed.hostname;
  } catch {
    raw = raw.split(/[/?#]/, 1)[0] || "";
    const atIndex = raw.lastIndexOf("@");
    if (atIndex >= 0) raw = raw.slice(atIndex + 1);
    if (raw.startsWith("[")) return "";
    const colonIndex = raw.lastIndexOf(":");
    if (colonIndex > -1 && raw.indexOf(":") === colonIndex) raw = raw.slice(0, colonIndex);
  }
  return raw.replace(/^\*+\./, "").replace(/^\.+/, "").replace(/\.+$/, "");
}
__name(extractHost, "extractHost");
function isValidHost(host) {
  if (!host || host.length > 253 || !host.includes(".")) return false;
  if (host.includes("..") || /[:/\s]/.test(host)) return false;
  if (/^\d{1,3}(?:\.\d{1,3}){3}$/.test(host)) return false;
  return host.split(".").every((label) => label.length > 0 && label.length <= 63 && /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(label));
}
__name(isValidHost, "isValidHost");
function normalizeEquivalentDomain(value) {
  const host = extractHost(String(value || ""));
  if (!isValidHost(host)) return "";
  const labels = host.split(".");
  for (let index = 0; index < labels.length; index += 1) {
    const suffix = labels.slice(index).join(".");
    if (!MULTI_LABEL_PUBLIC_SUFFIXES.has(suffix)) continue;
    if (index === 0) return "";
    return labels.slice(index - 1).join(".");
  }
  return labels.length >= 2 ? labels.slice(-2).join(".") : "";
}
__name(normalizeEquivalentDomain, "normalizeEquivalentDomain");

// src/services/domain-rules.ts
function normalizeDomain(value) {
  return normalizeEquivalentDomain(value);
}
__name(normalizeDomain, "normalizeDomain");
function normalizeGlobalDomain(entry) {
  const type = Number(entry.type ?? entry.Type);
  if (!Number.isInteger(type)) return null;
  const rawDomains = entry.domains ?? entry.Domains;
  if (!Array.isArray(rawDomains)) return null;
  const domains = Array.from(new Set(rawDomains.map(normalizeDomain).filter(Boolean)));
  if (domains.length < 2) return null;
  return {
    type,
    domains,
    excluded: Boolean(entry.excluded ?? entry.Excluded ?? false)
  };
}
__name(normalizeGlobalDomain, "normalizeGlobalDomain");
function normalizeGlobalDomains(input) {
  if (!Array.isArray(input)) return [];
  const seen = /* @__PURE__ */ new Set();
  const out = [];
  for (const entry of input) {
    const normalized = normalizeGlobalDomain(entry);
    if (!normalized || seen.has(normalized.type)) continue;
    seen.add(normalized.type);
    out.push(normalized);
  }
  return out;
}
__name(normalizeGlobalDomains, "normalizeGlobalDomains");
var bitwardenGlobalDomains = normalizeGlobalDomains(global_domains_bitwarden_default);
var customGlobalDomains = normalizeGlobalDomains(global_domains_custom_default);
var globalDomains = [
  ...bitwardenGlobalDomains,
  ...customGlobalDomains
];
function normalizeEquivalentDomains(input) {
  if (!Array.isArray(input)) return [];
  const groups = [];
  const seenGroups = /* @__PURE__ */ new Set();
  for (const group of input) {
    if (!Array.isArray(group)) continue;
    const domains = Array.from(new Set(group.map(normalizeDomain).filter(Boolean)));
    if (domains.length < 2) continue;
    const key = domains.slice().sort().join("\n");
    if (seenGroups.has(key)) continue;
    seenGroups.add(key);
    groups.push(domains);
  }
  return groups;
}
__name(normalizeEquivalentDomains, "normalizeEquivalentDomains");
function mergeEquivalentDomainGroups(input) {
  const parent = /* @__PURE__ */ new Map();
  function find(domain) {
    const current = parent.get(domain);
    if (!current) {
      parent.set(domain, domain);
      return domain;
    }
    if (current === domain) return domain;
    const root = find(current);
    parent.set(domain, root);
    return root;
  }
  __name(find, "find");
  function union(a, b) {
    const rootA = find(a);
    const rootB = find(b);
    if (rootA !== rootB) parent.set(rootB, rootA);
  }
  __name(union, "union");
  for (const group of normalizeEquivalentDomains(input)) {
    if (group.length < 2) continue;
    const [first, ...rest] = group;
    find(first);
    for (const domain of rest) union(first, domain);
  }
  const components = /* @__PURE__ */ new Map();
  for (const domain of parent.keys()) {
    const root = find(domain);
    const group = components.get(root) || [];
    group.push(domain);
    components.set(root, group);
  }
  return Array.from(components.values()).map((group) => group.sort()).filter((group) => group.length >= 2).sort((a, b) => a[0].localeCompare(b[0]));
}
__name(mergeEquivalentDomainGroups, "mergeEquivalentDomainGroups");
function expandCustomEquivalentDomainsWithGlobals(customGroups, activeGlobalGroups) {
  const normalizedCustomGroups = normalizeEquivalentDomains(customGroups);
  if (!normalizedCustomGroups.length) return [];
  const customDomains = new Set(normalizedCustomGroups.flat());
  return mergeEquivalentDomainGroups([
    ...activeGlobalGroups,
    ...normalizedCustomGroups
  ]).filter((group) => group.some((domain) => customDomains.has(domain)));
}
__name(expandCustomEquivalentDomainsWithGlobals, "expandCustomEquivalentDomainsWithGlobals");
function createCustomDomainId(domains, index) {
  return `custom:${domains.slice().sort().join("|")}:${index}`;
}
__name(createCustomDomainId, "createCustomDomainId");
function normalizeCustomEquivalentDomains(input) {
  if (!Array.isArray(input)) return [];
  const rules = [];
  const seenGroups = /* @__PURE__ */ new Set();
  for (const [index, item] of input.entries()) {
    const record = Array.isArray(item) ? { domains: item, excluded: false, id: "" } : item && typeof item === "object" ? item : null;
    if (!record) continue;
    const domains = normalizeEquivalentDomains([record.domains ?? record.Domains])[0];
    if (!domains) continue;
    const key = domains.slice().sort().join("\n");
    if (seenGroups.has(key)) continue;
    seenGroups.add(key);
    const rawId = String(record.id ?? record.Id ?? "").trim();
    rules.push({
      id: rawId || createCustomDomainId(domains, index),
      domains,
      excluded: Boolean(record.excluded ?? record.Excluded ?? false)
    });
  }
  return rules;
}
__name(normalizeCustomEquivalentDomains, "normalizeCustomEquivalentDomains");
function customRulesToActiveEquivalentDomains(rules) {
  return mergeEquivalentDomainGroups(rules.filter((rule) => !rule.excluded).map((rule) => rule.domains));
}
__name(customRulesToActiveEquivalentDomains, "customRulesToActiveEquivalentDomains");
function normalizeExcludedGlobalTypes(input) {
  if (!Array.isArray(input)) return [];
  const validTypes = new Set(globalDomains.map((entry) => entry.type));
  const seen = /* @__PURE__ */ new Set();
  const out = [];
  for (const item of input) {
    const type = Number(typeof item === "object" && item !== null ? item.type : item);
    const excluded = typeof item === "object" && item !== null ? Boolean(item.excluded) : true;
    if (!excluded || !Number.isInteger(type) || !validTypes.has(type) || seen.has(type)) continue;
    seen.add(type);
    out.push(type);
  }
  return out;
}
__name(normalizeExcludedGlobalTypes, "normalizeExcludedGlobalTypes");
function buildDomainsResponse(equivalentDomains, customEquivalentDomains, excludedGlobalEquivalentDomains, options = {}) {
  const excluded = new Set(excludedGlobalEquivalentDomains);
  const activeGlobalDomainGroups = globalDomains.filter((entry) => !excluded.has(entry.type)).map((entry) => entry.domains);
  const mergedEquivalentDomains = expandCustomEquivalentDomainsWithGlobals(
    equivalentDomains,
    activeGlobalDomainGroups
  );
  const globals = globalDomains.map((entry) => ({
    type: entry.type,
    domains: entry.domains,
    excluded: excluded.has(entry.type)
  })).filter((entry) => !options.omitExcludedGlobals || !entry.excluded);
  return {
    equivalentDomains: mergedEquivalentDomains,
    customEquivalentDomains,
    globalEquivalentDomains: globals,
    object: "domains"
  };
}
__name(buildDomainsResponse, "buildDomainsResponse");

// src/services/storage-domain-rules-repo.ts
function parseJsonArray(raw, fallback) {
  if (!raw) return fallback;
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : fallback;
  } catch {
    return fallback;
  }
}
__name(parseJsonArray, "parseJsonArray");
async function getUserDomainSettings(db, userId) {
  const row = await db.prepare("SELECT equivalent_domains, custom_equivalent_domains, excluded_global_equivalent_domains, updated_at FROM domain_settings WHERE user_id = ?").bind(userId).first();
  const equivalentDomains = normalizeEquivalentDomains(parseJsonArray(row?.equivalent_domains, []));
  const storedCustomEquivalentDomains = row?.custom_equivalent_domains ? normalizeCustomEquivalentDomains(parseJsonArray(row.custom_equivalent_domains, [])) : [];
  const customEquivalentDomains = storedCustomEquivalentDomains.length ? storedCustomEquivalentDomains : normalizeCustomEquivalentDomains(equivalentDomains);
  return {
    userId,
    equivalentDomains,
    customEquivalentDomains,
    excludedGlobalEquivalentDomains: parseJsonArray(row?.excluded_global_equivalent_domains, []),
    updatedAt: row?.updated_at || null
  };
}
__name(getUserDomainSettings, "getUserDomainSettings");
async function saveUserDomainSettings(db, userId, equivalentDomains, customEquivalentDomains, excludedGlobalEquivalentDomains, updatedAt) {
  await db.prepare(
    "INSERT INTO domain_settings(user_id, equivalent_domains, custom_equivalent_domains, excluded_global_equivalent_domains, updated_at) VALUES(?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET equivalent_domains = excluded.equivalent_domains, custom_equivalent_domains = excluded.custom_equivalent_domains, excluded_global_equivalent_domains = excluded.excluded_global_equivalent_domains, updated_at = excluded.updated_at"
  ).bind(
    userId,
    JSON.stringify(equivalentDomains),
    JSON.stringify(customEquivalentDomains),
    JSON.stringify(excludedGlobalEquivalentDomains),
    updatedAt
  ).run();
}
__name(saveUserDomainSettings, "saveUserDomainSettings");

// src/services/storage.ts
var TWO_FACTOR_REMEMBER_TTL_MS = 30 * 24 * 60 * 60 * 1e3;
var STORAGE_SCHEMA_VERSION_KEY = "schema.version";
var STORAGE_SCHEMA_VERSION = "2026-05-14-lightweight-audit-logs";
var StorageService = class _StorageService {
  constructor(db) {
    this.db = db;
  }
  static {
    __name(this, "StorageService");
  }
  static attachmentTokenTableReady = false;
  static schemaVerified = false;
  static lastRefreshTokenCleanupAt = 0;
  static lastAttachmentTokenCleanupAt = 0;
  static MAX_D1_SQL_VARIABLES = 100;
  static REFRESH_TOKEN_CLEANUP_INTERVAL_MS = LIMITS.cleanup.refreshTokenCleanupIntervalMs;
  static ATTACHMENT_TOKEN_CLEANUP_INTERVAL_MS = LIMITS.cleanup.attachmentTokenCleanupIntervalMs;
  static PERIODIC_CLEANUP_PROBABILITY = LIMITS.cleanup.cleanupProbability;
  /**
   * D1 .bind() throws on `undefined` values. This helper converts every
   * `undefined` in the argument list to `null` so we never hit that runtime
   * error - especially important after the opaque-passthrough change where
   * client-supplied JSON may omit fields we later reference as columns.
   */
  safeBind(stmt, ...values) {
    return stmt.bind(...values.map((v) => v === void 0 ? null : v));
  }
  sqlChunkSize(fixedBindCount) {
    return Math.max(
      1,
      Math.min(LIMITS.performance.bulkMoveChunkSize, _StorageService.MAX_D1_SQL_VARIABLES - fixedBindCount)
    );
  }
  async sha256Hex(input) {
    const bytes = new TextEncoder().encode(input);
    const digest = await crypto.subtle.digest("SHA-256", bytes);
    return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");
  }
  async refreshTokenKey(token) {
    const digest = await this.sha256Hex(token);
    return `sha256:${digest}`;
  }
  shouldRunPeriodicCleanup(lastRunAt, intervalMs) {
    const now = Date.now();
    if (now - lastRunAt < intervalMs) return false;
    return Math.random() < _StorageService.PERIODIC_CLEANUP_PROBABILITY;
  }
  async maybeCleanupExpiredRefreshTokens(nowMs) {
    if (!this.shouldRunPeriodicCleanup(_StorageService.lastRefreshTokenCleanupAt, _StorageService.REFRESH_TOKEN_CLEANUP_INTERVAL_MS)) {
      return;
    }
    await this.db.prepare("DELETE FROM refresh_tokens WHERE expires_at < ?").bind(nowMs).run();
    _StorageService.lastRefreshTokenCleanupAt = nowMs;
  }
  // --- Database initialization ---
  // Strategy:
  // - Run only once per isolate.
  // - Execute idempotent schema SQL on first request in each isolate.
  // - Keep statements idempotent so updates are safe.
  async initializeDatabase() {
    if (_StorageService.schemaVerified) return;
    await this.db.prepare("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL)").run();
    const schemaVersion = await getConfigValue(this.db, STORAGE_SCHEMA_VERSION_KEY);
    if (schemaVersion !== STORAGE_SCHEMA_VERSION) {
      await ensureStorageSchema(this.db);
      await setConfigValue(this.db, STORAGE_SCHEMA_VERSION_KEY, STORAGE_SCHEMA_VERSION);
    }
    _StorageService.schemaVerified = true;
  }
  // --- Config / setup ---
  async isRegistered() {
    return isRegistered(this.db);
  }
  async getConfigValue(key) {
    return getConfigValue(this.db, key);
  }
  async setConfigValue(key, value) {
    await setConfigValue(this.db, key, value);
  }
  async setRegistered() {
    await setRegistered(this.db);
  }
  // --- Users ---
  async getUser(email) {
    return getUser(this.db, email);
  }
  async getUserById(id) {
    return getUserById(this.db, id);
  }
  async getUserCount() {
    return getUserCount(this.db);
  }
  async getAllUsers() {
    return getAllUsers(this.db);
  }
  async saveUser(user) {
    await saveUser(this.db, this.safeBind.bind(this), user);
  }
  async createUser(user) {
    await createUser(this.db, this.safeBind.bind(this), user);
  }
  async createFirstUser(user) {
    return createFirstUser(this.db, this.safeBind.bind(this), user);
  }
  async deleteUserById(id) {
    return deleteUserById(this.db, id);
  }
  async createInvite(invite) {
    await createInvite(this.db, invite);
  }
  async getInvite(code) {
    return getInvite(this.db, code);
  }
  async listInvites(includeInactive = false) {
    return listInvites(this.db, includeInactive);
  }
  async markInviteUsed(code, userId) {
    return markInviteUsed(this.db, code, userId);
  }
  async revokeInvite(code) {
    return revokeInvite(this.db, code);
  }
  async deleteAllInvites() {
    return deleteAllInvites(this.db);
  }
  async createAuditLog(log) {
    await createAuditLog(this.db, log);
  }
  async listAuditLogs(options) {
    return listAuditLogs(this.db, options);
  }
  async pruneAuditLogs(beforeIso) {
    return pruneAuditLogs(this.db, beforeIso);
  }
  async pruneAuditLogsToMax(maxEntries) {
    return pruneAuditLogsToMax(this.db, maxEntries);
  }
  async clearAuditLogs() {
    return clearAuditLogs(this.db);
  }
  // --- Domain rules ---
  async getUserDomainSettings(userId) {
    return getUserDomainSettings(this.db, userId);
  }
  async saveUserDomainSettings(userId, equivalentDomains, customEquivalentDomains, excludedGlobalEquivalentDomains) {
    await saveUserDomainSettings(
      this.db,
      userId,
      equivalentDomains,
      customEquivalentDomains,
      excludedGlobalEquivalentDomains,
      (/* @__PURE__ */ new Date()).toISOString()
    );
    await this.updateRevisionDate(userId);
  }
  // --- Ciphers ---
  async getCipher(id) {
    return getCipher(this.db, id);
  }
  async saveCipher(cipher) {
    await saveCipher(this.db, this.safeBind.bind(this), cipher);
  }
  async deleteCipher(id, userId) {
    await deleteCipher(this.db, id, userId);
  }
  async bulkSoftDeleteCiphers(ids, userId) {
    return bulkSoftDeleteCiphers(this.db, this.sqlChunkSize.bind(this), this.updateRevisionDate.bind(this), ids, userId);
  }
  async bulkRestoreCiphers(ids, userId) {
    return bulkRestoreCiphers(this.db, this.sqlChunkSize.bind(this), this.updateRevisionDate.bind(this), ids, userId);
  }
  async bulkArchiveCiphers(ids, userId) {
    return bulkArchiveCiphers(this.db, this.sqlChunkSize.bind(this), this.updateRevisionDate.bind(this), ids, userId);
  }
  async bulkUnarchiveCiphers(ids, userId) {
    return bulkUnarchiveCiphers(this.db, this.sqlChunkSize.bind(this), this.updateRevisionDate.bind(this), ids, userId);
  }
  async bulkDeleteCiphers(ids, userId) {
    return bulkDeleteCiphers(this.db, this.sqlChunkSize.bind(this), this.updateRevisionDate.bind(this), ids, userId);
  }
  async getAllCiphers(userId) {
    return getAllCiphers(this.db, userId);
  }
  async getCiphersPage(userId, includeDeleted, limit, offset) {
    return getCiphersPage(this.db, userId, includeDeleted, limit, offset);
  }
  async getCiphersByIds(ids, userId) {
    return getCiphersByIds(this.db, this.sqlChunkSize.bind(this), ids, userId);
  }
  async bulkMoveCiphers(ids, folderId, userId) {
    return bulkMoveCiphers(this.db, this.sqlChunkSize.bind(this), this.updateRevisionDate.bind(this), ids, folderId, userId);
  }
  // --- Folders ---
  async getFolder(id) {
    return getFolder(this.db, id);
  }
  async saveFolder(folder) {
    await saveFolder(this.db, folder);
  }
  async deleteFolder(id, userId) {
    await deleteFolder(this.db, id, userId);
  }
  async bulkDeleteFolders(ids, userId) {
    return bulkDeleteFolders(
      this.db,
      userId,
      ids,
      this.sqlChunkSize.bind(this),
      this.updateRevisionDate.bind(this)
    );
  }
  // Clear folder references from all ciphers owned by the user.
  // Without this, deleting a folder leaves stale folderId values in cipher JSON.
  async clearFolderFromCiphers(userId, folderId) {
    await clearFolderFromCiphers(this.db, userId, folderId);
  }
  async getAllFolders(userId) {
    return getAllFolders(this.db, userId);
  }
  async getFoldersPage(userId, limit, offset) {
    return getFoldersPage(this.db, userId, limit, offset);
  }
  // --- Attachments ---
  async getAttachment(id) {
    return getAttachment(this.db, id);
  }
  async saveAttachment(attachment) {
    await saveAttachment(this.db, this.safeBind.bind(this), attachment);
  }
  async deleteAttachment(id) {
    await deleteAttachment(this.db, id);
  }
  async bulkDeleteAttachmentsByIds(ids) {
    await bulkDeleteAttachmentsByIds(this.db, this.sqlChunkSize.bind(this), ids);
  }
  async getAttachmentsByCipher(cipherId) {
    return getAttachmentsByCipher(this.db, cipherId);
  }
  async getAttachmentsByCipherIds(cipherIds) {
    return getAttachmentsByCipherIds(this.db, this.sqlChunkSize.bind(this), cipherIds);
  }
  async getAttachmentsByUserId(userId) {
    return getAttachmentsByUserId(this.db, userId);
  }
  async addAttachmentToCipher(cipherId, attachmentId) {
    await addAttachmentToCipher(this.db, cipherId, attachmentId);
  }
  async deleteAllAttachmentsByCipher(cipherId) {
    await deleteAllAttachmentsByCipher(this.db, cipherId);
  }
  async updateCipherRevisionDate(cipherId) {
    return updateCipherRevisionDate(
      this.getCipher.bind(this),
      this.saveCipher.bind(this),
      this.updateRevisionDate.bind(this),
      cipherId
    );
  }
  // --- Refresh tokens ---
  async saveRefreshToken(token, userId, expiresAtMs, deviceIdentifier, deviceSessionStamp) {
    const expiresAt = expiresAtMs ?? Date.now() + LIMITS.auth.refreshTokenTtlMs;
    await saveRefreshToken(
      this.db,
      this.refreshTokenKey.bind(this),
      this.maybeCleanupExpiredRefreshTokens.bind(this),
      token,
      userId,
      expiresAt,
      deviceIdentifier,
      deviceSessionStamp
    );
  }
  async getRefreshTokenRecord(token) {
    return getRefreshTokenRecord(
      this.db,
      this.refreshTokenKey.bind(this),
      this.maybeCleanupExpiredRefreshTokens.bind(this),
      this.saveRefreshToken.bind(this),
      this.deleteRefreshToken.bind(this),
      token
    );
  }
  async getRefreshTokenUserId(token) {
    const record = await this.getRefreshTokenRecord(token);
    return record?.userId ?? null;
  }
  async deleteRefreshToken(token) {
    await deleteRefreshToken(this.db, this.refreshTokenKey.bind(this), token);
  }
  // --- Sends ---
  async getSend(id) {
    return getSend(this.db, id);
  }
  async saveSend(send) {
    await saveSend(this.db, this.safeBind.bind(this), send);
  }
  /**
   * Atomically increment access_count and update updated_at.
   * Returns true if the row was updated (send still available),
   * false if max_access_count has already been reached.
   */
  async incrementSendAccessCount(sendId) {
    return incrementSendAccessCount(this.db, sendId);
  }
  async deleteSend(id, userId) {
    await deleteSend(this.db, id, userId);
  }
  async getSendsByIds(ids, userId) {
    return getSendsByIds(this.db, this.sqlChunkSize.bind(this), ids, userId);
  }
  async bulkDeleteSends(ids, userId) {
    return bulkDeleteSends(this.db, this.sqlChunkSize.bind(this), this.updateRevisionDate.bind(this), ids, userId);
  }
  async getAllSends(userId) {
    return getAllSends(this.db, userId);
  }
  async getSendsPage(userId, limit, offset) {
    return getSendsPage(this.db, userId, limit, offset);
  }
  async deleteRefreshTokensByUserId(userId) {
    return deleteRefreshTokensByUserId(this.db, userId);
  }
  async deleteRefreshTokensByDevice(userId, deviceIdentifier) {
    return deleteRefreshTokensByDevice(this.db, userId, deviceIdentifier);
  }
  // Keep a short overlap window for rotated refresh token to reduce
  // multi-context refresh races (e.g. browser extension popup/background).
  // Expiry is only tightened, never extended.
  async constrainRefreshTokenExpiry(token, maxExpiresAtMs) {
    await constrainRefreshTokenExpiry(this.db, this.refreshTokenKey.bind(this), token, maxExpiresAtMs);
  }
  async trustedTwoFactorTokenKey(token) {
    const digest = await this.sha256Hex(token);
    return `sha256:${digest}`;
  }
  // --- Devices ---
  async upsertDevice(userId, deviceIdentifier, name, type, sessionStamp, keys) {
    await upsertDevice(this.db, this.getDevice.bind(this), userId, deviceIdentifier, name, type, sessionStamp, keys);
  }
  async isKnownDevice(userId, deviceIdentifier) {
    return isKnownDevice(this.db, userId, deviceIdentifier);
  }
  async isKnownDeviceByEmail(email, deviceIdentifier) {
    return isKnownDeviceByEmail(this.getUser.bind(this), this.isKnownDevice.bind(this), email, deviceIdentifier);
  }
  async getDevicesByUserId(userId) {
    return getDevicesByUserId(this.db, userId);
  }
  async getDevice(userId, deviceIdentifier) {
    return getDevice(this.db, userId, deviceIdentifier);
  }
  async updateDeviceKeys(userId, deviceIdentifier, keys) {
    return updateDeviceKeys(this.db, userId, deviceIdentifier, keys);
  }
  async updateDeviceName(userId, deviceIdentifier, name) {
    return updateDeviceName(this.db, userId, deviceIdentifier, name);
  }
  async touchDeviceLastSeen(userId, deviceIdentifier) {
    return touchDeviceLastSeen(this.db, userId, deviceIdentifier);
  }
  async clearDeviceKeys(userId, deviceIdentifiers) {
    return clearDeviceKeys(this.db, userId, deviceIdentifiers);
  }
  async deleteDevice(userId, deviceIdentifier) {
    return deleteDevice(this.db, userId, deviceIdentifier);
  }
  async deleteDevicesByUserId(userId) {
    return deleteDevicesByUserId(this.db, userId);
  }
  async getTrustedDeviceTokenSummariesByUserId(userId) {
    return getTrustedDeviceTokenSummariesByUserId(this.db, userId);
  }
  async deleteTrustedTwoFactorTokensByDevice(userId, deviceIdentifier) {
    return deleteTrustedTwoFactorTokensByDevice(this.db, userId, deviceIdentifier);
  }
  async deleteTrustedTwoFactorTokensByUserId(userId) {
    return deleteTrustedTwoFactorTokensByUserId(this.db, userId);
  }
  async updateTrustedTwoFactorTokensExpiryByDevice(userId, deviceIdentifier, expiresAtMs) {
    return updateTrustedTwoFactorTokensExpiryByDevice(this.db, userId, deviceIdentifier, expiresAtMs);
  }
  // --- Trusted 2FA remember tokens (device-bound) ---
  async saveTrustedTwoFactorDeviceToken(token, userId, deviceIdentifier, expiresAtMs) {
    const expiresAt = expiresAtMs ?? Date.now() + TWO_FACTOR_REMEMBER_TTL_MS;
    await saveTrustedTwoFactorDeviceToken(this.db, this.trustedTwoFactorTokenKey.bind(this), token, userId, deviceIdentifier, expiresAt);
  }
  async getTrustedTwoFactorDeviceTokenUserId(token, deviceIdentifier) {
    return getTrustedTwoFactorDeviceTokenUserId(this.db, this.trustedTwoFactorTokenKey.bind(this), token, deviceIdentifier);
  }
  // --- Revision dates ---
  async getRevisionDate(userId) {
    return getRevisionDate(this.db, userId);
  }
  async updateRevisionDate(userId) {
    return updateRevisionDate(this.db, userId);
  }
  // --- One-time attachment download tokens ---
  async ensureUsedAttachmentDownloadTokenTable() {
    if (_StorageService.attachmentTokenTableReady) return;
    await ensureUsedAttachmentDownloadTokenTable(this.db);
    _StorageService.attachmentTokenTableReady = true;
  }
  // Marks an attachment download token JTI as consumed.
  // Returns true only on first use. Reuse returns false.
  async consumeAttachmentDownloadToken(jti, expUnixSeconds) {
    await this.ensureUsedAttachmentDownloadTokenTable();
    const result = await consumeAttachmentDownloadToken(
      this.db,
      this.shouldRunPeriodicCleanup.bind(this),
      _StorageService.lastAttachmentTokenCleanupAt,
      _StorageService.ATTACHMENT_TOKEN_CLEANUP_INTERVAL_MS,
      jti,
      expUnixSeconds
    );
    if (result.cleanedUpAt !== null) {
      _StorageService.lastAttachmentTokenCleanupAt = result.cleanedUpAt;
    }
    return result.consumed;
  }
};

// src/services/auth.ts
var SERVER_HASH_ITERATIONS = 1e5;
var AUTH_CONTEXT_CACHE_TTL_MS = 15 * 1e3;
var AuthService = class _AuthService {
  constructor(env) {
    this.env = env;
    this.storage = new StorageService(env.DB);
  }
  static {
    __name(this, "AuthService");
  }
  storage;
  static userCache = /* @__PURE__ */ new Map();
  static deviceCache = /* @__PURE__ */ new Map();
  static invalidateUserCache(userId) {
    const normalizedUserId = String(userId || "").trim();
    if (!normalizedUserId) return;
    _AuthService.userCache.delete(normalizedUserId);
    const prefix = `${normalizedUserId}:`;
    for (const key of _AuthService.deviceCache.keys()) {
      if (key.startsWith(prefix)) {
        _AuthService.deviceCache.delete(key);
      }
    }
  }
  static invalidateDeviceCache(userId, deviceId) {
    const normalizedUserId = String(userId || "").trim();
    const normalizedDeviceId = String(deviceId || "").trim();
    if (!normalizedUserId || !normalizedDeviceId) return;
    _AuthService.deviceCache.delete(`${normalizedUserId}:${normalizedDeviceId}`);
  }
  readCachedUser(userId) {
    const cached = _AuthService.userCache.get(userId);
    if (!cached) return void 0;
    if (cached.expiresAt <= Date.now()) {
      _AuthService.userCache.delete(userId);
      return void 0;
    }
    return cached.user;
  }
  writeCachedUser(userId, user) {
    _AuthService.userCache.set(userId, {
      user,
      expiresAt: Date.now() + AUTH_CONTEXT_CACHE_TTL_MS
    });
  }
  async getCachedUser(userId) {
    const cached = this.readCachedUser(userId);
    if (cached !== void 0) return cached;
    const user = await this.storage.getUserById(userId);
    this.writeCachedUser(userId, user);
    return user;
  }
  async getFreshUser(userId) {
    const user = await this.storage.getUserById(userId);
    this.writeCachedUser(userId, user);
    return user;
  }
  readCachedDevice(userId, deviceId) {
    const cacheKey = `${userId}:${deviceId}`;
    const cached = _AuthService.deviceCache.get(cacheKey);
    if (!cached) return void 0;
    if (cached.expiresAt <= Date.now()) {
      _AuthService.deviceCache.delete(cacheKey);
      return void 0;
    }
    return cached.device;
  }
  writeCachedDevice(userId, deviceId, device) {
    const cacheKey = `${userId}:${deviceId}`;
    _AuthService.deviceCache.set(cacheKey, {
      device,
      expiresAt: Date.now() + AUTH_CONTEXT_CACHE_TTL_MS
    });
  }
  async getCachedDevice(userId, deviceId) {
    const cached = this.readCachedDevice(userId, deviceId);
    if (cached !== void 0) return cached;
    const device = await this.storage.getDevice(userId, deviceId);
    this.writeCachedDevice(userId, deviceId, device);
    return device;
  }
  async getFreshDevice(userId, deviceId) {
    const device = await this.storage.getDevice(userId, deviceId);
    this.writeCachedDevice(userId, deviceId, device);
    return device;
  }
  // Second-layer hash: PBKDF2-SHA256(clientHash, email-salt, iterations).
  // Ensures database contents alone cannot be used to authenticate (pass-the-hash defense).
  // Result is prefixed with "$s$" to distinguish from legacy raw client hashes.
  async hashPasswordServer(clientHash, email) {
    const keyMaterial = await crypto.subtle.importKey(
      "raw",
      new TextEncoder().encode(clientHash),
      "PBKDF2",
      false,
      ["deriveBits"]
    );
    const salt = new TextEncoder().encode(email.toLowerCase().trim());
    const bits2 = await crypto.subtle.deriveBits(
      { name: "PBKDF2", hash: "SHA-256", salt, iterations: SERVER_HASH_ITERATIONS },
      keyMaterial,
      256
    );
    const bytes = new Uint8Array(bits2);
    let binary = "";
    for (const b of bytes) binary += String.fromCharCode(b);
    return "$s$" + btoa(binary);
  }
  // Verify password: hash the input the same way, then constant-time compare.
  async verifyPassword(inputHash, storedHash, email) {
    if (email && storedHash.startsWith("$s$")) {
      const serverHash = await this.hashPasswordServer(inputHash, email);
      return this.constantTimeEquals(serverHash, storedHash);
    }
    return this.constantTimeEquals(inputHash, storedHash);
  }
  constantTimeEquals(a, b) {
    const encA = new TextEncoder().encode(a);
    const encB = new TextEncoder().encode(b);
    if (encA.length !== encB.length) return false;
    let diff = 0;
    for (let i = 0; i < encA.length; i++) {
      diff |= encA[i] ^ encB[i];
    }
    return diff === 0;
  }
  // Generate access token
  async generateAccessToken(user, device) {
    return createJWT(
      {
        sub: user.id,
        email: user.email,
        name: user.name,
        sstamp: user.securityStamp,
        ...device?.identifier ? { did: device.identifier, dstamp: device.sessionStamp } : {}
      },
      this.env.JWT_SECRET
    );
  }
  // Generate refresh token
  async generateRefreshToken(userId, device) {
    const token = createRefreshToken();
    await this.storage.saveRefreshToken(token, userId, void 0, device?.identifier ?? null, device?.sessionStamp ?? null);
    return token;
  }
  async verifyAccessTokenWithUser(authHeader) {
    if (!authHeader) return null;
    const parts = authHeader.split(" ");
    if (parts.length !== 2 || parts[0].toLowerCase() !== "bearer") {
      return null;
    }
    const payload = await verifyJWT(parts[1], this.env.JWT_SECRET);
    if (!payload) return null;
    let user = await this.getCachedUser(payload.sub);
    if (!user || user.status !== "active" || payload.sstamp !== user.securityStamp) {
      user = await this.getFreshUser(payload.sub);
    }
    if (!user) return null;
    if (user.status !== "active") return null;
    if (payload.sstamp !== user.securityStamp) {
      return null;
    }
    if (payload.did) {
      let device = await this.getCachedDevice(user.id, payload.did);
      if (!device || !payload.dstamp || payload.dstamp !== device.sessionStamp) {
        device = await this.getFreshDevice(user.id, payload.did);
      }
      if (!device) return null;
      if (!payload.dstamp || payload.dstamp !== device.sessionStamp) return null;
    }
    return { payload, user };
  }
  // Verify access token from Authorization header
  async verifyAccessToken(authHeader) {
    const verified = await this.verifyAccessTokenWithUser(authHeader);
    return verified?.payload ?? null;
  }
  // Refresh access token
  async refreshAccessTokenDetailed(refreshToken) {
    const record = await this.storage.getRefreshTokenRecord(refreshToken);
    if (!record?.userId) return { ok: false, reason: "token_not_found_or_expired" };
    const user = await this.storage.getUserById(record.userId);
    if (!user) {
      await this.storage.deleteRefreshToken(refreshToken);
      return { ok: false, reason: "user_missing", userId: record.userId, deviceIdentifier: record.deviceIdentifier };
    }
    if (user.status !== "active") {
      await this.storage.deleteRefreshToken(refreshToken);
      return { ok: false, reason: "user_inactive", userId: user.id, deviceIdentifier: record.deviceIdentifier };
    }
    let device = null;
    if (record.deviceIdentifier) {
      const boundDevice = await this.storage.getDevice(user.id, record.deviceIdentifier);
      if (!boundDevice) {
        await this.storage.deleteRefreshToken(refreshToken);
        return { ok: false, reason: "device_missing", userId: user.id, deviceIdentifier: record.deviceIdentifier };
      }
      if (!record.deviceSessionStamp || boundDevice.sessionStamp !== record.deviceSessionStamp) {
        await this.storage.deleteRefreshToken(refreshToken);
        return { ok: false, reason: "device_session_mismatch", userId: user.id, deviceIdentifier: record.deviceIdentifier };
      }
      device = { identifier: boundDevice.deviceIdentifier, sessionStamp: boundDevice.sessionStamp };
    }
    const accessToken = await this.generateAccessToken(user, device);
    return { ok: true, accessToken, user, device };
  }
  async refreshAccessToken(refreshToken) {
    const result = await this.refreshAccessTokenDetailed(refreshToken);
    return result.ok ? result : null;
  }
};

// src/services/ratelimit.ts
var CONFIG = {
  LOGIN_MAX_ATTEMPTS: LIMITS.rateLimit.loginMaxAttempts,
  LOGIN_LOCKOUT_MINUTES: LIMITS.rateLimit.loginLockoutMinutes,
  API_WINDOW_SECONDS: LIMITS.rateLimit.apiWindowSeconds
};
var RateLimitService = class _RateLimitService {
  constructor(db) {
    this.db = db;
  }
  static {
    __name(this, "RateLimitService");
  }
  static loginIpTableReady = false;
  static lastLoginIpCleanupAt = 0;
  static PERIODIC_CLEANUP_PROBABILITY = LIMITS.rateLimit.cleanupProbability;
  static LOGIN_IP_CLEANUP_INTERVAL_MS = LIMITS.rateLimit.loginIpCleanupIntervalMs;
  static LOGIN_IP_RETENTION_MS = LIMITS.rateLimit.loginIpRetentionMs;
  shouldRunCleanup(lastRunAt, intervalMs) {
    const now = Date.now();
    if (now - lastRunAt < intervalMs) return false;
    return Math.random() < _RateLimitService.PERIODIC_CLEANUP_PROBABILITY;
  }
  async maybeCleanupLoginAttemptsIp(nowMs) {
    if (!this.shouldRunCleanup(_RateLimitService.lastLoginIpCleanupAt, _RateLimitService.LOGIN_IP_CLEANUP_INTERVAL_MS)) {
      return;
    }
    const cutoff = nowMs - _RateLimitService.LOGIN_IP_RETENTION_MS;
    await this.db.prepare(
      "DELETE FROM login_attempts_ip WHERE updated_at < ? AND (locked_until IS NULL OR locked_until < ?)"
    ).bind(cutoff, nowMs).run();
    _RateLimitService.lastLoginIpCleanupAt = nowMs;
  }
  async ensureLoginIpTable() {
    if (_RateLimitService.loginIpTableReady) return;
    await this.db.prepare(
      "CREATE TABLE IF NOT EXISTS login_attempts_ip (ip TEXT PRIMARY KEY, attempts INTEGER NOT NULL, locked_until INTEGER, updated_at INTEGER NOT NULL)"
    ).run();
    _RateLimitService.loginIpTableReady = true;
  }
  async checkLoginAttempt(ip) {
    await this.ensureLoginIpTable();
    const key = ip.trim() || "unknown";
    const now = Date.now();
    await this.maybeCleanupLoginAttemptsIp(now);
    const row = await this.db.prepare("SELECT attempts, locked_until FROM login_attempts_ip WHERE ip = ?").bind(key).first();
    if (!row) {
      return { allowed: true, remainingAttempts: CONFIG.LOGIN_MAX_ATTEMPTS };
    }
    if (row.locked_until && row.locked_until > now) {
      return {
        allowed: false,
        remainingAttempts: 0,
        retryAfterSeconds: Math.ceil((row.locked_until - now) / 1e3)
      };
    }
    if (row.locked_until && row.locked_until <= now) {
      await this.db.prepare("DELETE FROM login_attempts_ip WHERE ip = ?").bind(key).run();
      return { allowed: true, remainingAttempts: CONFIG.LOGIN_MAX_ATTEMPTS };
    }
    const remainingAttempts = Math.max(0, CONFIG.LOGIN_MAX_ATTEMPTS - (row.attempts || 0));
    return { allowed: true, remainingAttempts };
  }
  async recordFailedLogin(ip) {
    await this.ensureLoginIpTable();
    const key = ip.trim() || "unknown";
    const now = Date.now();
    await this.maybeCleanupLoginAttemptsIp(now);
    await this.db.prepare(
      "INSERT INTO login_attempts_ip(ip, attempts, locked_until, updated_at) VALUES(?, 1, NULL, ?) ON CONFLICT(ip) DO UPDATE SET attempts = attempts + 1, updated_at = excluded.updated_at"
    ).bind(key, now).run();
    const row = await this.db.prepare("SELECT attempts FROM login_attempts_ip WHERE ip = ?").bind(key).first();
    const attempts = row?.attempts || 1;
    if (attempts >= CONFIG.LOGIN_MAX_ATTEMPTS) {
      const lockedUntil = now + CONFIG.LOGIN_LOCKOUT_MINUTES * 60 * 1e3;
      await this.db.prepare("UPDATE login_attempts_ip SET locked_until = ?, updated_at = ? WHERE ip = ?").bind(lockedUntil, now, key).run();
      return { locked: true, retryAfterSeconds: CONFIG.LOGIN_LOCKOUT_MINUTES * 60 };
    }
    return { locked: false };
  }
  async clearLoginAttempts(ip) {
    await this.ensureLoginIpTable();
    const key = ip.trim() || "unknown";
    await this.db.prepare("DELETE FROM login_attempts_ip WHERE ip = ?").bind(key).run();
  }
  // Cache API-backed fixed-window rate limiter.
  // Uses Cloudflare edge cache instead of D1 鈥?zero database writes, auto-expires via TTL.
  // Per-colo isolation is acceptable (matches Cloudflare's own rate limiting behaviour).
  async consumeFixedWindowBudget(identifier, maxRequests, windowSeconds) {
    const nowSec = Math.floor(Date.now() / 1e3);
    const windowStart = nowSec - nowSec % windowSeconds;
    const windowEnd = windowStart + windowSeconds;
    const ttl = Math.max(1, windowEnd - nowSec);
    const cache = await caches.open("rate-limit");
    const cacheKey = new Request(`https://rl/${identifier}/${windowStart}`);
    const cached = await cache.match(cacheKey);
    let count = 0;
    if (cached) {
      count = parseInt(await cached.text(), 10) || 0;
    }
    if (count >= maxRequests) {
      return { allowed: false, remaining: 0, retryAfterSeconds: ttl };
    }
    count++;
    await cache.put(
      cacheKey,
      new Response(String(count), {
        headers: { "Cache-Control": `public, max-age=${ttl}` }
      })
    );
    return { allowed: true, remaining: Math.max(0, maxRequests - count) };
  }
  // General-purpose fixed-window budget.
  // Callers supply an identifier (must be unique per rate-limit category) and the
  // per-window maximum.  This single method replaces all previous specialised
  // budget helpers (write / sync / knownDevice / publicSend).
  async consumeBudget(identifier, maxRequests) {
    return this.consumeFixedWindowBudget(identifier, maxRequests, CONFIG.API_WINDOW_SECONDS);
  }
  async consumeBudgetWithWindow(identifier, maxRequests, windowSeconds) {
    return this.consumeFixedWindowBudget(identifier, maxRequests, windowSeconds);
  }
};
function parseIpv4Octets(input) {
  const parts = input.split(".");
  if (parts.length !== 4) return null;
  const octets = [];
  for (const part of parts) {
    if (!/^\d{1,3}$/.test(part)) return null;
    const value = Number(part);
    if (!Number.isInteger(value) || value < 0 || value > 255) return null;
    octets.push(value);
  }
  return octets;
}
__name(parseIpv4Octets, "parseIpv4Octets");
function parseIpv6Hextets(input) {
  let value = input.trim().toLowerCase();
  if (!value) return null;
  if (value.startsWith("[") && value.endsWith("]")) {
    value = value.slice(1, -1);
  }
  const zoneIndex = value.indexOf("%");
  if (zoneIndex >= 0) {
    value = value.slice(0, zoneIndex);
  }
  if (!value.includes(":")) return null;
  if (value.includes(".")) {
    const lastColon = value.lastIndexOf(":");
    if (lastColon < 0) return null;
    const ipv4Tail = value.slice(lastColon + 1);
    const octets = parseIpv4Octets(ipv4Tail);
    if (!octets) return null;
    const high = (octets[0] << 8 | octets[1]).toString(16);
    const low = (octets[2] << 8 | octets[3]).toString(16);
    value = `${value.slice(0, lastColon)}:${high}:${low}`;
  }
  const doubleColon = value.indexOf("::");
  if (doubleColon !== value.lastIndexOf("::")) return null;
  const parsePart = /* @__PURE__ */ __name((part) => {
    if (!/^[0-9a-f]{1,4}$/.test(part)) return null;
    const n = parseInt(part, 16);
    return Number.isNaN(n) ? null : n;
  }, "parsePart");
  const parseParts = /* @__PURE__ */ __name((parts) => {
    const out = [];
    for (const p of parts) {
      if (!p) return null;
      const n = parsePart(p);
      if (n === null) return null;
      out.push(n);
    }
    return out;
  }, "parseParts");
  if (doubleColon >= 0) {
    const [headRaw, tailRaw] = value.split("::");
    const head = headRaw ? headRaw.split(":") : [];
    const tail = tailRaw ? tailRaw.split(":") : [];
    const headNums = parseParts(head);
    const tailNums = parseParts(tail);
    if (!headNums || !tailNums) return null;
    const missing = 8 - (headNums.length + tailNums.length);
    if (missing < 1) return null;
    return [...headNums, ...new Array(missing).fill(0), ...tailNums];
  }
  const all = parseParts(value.split(":"));
  if (!all || all.length !== 8) return null;
  return all;
}
__name(parseIpv6Hextets, "parseIpv6Hextets");
function normalizeClientIpForRateLimit(rawIp) {
  const input = rawIp.trim();
  if (!input) return null;
  const ipv4 = parseIpv4Octets(input);
  if (ipv4) {
    return `ip4:${ipv4.join(".")}`;
  }
  const ipv6 = parseIpv6Hextets(input);
  if (!ipv6) return null;
  if (ipv6[0] === 0 && ipv6[1] === 0 && ipv6[2] === 0 && ipv6[3] === 0 && ipv6[4] === 0 && (ipv6[5] === 65535 || ipv6[5] === 0)) {
    const octets = [ipv6[6] >> 8, ipv6[6] & 255, ipv6[7] >> 8, ipv6[7] & 255];
    return `ip4:${octets.join(".")}`;
  }
  const prefix64 = ipv6.slice(0, 4).map((part) => part.toString(16).padStart(4, "0")).join(":");
  return `ip6:${prefix64}`;
}
__name(normalizeClientIpForRateLimit, "normalizeClientIpForRateLimit");
function isLocalRequest(request) {
  const isLoopbackHost = /* @__PURE__ */ __name((host) => {
    if (!host) return false;
    const normalized = host.split(":")[0].trim().toLowerCase();
    return normalized === "localhost" || normalized.endsWith(".localhost") || normalized === "127.0.0.1" || normalized === "0.0.0.0" || normalized === "::1" || normalized === "[::1]";
  }, "isLoopbackHost");
  try {
    if (isLoopbackHost(new URL(request.url).hostname)) return true;
  } catch {
  }
  return isLoopbackHost(request.headers.get("Host"));
}
__name(isLocalRequest, "isLocalRequest");
function getClientIdentifier(request) {
  const candidates = [
    request.headers.get("CF-Connecting-IP"),
    request.headers.get("X-Real-IP"),
    request.headers.get("X-Forwarded-For")?.split(",")[0]?.trim() || null
  ];
  for (const raw of candidates) {
    if (!raw) continue;
    const normalized = normalizeClientIpForRateLimit(raw);
    if (normalized) return normalized;
  }
  if (isLocalRequest(request)) {
    return "ip4:127.0.0.1";
  }
  return null;
}
__name(getClientIdentifier, "getClientIdentifier");

// src/utils/response.ts
var CORS_METHODS = "GET, POST, PUT, DELETE, PATCH, OPTIONS";
var DEFAULT_CORS_HEADERS = [
  "Content-Type",
  "Authorization",
  "Accept",
  "Device-Type",
  "Device-Identifier",
  "Device-Name",
  "Bitwarden-Client-Name",
  "Bitwarden-Client-Version",
  "Bitwarden-Package-Type",
  "Is-Prerelease",
  "X-Request-Email",
  "X-Device-Identifier",
  "X-Device-Name",
  "X-NodeWarden-Web-Session"
];
function isExtensionOrigin(origin) {
  return origin.startsWith("chrome-extension://") || origin.startsWith("moz-extension://") || origin.startsWith("safari-web-extension://");
}
__name(isExtensionOrigin, "isExtensionOrigin");
function isWildcardCorsPath(path) {
  return path.startsWith("/icons/") || path === "/config" || path === "/api/config" || path === "/api/version";
}
__name(isWildcardCorsPath, "isWildcardCorsPath");
function getCorsPolicy(request) {
  const url = new URL(request.url);
  const origin = request.headers.get("Origin");
  if (!origin) {
    return isWildcardCorsPath(url.pathname) ? { allowOrigin: "*", allowCredentials: false } : { allowOrigin: null, allowCredentials: false };
  }
  if (origin === url.origin) {
    return { allowOrigin: origin, allowCredentials: true };
  }
  if (isExtensionOrigin(origin)) {
    return { allowOrigin: origin, allowCredentials: true };
  }
  if (isWildcardCorsPath(url.pathname)) {
    return { allowOrigin: "*", allowCredentials: false };
  }
  return { allowOrigin: null, allowCredentials: false };
}
__name(getCorsPolicy, "getCorsPolicy");
function buildCorsHeaders(request) {
  const requestedHeaders = String(request.headers.get("Access-Control-Request-Headers") || "").split(",").map((value) => value.trim()).filter(Boolean);
  const allowHeaders = Array.from(/* @__PURE__ */ new Set([...DEFAULT_CORS_HEADERS, ...requestedHeaders]));
  const headers = {
    "Access-Control-Allow-Methods": CORS_METHODS,
    "Access-Control-Allow-Headers": allowHeaders.join(", "),
    "Access-Control-Expose-Headers": "*",
    "Access-Control-Max-Age": String(LIMITS.cors.preflightMaxAgeSeconds)
  };
  const corsPolicy = getCorsPolicy(request);
  if (corsPolicy.allowOrigin) {
    headers["Access-Control-Allow-Origin"] = corsPolicy.allowOrigin;
    if (corsPolicy.allowCredentials) {
      headers["Access-Control-Allow-Credentials"] = "true";
    }
    headers["Vary"] = "Origin, Access-Control-Request-Headers";
  }
  return headers;
}
__name(buildCorsHeaders, "buildCorsHeaders");
function applyCors(request, response) {
  const webSocket = response.webSocket;
  if (response.status === 101 || webSocket) {
    return response;
  }
  const headers = new Headers(response.headers);
  const corsHeaders = buildCorsHeaders(request);
  for (const [k, v] of Object.entries(corsHeaders)) {
    headers.set(k, v);
  }
  headers.set("X-Frame-Options", "DENY");
  headers.set("X-Content-Type-Options", "nosniff");
  headers.set("Referrer-Policy", "strict-origin-when-cross-origin");
  headers.set("Content-Security-Policy", "frame-ancestors 'none'; img-src 'self' data:");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}
__name(applyCors, "applyCors");
function jsonResponse(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...headers
    }
  });
}
__name(jsonResponse, "jsonResponse");
function errorResponse(message, status = 400) {
  return jsonResponse(
    {
      error: message,
      error_description: message,
      ErrorModel: {
        Message: message,
        Object: "error"
      }
    },
    status
  );
}
__name(errorResponse, "errorResponse");
function identityErrorResponse(message, error = "invalid_grant", status = 400) {
  return jsonResponse(
    {
      error,
      error_description: message,
      ErrorModel: {
        Message: message,
        Object: "error"
      }
    },
    status
  );
}
__name(identityErrorResponse, "identityErrorResponse");
function handleCors(request) {
  return new Response(null, {
    status: 204,
    headers: buildCorsHeaders(request)
  });
}
__name(handleCors, "handleCors");

// src/utils/uuid.ts
function generateUUID() {
  return crypto.randomUUID();
}
__name(generateUUID, "generateUUID");

// src/services/audit-events.ts
var SENSITIVE_KEY_RE = /(token|secret|password|key|hash|code|private)/i;
var MAX_METADATA_BYTES = 2048;
var AUDIT_CLEANUP_INTERVAL_MS = 6 * 60 * 60 * 1e3;
var AUDIT_CLEANUP_PROBABILITY = 0.02;
var AUDIT_LOG_SETTINGS_KEY = "audit.logs.settings.v1";
var DEFAULT_AUDIT_LOG_SETTINGS = {
  retentionDays: 90,
  maxEntries: null
};
var lastAuditCleanupAt = 0;
var ALLOWED_METADATA_KEYS = /* @__PURE__ */ new Set([
  "method",
  "path",
  "ip",
  "userAgent",
  "email",
  "targetEmail",
  "grantType",
  "webSession",
  "deviceIdentifier",
  "deviceType",
  "reason",
  "status",
  "verifyDevices",
  "changed",
  "removed",
  "updated",
  "deleted",
  "removedTrusted",
  "removedSessions",
  "removedDevices",
  "requested",
  "count",
  "requestedCount",
  "type",
  "folderId",
  "cipherId",
  "size",
  "users",
  "ciphers",
  "attachments",
  "skippedAttachments",
  "skippedReason",
  "replaceExisting",
  "provider",
  "fileName",
  "fileBytes",
  "bytes",
  "compressedBytes",
  "includesAttachments",
  "destinationName",
  "destinationId",
  "destinationType",
  "destinationCount",
  "scheduledDestinationCount",
  "retentionDays",
  "maxEntries",
  "remotePath",
  "trigger",
  "prunedFileCount",
  "pruneError",
  "uploadVerificationAttempts",
  "error",
  "expiresInHours",
  "checksumMismatchAccepted"
]);
function normalizePositiveInteger(value, allowed) {
  if (value === null || value === 0 || value === "0" || value === "forever" || value === "unlimited") return null;
  const parsed = Math.floor(Number(value));
  return allowed.includes(parsed) ? parsed : null;
}
__name(normalizePositiveInteger, "normalizePositiveInteger");
function normalizeAuditLogSettings(value) {
  const input = value && typeof value === "object" ? value : {};
  const retentionDays = normalizePositiveInteger(input.retentionDays, [7, 30, 90, 180, 365]);
  const maxEntries = normalizePositiveInteger(input.maxEntries, [1e3, 5e3, 1e4, 5e4]);
  if (retentionDays) return { retentionDays, maxEntries: null };
  if (maxEntries) return { retentionDays: null, maxEntries };
  if (input.retentionDays === null || input.retentionDays === 0 || input.retentionDays === "0") {
    return { retentionDays: null, maxEntries: null };
  }
  if (input.maxEntries === null || input.maxEntries === 0 || input.maxEntries === "0") {
    return { retentionDays: null, maxEntries: null };
  }
  return {
    ...DEFAULT_AUDIT_LOG_SETTINGS
  };
}
__name(normalizeAuditLogSettings, "normalizeAuditLogSettings");
function auditRequestMetadata(request) {
  const url = new URL(request.url);
  return {
    method: request.method,
    path: url.pathname,
    ip: request.headers.get("CF-Connecting-IP") || request.headers.get("X-Forwarded-For") || null,
    userAgent: request.headers.get("User-Agent") || null
  };
}
__name(auditRequestMetadata, "auditRequestMetadata");
function sanitizeMetadata(metadata) {
  const clean = {};
  for (const [key, value] of Object.entries(metadata)) {
    if (!ALLOWED_METADATA_KEYS.has(key)) continue;
    if (value === void 0 || value === null || value === "") continue;
    if (SENSITIVE_KEY_RE.test(key)) continue;
    if (Array.isArray(value)) {
      clean[key] = value.length;
      continue;
    }
    if (typeof value === "object") continue;
    clean[key] = value;
  }
  return clean;
}
__name(sanitizeMetadata, "sanitizeMetadata");
async function getAuditLogSettings(storage) {
  const raw = await storage.getConfigValue(AUDIT_LOG_SETTINGS_KEY);
  if (!raw) return { ...DEFAULT_AUDIT_LOG_SETTINGS };
  try {
    return normalizeAuditLogSettings(JSON.parse(raw));
  } catch {
    return { ...DEFAULT_AUDIT_LOG_SETTINGS };
  }
}
__name(getAuditLogSettings, "getAuditLogSettings");
async function saveAuditLogSettings(storage, settings) {
  const normalized = normalizeAuditLogSettings(settings);
  await storage.setConfigValue(AUDIT_LOG_SETTINGS_KEY, JSON.stringify(normalized));
  await applyAuditLogRetention(storage, normalized);
  return normalized;
}
__name(saveAuditLogSettings, "saveAuditLogSettings");
async function applyAuditLogRetention(storage, settings) {
  const current = settings || await getAuditLogSettings(storage);
  if (current.retentionDays) {
    const before = new Date(Date.now() - current.retentionDays * 24 * 60 * 60 * 1e3).toISOString();
    await storage.pruneAuditLogs(before);
  }
  if (current.maxEntries) {
    await storage.pruneAuditLogsToMax(current.maxEntries);
  }
}
__name(applyAuditLogRetention, "applyAuditLogRetention");
async function maybePruneAuditLogs(storage) {
  const now = Date.now();
  if (now - lastAuditCleanupAt < AUDIT_CLEANUP_INTERVAL_MS) return;
  if (Math.random() > AUDIT_CLEANUP_PROBABILITY) return;
  lastAuditCleanupAt = now;
  await applyAuditLogRetention(storage);
}
__name(maybePruneAuditLogs, "maybePruneAuditLogs");
async function insertAuditEvent(storage, event) {
  const metadata = sanitizeMetadata(event.metadata || {});
  let metadataJson = JSON.stringify(metadata);
  if (new TextEncoder().encode(metadataJson).byteLength > MAX_METADATA_BYTES) {
    metadataJson = JSON.stringify({ truncated: true });
  }
  await storage.createAuditLog({
    id: generateUUID(),
    actorUserId: event.actorUserId ?? null,
    action: event.action,
    category: event.category,
    level: event.level || "info",
    targetType: event.targetType ?? null,
    targetId: event.targetId ?? null,
    metadata: metadataJson,
    createdAt: (/* @__PURE__ */ new Date()).toISOString()
  });
  await maybePruneAuditLogs(storage);
}
__name(insertAuditEvent, "insertAuditEvent");
async function writeAuditEvent(storage, event) {
  try {
    await insertAuditEvent(storage, event);
  } catch (error) {
    console.error("audit log write failed", error);
  }
}
__name(writeAuditEvent, "writeAuditEvent");
async function safeWriteAuditEvent(env, event) {
  await writeAuditEvent(new StorageService(env.DB), event);
}
__name(safeWriteAuditEvent, "safeWriteAuditEvent");

// src/utils/totp.ts
var TOTP_STEP_SECONDS = 30;
var TOTP_DIGITS = 6;
var TOTP_WINDOW = 1;
function normalizeBase32(input) {
  const raw = String(input || "").toUpperCase();
  let out = "";
  for (const char of raw) {
    if (char === " " || char === "	" || char === "\n" || char === "\r" || char === "-") continue;
    out += char;
  }
  while (out.endsWith("=")) {
    out = out.slice(0, -1);
  }
  return out;
}
__name(normalizeBase32, "normalizeBase32");
function base32Decode(input) {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  const normalized = normalizeBase32(input);
  if (!normalized) return null;
  let bits2 = 0;
  let value = 0;
  const output = [];
  for (const char of normalized) {
    const idx = alphabet.indexOf(char);
    if (idx === -1) return null;
    value = value << 5 | idx;
    bits2 += 5;
    if (bits2 >= 8) {
      bits2 -= 8;
      output.push(value >> bits2 & 255);
    }
  }
  return output.length > 0 ? new Uint8Array(output) : null;
}
__name(base32Decode, "base32Decode");
async function hotp(secret, counter) {
  const counterBytes = new Uint8Array(8);
  let c = counter;
  for (let i = 7; i >= 0; i--) {
    counterBytes[i] = c & 255;
    c = Math.floor(c / 256);
  }
  const key = await crypto.subtle.importKey(
    "raw",
    secret,
    { name: "HMAC", hash: "SHA-1" },
    false,
    ["sign"]
  );
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, counterBytes));
  const offset = signature[signature.length - 1] & 15;
  const binary = (signature[offset] & 127) << 24 | (signature[offset + 1] & 255) << 16 | (signature[offset + 2] & 255) << 8 | signature[offset + 3] & 255;
  const otp = binary % 10 ** TOTP_DIGITS;
  return otp.toString().padStart(TOTP_DIGITS, "0");
}
__name(hotp, "hotp");
function normalizeToken(token) {
  return token.replace(/\s+/g, "");
}
__name(normalizeToken, "normalizeToken");
async function verifyTotpToken(secretRaw, tokenRaw, nowMs = Date.now()) {
  const token = normalizeToken(tokenRaw);
  if (!/^\d{6}$/.test(token)) return false;
  const secret = base32Decode(secretRaw);
  if (!secret) return false;
  const currentCounter = Math.floor(nowMs / 1e3 / TOTP_STEP_SECONDS);
  let matched = false;
  for (let delta = -TOTP_WINDOW; delta <= TOTP_WINDOW; delta++) {
    const expected = await hotp(secret, currentCounter + delta);
    const a = new TextEncoder().encode(expected);
    const b = new TextEncoder().encode(token);
    let diff = a.length ^ b.length;
    for (let i = 0; i < a.length && i < b.length; i++) {
      diff |= a[i] ^ b[i];
    }
    if (diff === 0) matched = true;
  }
  return matched;
}
__name(verifyTotpToken, "verifyTotpToken");
function isTotpEnabled(secretRaw) {
  return Boolean(secretRaw && normalizeBase32(secretRaw).length > 0);
}
__name(isTotpEnabled, "isTotpEnabled");

// src/utils/recovery-code.ts
var RECOVERY_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
var RECOVERY_ALPHABET_LENGTH = RECOVERY_ALPHABET.length;
var RECOVERY_MAX_UNBIASED_BYTE = Math.floor(256 / RECOVERY_ALPHABET_LENGTH) * RECOVERY_ALPHABET_LENGTH;
function normalizeRecoveryCode(raw) {
  return String(raw || "").toUpperCase().replace(/[^A-Z2-7]/g, "");
}
__name(normalizeRecoveryCode, "normalizeRecoveryCode");
function formatRecoveryCode(compact) {
  return compact.replace(/(.{4})/g, "$1 ").trim();
}
__name(formatRecoveryCode, "formatRecoveryCode");
function createRecoveryCode() {
  let compact = "";
  while (compact.length < 32) {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    for (const b of bytes) {
      if (b >= RECOVERY_MAX_UNBIASED_BYTE) continue;
      compact += RECOVERY_ALPHABET[b % RECOVERY_ALPHABET_LENGTH];
      if (compact.length >= 32) break;
    }
  }
  return formatRecoveryCode(compact.slice(0, 32));
}
__name(createRecoveryCode, "createRecoveryCode");
function recoveryCodeEquals(input, storedCode) {
  if (!storedCode) return false;
  const a = new TextEncoder().encode(normalizeRecoveryCode(input));
  const b = new TextEncoder().encode(normalizeRecoveryCode(storedCode));
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= a[i] ^ b[i];
  }
  return diff === 0;
}
__name(recoveryCodeEquals, "recoveryCodeEquals");

// src/utils/user-decryption.ts
function normalizeOptionalPublicKey(value) {
  if (value == null) return "";
  return String(value);
}
__name(normalizeOptionalPublicKey, "normalizeOptionalPublicKey");
function buildAccountKeys(user) {
  if (!user.privateKey) {
    return null;
  }
  const publicKey = normalizeOptionalPublicKey(user.publicKey);
  return {
    publicKeyEncryptionKeyPair: {
      wrappedPrivateKey: user.privateKey,
      publicKey,
      Object: "publicKeyEncryptionKeyPair"
    },
    Object: "privateKeys"
  };
}
__name(buildAccountKeys, "buildAccountKeys");
function buildMasterPasswordUnlock(user) {
  return {
    Kdf: {
      KdfType: user.kdfType,
      Iterations: user.kdfIterations,
      Memory: user.kdfMemory ?? null,
      Parallelism: user.kdfParallelism ?? null
    },
    MasterKeyEncryptedUserKey: user.key,
    MasterKeyWrappedUserKey: user.key,
    Salt: user.email.toLowerCase(),
    Object: "masterPasswordUnlock"
  };
}
__name(buildMasterPasswordUnlock, "buildMasterPasswordUnlock");
function buildUserDecryptionOptions(user) {
  return {
    HasMasterPassword: true,
    Object: "userDecryptionOptions",
    MasterPasswordUnlock: buildMasterPasswordUnlock(user),
    TrustedDeviceOption: null,
    KeyConnectorOption: null
  };
}
__name(buildUserDecryptionOptions, "buildUserDecryptionOptions");
function buildUserDecryptionCompat(user) {
  return {
    masterPasswordUnlock: {
      kdf: {
        kdfType: user.kdfType,
        iterations: user.kdfIterations,
        memory: user.kdfMemory ?? null,
        parallelism: user.kdfParallelism ?? null
      },
      masterKeyWrappedUserKey: user.key,
      masterKeyEncryptedUserKey: user.key,
      salt: user.email.toLowerCase()
    }
  };
}
__name(buildUserDecryptionCompat, "buildUserDecryptionCompat");

// src/handlers/accounts.ts
function looksLikeEncString(value) {
  if (!value) return false;
  const firstDot = value.indexOf(".");
  if (firstDot <= 0 || firstDot === value.length - 1) return false;
  const payload = value.slice(firstDot + 1);
  const parts = payload.split("|");
  return parts.length >= 2;
}
__name(looksLikeEncString, "looksLikeEncString");
function validateKdfParams(kdfType, kdfIterations, kdfMemory, kdfParallelism) {
  const type = kdfType ?? 0;
  if (type === 0) {
    if (typeof kdfIterations === "number" && kdfIterations < 1e5) {
      return "PBKDF2 iterations must be at least 100000";
    }
  } else if (type === 1) {
    if (typeof kdfIterations === "number" && kdfIterations < 2) {
      return "Argon2id iterations must be at least 2";
    }
    if (typeof kdfMemory === "number" && kdfMemory < 16) {
      return "Argon2id memory must be at least 16 MiB";
    }
    if (typeof kdfParallelism === "number" && kdfParallelism < 1) {
      return "Argon2id parallelism must be at least 1";
    }
  }
  return null;
}
__name(validateKdfParams, "validateKdfParams");
function normalizeTotpSecret(input) {
  const raw = String(input || "").toUpperCase();
  let out = "";
  for (const char of raw) {
    if (char === " " || char === "	" || char === "\n" || char === "\r" || char === "-") continue;
    out += char;
  }
  while (out.endsWith("=")) {
    out = out.slice(0, -1);
  }
  return out;
}
__name(normalizeTotpSecret, "normalizeTotpSecret");
function normalizeRecoveryCodeInput(input) {
  return String(input || "").toUpperCase().replace(/[^A-Z2-7]/g, "");
}
__name(normalizeRecoveryCodeInput, "normalizeRecoveryCodeInput");
function normalizeMasterPasswordHint(input) {
  const normalized = String(input || "").trim();
  return normalized ? normalized : null;
}
__name(normalizeMasterPasswordHint, "normalizeMasterPasswordHint");
function jwtSecretUnsafeReason(env) {
  const secret = (env.JWT_SECRET || "").trim();
  if (!secret) return "missing";
  if (secret === DEFAULT_DEV_SECRET) return "default";
  if (secret.length < LIMITS.auth.jwtSecretMinLength) return "too_short";
  return null;
}
__name(jwtSecretUnsafeReason, "jwtSecretUnsafeReason");
async function verifyUserSecret(auth, user, secret) {
  const normalized = String(secret || "").trim();
  if (!normalized) return false;
  return auth.verifyPassword(normalized, user.masterPasswordHash, user.email);
}
__name(verifyUserSecret, "verifyUserSecret");
function toProfile(user, env) {
  void env;
  const accountKeys = buildAccountKeys(user);
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    emailVerified: true,
    premium: true,
    premiumFromOrganization: false,
    usesKeyConnector: false,
    masterPasswordHint: user.masterPasswordHint,
    culture: "en-US",
    twoFactorEnabled: !!user.totpSecret,
    key: user.key,
    privateKey: user.privateKey,
    accountKeys,
    securityStamp: user.securityStamp || user.id,
    organizations: [],
    providers: [],
    providerOrganizations: [],
    forcePasswordReset: false,
    avatarColor: null,
    creationDate: user.createdAt,
    verifyDevices: user.verifyDevices,
    role: user.role,
    status: user.status,
    object: "profile"
  };
}
__name(toProfile, "toProfile");
async function handleRegister(request, env) {
  const storage = new StorageService(env.DB);
  const unsafe = jwtSecretUnsafeReason(env);
  if (unsafe) {
    const message = unsafe === "missing" ? "JWT_SECRET is not set" : unsafe === "default" ? "JWT_SECRET is using the default/sample value. Please change it." : "JWT_SECRET must be at least 32 characters";
    return errorResponse(message, 400);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const email = body.email?.toLowerCase().trim();
  const name = body.name?.trim() || email;
  const masterPasswordHash = body.masterPasswordHash;
  const key = body.key;
  const privateKey = body.keys?.encryptedPrivateKey;
  const publicKey = body.keys?.publicKey;
  const inviteCode = (body.inviteCode || "").trim();
  const masterPasswordHint = normalizeMasterPasswordHint(body.masterPasswordHint);
  if (!email || !masterPasswordHash || !key) {
    return errorResponse("Email, masterPasswordHash, and key are required", 400);
  }
  if (!email.includes("@") || email.length < 3) {
    return errorResponse("Invalid email address", 400);
  }
  if (!privateKey || !publicKey) {
    return errorResponse("Private key and public key are required", 400);
  }
  if (!looksLikeEncString(key)) {
    return errorResponse("key is not a valid encrypted string", 400);
  }
  if (!looksLikeEncString(privateKey)) {
    return errorResponse("encryptedPrivateKey is not a valid encrypted string", 400);
  }
  if (masterPasswordHint && masterPasswordHint.length > 120) {
    return errorResponse("masterPasswordHint must be 120 characters or fewer", 400);
  }
  const kdfErr = validateKdfParams(body.kdf, body.kdfIterations, body.kdfMemory, body.kdfParallelism);
  if (kdfErr) return errorResponse(kdfErr, 400);
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const auth = new AuthService(env);
  const serverHash = await auth.hashPasswordServer(masterPasswordHash, email);
  const user = {
    id: generateUUID(),
    email,
    name: name || email,
    masterPasswordHint,
    masterPasswordHash: serverHash,
    key,
    privateKey,
    publicKey,
    kdfType: body.kdf ?? 0,
    kdfIterations: body.kdfIterations ?? LIMITS.auth.defaultKdfIterations,
    kdfMemory: body.kdfMemory,
    kdfParallelism: body.kdfParallelism,
    securityStamp: generateUUID(),
    role: "user",
    status: "active",
    verifyDevices: true,
    totpSecret: null,
    totpRecoveryCode: null,
    apiKey: null,
    createdAt: now,
    updatedAt: now
  };
  const userCount = await storage.getUserCount();
  if (userCount === 0) {
    user.role = "admin";
    const created = await storage.createFirstUser(user);
    if (!created) {
      return errorResponse("Registration is temporarily unavailable, retry once", 409);
    }
    await storage.setRegistered();
    await writeAuditEvent(storage, {
      actorUserId: user.id,
      action: "user.register.first_admin",
      targetType: "user",
      targetId: user.id,
      category: "security",
      level: "security",
      metadata: { email: user.email, ...auditRequestMetadata(request) }
    });
    return jsonResponse({ success: true, role: user.role }, 200);
  }
  if (!inviteCode) {
    return errorResponse("Invite code is required", 403);
  }
  try {
    await storage.createUser(user);
  } catch (error) {
    const msg = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
    if (msg.includes("unique") || msg.includes("constraint")) {
      return errorResponse("Email already registered", 409);
    }
    throw error;
  }
  const inviteMarked = await storage.markInviteUsed(inviteCode, user.id);
  if (!inviteMarked) {
    await storage.deleteUserById(user.id);
    return errorResponse("Invite code is invalid or expired", 403);
  }
  await writeAuditEvent(storage, {
    actorUserId: user.id,
    action: "user.register.invite",
    targetType: "user",
    targetId: user.id,
    category: "security",
    level: "info",
    metadata: { email: user.email, inviteCode, ...auditRequestMetadata(request) }
  });
  return jsonResponse({ success: true, role: user.role }, 200);
}
__name(handleRegister, "handleRegister");
async function handleGetPasswordHint(request, env) {
  const storage = new StorageService(env.DB);
  const clientIdentifier = getClientIdentifier(request);
  if (!clientIdentifier) {
    return errorResponse("Client IP is required", 403);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const email = String(body.email || "").trim().toLowerCase();
  if (!email) {
    return errorResponse("Email is required", 400);
  }
  const rateLimit = new RateLimitService(env.DB);
  const minuteBudget = await rateLimit.consumeBudgetWithWindow(
    `${clientIdentifier}:password-hint`,
    LIMITS.rateLimit.passwordHintRequestsPerMinute,
    60
  );
  if (!minuteBudget.allowed) {
    return new Response(
      JSON.stringify({
        error: "Too many requests",
        error_description: `Rate limit exceeded. Try again in ${minuteBudget.retryAfterSeconds || 60} seconds.`
      }),
      {
        status: 429,
        headers: {
          "Content-Type": "application/json",
          "Retry-After": String(minuteBudget.retryAfterSeconds || 60),
          "X-RateLimit-Remaining": "0"
        }
      }
    );
  }
  const hourlyBudget = await rateLimit.consumeBudgetWithWindow(
    `${clientIdentifier}:password-hint-hour`,
    LIMITS.rateLimit.passwordHintRequestsPerHour,
    60 * 60
  );
  if (!hourlyBudget.allowed) {
    return new Response(
      JSON.stringify({
        error: "Too many requests",
        error_description: `Rate limit exceeded. Try again in ${hourlyBudget.retryAfterSeconds || 3600} seconds.`
      }),
      {
        status: 429,
        headers: {
          "Content-Type": "application/json",
          "Retry-After": String(hourlyBudget.retryAfterSeconds || 3600),
          "X-RateLimit-Remaining": "0"
        }
      }
    );
  }
  const user = await storage.getUser(email);
  const hint = user?.status === "active" ? normalizeMasterPasswordHint(user.masterPasswordHint) : null;
  return jsonResponse({
    object: "passwordHint",
    hasHint: !!hint,
    masterPasswordHint: hint
  });
}
__name(handleGetPasswordHint, "handleGetPasswordHint");
async function handleGetProfile(request, env, userId) {
  void request;
  const storage = new StorageService(env.DB);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  return jsonResponse(toProfile(user, env));
}
__name(handleGetProfile, "handleGetProfile");
async function handleUpdateProfile(request, env, userId) {
  const storage = new StorageService(env.DB);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const masterPasswordHint = normalizeMasterPasswordHint(body.masterPasswordHint);
  if (masterPasswordHint && masterPasswordHint.length > 120) {
    return errorResponse("masterPasswordHint must be 120 characters or fewer", 400);
  }
  user.masterPasswordHint = masterPasswordHint;
  user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveUser(user);
  await writeAuditEvent(storage, {
    actorUserId: user.id,
    action: "account.profile.update",
    category: "security",
    level: "info",
    targetType: "user",
    targetId: user.id,
    metadata: {
      updatedMasterPasswordHint: true,
      ...auditRequestMetadata(request)
    }
  });
  return jsonResponse(toProfile(user, env));
}
__name(handleUpdateProfile, "handleUpdateProfile");
async function handleSetVerifyDevices(request, env, userId) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (typeof body.verifyDevices !== "boolean") {
    return errorResponse("verifyDevices must be true or false", 400);
  }
  const verified = await verifyUserSecret(auth, user, body.secret || body.masterPasswordHash);
  if (!verified) {
    return errorResponse("User verification failed.", 400);
  }
  user.verifyDevices = body.verifyDevices;
  user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveUser(user);
  await writeAuditEvent(storage, {
    actorUserId: user.id,
    action: "account.verify_devices.update",
    category: "security",
    level: "security",
    targetType: "user",
    targetId: user.id,
    metadata: {
      verifyDevices: user.verifyDevices,
      ...auditRequestMetadata(request)
    }
  });
  return new Response(null, { status: 200 });
}
__name(handleSetVerifyDevices, "handleSetVerifyDevices");
async function handleSetKeys(request, env, userId) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const user = await storage.getUserById(userId);
  if (!user) {
    return errorResponse("User not found", 404);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.masterPasswordHash) {
    return errorResponse("masterPasswordHash is required", 400);
  }
  const passwordValid = await auth.verifyPassword(body.masterPasswordHash, user.masterPasswordHash, user.email);
  if (!passwordValid) {
    return errorResponse("Invalid password", 400);
  }
  if (body.key && !looksLikeEncString(body.key)) {
    return errorResponse("key is not a valid encrypted string", 400);
  }
  if (body.encryptedPrivateKey && !looksLikeEncString(body.encryptedPrivateKey)) {
    return errorResponse("encryptedPrivateKey is not a valid encrypted string", 400);
  }
  if (body.key) user.key = body.key;
  if (body.encryptedPrivateKey) user.privateKey = body.encryptedPrivateKey;
  if (body.publicKey) user.publicKey = body.publicKey;
  user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveUser(user);
  await writeAuditEvent(storage, {
    actorUserId: user.id,
    action: "account.keys.update",
    category: "security",
    level: "security",
    targetType: "user",
    targetId: user.id,
    metadata: {
      updatedKey: !!body.key,
      updatedPrivateKey: !!body.encryptedPrivateKey,
      updatedPublicKey: !!body.publicKey,
      ...auditRequestMetadata(request)
    }
  });
  return handleGetProfile(request, env, userId);
}
__name(handleSetKeys, "handleSetKeys");
async function handleChangePassword(request, env, userId) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const currentHash = body.currentPasswordHash || body.masterPasswordHash;
  if (!currentHash) return errorResponse("Current password hash is required", 400);
  const valid = await auth.verifyPassword(currentHash, user.masterPasswordHash, user.email);
  if (!valid) return errorResponse("Invalid password", 400);
  if (!body.newMasterPasswordHash) {
    return errorResponse("newMasterPasswordHash is required", 400);
  }
  const nextKey = body.newKey || body.key;
  const nextPrivateKey = body.newEncryptedPrivateKey || body.encryptedPrivateKey;
  const nextPublicKey = body.newPublicKey || body.publicKey;
  if (nextKey && !looksLikeEncString(nextKey)) {
    return errorResponse("new key is not a valid encrypted string", 400);
  }
  if (nextPrivateKey && !looksLikeEncString(nextPrivateKey)) {
    return errorResponse("new encryptedPrivateKey is not a valid encrypted string", 400);
  }
  const kdfErr = validateKdfParams(body.kdf ?? user.kdfType, body.kdfIterations, body.kdfMemory, body.kdfParallelism);
  if (kdfErr) return errorResponse(kdfErr, 400);
  user.masterPasswordHash = await auth.hashPasswordServer(body.newMasterPasswordHash, user.email);
  if (nextKey) user.key = nextKey;
  if (nextPrivateKey) user.privateKey = nextPrivateKey;
  if (nextPublicKey) user.publicKey = nextPublicKey;
  if (typeof body.kdf === "number") user.kdfType = body.kdf;
  if (typeof body.kdfIterations === "number") user.kdfIterations = body.kdfIterations;
  if (typeof body.kdfMemory === "number") user.kdfMemory = body.kdfMemory;
  if (typeof body.kdfParallelism === "number") user.kdfParallelism = body.kdfParallelism;
  user.securityStamp = generateUUID();
  user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveUser(user);
  await storage.deleteRefreshTokensByUserId(user.id);
  AuthService.invalidateUserCache(user.id);
  await writeAuditEvent(storage, {
    actorUserId: user.id,
    action: "user.password.change",
    targetType: "user",
    targetId: user.id,
    category: "security",
    level: "security",
    metadata: { email: user.email, ...auditRequestMetadata(request) }
  });
  return new Response(null, { status: 200 });
}
__name(handleChangePassword, "handleChangePassword");
async function handleGetTotpStatus(request, env, userId) {
  void request;
  const storage = new StorageService(env.DB);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  return jsonResponse({
    enabled: !!user.totpSecret,
    object: "twoFactor"
  });
}
__name(handleGetTotpStatus, "handleGetTotpStatus");
async function handleSetTotpStatus(request, env, userId) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (body.enabled === true) {
    const normalizedSecret = normalizeTotpSecret(body.secret || "");
    if (!isTotpEnabled(normalizedSecret)) {
      return errorResponse("Invalid TOTP secret", 400);
    }
    if (!body.token) {
      return errorResponse("TOTP token is required", 400);
    }
    const verified = await verifyTotpToken(normalizedSecret, body.token);
    if (!verified) {
      return errorResponse("Invalid TOTP token", 400);
    }
    user.totpSecret = normalizedSecret;
    if (!user.totpRecoveryCode) {
      user.totpRecoveryCode = createRecoveryCode();
    }
    user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
    await storage.saveUser(user);
    await storage.deleteRefreshTokensByUserId(user.id);
    AuthService.invalidateUserCache(user.id);
    await writeAuditEvent(storage, {
      actorUserId: user.id,
      action: "account.totp.enable",
      category: "security",
      level: "security",
      targetType: "user",
      targetId: user.id,
      metadata: auditRequestMetadata(request)
    });
    return jsonResponse({ enabled: true, recoveryCode: user.totpRecoveryCode, object: "twoFactor" });
  }
  if (body.enabled === false) {
    if (!body.masterPasswordHash) {
      return errorResponse("masterPasswordHash is required to disable TOTP", 400);
    }
    const valid = await auth.verifyPassword(body.masterPasswordHash, user.masterPasswordHash, user.email);
    if (!valid) return errorResponse("Invalid password", 400);
    user.totpSecret = null;
    user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
    await storage.saveUser(user);
    await storage.deleteRefreshTokensByUserId(user.id);
    AuthService.invalidateUserCache(user.id);
    await writeAuditEvent(storage, {
      actorUserId: user.id,
      action: "account.totp.disable",
      category: "security",
      level: "security",
      targetType: "user",
      targetId: user.id,
      metadata: auditRequestMetadata(request)
    });
    return jsonResponse({ enabled: false, object: "twoFactor" });
  }
  return errorResponse("enabled must be true or false", 400);
}
__name(handleSetTotpStatus, "handleSetTotpStatus");
async function handleGetTotpRecoveryCode(request, env, userId) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  let body;
  try {
    const contentType = request.headers.get("content-type") || "";
    if (contentType.includes("application/x-www-form-urlencoded")) {
      const formData = await request.formData();
      body = Object.fromEntries(formData.entries());
    } else {
      body = await request.json();
    }
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const currentHash = String(body.masterPasswordHash || body.master_password_hash || body.password || "").trim();
  if (!currentHash) return errorResponse("masterPasswordHash is required", 400);
  const valid = await auth.verifyPassword(currentHash, user.masterPasswordHash, user.email);
  if (!valid) return errorResponse("Invalid password", 400);
  if (!user.totpRecoveryCode) {
    user.totpRecoveryCode = createRecoveryCode();
    user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
    await storage.saveUser(user);
  }
  return jsonResponse({
    code: user.totpRecoveryCode,
    object: "twoFactorRecover"
  });
}
__name(handleGetTotpRecoveryCode, "handleGetTotpRecoveryCode");
async function handleRecoverTwoFactor(request, env) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const rateLimit = new RateLimitService(env.DB);
  let body;
  try {
    const contentType = request.headers.get("content-type") || "";
    if (contentType.includes("application/x-www-form-urlencoded")) {
      const formData = await request.formData();
      body = Object.fromEntries(formData.entries());
    } else {
      body = await request.json();
    }
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const email = String(body.email || body.username || "").trim().toLowerCase();
  const masterPasswordHash = String(body.masterPasswordHash || body.password || "").trim();
  const recoveryCode = normalizeRecoveryCodeInput(String(body.recoveryCode || body.twoFactorToken || body.recovery_code || ""));
  const clientIdentifier = getClientIdentifier(request);
  if (!clientIdentifier) {
    return errorResponse("Client IP is required", 403);
  }
  const recoverLimitKey = `${clientIdentifier}:recover-2fa:${email || "unknown"}`;
  const recoverAttemptCheck = await rateLimit.checkLoginAttempt(recoverLimitKey);
  if (!recoverAttemptCheck.allowed) {
    return errorResponse(
      `Too many failed recovery attempts. Try again in ${Math.ceil((recoverAttemptCheck.retryAfterSeconds || 60) / 60)} minutes.`,
      429
    );
  }
  if (!email || !masterPasswordHash || !recoveryCode) {
    return errorResponse("Email, masterPasswordHash and recoveryCode are required", 400);
  }
  const user = await storage.getUser(email);
  if (!user || user.status !== "active") {
    await rateLimit.recordFailedLogin(recoverLimitKey);
    return errorResponse("Invalid credentials or recovery code", 400);
  }
  const validPassword = await auth.verifyPassword(masterPasswordHash, user.masterPasswordHash, user.email);
  if (!validPassword) {
    await rateLimit.recordFailedLogin(recoverLimitKey);
    return errorResponse("Invalid credentials or recovery code", 400);
  }
  if (!recoveryCodeEquals(recoveryCode, user.totpRecoveryCode)) {
    await rateLimit.recordFailedLogin(recoverLimitKey);
    return errorResponse("Invalid credentials or recovery code", 400);
  }
  user.totpSecret = null;
  user.totpRecoveryCode = createRecoveryCode();
  user.securityStamp = generateUUID();
  user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveUser(user);
  await storage.deleteRefreshTokensByUserId(user.id);
  AuthService.invalidateUserCache(user.id);
  await rateLimit.clearLoginAttempts(recoverLimitKey);
  await safeWriteAuditEvent(env, {
    actorUserId: user.id,
    action: "account.totp.recover",
    category: "security",
    level: "security",
    targetType: "user",
    targetId: user.id,
    metadata: auditRequestMetadata(request)
  });
  return jsonResponse({
    success: true,
    twoFactorEnabled: false,
    newRecoveryCode: user.totpRecoveryCode,
    object: "twoFactorRecovery"
  });
}
__name(handleRecoverTwoFactor, "handleRecoverTwoFactor");
async function handleGetRevisionDate(request, env, userId) {
  void request;
  const storage = new StorageService(env.DB);
  const revisionDate = await storage.getRevisionDate(userId);
  const timestamp = new Date(revisionDate).getTime();
  return jsonResponse(timestamp);
}
__name(handleGetRevisionDate, "handleGetRevisionDate");
async function handleVerifyPassword(request, env, userId) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const user = await storage.getUserById(userId);
  if (!user) {
    return errorResponse("User not found", 404);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.masterPasswordHash) {
    return errorResponse("masterPasswordHash is required", 400);
  }
  const valid = await auth.verifyPassword(body.masterPasswordHash, user.masterPasswordHash, user.email);
  if (!valid) {
    return errorResponse("Invalid password", 400);
  }
  return new Response(null, { status: 200 });
}
__name(handleVerifyPassword, "handleVerifyPassword");
async function handleGetApiKey(request, env, userId) {
  return apiKey(request, env, userId, false);
}
__name(handleGetApiKey, "handleGetApiKey");
async function handleRotateApiKey(request, env, userId) {
  return apiKey(request, env, userId, true);
}
__name(handleRotateApiKey, "handleRotateApiKey");
async function apiKey(request, env, userId, rotate) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  let body;
  try {
    const contentType = request.headers.get("content-type") || "";
    if (contentType.includes("application/x-www-form-urlencoded")) {
      const formData = await request.formData();
      body = Object.fromEntries(formData.entries());
    } else {
      body = await request.json();
    }
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const currentHash = String(body.masterPasswordHash || body.master_password_hash || body.password || "").trim();
  if (!currentHash) return errorResponse("masterPasswordHash is required", 400);
  const valid = await auth.verifyPassword(currentHash, user.masterPasswordHash, user.email);
  if (!valid) return errorResponse("Invalid password", 400);
  if (rotate || user.apiKey === null) {
    user.apiKey = randomStringAlphanum(LIMITS.auth.clientSecretLength);
    if (rotate) {
      user.securityStamp = generateUUID();
      await storage.deleteRefreshTokensByUserId(user.id);
    }
    user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
    await storage.saveUser(user);
    AuthService.invalidateUserCache(user.id);
    await writeAuditEvent(storage, {
      actorUserId: user.id,
      action: rotate ? "account.api_key.rotate" : "account.api_key.create",
      category: "security",
      level: rotate ? "security" : "info",
      targetType: "user",
      targetId: user.id,
      metadata: auditRequestMetadata(request)
    });
  }
  return jsonResponse({
    apiKey: user.apiKey,
    revisionDate: user.updatedAt,
    object: "apiKey"
  });
}
__name(apiKey, "apiKey");
function randomStringAlphanum(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  const maxUnbiased = Math.floor(256 / chars.length) * chars.length;
  const bytes = new Uint8Array(Math.max(16, length));
  while (result.length < length) {
    crypto.getRandomValues(bytes);
    for (const value of bytes) {
      if (value >= maxUnbiased) continue;
      result += chars[value % chars.length];
      if (result.length >= length) break;
    }
  }
  return result;
}
__name(randomStringAlphanum, "randomStringAlphanum");

// src/utils/direct-upload.ts
function buildDirectUploadUrl(request, path, token) {
  const version = "2023-11-03";
  const expiresAt = "2099-12-31T23:59:59Z";
  const origin = new URL(request.url).origin;
  return `${origin}${path}?sv=${encodeURIComponent(version)}&se=${encodeURIComponent(expiresAt)}&token=${encodeURIComponent(token)}`;
}
__name(buildDirectUploadUrl, "buildDirectUploadUrl");
function getSafeJwtSecret(env) {
  const secret = (env.JWT_SECRET || "").trim();
  if (!secret || secret.length < LIMITS.auth.jwtSecretMinLength || secret === DEFAULT_DEV_SECRET) {
    return null;
  }
  return secret;
}
__name(getSafeJwtSecret, "getSafeJwtSecret");
function parseContentLength(request) {
  const raw = request.headers.get("content-length");
  if (!raw) return null;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) return null;
  return Math.floor(value);
}
__name(parseContentLength, "parseContentLength");
async function parseDirectUploadPayload(request, options) {
  const {
    expectedSize = null,
    expectedFileName = null,
    maxFileSize,
    tooLargeMessage,
    missingBodyMessage = "No file uploaded",
    contentLengthRequiredMessage = "Content-Length is required for direct uploads",
    sizeMismatchMessage,
    fileNameMismatchMessage
  } = options;
  const contentType = request.headers.get("content-type") || "";
  if (contentType.includes("multipart/form-data")) {
    const formData = await request.formData();
    const file = formData.get("data");
    if (!file) {
      return errorResponse(missingBodyMessage, 400);
    }
    if (file.size > maxFileSize) {
      return errorResponse(tooLargeMessage, 413);
    }
    if (expectedFileName && file.name !== expectedFileName) {
      return errorResponse(fileNameMismatchMessage || "File name does not match.", 400);
    }
    if (expectedSize !== null && expectedSize !== void 0 && file.size !== expectedSize) {
      return errorResponse(sizeMismatchMessage || "File size does not match.", 400);
    }
    return {
      body: file.stream(),
      contentType: file.type || "application/octet-stream",
      size: file.size
    };
  }
  if (!request.body) {
    return errorResponse(missingBodyMessage, 400);
  }
  const declaredSize = parseContentLength(request);
  const uploadSize = declaredSize ?? (expectedSize && expectedSize > 0 ? expectedSize : null);
  if (uploadSize === null) {
    return errorResponse(contentLengthRequiredMessage, 400);
  }
  if (uploadSize > maxFileSize) {
    return errorResponse(tooLargeMessage, 413);
  }
  if (expectedSize !== null && expectedSize !== void 0 && uploadSize !== expectedSize) {
    return errorResponse(sizeMismatchMessage || "File size does not match.", 400);
  }
  return {
    body: request.body,
    contentType: contentType || "application/octet-stream",
    size: uploadSize
  };
}
__name(parseDirectUploadPayload, "parseDirectUploadPayload");

// src/utils/device.ts
var DEFAULT_DEVICE_NAME = "Unknown device";
var DEFAULT_DEVICE_TYPE = 14;
function decodeBase64UrlUtf8(value) {
  try {
    const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
    const padding = normalized.length % 4;
    const padded = padding === 0 ? normalized : normalized + "=".repeat(4 - padding);
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return new TextDecoder().decode(bytes);
  } catch {
    return null;
  }
}
__name(decodeBase64UrlUtf8, "decodeBase64UrlUtf8");
function normalizeDeviceIdentifier(value) {
  if (!value) return null;
  const normalized = String(value).trim();
  if (!normalized) return null;
  return normalized.slice(0, 128);
}
__name(normalizeDeviceIdentifier, "normalizeDeviceIdentifier");
function normalizeDeviceName(value) {
  const normalized = String(value || "").trim();
  if (!normalized) return DEFAULT_DEVICE_NAME;
  return normalized.slice(0, 128);
}
__name(normalizeDeviceName, "normalizeDeviceName");
function parseDeviceType(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(0, Math.floor(value));
  }
  const parsed = Number.parseInt(String(value || ""), 10);
  if (Number.isFinite(parsed) && parsed >= 0) return parsed;
  return DEFAULT_DEVICE_TYPE;
}
__name(parseDeviceType, "parseDeviceType");
function readAuthRequestDeviceInfo(body, request) {
  const bodyIdentifier = body.deviceIdentifier || body.device_identifier;
  const headerIdentifier = request.headers.get("X-Device-Identifier") || void 0;
  const bodyName = body.deviceName || body.device_name;
  const headerName = request.headers.get("X-Device-Name") || void 0;
  const bodyType = body.deviceType || body.device_type;
  const headerType = request.headers.get("Device-Type") || void 0;
  return {
    deviceIdentifier: normalizeDeviceIdentifier(bodyIdentifier || headerIdentifier),
    deviceName: normalizeDeviceName(bodyName || headerName),
    deviceType: parseDeviceType(bodyType || headerType)
  };
}
__name(readAuthRequestDeviceInfo, "readAuthRequestDeviceInfo");
function readKnownDeviceProbe(request) {
  const encodedEmail = request.headers.get("X-Request-Email") || "";
  const decodedEmail = decodeBase64UrlUtf8(encodedEmail);
  const fallbackRawEmail = request.headers.get("X-Request-Email");
  const email = (decodedEmail || fallbackRawEmail || "").trim().toLowerCase() || null;
  const deviceIdentifier = normalizeDeviceIdentifier(request.headers.get("X-Device-Identifier"));
  return { email, deviceIdentifier };
}
__name(readKnownDeviceProbe, "readKnownDeviceProbe");
function readActingDeviceIdentifier(request) {
  return normalizeDeviceIdentifier(request.headers.get("X-NodeWarden-Acting-Device-Id"));
}
__name(readActingDeviceIdentifier, "readActingDeviceIdentifier");

// src/services/blob-store.ts
var DEFAULT_CONTENT_TYPE = "application/octet-stream";
var KV_MAX_OBJECT_BYTES = 25 * 1024 * 1024;
function hasR2Storage(env) {
  return !!env.ATTACHMENTS;
}
__name(hasR2Storage, "hasR2Storage");
function hasKvStorage(env) {
  return !!env.ATTACHMENTS_KV;
}
__name(hasKvStorage, "hasKvStorage");
function getBlobStorageKind(env) {
  if (hasR2Storage(env)) return "r2";
  if (hasKvStorage(env)) return "kv";
  return null;
}
__name(getBlobStorageKind, "getBlobStorageKind");
function getBlobStorageMaxBytes(env, configuredLimit) {
  if (getBlobStorageKind(env) === "kv") {
    return Math.min(configuredLimit, KV_MAX_OBJECT_BYTES);
  }
  return configuredLimit;
}
__name(getBlobStorageMaxBytes, "getBlobStorageMaxBytes");
function getAttachmentObjectKey(cipherId, attachmentId) {
  return `${cipherId}/${attachmentId}`;
}
__name(getAttachmentObjectKey, "getAttachmentObjectKey");
function getSendFileObjectKey(sendId, fileId) {
  return `sends/${sendId}/${fileId}`;
}
__name(getSendFileObjectKey, "getSendFileObjectKey");
async function putBlobObject(env, key, value, options) {
  const contentType = options.contentType || DEFAULT_CONTENT_TYPE;
  if (hasR2Storage(env)) {
    await env.ATTACHMENTS.put(key, value, {
      httpMetadata: { contentType },
      customMetadata: options.customMetadata
    });
    return;
  }
  if (hasKvStorage(env)) {
    if (options.size > KV_MAX_OBJECT_BYTES) {
      throw new Error("KV object too large");
    }
    const metadata = {
      size: options.size,
      contentType,
      customMetadata: options.customMetadata || null
    };
    await env.ATTACHMENTS_KV.put(key, value, { metadata });
    return;
  }
  throw new Error("Attachment storage is not configured");
}
__name(putBlobObject, "putBlobObject");
async function getBlobObject(env, key) {
  if (hasR2Storage(env)) {
    const object = await env.ATTACHMENTS.get(key);
    if (!object) return null;
    return {
      body: object.body,
      size: Number(object.size) || 0,
      contentType: object.httpMetadata?.contentType || DEFAULT_CONTENT_TYPE
    };
  }
  if (hasKvStorage(env)) {
    const result = await env.ATTACHMENTS_KV.getWithMetadata(key, "arrayBuffer");
    if (!result.value) return null;
    const sizeFromMeta = Number(result.metadata?.size || 0);
    const size = sizeFromMeta > 0 ? sizeFromMeta : result.value.byteLength;
    const body = new Response(result.value).body;
    return {
      body,
      size,
      contentType: result.metadata?.contentType || DEFAULT_CONTENT_TYPE
    };
  }
  return null;
}
__name(getBlobObject, "getBlobObject");
async function deleteBlobObject(env, key) {
  if (hasR2Storage(env)) {
    await env.ATTACHMENTS.delete(key);
    return;
  }
  if (hasKvStorage(env)) {
    await env.ATTACHMENTS_KV.delete(key);
    return;
  }
}
__name(deleteBlobObject, "deleteBlobObject");

// src/handlers/attachments.ts
function notifyVaultSyncForRequest(request, env, userId, revisionDate) {
  notifyUserVaultSync(env, userId, revisionDate, readActingDeviceIdentifier(request));
}
__name(notifyVaultSyncForRequest, "notifyVaultSyncForRequest");
async function writeAttachmentAudit(storage, request, userId, action, metadata) {
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action,
    category: "data",
    level: action.includes("delete") ? "security" : "info",
    targetType: "attachment",
    targetId: typeof metadata.id === "string" ? metadata.id : null,
    metadata: {
      ...metadata,
      ...auditRequestMetadata(request)
    }
  });
}
__name(writeAttachmentAudit, "writeAttachmentAudit");
function formatSize(bytes) {
  if (bytes < 1024) return `${bytes} Bytes`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}
__name(formatSize, "formatSize");
async function runWithConcurrency(items, concurrency, worker) {
  if (items.length === 0) return;
  const limit = Math.max(1, concurrency);
  for (let index = 0; index < items.length; index += limit) {
    await Promise.all(items.slice(index, index + limit).map(worker));
  }
}
__name(runWithConcurrency, "runWithConcurrency");
async function processAttachmentUpload(request, env, attachment, cipherId) {
  const storage = new StorageService(env.DB);
  const maxFileSize = getBlobStorageMaxBytes(env, LIMITS.attachment.maxFileSizeBytes);
  const upload = await parseDirectUploadPayload(request, {
    expectedSize: Number(attachment.size) || 0,
    maxFileSize,
    tooLargeMessage: `File too large. Maximum size is ${Math.floor(maxFileSize / (1024 * 1024))}MB`
  });
  if (upload instanceof Response) {
    return upload;
  }
  const path = getAttachmentObjectKey(cipherId, attachment.id);
  try {
    await putBlobObject(env, path, upload.body, {
      size: upload.size,
      contentType: upload.contentType,
      customMetadata: {
        cipherId,
        attachmentId: attachment.id
      }
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes("KV object too large")) {
      return errorResponse(`File too large. Maximum size is ${Math.floor(maxFileSize / (1024 * 1024))}MB`, 413);
    }
    return errorResponse("Attachment storage is not configured", 500);
  }
  if (upload.size !== attachment.size) {
    attachment.size = upload.size;
    attachment.sizeName = formatSize(upload.size);
    await storage.saveAttachment(attachment);
  }
  const revisionInfo = await storage.updateCipherRevisionDate(cipherId);
  if (revisionInfo) {
    notifyVaultSyncForRequest(request, env, revisionInfo.userId, revisionInfo.revisionDate);
  }
  return new Response(null, { status: 201 });
}
__name(processAttachmentUpload, "processAttachmentUpload");
async function handleCreateAttachment(request, env, userId, cipherId) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(cipherId);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.fileName || !body.key) {
    return errorResponse("fileName and key are required", 400);
  }
  const fileSize = body.fileSize || 0;
  const attachmentId = generateUUID();
  const attachment = {
    id: attachmentId,
    cipherId,
    fileName: body.fileName,
    size: fileSize,
    sizeName: formatSize(fileSize),
    key: body.key
  };
  await storage.saveAttachment(attachment);
  await storage.addAttachmentToCipher(cipherId, attachmentId);
  const revisionInfo = await storage.updateCipherRevisionDate(cipherId);
  if (revisionInfo) {
    notifyVaultSyncForRequest(request, env, revisionInfo.userId, revisionInfo.revisionDate);
  }
  const updatedCipher = await storage.getCipher(cipherId);
  const attachments = await storage.getAttachmentsByCipher(cipherId);
  const jwtSecret = getSafeJwtSecret(env);
  if (!jwtSecret) {
    return errorResponse("Server configuration error", 500);
  }
  const uploadToken = await createAttachmentUploadToken(userId, cipherId, attachmentId, jwtSecret);
  return jsonResponse({
    object: "attachment-fileUpload",
    attachmentId,
    url: buildDirectUploadUrl(request, `/api/ciphers/${cipherId}/attachment/${attachmentId}`, uploadToken),
    fileUploadType: 1,
    cipherResponse: cipherToResponse(updatedCipher, attachments)
  });
}
__name(handleCreateAttachment, "handleCreateAttachment");
async function handleUploadAttachment(request, env, userId, cipherId, attachmentId) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(cipherId);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  const attachment = await storage.getAttachment(attachmentId);
  if (!attachment || attachment.cipherId !== cipherId) {
    return errorResponse("Attachment not found", 404);
  }
  return processAttachmentUpload(request, env, attachment, cipherId);
}
__name(handleUploadAttachment, "handleUploadAttachment");
async function handlePublicUploadAttachment(request, env, cipherId, attachmentId) {
  const jwtSecret = getSafeJwtSecret(env);
  if (!jwtSecret) {
    return errorResponse("Server configuration error", 500);
  }
  const token = new URL(request.url).searchParams.get("token");
  if (!token) {
    return errorResponse("Token required", 401);
  }
  const claims = await verifyAttachmentUploadToken(token, jwtSecret);
  if (!claims) {
    return errorResponse("Invalid or expired token", 401);
  }
  if (claims.cipherId !== cipherId || claims.attachmentId !== attachmentId) {
    return errorResponse("Token mismatch", 401);
  }
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(cipherId);
  if (!cipher || cipher.userId !== claims.userId) {
    return errorResponse("Cipher not found", 404);
  }
  const attachment = await storage.getAttachment(attachmentId);
  if (!attachment || attachment.cipherId !== cipherId) {
    return errorResponse("Attachment not found", 404);
  }
  return processAttachmentUpload(request, env, attachment, cipherId);
}
__name(handlePublicUploadAttachment, "handlePublicUploadAttachment");
async function handleGetAttachment(request, env, userId, cipherId, attachmentId) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(cipherId);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  const attachment = await storage.getAttachment(attachmentId);
  if (!attachment || attachment.cipherId !== cipherId) {
    return errorResponse("Attachment not found", 404);
  }
  const responseAttachment = applyCipherEmbeddedAttachmentMetadata(cipher, [attachment])[0] || attachment;
  const token = await createFileDownloadToken(cipherId, attachmentId, env.JWT_SECRET);
  const url = new URL(request.url);
  const downloadUrl = `${url.origin}/api/attachments/${cipherId}/${attachmentId}?token=${token}`;
  return jsonResponse({
    object: "attachment",
    id: responseAttachment.id,
    url: downloadUrl,
    fileName: responseAttachment.fileName,
    key: responseAttachment.key,
    size: String(Number(responseAttachment.size) || 0),
    sizeName: responseAttachment.sizeName
  });
}
__name(handleGetAttachment, "handleGetAttachment");
async function handleUpdateAttachmentMetadata(request, env, userId, cipherId, attachmentId) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(cipherId);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  const attachment = await storage.getAttachment(attachmentId);
  if (!attachment || attachment.cipherId !== cipherId) {
    return errorResponse("Attachment not found", 404);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!Object.prototype.hasOwnProperty.call(body, "fileName") && !Object.prototype.hasOwnProperty.call(body, "key")) {
    return errorResponse("No metadata fields supplied", 400);
  }
  if (Object.prototype.hasOwnProperty.call(body, "fileName")) {
    const fileName = String(body.fileName || "").trim();
    if (!fileName) return errorResponse("fileName is required", 400);
    attachment.fileName = fileName;
  }
  if (Object.prototype.hasOwnProperty.call(body, "key")) {
    const key = body.key == null ? null : String(body.key || "").trim();
    attachment.key = key || null;
  }
  await storage.saveAttachment(attachment);
  const revisionInfo = await storage.updateCipherRevisionDate(cipherId);
  if (revisionInfo) {
    notifyVaultSyncForRequest(request, env, revisionInfo.userId, revisionInfo.revisionDate);
  }
  return jsonResponse({
    object: "attachment",
    id: attachment.id,
    fileName: attachment.fileName,
    key: attachment.key,
    size: String(Number(attachment.size) || 0),
    sizeName: attachment.sizeName
  });
}
__name(handleUpdateAttachmentMetadata, "handleUpdateAttachmentMetadata");
async function handlePublicDownloadAttachment(request, env, cipherId, attachmentId) {
  const secret = (env.JWT_SECRET || "").trim();
  if (!secret || secret.length < LIMITS.auth.jwtSecretMinLength || secret === DEFAULT_DEV_SECRET) {
    return errorResponse("Server configuration error", 500);
  }
  const url = new URL(request.url);
  const token = url.searchParams.get("token");
  if (!token) {
    return errorResponse("Token required", 401);
  }
  const claims = await verifyFileDownloadToken(token, env.JWT_SECRET);
  if (!claims) {
    return errorResponse("Invalid or expired token", 401);
  }
  if (claims.cipherId !== cipherId || claims.attachmentId !== attachmentId) {
    return errorResponse("Token mismatch", 401);
  }
  const storage = new StorageService(env.DB);
  const attachment = await storage.getAttachment(attachmentId);
  if (!attachment || attachment.cipherId !== cipherId) {
    return errorResponse("Attachment not found", 404);
  }
  const path = getAttachmentObjectKey(cipherId, attachmentId);
  const object = await getBlobObject(env, path);
  if (!object) {
    return errorResponse("Attachment file not found", 404);
  }
  const firstUse = await storage.consumeAttachmentDownloadToken(claims.jti, claims.exp);
  if (!firstUse) {
    return errorResponse("Invalid or expired token", 401);
  }
  return new Response(object.body, {
    headers: {
      "Content-Type": object.contentType || "application/octet-stream",
      "Content-Length": String(object.size),
      "Cache-Control": "private, no-cache"
    }
  });
}
__name(handlePublicDownloadAttachment, "handlePublicDownloadAttachment");
async function handleDeleteAttachment(request, env, userId, cipherId, attachmentId) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(cipherId);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  const attachment = await storage.getAttachment(attachmentId);
  if (!attachment || attachment.cipherId !== cipherId) {
    return errorResponse("Attachment not found", 404);
  }
  const path = getAttachmentObjectKey(cipherId, attachmentId);
  await deleteBlobObject(env, path);
  await storage.deleteAttachment(attachmentId);
  const revisionInfo = await storage.updateCipherRevisionDate(cipherId);
  if (revisionInfo) {
    notifyVaultSyncForRequest(request, env, revisionInfo.userId, revisionInfo.revisionDate);
    await writeAttachmentAudit(storage, request, revisionInfo.userId, "attachment.delete", {
      id: attachmentId,
      cipherId,
      size: attachment.size
    });
  }
  const updatedCipher = await storage.getCipher(cipherId);
  const attachments = await storage.getAttachmentsByCipher(cipherId);
  return jsonResponse({
    cipher: cipherToResponse(updatedCipher, attachments)
  });
}
__name(handleDeleteAttachment, "handleDeleteAttachment");
async function deleteAllAttachmentsForCipher(env, cipherId) {
  await deleteAllAttachmentsForCiphers(env, [cipherId]);
}
__name(deleteAllAttachmentsForCipher, "deleteAllAttachmentsForCipher");
async function deleteAllAttachmentsForCiphers(env, cipherIds) {
  const storage = new StorageService(env.DB);
  const attachmentsByCipher = await storage.getAttachmentsByCipherIds(cipherIds);
  const attachments = Array.from(attachmentsByCipher.entries()).flatMap(
    ([ownedCipherId, items]) => items.map((attachment) => ({ attachment, cipherId: ownedCipherId }))
  );
  if (!attachments.length) return;
  await runWithConcurrency(attachments, LIMITS.performance.attachmentDeleteConcurrency, async ({ attachment, cipherId }) => {
    const path = getAttachmentObjectKey(cipherId, attachment.id);
    await deleteBlobObject(env, path);
  });
  await storage.bulkDeleteAttachmentsByIds(attachments.map(({ attachment }) => attachment.id));
}
__name(deleteAllAttachmentsForCiphers, "deleteAllAttachmentsForCiphers");

// src/utils/pagination.ts
var MAX_PAGE_SIZE = LIMITS.pagination.maxPageSize;
function parsePagination(url) {
  const pageSizeRaw = url.searchParams.get("pageSize");
  const continuationToken = url.searchParams.get("continuationToken");
  if (!pageSizeRaw && !continuationToken) return null;
  const pageSize = pageSizeRaw ? Number(pageSizeRaw) : LIMITS.pagination.defaultPageSize;
  if (!Number.isInteger(pageSize) || pageSize <= 0) return null;
  const limit = Math.min(pageSize, MAX_PAGE_SIZE);
  const offset = decodeContinuationToken(continuationToken);
  return { limit, offset };
}
__name(parsePagination, "parsePagination");
function encodeContinuationToken(offset) {
  return btoa(String(offset));
}
__name(encodeContinuationToken, "encodeContinuationToken");
function decodeContinuationToken(token) {
  if (!token) return 0;
  try {
    const decoded = atob(token);
    const offset = Number(decoded);
    if (!Number.isInteger(offset) || offset < 0) return 0;
    return offset;
  } catch {
    return 0;
  }
}
__name(decodeContinuationToken, "decodeContinuationToken");

// src/handlers/ciphers.ts
function normalizeOptionalId2(value) {
  if (value == null) return null;
  const normalized = String(value).trim();
  return normalized ? normalized : null;
}
__name(normalizeOptionalId2, "normalizeOptionalId");
function notifyVaultSyncForRequest2(request, env, userId, revisionDate) {
  notifyUserVaultSync(env, userId, revisionDate, readActingDeviceIdentifier(request));
}
__name(notifyVaultSyncForRequest2, "notifyVaultSyncForRequest");
function getAliasedProp(source, aliases) {
  if (!source || typeof source !== "object") return { present: false, value: void 0 };
  for (const key of aliases) {
    if (Object.prototype.hasOwnProperty.call(source, key)) {
      return { present: true, value: source[key] };
    }
  }
  return { present: false, value: void 0 };
}
__name(getAliasedProp, "getAliasedProp");
function readCipherProp(source, aliases) {
  return getAliasedProp(source, aliases);
}
__name(readCipherProp, "readCipherProp");
function normalizeCipherTimestamp(value) {
  if (value == null || value === "") return null;
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}
__name(normalizeCipherTimestamp, "normalizeCipherTimestamp");
function readCipherArchivedAt(source, fallback = null) {
  const archived = getAliasedProp(source, ["archivedAt", "ArchivedAt", "archivedDate", "ArchivedDate"]);
  return archived.present ? normalizeCipherTimestamp(archived.value) : fallback;
}
__name(readCipherArchivedAt, "readCipherArchivedAt");
function readCipherRevisionDate(source) {
  const revision = getAliasedProp(source, ["lastKnownRevisionDate", "LastKnownRevisionDate"]);
  return revision.present ? normalizeCipherTimestamp(revision.value) : null;
}
__name(readCipherRevisionDate, "readCipherRevisionDate");
function isStaleCipherUpdate(existingUpdatedAt, clientRevisionDate) {
  if (!clientRevisionDate) return false;
  const existingTs = Date.parse(existingUpdatedAt);
  const clientTs = Date.parse(clientRevisionDate);
  if (Number.isNaN(existingTs) || Number.isNaN(clientTs)) return false;
  return existingTs - clientTs > 1e3;
}
__name(isStaleCipherUpdate, "isStaleCipherUpdate");
function syncCipherComputedAliases(cipher) {
  cipher.archivedDate = cipher.archivedAt ?? null;
  cipher.deletedDate = cipher.deletedAt ?? null;
  return cipher;
}
__name(syncCipherComputedAliases, "syncCipherComputedAliases");
async function writeCipherAudit(storage, request, userId, action, metadata) {
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action,
    category: "data",
    level: action.includes("delete") ? "security" : "info",
    targetType: "cipher",
    targetId: typeof metadata.id === "string" ? metadata.id : null,
    metadata: {
      ...metadata,
      ...auditRequestMetadata(request)
    }
  });
}
__name(writeCipherAudit, "writeCipherAudit");
function isValidEncString(value) {
  if (typeof value !== "string") return false;
  const trimmed = value.trim();
  const dot = trimmed.indexOf(".");
  if (dot <= 0) return false;
  const type = Number(trimmed.slice(0, dot));
  if (!Number.isInteger(type) || type < 0) return false;
  const parts = trimmed.slice(dot + 1).split("|");
  if (parts.some((part) => part.length === 0)) return false;
  if (type === 0 || type === 1 || type === 4) return parts.length >= 2;
  if (type === 2) return parts.length === 3;
  return parts.length >= 1;
}
__name(isValidEncString, "isValidEncString");
function optionalEncString(value) {
  if (value == null || value === "") return null;
  return isValidEncString(value) ? value.trim() : null;
}
__name(optionalEncString, "optionalEncString");
function sanitizeEncryptedObject(source, encryptedKeys) {
  if (!source || typeof source !== "object") return source ?? null;
  const next = { ...source };
  for (const key of encryptedKeys) {
    if (!Object.prototype.hasOwnProperty.call(next, key)) continue;
    next[key] = optionalEncString(next[key]);
  }
  return next;
}
__name(sanitizeEncryptedObject, "sanitizeEncryptedObject");
function normalizeCipherForStorage(cipher) {
  cipher.login = normalizeCipherLoginForStorage(cipher.login);
  cipher.sshKey = normalizeCipherSshKeyForCompatibility(cipher.sshKey);
  cipher.folderId = normalizeOptionalId2(cipher.folderId);
  const hasArchivedAt = Object.prototype.hasOwnProperty.call(cipher, "archivedAt");
  cipher.archivedAt = hasArchivedAt ? normalizeCipherTimestamp(cipher.archivedAt) ?? null : normalizeCipherTimestamp(cipher.archivedDate) ?? null;
  return syncCipherComputedAliases(cipher);
}
__name(normalizeCipherForStorage, "normalizeCipherForStorage");
function normalizeCipherLoginForStorage(login) {
  if (!login || typeof login !== "object") return login ?? null;
  return {
    ...login,
    fido2Credentials: Array.isArray(login.fido2Credentials) ? login.fido2Credentials : null
  };
}
__name(normalizeCipherLoginForStorage, "normalizeCipherLoginForStorage");
function normalizeCipherLoginForCompatibility(login) {
  const normalized = normalizeCipherLoginForStorage(login);
  if (!normalized || typeof normalized !== "object") return normalized ?? null;
  const next = sanitizeEncryptedObject(normalized, ["username", "password", "totp", "uri"]);
  if (!next) return null;
  next.uris = Array.isArray(next.uris) ? next.uris.map((uri) => sanitizeEncryptedObject(uri, ["uri", "uriChecksum"])).filter((uri) => !!uri && (uri.uri || uri.uriChecksum || uri.match != null)) : null;
  next.fido2Credentials = normalizeFido2CredentialsForCompatibility(next.fido2Credentials);
  return next;
}
__name(normalizeCipherLoginForCompatibility, "normalizeCipherLoginForCompatibility");
function hasMissingLoginUriChecksum(cipher) {
  if (!cipher.key || !cipher.login || typeof cipher.login !== "object") return false;
  const uris = cipher.login.uris;
  if (!Array.isArray(uris)) return false;
  return uris.some((uri) => {
    if (!uri || typeof uri !== "object") return false;
    return isValidEncString(uri.uri) && !isValidEncString(uri.uriChecksum);
  });
}
__name(hasMissingLoginUriChecksum, "hasMissingLoginUriChecksum");
function normalizeFido2CredentialsForCompatibility(credentials) {
  if (!Array.isArray(credentials) || credentials.length === 0) return null;
  const requiredEncryptedKeys = [
    "credentialId",
    "keyType",
    "keyAlgorithm",
    "keyCurve",
    "keyValue",
    "rpId",
    "counter",
    "discoverable"
  ];
  const optionalEncryptedKeys = ["userHandle", "userName", "rpName", "userDisplayName"];
  const out = [];
  for (const credential of credentials) {
    if (!credential || typeof credential !== "object") continue;
    const next = { ...credential };
    let valid = true;
    for (const key of requiredEncryptedKeys) {
      if (!isValidEncString(next[key])) {
        valid = false;
        break;
      }
      next[key] = String(next[key]).trim();
    }
    if (!valid) continue;
    for (const key of optionalEncryptedKeys) {
      if (Object.prototype.hasOwnProperty.call(next, key)) {
        next[key] = optionalEncString(next[key]);
      }
    }
    out.push(next);
  }
  return out.length ? out : null;
}
__name(normalizeFido2CredentialsForCompatibility, "normalizeFido2CredentialsForCompatibility");
function normalizeCipherSshKeyForCompatibility(sshKey) {
  if (!sshKey || typeof sshKey !== "object") return sshKey ?? null;
  const candidate = sshKey.keyFingerprint !== void 0 && sshKey.keyFingerprint !== null ? sshKey.keyFingerprint : sshKey.fingerprint;
  const normalizedFingerprint = candidate === void 0 || candidate === null ? "" : String(candidate);
  if (!isValidEncString(sshKey.privateKey) || !isValidEncString(sshKey.publicKey) || !isValidEncString(normalizedFingerprint)) {
    return null;
  }
  return {
    ...sshKey,
    privateKey: String(sshKey.privateKey).trim(),
    publicKey: String(sshKey.publicKey).trim(),
    keyFingerprint: normalizedFingerprint,
    fingerprint: normalizedFingerprint
  };
}
__name(normalizeCipherSshKeyForCompatibility, "normalizeCipherSshKeyForCompatibility");
function formatAttachments(attachments) {
  if (attachments.length === 0) return null;
  const formatted = attachments.filter((a) => isValidEncString(a.fileName)).map((a) => ({
    id: a.id,
    fileName: a.fileName.trim(),
    // Bitwarden clients decode attachment size as string in cipher payloads.
    size: String(Number(a.size) || 0),
    sizeName: a.sizeName,
    key: optionalEncString(a.key),
    url: `/api/ciphers/${a.cipherId}/attachment/${a.id}`,
    // Android requires non-null url!
    object: "attachment"
  }));
  return formatted.length ? formatted : null;
}
__name(formatAttachments, "formatAttachments");
function formatAttachmentSize(bytes) {
  if (bytes < 1024) return `${bytes} Bytes`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}
__name(formatAttachmentSize, "formatAttachmentSize");
function readIncomingAttachmentMetadataMap(value, options = {}) {
  if (!value || typeof value !== "object") return [];
  const out = [];
  if (Array.isArray(value)) {
    for (const item of value) {
      if (!item || typeof item !== "object") continue;
      const row = item;
      const id = String(row.id ?? row.Id ?? "").trim();
      if (!id) continue;
      const fileName = getAliasedProp(row, ["fileName", "FileName"]);
      const key = getAliasedProp(row, ["key", "Key"]);
      const fileSize = getAliasedProp(row, ["fileSize", "FileSize", "size", "Size"]);
      out.push({
        id,
        fileName: fileName.value,
        key: key.value,
        fileSize: fileSize.value,
        hasFileName: fileName.present,
        hasKey: key.present,
        hasFileSize: fileSize.present
      });
    }
    return out;
  }
  for (const [rawId, rawValue] of Object.entries(value)) {
    const id = String(rawId || "").trim();
    if (!id) continue;
    if (options.legacyFileNameMap && (typeof rawValue === "string" || rawValue == null)) {
      out.push({
        id,
        fileName: rawValue,
        key: void 0,
        fileSize: void 0,
        hasFileName: rawValue != null,
        hasKey: false,
        hasFileSize: false
      });
      continue;
    }
    if (!rawValue || typeof rawValue !== "object") continue;
    const row = rawValue;
    const fileName = getAliasedProp(row, ["fileName", "FileName"]);
    const key = getAliasedProp(row, ["key", "Key"]);
    const fileSize = getAliasedProp(row, ["fileSize", "FileSize", "size", "Size"]);
    out.push({
      id,
      fileName: fileName.value,
      key: key.value,
      fileSize: fileSize.value,
      hasFileName: fileName.present,
      hasKey: key.present,
      hasFileSize: fileSize.present
    });
  }
  return out;
}
__name(readIncomingAttachmentMetadataMap, "readIncomingAttachmentMetadataMap");
function readIncomingAttachmentMetadata(source) {
  const merged = /* @__PURE__ */ new Map();
  const legacy = getAliasedProp(source, ["attachments", "Attachments"]);
  const current = getAliasedProp(source, ["attachments2", "Attachments2"]);
  if (legacy.present) {
    for (const item of readIncomingAttachmentMetadataMap(legacy.value, { legacyFileNameMap: true })) {
      merged.set(item.id, item);
    }
  }
  if (current.present) {
    for (const item of readIncomingAttachmentMetadataMap(current.value)) {
      const previous = merged.get(item.id);
      merged.set(item.id, {
        id: item.id,
        fileName: item.hasFileName ? item.fileName : previous?.fileName,
        key: item.hasKey ? item.key : previous?.key,
        fileSize: item.hasFileSize ? item.fileSize : previous?.fileSize,
        hasFileName: item.hasFileName || previous?.hasFileName || false,
        hasKey: item.hasKey || previous?.hasKey || false,
        hasFileSize: item.hasFileSize || previous?.hasFileSize || false
      });
    }
  }
  return [...merged.values()];
}
__name(readIncomingAttachmentMetadata, "readIncomingAttachmentMetadata");
function hasIncomingAttachmentMetadata(source) {
  return readIncomingAttachmentMetadata(source).length > 0;
}
__name(hasIncomingAttachmentMetadata, "hasIncomingAttachmentMetadata");
async function syncIncomingAttachmentMetadata(storage, cipherId, cipherData) {
  const incoming = readIncomingAttachmentMetadata(cipherData);
  if (!incoming.length) return;
  const currentById = new Map((await storage.getAttachmentsByCipher(cipherId)).map((attachment) => [attachment.id, attachment]));
  for (const item of incoming) {
    const attachment = currentById.get(item.id);
    if (!attachment) continue;
    let changed = false;
    if (item.hasFileName) {
      const fileName = String(item.fileName || "").trim();
      if (isValidEncString(fileName) && fileName !== attachment.fileName) {
        attachment.fileName = fileName;
        changed = true;
      }
    }
    if (item.hasKey) {
      const key = optionalEncString(item.key);
      if (key !== attachment.key) {
        attachment.key = key;
        changed = true;
      }
    }
    if (item.hasFileSize) {
      const size = Number(item.fileSize);
      if (Number.isFinite(size) && size >= 0 && size !== Number(attachment.size || 0)) {
        attachment.size = size;
        attachment.sizeName = formatAttachmentSize(size);
        changed = true;
      }
    }
    if (changed) {
      await storage.saveAttachment(attachment);
    }
  }
}
__name(syncIncomingAttachmentMetadata, "syncIncomingAttachmentMetadata");
function applyCipherEmbeddedAttachmentMetadata(cipherData, attachments) {
  const incoming = readIncomingAttachmentMetadata(cipherData);
  if (!incoming.length || !attachments.length) return attachments;
  const incomingById = new Map(incoming.map((item) => [item.id, item]));
  return attachments.map((attachment) => {
    const item = incomingById.get(attachment.id);
    if (!item) return attachment;
    const next = { ...attachment };
    if (item.hasFileName) {
      const fileName = String(item.fileName || "").trim();
      if (isValidEncString(fileName)) {
        next.fileName = fileName;
      }
    }
    if (item.hasKey) {
      next.key = optionalEncString(item.key);
    }
    if (item.hasFileSize) {
      const size = Number(item.fileSize);
      if (Number.isFinite(size) && size >= 0) {
        next.size = size;
        next.sizeName = formatAttachmentSize(size);
      }
    }
    return next;
  });
}
__name(applyCipherEmbeddedAttachmentMetadata, "applyCipherEmbeddedAttachmentMetadata");
function normalizeCipherFieldsForCompatibility(fields) {
  if (!Array.isArray(fields) || fields.length === 0) return null;
  const out = fields.map((field) => {
    if (!field || typeof field !== "object") return null;
    return {
      ...field,
      name: optionalEncString(field.name),
      value: optionalEncString(field.value),
      type: Number(field.type) || 0,
      linkedId: field.linkedId ?? null
    };
  }).filter(Boolean);
  return out.length ? out : null;
}
__name(normalizeCipherFieldsForCompatibility, "normalizeCipherFieldsForCompatibility");
function normalizePasswordHistoryForCompatibility(passwordHistory) {
  if (!Array.isArray(passwordHistory) || passwordHistory.length === 0) return null;
  const out = passwordHistory.filter((entry) => entry && typeof entry === "object" && isValidEncString(entry.password)).map((entry) => ({
    ...entry,
    password: String(entry.password).trim(),
    lastUsedDate: normalizeCipherTimestamp(entry.lastUsedDate) ?? (/* @__PURE__ */ new Date()).toISOString()
  }));
  return out.length ? out : null;
}
__name(normalizePasswordHistoryForCompatibility, "normalizePasswordHistoryForCompatibility");
function isCipherResponseSyncCompatible(cipher) {
  return isValidEncString(cipher.name);
}
__name(isCipherResponseSyncCompatible, "isCipherResponseSyncCompatible");
function cipherToResponse(cipher, attachments = []) {
  const { userId, createdAt, updatedAt, archivedAt, deletedAt, ...passthrough } = cipher;
  const normalizedLogin = normalizeCipherLoginForCompatibility(passthrough.login ?? null);
  const normalizedCard = sanitizeEncryptedObject(passthrough.card ?? null, ["cardholderName", "brand", "number", "expMonth", "expYear", "code"]);
  const normalizedIdentity = sanitizeEncryptedObject(passthrough.identity ?? null, [
    "title",
    "firstName",
    "middleName",
    "lastName",
    "address1",
    "address2",
    "address3",
    "city",
    "state",
    "postalCode",
    "country",
    "company",
    "email",
    "phone",
    "ssn",
    "username",
    "passportNumber",
    "licenseNumber"
  ]);
  const normalizedSshKey = normalizeCipherSshKeyForCompatibility(passthrough.sshKey ?? null);
  const responseAttachments = applyCipherEmbeddedAttachmentMetadata(cipher, attachments);
  return {
    // Pass through ALL stored cipher fields (known + unknown)
    ...passthrough,
    // Server-computed / enforced fields (always override)
    folderId: normalizeOptionalId2(cipher.folderId),
    type: Number(cipher.type) || 1,
    organizationId: normalizeOptionalId2(passthrough.organizationId ?? null),
    organizationUseTotp: !!(passthrough.organizationUseTotp ?? false),
    creationDate: createdAt,
    revisionDate: updatedAt,
    deletedDate: deletedAt,
    archivedDate: archivedAt ?? null,
    edit: true,
    viewPassword: true,
    permissions: {
      delete: true,
      restore: true
    },
    object: "cipherDetails",
    collectionIds: Array.isArray(passthrough.collectionIds) ? passthrough.collectionIds : [],
    attachments: formatAttachments(responseAttachments),
    name: isValidEncString(cipher.name) ? cipher.name.trim() : cipher.name,
    notes: optionalEncString(cipher.notes),
    login: normalizedLogin,
    card: normalizedCard,
    identity: normalizedIdentity,
    fields: normalizeCipherFieldsForCompatibility(passthrough.fields),
    passwordHistory: normalizePasswordHistoryForCompatibility(passthrough.passwordHistory),
    sshKey: normalizedSshKey,
    key: optionalEncString(cipher.key),
    encryptedFor: passthrough.encryptedFor ?? null
  };
}
__name(cipherToResponse, "cipherToResponse");
async function handleGetCiphers(request, env, userId) {
  const storage = new StorageService(env.DB);
  const url = new URL(request.url);
  const includeDeleted = url.searchParams.get("deleted") === "true";
  const pagination = parsePagination(url);
  let filteredCiphers;
  let continuationToken = null;
  if (pagination) {
    const pageRows = await storage.getCiphersPage(
      userId,
      includeDeleted,
      pagination.limit + 1,
      pagination.offset
    );
    const hasNext = pageRows.length > pagination.limit;
    filteredCiphers = hasNext ? pageRows.slice(0, pagination.limit) : pageRows;
    continuationToken = hasNext ? encodeContinuationToken(pagination.offset + filteredCiphers.length) : null;
  } else {
    const ciphers = await storage.getAllCiphers(userId);
    filteredCiphers = includeDeleted ? ciphers : ciphers.filter((c) => !c.deletedAt);
  }
  const attachmentsByCipher = await storage.getAttachmentsByCipherIds(
    filteredCiphers.map((cipher) => cipher.id)
  );
  const cipherResponses = [];
  for (const cipher of filteredCiphers) {
    const attachments = attachmentsByCipher.get(cipher.id) || [];
    cipherResponses.push(cipherToResponse(cipher, attachments));
  }
  return jsonResponse({
    data: cipherResponses,
    object: "list",
    continuationToken
  });
}
__name(handleGetCiphers, "handleGetCiphers");
async function handleGetCipher(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(id);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  const attachments = await storage.getAttachmentsByCipher(cipher.id);
  return jsonResponse(
    cipherToResponse(cipher, attachments)
  );
}
__name(handleGetCipher, "handleGetCipher");
async function verifyFolderOwnership(storage, folderId, userId) {
  if (!folderId) return true;
  const folder = await storage.getFolder(folderId);
  return !!(folder && folder.userId === userId);
}
__name(verifyFolderOwnership, "verifyFolderOwnership");
async function handleCreateCipher(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const cipherData = body.Cipher || body.cipher || body;
  const createFolderId = readCipherProp(cipherData, ["folderId", "FolderId"]);
  const createKey = readCipherProp(cipherData, ["key", "Key"]);
  const createLogin = readCipherProp(cipherData, ["login", "Login"]);
  const createCard = readCipherProp(cipherData, ["card", "Card"]);
  const createIdentity = readCipherProp(cipherData, ["identity", "Identity"]);
  const createSecureNote = readCipherProp(cipherData, ["secureNote", "SecureNote"]);
  const createSshKey = readCipherProp(cipherData, ["sshKey", "SshKey"]);
  const createPasswordHistory = readCipherProp(cipherData, ["passwordHistory", "PasswordHistory"]);
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const cipher = {
    ...cipherData,
    // Server-controlled fields (always override client values)
    id: generateUUID(),
    userId,
    type: Number(cipherData.type) || 1,
    favorite: !!cipherData.favorite,
    reprompt: cipherData.reprompt || 0,
    createdAt: now,
    updatedAt: now,
    archivedAt: readCipherArchivedAt(cipherData, null),
    deletedAt: null
  };
  cipher.folderId = createFolderId.present ? normalizeOptionalId2(createFolderId.value) : normalizeOptionalId2(cipher.folderId);
  cipher.key = createKey.present ? createKey.value ?? null : cipher.key ?? null;
  cipher.login = createLogin.present ? createLogin.value ?? null : cipher.login ?? null;
  cipher.card = createCard.present ? createCard.value ?? null : cipher.card ?? null;
  cipher.identity = createIdentity.present ? createIdentity.value ?? null : cipher.identity ?? null;
  cipher.secureNote = createSecureNote.present ? createSecureNote.value ?? null : cipher.secureNote ?? null;
  cipher.sshKey = createSshKey.present ? createSshKey.value ?? null : cipher.sshKey ?? null;
  cipher.passwordHistory = createPasswordHistory.present ? createPasswordHistory.value ?? null : cipher.passwordHistory ?? null;
  const createFields = getAliasedProp(cipherData, ["fields", "Fields"]);
  cipher.fields = createFields.present ? createFields.value ?? null : cipher.fields ?? null;
  normalizeCipherForStorage(cipher);
  if (cipher.folderId) {
    const folderOk = await verifyFolderOwnership(storage, cipher.folderId, userId);
    if (!folderOk) return errorResponse("Folder not found", 404);
  }
  if (hasMissingLoginUriChecksum(cipher)) {
    return errorResponse("Login URI checksum is required for item-key encrypted ciphers. Refresh NodeWarden and save the item again.", 400);
  }
  await storage.saveCipher(cipher);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  return jsonResponse(
    cipherToResponse(cipher, []),
    200
  );
}
__name(handleCreateCipher, "handleCreateCipher");
async function handleUpdateCipher(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const existingCipher = await storage.getCipher(id);
  if (!existingCipher || existingCipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const cipherData = body.Cipher || body.cipher || body;
  const incomingFolderId = readCipherProp(cipherData, ["folderId", "FolderId"]);
  const incomingKey = readCipherProp(cipherData, ["key", "Key"]);
  const incomingLogin = readCipherProp(cipherData, ["login", "Login"]);
  const incomingCard = readCipherProp(cipherData, ["card", "Card"]);
  const incomingIdentity = readCipherProp(cipherData, ["identity", "Identity"]);
  const incomingSecureNote = readCipherProp(cipherData, ["secureNote", "SecureNote"]);
  const incomingSshKey = readCipherProp(cipherData, ["sshKey", "SshKey"]);
  const incomingPasswordHistory = readCipherProp(cipherData, ["passwordHistory", "PasswordHistory"]);
  const incomingRevisionDate = readCipherRevisionDate(cipherData);
  const hasAttachmentMigrationMetadata = hasIncomingAttachmentMetadata(cipherData);
  if (!hasAttachmentMigrationMetadata && isStaleCipherUpdate(existingCipher.updatedAt, incomingRevisionDate)) {
    return errorResponse("The client copy of this cipher is out of date. Resync the client and try again.", 400);
  }
  const nextType = Number(cipherData.type) || existingCipher.type;
  const cipher = {
    ...existingCipher,
    // start with all existing stored data (including unknowns)
    ...cipherData,
    // overlay all client data (including new/unknown fields)
    // Server-controlled fields (never from client)
    id: existingCipher.id,
    userId: existingCipher.userId,
    type: nextType,
    favorite: cipherData.favorite ?? existingCipher.favorite,
    reprompt: cipherData.reprompt ?? existingCipher.reprompt,
    createdAt: existingCipher.createdAt,
    updatedAt: (/* @__PURE__ */ new Date()).toISOString(),
    archivedAt: readCipherArchivedAt(cipherData, existingCipher.archivedAt ?? null),
    deletedAt: existingCipher.deletedAt
  };
  if (incomingFolderId.present) {
    cipher.folderId = normalizeOptionalId2(incomingFolderId.value);
  }
  if (incomingKey.present) {
    cipher.key = incomingKey.value ?? null;
  }
  cipher.login = nextType === 1 ? incomingLogin.present ? incomingLogin.value ?? null : existingCipher.login ?? null : null;
  cipher.secureNote = nextType === 2 ? incomingSecureNote.present ? incomingSecureNote.value ?? null : existingCipher.secureNote ?? null : null;
  cipher.card = nextType === 3 ? incomingCard.present ? incomingCard.value ?? null : existingCipher.card ?? null : null;
  cipher.identity = nextType === 4 ? incomingIdentity.present ? incomingIdentity.value ?? null : existingCipher.identity ?? null : null;
  cipher.sshKey = nextType === 5 ? incomingSshKey.present ? incomingSshKey.value ?? null : existingCipher.sshKey ?? null : null;
  if (incomingPasswordHistory.present) {
    cipher.passwordHistory = incomingPasswordHistory.value ?? null;
  }
  const incomingFields = getAliasedProp(cipherData, ["fields", "Fields"]);
  if (incomingFields.present) {
    cipher.fields = incomingFields.value ?? null;
  } else if (request.method === "PUT" || request.method === "POST") {
    cipher.fields = null;
  }
  normalizeCipherForStorage(cipher);
  if (cipher.folderId) {
    const folderOk = await verifyFolderOwnership(storage, cipher.folderId, userId);
    if (!folderOk) return errorResponse("Folder not found", 404);
  }
  if (hasMissingLoginUriChecksum(cipher)) {
    return errorResponse("Login URI checksum is required for item-key encrypted ciphers. Refresh NodeWarden and save the item again.", 400);
  }
  await syncIncomingAttachmentMetadata(storage, cipher.id, cipherData);
  await storage.saveCipher(cipher);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  const attachments = await storage.getAttachmentsByCipher(cipher.id);
  return jsonResponse(
    cipherToResponse(cipher, attachments)
  );
}
__name(handleUpdateCipher, "handleUpdateCipher");
async function handleDeleteCipher(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(id);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  cipher.deletedAt = (/* @__PURE__ */ new Date()).toISOString();
  cipher.updatedAt = cipher.deletedAt;
  syncCipherComputedAliases(cipher);
  await storage.saveCipher(cipher);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  await writeCipherAudit(storage, request, userId, "cipher.delete.soft", {
    id: cipher.id,
    type: cipher.type,
    folderId: cipher.folderId ?? null
  });
  return jsonResponse(
    cipherToResponse(cipher, [])
  );
}
__name(handleDeleteCipher, "handleDeleteCipher");
async function handleDeleteCipherCompat(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(id);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  if (cipher.deletedAt) {
    await deleteAllAttachmentsForCipher(env, id);
    await storage.deleteCipher(id, userId);
    const revisionDate = await storage.updateRevisionDate(userId);
    notifyVaultSyncForRequest2(request, env, userId, revisionDate);
    await writeCipherAudit(storage, request, userId, "cipher.delete.permanent", {
      id,
      type: cipher.type,
      folderId: cipher.folderId ?? null,
      compat: true
    });
    return new Response(null, { status: 204 });
  }
  return handleDeleteCipher(request, env, userId, id);
}
__name(handleDeleteCipherCompat, "handleDeleteCipherCompat");
async function handlePermanentDeleteCipher(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(id);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  await deleteAllAttachmentsForCipher(env, id);
  await storage.deleteCipher(id, userId);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  await writeCipherAudit(storage, request, userId, "cipher.delete.permanent", {
    id,
    type: cipher.type,
    folderId: cipher.folderId ?? null
  });
  return new Response(null, { status: 204 });
}
__name(handlePermanentDeleteCipher, "handlePermanentDeleteCipher");
async function handleRestoreCipher(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(id);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  cipher.deletedAt = null;
  cipher.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  syncCipherComputedAliases(cipher);
  await storage.saveCipher(cipher);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  return jsonResponse(
    cipherToResponse(cipher, [])
  );
}
__name(handleRestoreCipher, "handleRestoreCipher");
async function handlePartialUpdateCipher(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(id);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (body.folderId !== void 0) {
    const folderId = normalizeOptionalId2(body.folderId);
    if (folderId) {
      const folderOk = await verifyFolderOwnership(storage, folderId, userId);
      if (!folderOk) return errorResponse("Folder not found", 404);
    }
    cipher.folderId = folderId;
  }
  if (body.favorite !== void 0) {
    cipher.favorite = body.favorite;
  }
  cipher.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  syncCipherComputedAliases(cipher);
  await storage.saveCipher(cipher);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  return jsonResponse(
    cipherToResponse(cipher, [])
  );
}
__name(handlePartialUpdateCipher, "handlePartialUpdateCipher");
async function handleBulkMoveCiphers(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.ids || !Array.isArray(body.ids)) {
    return errorResponse("ids array is required", 400);
  }
  const folderId = normalizeOptionalId2(body.folderId);
  if (folderId) {
    const folderOk = await verifyFolderOwnership(storage, folderId, userId);
    if (!folderOk) return errorResponse("Folder not found", 404);
  }
  const revisionDate = await storage.bulkMoveCiphers(body.ids, folderId, userId);
  if (revisionDate) {
    notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  }
  return new Response(null, { status: 204 });
}
__name(handleBulkMoveCiphers, "handleBulkMoveCiphers");
async function buildCipherListResponse(request, storage, userId, ids) {
  const ciphers = await storage.getCiphersByIds(ids, userId);
  const attachmentsByCipher = await storage.getAttachmentsByCipherIds(ciphers.map((cipher) => cipher.id));
  return jsonResponse({
    data: ciphers.map(
      (cipher) => cipherToResponse(cipher, attachmentsByCipher.get(cipher.id) || [])
    ),
    object: "list",
    continuationToken: null
  });
}
__name(buildCipherListResponse, "buildCipherListResponse");
function parseCipherIdList(body) {
  if (!Array.isArray(body.ids)) return null;
  return Array.from(new Set(body.ids.map((id) => String(id || "").trim()).filter(Boolean)));
}
__name(parseCipherIdList, "parseCipherIdList");
async function handleArchiveCipher(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(id);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  if (cipher.deletedAt) {
    return errorResponse("Cannot archive a deleted cipher", 400);
  }
  cipher.archivedAt = (/* @__PURE__ */ new Date()).toISOString();
  cipher.updatedAt = cipher.archivedAt;
  normalizeCipherForStorage(cipher);
  await storage.saveCipher(cipher);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  const attachments = await storage.getAttachmentsByCipher(cipher.id);
  return jsonResponse(
    cipherToResponse(cipher, attachments)
  );
}
__name(handleArchiveCipher, "handleArchiveCipher");
async function handleUnarchiveCipher(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const cipher = await storage.getCipher(id);
  if (!cipher || cipher.userId !== userId) {
    return errorResponse("Cipher not found", 404);
  }
  cipher.archivedAt = null;
  cipher.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  normalizeCipherForStorage(cipher);
  await storage.saveCipher(cipher);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  const attachments = await storage.getAttachmentsByCipher(cipher.id);
  return jsonResponse(
    cipherToResponse(cipher, attachments)
  );
}
__name(handleUnarchiveCipher, "handleUnarchiveCipher");
async function handleBulkArchiveCiphers(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const ids = parseCipherIdList(body);
  if (!ids) {
    return errorResponse("ids array is required", 400);
  }
  const revisionDate = await storage.bulkArchiveCiphers(ids, userId);
  if (revisionDate) {
    notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  }
  return buildCipherListResponse(request, storage, userId, ids);
}
__name(handleBulkArchiveCiphers, "handleBulkArchiveCiphers");
async function handleBulkUnarchiveCiphers(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const ids = parseCipherIdList(body);
  if (!ids) {
    return errorResponse("ids array is required", 400);
  }
  const revisionDate = await storage.bulkUnarchiveCiphers(ids, userId);
  if (revisionDate) {
    notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  }
  return buildCipherListResponse(request, storage, userId, ids);
}
__name(handleBulkUnarchiveCiphers, "handleBulkUnarchiveCiphers");
async function handleBulkDeleteCiphers(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.ids || !Array.isArray(body.ids)) {
    return errorResponse("ids array is required", 400);
  }
  const revisionDate = await storage.bulkSoftDeleteCiphers(body.ids, userId);
  if (revisionDate) {
    notifyVaultSyncForRequest2(request, env, userId, revisionDate);
    await writeCipherAudit(storage, request, userId, "cipher.delete.soft.bulk", {
      count: body.ids.length
    });
  }
  return new Response(null, { status: 204 });
}
__name(handleBulkDeleteCiphers, "handleBulkDeleteCiphers");
async function handleBulkRestoreCiphers(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.ids || !Array.isArray(body.ids)) {
    return errorResponse("ids array is required", 400);
  }
  const revisionDate = await storage.bulkRestoreCiphers(body.ids, userId);
  if (revisionDate) {
    notifyVaultSyncForRequest2(request, env, userId, revisionDate);
  }
  return new Response(null, { status: 204 });
}
__name(handleBulkRestoreCiphers, "handleBulkRestoreCiphers");
async function handleBulkPermanentDeleteCiphers(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.ids || !Array.isArray(body.ids)) {
    return errorResponse("ids array is required", 400);
  }
  const ids = Array.from(new Set(body.ids.map((id) => String(id || "").trim()).filter(Boolean)));
  if (!ids.length) {
    return new Response(null, { status: 204 });
  }
  const ownedCiphers = await storage.getCiphersByIds(ids, userId);
  const ownedIds = ownedCiphers.map((cipher) => cipher.id);
  if (!ownedIds.length) {
    return new Response(null, { status: 204 });
  }
  await deleteAllAttachmentsForCiphers(env, ownedIds);
  const revisionDate = await storage.bulkDeleteCiphers(ownedIds, userId);
  if (revisionDate) {
    notifyVaultSyncForRequest2(request, env, userId, revisionDate);
    await writeCipherAudit(storage, request, userId, "cipher.delete.permanent.bulk", {
      count: ownedIds.length,
      requestedCount: ids.length
    });
  }
  return new Response(null, { status: 204 });
}
__name(handleBulkPermanentDeleteCiphers, "handleBulkPermanentDeleteCiphers");

// src/handlers/folders.ts
function notifyVaultSyncForRequest3(request, env, userId, revisionDate) {
  notifyUserVaultSync(env, userId, revisionDate, readActingDeviceIdentifier(request));
}
__name(notifyVaultSyncForRequest3, "notifyVaultSyncForRequest");
async function writeFolderAudit(storage, request, userId, action, metadata) {
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action,
    category: "data",
    level: action.includes("delete") ? "security" : "info",
    targetType: "folder",
    targetId: typeof metadata.id === "string" ? metadata.id : null,
    metadata: {
      ...metadata,
      ...auditRequestMetadata(request)
    }
  });
}
__name(writeFolderAudit, "writeFolderAudit");
function folderToResponse(folder) {
  return {
    id: folder.id,
    name: folder.name,
    revisionDate: folder.updatedAt,
    creationDate: folder.createdAt,
    object: "folder"
  };
}
__name(folderToResponse, "folderToResponse");
async function handleGetFolders(request, env, userId) {
  const storage = new StorageService(env.DB);
  const url = new URL(request.url);
  const pagination = parsePagination(url);
  let folders;
  let continuationToken = null;
  if (pagination) {
    const pageRows = await storage.getFoldersPage(userId, pagination.limit + 1, pagination.offset);
    const hasNext = pageRows.length > pagination.limit;
    folders = hasNext ? pageRows.slice(0, pagination.limit) : pageRows;
    continuationToken = hasNext ? encodeContinuationToken(pagination.offset + folders.length) : null;
  } else {
    folders = await storage.getAllFolders(userId);
  }
  return jsonResponse({
    data: folders.map(folderToResponse),
    object: "list",
    continuationToken
  });
}
__name(handleGetFolders, "handleGetFolders");
async function handleGetFolder(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const folder = await storage.getFolder(id);
  if (!folder || folder.userId !== userId) {
    return errorResponse("Folder not found", 404);
  }
  return jsonResponse(folderToResponse(folder));
}
__name(handleGetFolder, "handleGetFolder");
async function handleCreateFolder(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.name) {
    return errorResponse("Name is required", 400);
  }
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const folder = {
    id: generateUUID(),
    userId,
    name: body.name,
    createdAt: now,
    updatedAt: now
  };
  await storage.saveFolder(folder);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest3(request, env, userId, revisionDate);
  return jsonResponse(folderToResponse(folder), 200);
}
__name(handleCreateFolder, "handleCreateFolder");
async function handleUpdateFolder(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const folder = await storage.getFolder(id);
  if (!folder || folder.userId !== userId) {
    return errorResponse("Folder not found", 404);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (body.name) {
    folder.name = body.name;
  }
  folder.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveFolder(folder);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest3(request, env, userId, revisionDate);
  return jsonResponse(folderToResponse(folder));
}
__name(handleUpdateFolder, "handleUpdateFolder");
async function handleDeleteFolder(request, env, userId, id) {
  const storage = new StorageService(env.DB);
  const folder = await storage.getFolder(id);
  if (!folder || folder.userId !== userId) {
    return errorResponse("Folder not found", 404);
  }
  await storage.clearFolderFromCiphers(userId, id);
  await storage.deleteFolder(id, userId);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest3(request, env, userId, revisionDate);
  await writeFolderAudit(storage, request, userId, "folder.delete", {
    id
  });
  return new Response(null, { status: 204 });
}
__name(handleDeleteFolder, "handleDeleteFolder");
async function handleBulkDeleteFolders(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const ids = Array.isArray(body.ids) ? body.ids.map((id) => String(id || "").trim()).filter(Boolean) : [];
  if (!ids.length) {
    return errorResponse("Folder ids are required", 400);
  }
  const revisionDate = await storage.bulkDeleteFolders(ids, userId);
  if (revisionDate) {
    notifyVaultSyncForRequest3(request, env, userId, revisionDate);
    await writeFolderAudit(storage, request, userId, "folder.delete.bulk", {
      count: ids.length
    });
  }
  return new Response(null, { status: 204 });
}
__name(handleBulkDeleteFolders, "handleBulkDeleteFolders");

// src/handlers/sends-shared.ts
var SEND_INACCESSIBLE_MSG = "Send does not exist or is no longer available";
var SEND_PASSWORD_ITERATIONS = 1e5;
var SEND_PASSWORD_LIMIT_SCOPE = "send-password";
function notifyVaultSyncForRequest4(request, env, userId, revisionDate) {
  notifyUserVaultSync(env, userId, revisionDate, readActingDeviceIdentifier(request));
}
__name(notifyVaultSyncForRequest4, "notifyVaultSyncForRequest");
function getAliasedProp2(source, aliases) {
  if (!source || typeof source !== "object") return { present: false, value: void 0 };
  for (const key of aliases) {
    if (Object.prototype.hasOwnProperty.call(source, key)) {
      const value = source[key];
      return { present: true, value };
    }
  }
  return { present: false, value: void 0 };
}
__name(getAliasedProp2, "getAliasedProp");
function base64UrlEncode2(data) {
  const base64 = btoa(String.fromCharCode(...data));
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
__name(base64UrlEncode2, "base64UrlEncode");
function base64UrlDecode2(input) {
  try {
    let normalized = input.replace(/-/g, "+").replace(/_/g, "/");
    while (normalized.length % 4) normalized += "=";
    const raw = atob(normalized);
    const out = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
    return out;
  } catch {
    return null;
  }
}
__name(base64UrlDecode2, "base64UrlDecode");
function uuidToBytes(uuid) {
  const hex = uuid.replace(/-/g, "").toLowerCase();
  if (!/^[0-9a-f]{32}$/.test(hex)) return null;
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}
__name(uuidToBytes, "uuidToBytes");
function bytesToUuid(bytes) {
  if (bytes.length !== 16) return null;
  const hex = Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20, 32)
  ].join("-");
}
__name(bytesToUuid, "bytesToUuid");
function toAccessId(sendId) {
  const bytes = uuidToBytes(sendId);
  if (!bytes) return "";
  return base64UrlEncode2(bytes);
}
__name(toAccessId, "toAccessId");
function fromAccessId(accessId) {
  const bytes = base64UrlDecode2(accessId);
  if (!bytes || bytes.length !== 16) return null;
  return bytesToUuid(bytes);
}
__name(fromAccessId, "fromAccessId");
function isLikelyUuid(value) {
  return /^[a-f0-9-]{36}$/i.test(value);
}
__name(isLikelyUuid, "isLikelyUuid");
async function resolveSendFromIdOrAccessId(storage, idOrAccessId) {
  if (isLikelyUuid(idOrAccessId)) {
    const send = await storage.getSend(idOrAccessId);
    if (send) return send;
  }
  const sendId = fromAccessId(idOrAccessId);
  if (!sendId) return null;
  return storage.getSend(sendId);
}
__name(resolveSendFromIdOrAccessId, "resolveSendFromIdOrAccessId");
function formatSize2(bytes) {
  if (bytes < 1024) return `${bytes} Bytes`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}
__name(formatSize2, "formatSize");
function parseDate(raw) {
  if (typeof raw !== "string" || !raw.trim()) return null;
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return null;
  return date;
}
__name(parseDate, "parseDate");
function parseInteger(raw) {
  if (raw === null || raw === void 0 || raw === "") return null;
  const value = typeof raw === "string" ? Number(raw) : raw;
  if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value)) return null;
  return value;
}
__name(parseInteger, "parseInteger");
function sanitizeSendData(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const data = { ...raw };
  delete data.response;
  return data;
}
__name(sanitizeSendData, "sanitizeSendData");
function parseStoredSendData(send) {
  try {
    const parsed = JSON.parse(send.data);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return { ...parsed };
    }
    return {};
  } catch {
    return {};
  }
}
__name(parseStoredSendData, "parseStoredSendData");
function normalizeSendDataSizeField(data) {
  const normalized = { ...data };
  if (typeof normalized.size === "number" && Number.isFinite(normalized.size)) {
    normalized.size = String(Math.trunc(normalized.size));
  }
  return normalized;
}
__name(normalizeSendDataSizeField, "normalizeSendDataSizeField");
function isSendAvailable(send) {
  const now = Date.now();
  if (send.maxAccessCount !== null && send.accessCount >= send.maxAccessCount) {
    return false;
  }
  if (send.expirationDate) {
    const expirationMs = new Date(send.expirationDate).getTime();
    if (!Number.isNaN(expirationMs) && now >= expirationMs) {
      return false;
    }
  }
  const deletionMs = new Date(send.deletionDate).getTime();
  if (!Number.isNaN(deletionMs) && now >= deletionMs) {
    return false;
  }
  if (send.disabled) {
    return false;
  }
  return true;
}
__name(isSendAvailable, "isSendAvailable");
async function deriveSendPasswordHash(password, salt, iterations) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey("raw", encoder.encode(password), { name: "PBKDF2" }, false, ["deriveBits"]);
  const bits2 = await crypto.subtle.deriveBits(
    {
      name: "PBKDF2",
      salt,
      iterations,
      hash: "SHA-256"
    },
    key,
    256
  );
  return new Uint8Array(bits2);
}
__name(deriveSendPasswordHash, "deriveSendPasswordHash");
function constantTimeEqual(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= a[i] ^ b[i];
  }
  return diff === 0;
}
__name(constantTimeEqual, "constantTimeEqual");
function isLikelyHashB64(value) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  if (!/^[A-Za-z0-9+/_=-]+$/.test(raw)) return false;
  const decoded = base64UrlDecode2(raw);
  return !!decoded && decoded.length === 32;
}
__name(isLikelyHashB64, "isLikelyHashB64");
async function setSendPassword(send, password) {
  if (!password) {
    send.passwordHash = null;
    send.passwordSalt = null;
    send.passwordIterations = null;
    if (send.authType === 1 /* Password */) {
      send.authType = 2 /* None */;
    }
    return;
  }
  if (isLikelyHashB64(password)) {
    send.passwordHash = password.trim();
    send.passwordSalt = null;
    send.passwordIterations = null;
    send.authType = 1 /* Password */;
    return;
  }
  const salt = crypto.getRandomValues(new Uint8Array(64));
  const hash = await deriveSendPasswordHash(password, salt, SEND_PASSWORD_ITERATIONS);
  send.passwordSalt = base64UrlEncode2(salt);
  send.passwordHash = base64UrlEncode2(hash);
  send.passwordIterations = SEND_PASSWORD_ITERATIONS;
  send.authType = 1 /* Password */;
}
__name(setSendPassword, "setSendPassword");
async function verifySendPassword(send, password) {
  if (!send.passwordHash) {
    return false;
  }
  if (!send.passwordSalt || !send.passwordIterations) {
    return verifySendPasswordHashB64(send, password);
  }
  const salt = base64UrlDecode2(send.passwordSalt);
  const expected = base64UrlDecode2(send.passwordHash);
  if (!salt || !expected) return false;
  const actual = await deriveSendPasswordHash(password, salt, send.passwordIterations);
  return constantTimeEqual(actual, expected);
}
__name(verifySendPassword, "verifySendPassword");
function verifySendPasswordHashB64(send, passwordHashB64) {
  if (!send.passwordHash || !passwordHashB64) return false;
  const expected = base64UrlDecode2(send.passwordHash);
  const provided = base64UrlDecode2(passwordHashB64);
  if (!expected || !provided) return false;
  return constantTimeEqual(expected, provided);
}
__name(verifySendPasswordHashB64, "verifySendPasswordHashB64");
function validateDeletionDate(date) {
  const maxMs = Date.now() + LIMITS.send.maxDeletionDays * 24 * 60 * 60 * 1e3;
  if (date.getTime() > maxMs) {
    return errorResponse(
      "You cannot have a Send with a deletion date that far into the future. Adjust the Deletion Date to a value less than 31 days from now and try again.",
      400
    );
  }
  return null;
}
__name(validateDeletionDate, "validateDeletionDate");
function parseMaxAccessCount(value) {
  const parsed = parseInteger(value);
  if (value === void 0 || value === null || value === "") {
    return { ok: true, value: null };
  }
  if (parsed === null || parsed < 0) {
    return { ok: false, response: errorResponse("Invalid maxAccessCount", 400) };
  }
  return { ok: true, value: parsed };
}
__name(parseMaxAccessCount, "parseMaxAccessCount");
function parseFileLength(value) {
  const parsed = parseInteger(value);
  if (parsed === null) {
    return { ok: false, response: errorResponse("Invalid send length", 400) };
  }
  if (parsed < 0) {
    return { ok: false, response: errorResponse("Send size can't be negative", 400) };
  }
  return { ok: true, value: parsed };
}
__name(parseFileLength, "parseFileLength");
function parseSendType(value) {
  const type = parseInteger(value);
  if (type === 0 /* Text */ || type === 1 /* File */) return type;
  return null;
}
__name(parseSendType, "parseSendType");
function parseSendAuthType(value) {
  if (value === void 0 || value === null || value === "") return null;
  const parsed = parseInteger(value);
  if (parsed === 0 /* Email */ || parsed === 1 /* Password */ || parsed === 2 /* None */) {
    return parsed;
  }
  return null;
}
__name(parseSendAuthType, "parseSendAuthType");
function normalizeEmails(value) {
  if (value === null || value === void 0 || value === "") return null;
  if (typeof value === "string") return value;
  if (Array.isArray(value)) {
    const strings = value.filter((v) => typeof v === "string").map((v) => String(v));
    if (strings.length === 0) return null;
    return strings.join(",");
  }
  return null;
}
__name(normalizeEmails, "normalizeEmails");
function hasEmailAuth(send) {
  return send.authType === 0 /* Email */;
}
__name(hasEmailAuth, "hasEmailAuth");
function getSafeJwtSecret2(env) {
  const secret = (env.JWT_SECRET || "").trim();
  if (!secret || secret.length < LIMITS.auth.jwtSecretMinLength || secret === DEFAULT_DEV_SECRET) {
    return { ok: false, response: errorResponse("Server configuration error", 500) };
  }
  return { ok: true, secret };
}
__name(getSafeJwtSecret2, "getSafeJwtSecret");
function extractBearerToken(request) {
  const authHeader = request.headers.get("Authorization");
  if (!authHeader) return null;
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : null;
}
__name(extractBearerToken, "extractBearerToken");
function sendToResponse(send) {
  const data = normalizeSendDataSizeField(parseStoredSendData(send));
  return {
    id: send.id,
    accessId: toAccessId(send.id),
    type: Number(send.type) || 0,
    name: send.name,
    notes: send.notes,
    text: send.type === 0 /* Text */ ? data : null,
    file: send.type === 1 /* File */ ? data : null,
    key: send.key,
    maxAccessCount: send.maxAccessCount,
    accessCount: send.accessCount,
    password: send.passwordHash,
    emails: send.emails,
    authType: send.authType,
    disabled: send.disabled,
    hideEmail: send.hideEmail,
    revisionDate: send.updatedAt,
    expirationDate: send.expirationDate,
    deletionDate: send.deletionDate,
    object: "send"
  };
}
__name(sendToResponse, "sendToResponse");
function sendToAccessResponse(send, creatorIdentifier) {
  const data = normalizeSendDataSizeField(parseStoredSendData(send));
  return {
    id: send.id,
    type: Number(send.type) || 0,
    name: send.name,
    text: send.type === 0 /* Text */ ? data : null,
    file: send.type === 1 /* File */ ? data : null,
    expirationDate: send.expirationDate,
    deletionDate: send.deletionDate,
    creatorIdentifier,
    object: "send-access"
  };
}
__name(sendToAccessResponse, "sendToAccessResponse");
async function getCreatorIdentifier(storage, send) {
  if (send.hideEmail) return null;
  const owner = await storage.getUserById(send.userId);
  return owner?.email ?? null;
}
__name(getCreatorIdentifier, "getCreatorIdentifier");
function sendPasswordLimitKey(clientIdentifier) {
  return `${clientIdentifier}:${SEND_PASSWORD_LIMIT_SCOPE}`;
}
__name(sendPasswordLimitKey, "sendPasswordLimitKey");
function sendPasswordLockMessage(retryAfterSeconds) {
  return `Too many failed send password attempts. Try again in ${Math.ceil(retryAfterSeconds / 60)} minutes.`;
}
__name(sendPasswordLockMessage, "sendPasswordLockMessage");
function sendPasswordLockedErrorResponse(retryAfterSeconds) {
  return errorResponse(sendPasswordLockMessage(retryAfterSeconds), 429);
}
__name(sendPasswordLockedErrorResponse, "sendPasswordLockedErrorResponse");
function sendPasswordLockedOAuthResponse(retryAfterSeconds) {
  const message = sendPasswordLockMessage(retryAfterSeconds);
  return jsonResponse(
    {
      error: "invalid_grant",
      error_description: message,
      send_access_error_type: "too_many_password_attempts",
      ErrorModel: {
        Message: message,
        Object: "error"
      }
    },
    429
  );
}
__name(sendPasswordLockedOAuthResponse, "sendPasswordLockedOAuthResponse");
async function validatePublicSendAccess(send, body) {
  if (hasEmailAuth(send)) {
    return { ok: false, response: errorResponse(SEND_INACCESSIBLE_MSG, 404), reason: "email_auth_unsupported" };
  }
  if (!send.passwordHash) return { ok: true };
  const passwordRaw = getAliasedProp2(body, ["password", "Password"]);
  const passwordHashB64Raw = getAliasedProp2(body, [
    "password_hash_b64",
    "passwordHashB64",
    "passwordHash",
    "password_hash"
  ]);
  let validPassword = false;
  if (send.passwordSalt && send.passwordIterations) {
    if (typeof passwordRaw.value !== "string") {
      return { ok: false, response: errorResponse("Password not provided", 401), reason: "password_missing" };
    }
    validPassword = await verifySendPassword(send, passwordRaw.value);
  } else {
    const candidate = typeof passwordHashB64Raw.value === "string" ? passwordHashB64Raw.value : typeof passwordRaw.value === "string" ? passwordRaw.value : "";
    if (!candidate) return { ok: false, response: errorResponse("Password not provided", 401), reason: "password_missing" };
    validPassword = verifySendPasswordHashB64(send, candidate);
  }
  if (!validPassword) {
    return { ok: false, response: errorResponse("Invalid password", 400), reason: "invalid_password" };
  }
  return { ok: true };
}
__name(validatePublicSendAccess, "validatePublicSendAccess");

// src/handlers/sends-private.ts
async function writeSendAudit(storage, request, userId, action, metadata) {
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action,
    category: "data",
    level: action.includes("delete") ? "security" : "info",
    targetType: "send",
    targetId: typeof metadata.id === "string" ? metadata.id : null,
    metadata: {
      ...metadata,
      ...auditRequestMetadata(request)
    }
  });
}
__name(writeSendAudit, "writeSendAudit");
async function processSendFileUpload(request, env, send, fileId) {
  const maxFileSize = getBlobStorageMaxBytes(env, LIMITS.send.maxFileSizeBytes);
  const sendData = parseStoredSendData(send);
  const expectedFileId = typeof sendData.id === "string" ? sendData.id : null;
  if (!expectedFileId || expectedFileId !== fileId) {
    return errorResponse("Send file does not match send data.", 400);
  }
  const expectedFileName = typeof sendData.fileName === "string" ? sendData.fileName : null;
  const expectedSize = parseInteger(sendData.size);
  const upload = await parseDirectUploadPayload(request, {
    expectedSize,
    expectedFileName,
    maxFileSize,
    tooLargeMessage: "Send storage limit exceeded with this file",
    sizeMismatchMessage: "Send file size does not match.",
    fileNameMismatchMessage: "Send file name does not match."
  });
  if (upload instanceof Response) {
    return upload;
  }
  try {
    await putBlobObject(env, getSendFileObjectKey(send.id, fileId), upload.body, {
      size: upload.size,
      contentType: upload.contentType,
      customMetadata: {
        sendId: send.id,
        fileId
      }
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes("KV object too large")) {
      return errorResponse("Send storage limit exceeded with this file", 413);
    }
    return errorResponse("Attachment storage is not configured", 500);
  }
  const storage = new StorageService(env.DB);
  const revisionDate = await storage.updateRevisionDate(send.userId);
  notifyVaultSyncForRequest4(request, env, send.userId, revisionDate);
  return new Response(null, { status: 201 });
}
__name(processSendFileUpload, "processSendFileUpload");
async function handleGetSends(request, env, userId) {
  const storage = new StorageService(env.DB);
  const url = new URL(request.url);
  const pagination = parsePagination(url);
  let sends;
  let continuationToken = null;
  if (pagination) {
    const pageRows = await storage.getSendsPage(userId, pagination.limit + 1, pagination.offset);
    const hasNext = pageRows.length > pagination.limit;
    sends = hasNext ? pageRows.slice(0, pagination.limit) : pageRows;
    continuationToken = hasNext ? encodeContinuationToken(pagination.offset + sends.length) : null;
  } else {
    sends = await storage.getAllSends(userId);
  }
  const sendResponses = sends.map(sendToResponse);
  return jsonResponse({
    data: sendResponses,
    object: "list",
    continuationToken
  });
}
__name(handleGetSends, "handleGetSends");
async function handleGetSend(request, env, userId, sendId) {
  void request;
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(sendId);
  if (!send || send.userId !== userId) {
    return errorResponse("Send not found", 404);
  }
  return jsonResponse(sendToResponse(send));
}
__name(handleGetSend, "handleGetSend");
async function handleCreateSend(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const typeRaw = getAliasedProp2(body, ["type", "Type"]);
  const sendType = parseSendType(typeRaw.value);
  if (sendType === null) {
    return errorResponse("Invalid Send type", 400);
  }
  if (sendType === 1 /* File */) {
    return errorResponse("File sends should use /api/sends/file/v2", 400);
  }
  const nameRaw = getAliasedProp2(body, ["name", "Name"]);
  const keyRaw = getAliasedProp2(body, ["key", "Key"]);
  const deletionDateRaw = getAliasedProp2(body, ["deletionDate", "DeletionDate"]);
  const textRaw = getAliasedProp2(body, ["text", "Text"]);
  if (typeof nameRaw.value !== "string" || !nameRaw.value.trim()) {
    return errorResponse("Name is required", 400);
  }
  if (typeof keyRaw.value !== "string" || !keyRaw.value.trim()) {
    return errorResponse("Key is required", 400);
  }
  const deletionDate = parseDate(deletionDateRaw.value);
  if (!deletionDate) {
    return errorResponse("Invalid deletionDate", 400);
  }
  const deletionValidation = validateDeletionDate(deletionDate);
  if (deletionValidation) return deletionValidation;
  const sendData = sanitizeSendData(textRaw.value);
  if (!sendData) {
    return errorResponse("Send data not provided", 400);
  }
  const maxAccessRaw = getAliasedProp2(body, ["maxAccessCount", "MaxAccessCount"]);
  const maxAccess = parseMaxAccessCount(maxAccessRaw.value);
  if (!maxAccess.ok) return maxAccess.response;
  const expirationRaw = getAliasedProp2(body, ["expirationDate", "ExpirationDate"]);
  const expirationDate = expirationRaw.value === null || expirationRaw.value === void 0 ? null : parseDate(expirationRaw.value);
  if (expirationRaw.value !== null && expirationRaw.value !== void 0 && !expirationDate) {
    return errorResponse("Invalid expirationDate", 400);
  }
  const disabledRaw = getAliasedProp2(body, ["disabled", "Disabled"]);
  const hideEmailRaw = getAliasedProp2(body, ["hideEmail", "HideEmail"]);
  const notesRaw = getAliasedProp2(body, ["notes", "Notes"]);
  const passwordRaw = getAliasedProp2(body, ["password", "Password"]);
  const authTypeRaw = getAliasedProp2(body, ["authType", "AuthType"]);
  const emailsRaw = getAliasedProp2(body, ["emails", "Emails"]);
  const requestedAuthType = parseSendAuthType(authTypeRaw.value);
  if (authTypeRaw.present && requestedAuthType === null) {
    return errorResponse("Invalid authType", 400);
  }
  const normalizedEmails = normalizeEmails(emailsRaw.value);
  if (emailsRaw.present && emailsRaw.value !== null && normalizedEmails === null) {
    return errorResponse("Invalid emails", 400);
  }
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const send = {
    id: generateUUID(),
    userId,
    type: sendType,
    name: nameRaw.value.trim(),
    notes: typeof notesRaw.value === "string" ? notesRaw.value : null,
    data: JSON.stringify(sendData),
    key: keyRaw.value,
    passwordHash: null,
    passwordSalt: null,
    passwordIterations: null,
    authType: requestedAuthType ?? 2 /* None */,
    emails: normalizedEmails,
    maxAccessCount: maxAccess.value,
    accessCount: 0,
    disabled: typeof disabledRaw.value === "boolean" ? disabledRaw.value : false,
    hideEmail: typeof hideEmailRaw.value === "boolean" ? hideEmailRaw.value : null,
    createdAt: now,
    updatedAt: now,
    expirationDate: expirationDate ? expirationDate.toISOString() : null,
    deletionDate: deletionDate.toISOString()
  };
  if (typeof passwordRaw.value === "string" && passwordRaw.value.length > 0) {
    await setSendPassword(send, passwordRaw.value);
  } else if (send.authType === 1 /* Password */) {
    return errorResponse("Password is required for password auth", 400);
  }
  if (send.authType !== 0 /* Email */) {
    send.emails = null;
  }
  await storage.saveSend(send);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest4(request, env, userId, revisionDate);
  return jsonResponse(sendToResponse(send));
}
__name(handleCreateSend, "handleCreateSend");
async function handleCreateFileSendV2(request, env, userId) {
  const storage = new StorageService(env.DB);
  const maxFileSize = getBlobStorageMaxBytes(env, LIMITS.send.maxFileSizeBytes);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const typeRaw = getAliasedProp2(body, ["type", "Type"]);
  const sendType = parseSendType(typeRaw.value);
  if (sendType !== 1 /* File */) {
    return errorResponse("Send content is not a file", 400);
  }
  const fileLengthRaw = getAliasedProp2(body, ["fileLength", "FileLength"]);
  const fileLengthParsed = parseFileLength(fileLengthRaw.value);
  if (!fileLengthParsed.ok) return fileLengthParsed.response;
  if (fileLengthParsed.value > maxFileSize) {
    return errorResponse("Send storage limit exceeded with this file", 400);
  }
  const nameRaw = getAliasedProp2(body, ["name", "Name"]);
  const keyRaw = getAliasedProp2(body, ["key", "Key"]);
  const deletionDateRaw = getAliasedProp2(body, ["deletionDate", "DeletionDate"]);
  const fileRaw = getAliasedProp2(body, ["file", "File"]);
  if (typeof nameRaw.value !== "string" || !nameRaw.value.trim()) {
    return errorResponse("Name is required", 400);
  }
  if (typeof keyRaw.value !== "string" || !keyRaw.value.trim()) {
    return errorResponse("Key is required", 400);
  }
  const deletionDate = parseDate(deletionDateRaw.value);
  if (!deletionDate) {
    return errorResponse("Invalid deletionDate", 400);
  }
  const deletionValidation = validateDeletionDate(deletionDate);
  if (deletionValidation) return deletionValidation;
  const fileData = sanitizeSendData(fileRaw.value);
  if (!fileData) {
    return errorResponse("Send data not provided", 400);
  }
  const fileId = generateUUID();
  fileData.id = fileId;
  fileData.size = fileLengthParsed.value;
  fileData.sizeName = formatSize2(fileLengthParsed.value);
  const maxAccessRaw = getAliasedProp2(body, ["maxAccessCount", "MaxAccessCount"]);
  const maxAccess = parseMaxAccessCount(maxAccessRaw.value);
  if (!maxAccess.ok) return maxAccess.response;
  const expirationRaw = getAliasedProp2(body, ["expirationDate", "ExpirationDate"]);
  const expirationDate = expirationRaw.value === null || expirationRaw.value === void 0 ? null : parseDate(expirationRaw.value);
  if (expirationRaw.value !== null && expirationRaw.value !== void 0 && !expirationDate) {
    return errorResponse("Invalid expirationDate", 400);
  }
  const disabledRaw = getAliasedProp2(body, ["disabled", "Disabled"]);
  const hideEmailRaw = getAliasedProp2(body, ["hideEmail", "HideEmail"]);
  const notesRaw = getAliasedProp2(body, ["notes", "Notes"]);
  const passwordRaw = getAliasedProp2(body, ["password", "Password"]);
  const authTypeRaw = getAliasedProp2(body, ["authType", "AuthType"]);
  const emailsRaw = getAliasedProp2(body, ["emails", "Emails"]);
  const requestedAuthType = parseSendAuthType(authTypeRaw.value);
  if (authTypeRaw.present && requestedAuthType === null) {
    return errorResponse("Invalid authType", 400);
  }
  const normalizedEmails = normalizeEmails(emailsRaw.value);
  if (emailsRaw.present && emailsRaw.value !== null && normalizedEmails === null) {
    return errorResponse("Invalid emails", 400);
  }
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const send = {
    id: generateUUID(),
    userId,
    type: sendType,
    name: nameRaw.value.trim(),
    notes: typeof notesRaw.value === "string" ? notesRaw.value : null,
    data: JSON.stringify(fileData),
    key: keyRaw.value,
    passwordHash: null,
    passwordSalt: null,
    passwordIterations: null,
    authType: requestedAuthType ?? 2 /* None */,
    emails: normalizedEmails,
    maxAccessCount: maxAccess.value,
    accessCount: 0,
    disabled: typeof disabledRaw.value === "boolean" ? disabledRaw.value : false,
    hideEmail: typeof hideEmailRaw.value === "boolean" ? hideEmailRaw.value : null,
    createdAt: now,
    updatedAt: now,
    expirationDate: expirationDate ? expirationDate.toISOString() : null,
    deletionDate: deletionDate.toISOString()
  };
  if (typeof passwordRaw.value === "string" && passwordRaw.value.length > 0) {
    await setSendPassword(send, passwordRaw.value);
  } else if (send.authType === 1 /* Password */) {
    return errorResponse("Password is required for password auth", 400);
  }
  if (send.authType !== 0 /* Email */) {
    send.emails = null;
  }
  await storage.saveSend(send);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest4(request, env, userId, revisionDate);
  const jwtSecret = getSafeJwtSecret(env);
  if (!jwtSecret) {
    return errorResponse("Server configuration error", 500);
  }
  const uploadToken = await createSendFileUploadToken(userId, send.id, fileId, jwtSecret);
  return jsonResponse({
    fileUploadType: 1,
    object: "send-fileUpload",
    url: buildDirectUploadUrl(request, `/api/sends/${send.id}/file/${fileId}`, uploadToken),
    sendResponse: sendToResponse(send)
  });
}
__name(handleCreateFileSendV2, "handleCreateFileSendV2");
async function handleGetSendFileUpload(request, env, userId, sendId, fileId) {
  void request;
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(sendId);
  if (!send || send.userId !== userId) {
    return errorResponse("Send not found", 404);
  }
  if (send.type !== 1 /* File */) {
    return errorResponse("Send is not a file type send.", 400);
  }
  const sendData = parseStoredSendData(send);
  const expectedFileId = typeof sendData.id === "string" ? sendData.id : null;
  if (!expectedFileId || expectedFileId !== fileId) {
    return errorResponse("Send file does not match send data.", 400);
  }
  const jwtSecret = getSafeJwtSecret(env);
  if (!jwtSecret) {
    return errorResponse("Server configuration error", 500);
  }
  const uploadToken = await createSendFileUploadToken(userId, send.id, fileId, jwtSecret);
  return jsonResponse({
    fileUploadType: 1,
    object: "send-fileUpload",
    url: buildDirectUploadUrl(request, `/api/sends/${send.id}/file/${fileId}`, uploadToken),
    sendResponse: sendToResponse(send)
  });
}
__name(handleGetSendFileUpload, "handleGetSendFileUpload");
async function handleUploadSendFile(request, env, userId, sendId, fileId) {
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(sendId);
  if (!send || send.userId !== userId) {
    return errorResponse("Send not found. Unable to save the file.", 404);
  }
  if (send.type !== 1 /* File */) {
    return errorResponse("Send is not a file type send.", 400);
  }
  return processSendFileUpload(request, env, send, fileId);
}
__name(handleUploadSendFile, "handleUploadSendFile");
async function handlePublicUploadSendFile(request, env, sendId, fileId) {
  const jwtSecret = getSafeJwtSecret(env);
  if (!jwtSecret) {
    return errorResponse("Server configuration error", 500);
  }
  const token = new URL(request.url).searchParams.get("token");
  if (!token) {
    return errorResponse("Token required", 401);
  }
  const claims = await verifySendFileUploadToken(token, jwtSecret);
  if (!claims) {
    return errorResponse("Invalid or expired token", 401);
  }
  if (claims.sendId !== sendId || claims.fileId !== fileId) {
    return errorResponse("Token mismatch", 401);
  }
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(sendId);
  if (!send || send.userId !== claims.userId) {
    return errorResponse("Send not found. Unable to save the file.", 404);
  }
  if (send.type !== 1 /* File */) {
    return errorResponse("Send is not a file type send.", 400);
  }
  return processSendFileUpload(request, env, send, fileId);
}
__name(handlePublicUploadSendFile, "handlePublicUploadSendFile");
async function handleUpdateSend(request, env, userId, sendId) {
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(sendId);
  if (!send || send.userId !== userId) {
    return errorResponse("Send not found", 404);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const typeRaw = getAliasedProp2(body, ["type", "Type"]);
  if (typeRaw.present) {
    const incomingType = parseSendType(typeRaw.value);
    if (incomingType === null) {
      return errorResponse("Invalid Send type", 400);
    }
    if (incomingType !== send.type) {
      return errorResponse("Sends can't change type", 400);
    }
  }
  const deletionRaw = getAliasedProp2(body, ["deletionDate", "DeletionDate"]);
  if (deletionRaw.present) {
    const deletionDate = parseDate(deletionRaw.value);
    if (!deletionDate) return errorResponse("Invalid deletionDate", 400);
    const deletionValidation = validateDeletionDate(deletionDate);
    if (deletionValidation) return deletionValidation;
    send.deletionDate = deletionDate.toISOString();
  }
  const expirationRaw = getAliasedProp2(body, ["expirationDate", "ExpirationDate"]);
  if (expirationRaw.present) {
    if (expirationRaw.value === null || expirationRaw.value === "") {
      send.expirationDate = null;
    } else {
      const expiration = parseDate(expirationRaw.value);
      if (!expiration) return errorResponse("Invalid expirationDate", 400);
      send.expirationDate = expiration.toISOString();
    }
  }
  const nameRaw = getAliasedProp2(body, ["name", "Name"]);
  if (nameRaw.present) {
    if (typeof nameRaw.value !== "string" || !nameRaw.value.trim()) {
      return errorResponse("Name is required", 400);
    }
    send.name = nameRaw.value.trim();
  }
  const keyRaw = getAliasedProp2(body, ["key", "Key"]);
  if (keyRaw.present) {
    if (typeof keyRaw.value !== "string" || !keyRaw.value.trim()) {
      return errorResponse("Key is required", 400);
    }
    send.key = keyRaw.value;
  }
  const notesRaw = getAliasedProp2(body, ["notes", "Notes"]);
  if (notesRaw.present) {
    send.notes = typeof notesRaw.value === "string" ? notesRaw.value : null;
  }
  const disabledRaw = getAliasedProp2(body, ["disabled", "Disabled"]);
  if (disabledRaw.present) {
    if (typeof disabledRaw.value !== "boolean") {
      return errorResponse("Invalid disabled", 400);
    }
    send.disabled = disabledRaw.value;
  }
  const hideEmailRaw = getAliasedProp2(body, ["hideEmail", "HideEmail"]);
  if (hideEmailRaw.present) {
    if (hideEmailRaw.value === null) {
      send.hideEmail = null;
    } else if (typeof hideEmailRaw.value === "boolean") {
      send.hideEmail = hideEmailRaw.value;
    } else {
      return errorResponse("Invalid hideEmail", 400);
    }
  }
  const maxAccessRaw = getAliasedProp2(body, ["maxAccessCount", "MaxAccessCount"]);
  if (maxAccessRaw.present) {
    const parsedMax = parseMaxAccessCount(maxAccessRaw.value);
    if (!parsedMax.ok) return parsedMax.response;
    send.maxAccessCount = parsedMax.value;
  }
  if (send.type === 0 /* Text */) {
    const textRaw = getAliasedProp2(body, ["text", "Text"]);
    if (textRaw.present) {
      const textData = sanitizeSendData(textRaw.value);
      if (!textData) {
        return errorResponse("Send data not provided", 400);
      }
      send.data = JSON.stringify(textData);
    }
  }
  const authTypeRaw = getAliasedProp2(body, ["authType", "AuthType"]);
  if (authTypeRaw.present) {
    const parsedAuthType = parseSendAuthType(authTypeRaw.value);
    if (parsedAuthType === null) {
      return errorResponse("Invalid authType", 400);
    }
    send.authType = parsedAuthType;
    if (parsedAuthType !== 0 /* Email */) {
      send.emails = null;
    }
  }
  const emailsRaw = getAliasedProp2(body, ["emails", "Emails"]);
  if (emailsRaw.present) {
    const normalizedEmails = normalizeEmails(emailsRaw.value);
    if (emailsRaw.value !== null && normalizedEmails === null) {
      return errorResponse("Invalid emails", 400);
    }
    send.emails = normalizedEmails;
    if (send.emails) {
      send.authType = 0 /* Email */;
    } else if (send.authType === 0 /* Email */) {
      send.authType = 2 /* None */;
    }
  }
  const passwordRaw = getAliasedProp2(body, ["password", "Password"]);
  if (passwordRaw.present && typeof passwordRaw.value === "string") {
    await setSendPassword(send, passwordRaw.value);
  }
  if (send.authType === 1 /* Password */ && !send.passwordHash) {
    return errorResponse("Password is required for password auth", 400);
  }
  send.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveSend(send);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest4(request, env, userId, revisionDate);
  return jsonResponse(sendToResponse(send));
}
__name(handleUpdateSend, "handleUpdateSend");
async function handleDeleteSend(request, env, userId, sendId) {
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(sendId);
  if (!send || send.userId !== userId) {
    return errorResponse("Send not found", 404);
  }
  if (send.type === 1 /* File */) {
    const data = parseStoredSendData(send);
    const fileId = typeof data.id === "string" ? data.id : null;
    if (fileId) {
      await deleteBlobObject(env, getSendFileObjectKey(send.id, fileId));
    }
  }
  await storage.deleteSend(sendId, userId);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest4(request, env, userId, revisionDate);
  await writeSendAudit(storage, request, userId, "send.delete", {
    id: sendId,
    type: send.type
  });
  return new Response(null, { status: 200 });
}
__name(handleDeleteSend, "handleDeleteSend");
async function handleBulkDeleteSends(request, env, userId) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  if (!body.ids || !Array.isArray(body.ids)) {
    return errorResponse("ids array is required", 400);
  }
  const sends = await storage.getSendsByIds(body.ids, userId);
  for (const send of sends) {
    if (send.type !== 1 /* File */) continue;
    const data = parseStoredSendData(send);
    const fileId = typeof data.id === "string" ? data.id : null;
    if (fileId) {
      await deleteBlobObject(env, getSendFileObjectKey(send.id, fileId));
    }
  }
  const revisionDate = await storage.bulkDeleteSends(body.ids, userId);
  if (revisionDate) {
    notifyVaultSyncForRequest4(request, env, userId, revisionDate);
    await writeSendAudit(storage, request, userId, "send.delete.bulk", {
      count: sends.length,
      requestedCount: body.ids.length
    });
  }
  return new Response(null, { status: 200 });
}
__name(handleBulkDeleteSends, "handleBulkDeleteSends");
async function handleRemoveSendPassword(request, env, userId, sendId) {
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(sendId);
  if (!send || send.userId !== userId) {
    return errorResponse("Send not found", 404);
  }
  await setSendPassword(send, null);
  send.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveSend(send);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest4(request, env, userId, revisionDate);
  await writeSendAudit(storage, request, userId, "send.password.remove", {
    id: send.id,
    type: send.type
  });
  return jsonResponse(sendToResponse(send));
}
__name(handleRemoveSendPassword, "handleRemoveSendPassword");
async function handleRemoveSendAuth(request, env, userId, sendId) {
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(sendId);
  if (!send || send.userId !== userId) {
    return errorResponse("Send not found", 404);
  }
  send.authType = 2 /* None */;
  send.emails = null;
  send.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveSend(send);
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyVaultSyncForRequest4(request, env, userId, revisionDate);
  await writeSendAudit(storage, request, userId, "send.auth.remove", {
    id: send.id,
    type: send.type
  });
  return jsonResponse(sendToResponse(send));
}
__name(handleRemoveSendAuth, "handleRemoveSendAuth");

// src/handlers/sends-public.ts
async function handleAccessSend(request, env, accessId) {
  const storage = new StorageService(env.DB);
  const sendId = fromAccessId(accessId);
  if (!sendId) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  const send = await storage.getSend(sendId);
  if (!send || !isSendAvailable(send)) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  let body = {};
  try {
    body = await request.json();
  } catch {
    body = {};
  }
  let sendPasswordLimitIpKey = null;
  let sendPasswordRateLimit = null;
  if (send.passwordHash) {
    const clientIdentifier = getClientIdentifier(request);
    if (!clientIdentifier) {
      return errorResponse("Client IP is required", 403);
    }
    sendPasswordLimitIpKey = sendPasswordLimitKey(clientIdentifier);
    sendPasswordRateLimit = new RateLimitService(env.DB);
    const sendPasswordCheck = await sendPasswordRateLimit.checkLoginAttempt(sendPasswordLimitIpKey);
    if (!sendPasswordCheck.allowed) {
      return sendPasswordLockedErrorResponse(sendPasswordCheck.retryAfterSeconds || 60);
    }
  }
  const validation = await validatePublicSendAccess(send, body);
  if (!validation.ok) {
    if (validation.reason === "invalid_password" && sendPasswordRateLimit && sendPasswordLimitIpKey) {
      const failed = await sendPasswordRateLimit.recordFailedLogin(sendPasswordLimitIpKey);
      if (failed.locked) {
        return sendPasswordLockedErrorResponse(failed.retryAfterSeconds || 60);
      }
    }
    return validation.response;
  }
  if (send.passwordHash && sendPasswordRateLimit && sendPasswordLimitIpKey) {
    await sendPasswordRateLimit.clearLoginAttempts(sendPasswordLimitIpKey);
  }
  if (send.type === 0 /* Text */) {
    const updated = await storage.incrementSendAccessCount(send.id);
    if (!updated) {
      return errorResponse(SEND_INACCESSIBLE_MSG, 404);
    }
    send.accessCount += 1;
    const revisionDate = await storage.updateRevisionDate(send.userId);
    notifyVaultSyncForRequest4(request, env, send.userId, revisionDate);
  }
  const creatorIdentifier = await getCreatorIdentifier(storage, send);
  return jsonResponse(sendToAccessResponse(send, creatorIdentifier));
}
__name(handleAccessSend, "handleAccessSend");
async function handleAccessSendFile(request, env, idOrAccessId, fileId) {
  const secret = (env.JWT_SECRET || "").trim();
  if (!secret || secret.length < LIMITS.auth.jwtSecretMinLength) {
    return errorResponse("Server configuration error", 500);
  }
  const storage = new StorageService(env.DB);
  const send = await resolveSendFromIdOrAccessId(storage, idOrAccessId);
  if (!send || !isSendAvailable(send) || send.type !== 1 /* File */) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  const data = parseStoredSendData(send);
  const expectedFileId = typeof data.id === "string" ? data.id : null;
  if (!expectedFileId || expectedFileId !== fileId) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  let body = {};
  try {
    body = await request.json();
  } catch {
    body = {};
  }
  let sendPasswordLimitIpKey = null;
  let sendPasswordRateLimit = null;
  if (send.passwordHash) {
    const clientIdentifier = getClientIdentifier(request);
    if (!clientIdentifier) {
      return errorResponse("Client IP is required", 403);
    }
    sendPasswordLimitIpKey = sendPasswordLimitKey(clientIdentifier);
    sendPasswordRateLimit = new RateLimitService(env.DB);
    const sendPasswordCheck = await sendPasswordRateLimit.checkLoginAttempt(sendPasswordLimitIpKey);
    if (!sendPasswordCheck.allowed) {
      return sendPasswordLockedErrorResponse(sendPasswordCheck.retryAfterSeconds || 60);
    }
  }
  const validation = await validatePublicSendAccess(send, body);
  if (!validation.ok) {
    if (validation.reason === "invalid_password" && sendPasswordRateLimit && sendPasswordLimitIpKey) {
      const failed = await sendPasswordRateLimit.recordFailedLogin(sendPasswordLimitIpKey);
      if (failed.locked) {
        return sendPasswordLockedErrorResponse(failed.retryAfterSeconds || 60);
      }
    }
    return validation.response;
  }
  if (send.passwordHash && sendPasswordRateLimit && sendPasswordLimitIpKey) {
    await sendPasswordRateLimit.clearLoginAttempts(sendPasswordLimitIpKey);
  }
  const updated = await storage.incrementSendAccessCount(send.id);
  if (!updated) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  send.accessCount += 1;
  const revisionDate = await storage.updateRevisionDate(send.userId);
  notifyVaultSyncForRequest4(request, env, send.userId, revisionDate);
  const token = await createSendFileDownloadToken(send.id, fileId, secret);
  const url = new URL(request.url);
  const downloadUrl = `${url.origin}/api/sends/${send.id}/${fileId}?t=${token}`;
  return jsonResponse({
    object: "send-fileDownload",
    id: fileId,
    url: downloadUrl
  });
}
__name(handleAccessSendFile, "handleAccessSendFile");
async function handleAccessSendV2(request, env) {
  const jwt = getSafeJwtSecret2(env);
  if (!jwt.ok) return jwt.response;
  const token = extractBearerToken(request);
  if (!token) {
    return errorResponse("Unauthorized", 401);
  }
  const claims = await verifySendAccessToken(token, jwt.secret);
  if (!claims) {
    return errorResponse("Unauthorized", 401);
  }
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(claims.sub);
  if (!send || !isSendAvailable(send)) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  if (send.type === 0 /* Text */) {
    const updated = await storage.incrementSendAccessCount(send.id);
    if (!updated) {
      return errorResponse(SEND_INACCESSIBLE_MSG, 404);
    }
    send.accessCount += 1;
    const revisionDate = await storage.updateRevisionDate(send.userId);
    notifyVaultSyncForRequest4(request, env, send.userId, revisionDate);
  }
  const creatorIdentifier = await getCreatorIdentifier(storage, send);
  return jsonResponse(sendToAccessResponse(send, creatorIdentifier));
}
__name(handleAccessSendV2, "handleAccessSendV2");
async function handleAccessSendFileV2(request, env, fileId) {
  const jwt = getSafeJwtSecret2(env);
  if (!jwt.ok) return jwt.response;
  const token = extractBearerToken(request);
  if (!token) {
    return errorResponse("Unauthorized", 401);
  }
  const claims = await verifySendAccessToken(token, jwt.secret);
  if (!claims) {
    return errorResponse("Unauthorized", 401);
  }
  const storage = new StorageService(env.DB);
  const send = await storage.getSend(claims.sub);
  if (!send || !isSendAvailable(send) || send.type !== 1 /* File */) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  const data = parseStoredSendData(send);
  const expectedFileId = typeof data.id === "string" ? data.id : null;
  if (!expectedFileId || expectedFileId !== fileId) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  const updated = await storage.incrementSendAccessCount(send.id);
  if (!updated) {
    return errorResponse(SEND_INACCESSIBLE_MSG, 404);
  }
  send.accessCount += 1;
  const revisionDate = await storage.updateRevisionDate(send.userId);
  notifyVaultSyncForRequest4(request, env, send.userId, revisionDate);
  const downloadToken = await createSendFileDownloadToken(send.id, fileId, jwt.secret);
  const url = new URL(request.url);
  const downloadUrl = `${url.origin}/api/sends/${send.id}/${fileId}?t=${downloadToken}`;
  return jsonResponse({
    object: "send-fileDownload",
    id: fileId,
    url: downloadUrl
  });
}
__name(handleAccessSendFileV2, "handleAccessSendFileV2");
async function handleDownloadSendFile(request, env, sendId, fileId) {
  const jwt = getSafeJwtSecret2(env);
  if (!jwt.ok) return jwt.response;
  const url = new URL(request.url);
  const token = url.searchParams.get("t") || url.searchParams.get("token");
  if (!token) {
    return errorResponse("Token required", 401);
  }
  const claims = await verifySendFileDownloadToken(token, jwt.secret);
  if (!claims) {
    return errorResponse("Invalid or expired token", 401);
  }
  if (claims.sendId !== sendId || claims.fileId !== fileId) {
    return errorResponse("Token mismatch", 401);
  }
  const storage = new StorageService(env.DB);
  const object = await getBlobObject(env, getSendFileObjectKey(sendId, fileId));
  if (!object) {
    return errorResponse("Send file not found", 404);
  }
  const firstUse = await storage.consumeAttachmentDownloadToken(`send:${claims.jti}`, claims.exp);
  if (!firstUse) {
    return errorResponse("Invalid or expired token", 401);
  }
  return new Response(object.body, {
    headers: {
      "Content-Type": object.contentType || "application/octet-stream",
      "Content-Length": String(object.size),
      "Cache-Control": "private, no-cache"
    }
  });
}
__name(handleDownloadSendFile, "handleDownloadSendFile");
async function issueSendAccessToken(env, sendIdOrAccessId, passwordHashB64, password, rateLimit, sendPasswordLimitIpKey) {
  const jwt = getSafeJwtSecret2(env);
  if (!jwt.ok) {
    return { error: jwt.response };
  }
  const storage = new StorageService(env.DB);
  const send = await resolveSendFromIdOrAccessId(storage, sendIdOrAccessId);
  if (!send || !isSendAvailable(send)) {
    return {
      error: jsonResponse(
        {
          error: "invalid_grant",
          error_description: SEND_INACCESSIBLE_MSG,
          send_access_error_type: "send_not_available",
          ErrorModel: {
            Message: SEND_INACCESSIBLE_MSG,
            Object: "error"
          }
        },
        400
      )
    };
  }
  if (hasEmailAuth(send)) {
    const message = "Email verification for this Send is not supported by this server.";
    return {
      error: jsonResponse(
        {
          error: "invalid_grant",
          error_description: message,
          send_access_error_type: "email_verification_not_supported",
          ErrorModel: {
            Message: message,
            Object: "error"
          }
        },
        400
      )
    };
  }
  if (send.passwordHash) {
    if (rateLimit && sendPasswordLimitIpKey) {
      const sendPasswordCheck = await rateLimit.checkLoginAttempt(sendPasswordLimitIpKey);
      if (!sendPasswordCheck.allowed) {
        return {
          error: sendPasswordLockedOAuthResponse(sendPasswordCheck.retryAfterSeconds || 60)
        };
      }
    }
    let ok = false;
    if (passwordHashB64) {
      ok = verifySendPasswordHashB64(send, passwordHashB64);
    } else if (password) {
      ok = await verifySendPassword(send, password);
    }
    if (!ok) {
      if (rateLimit && sendPasswordLimitIpKey) {
        const failed = await rateLimit.recordFailedLogin(sendPasswordLimitIpKey);
        if (failed.locked) {
          return {
            error: sendPasswordLockedOAuthResponse(failed.retryAfterSeconds || 60)
          };
        }
      }
      return {
        error: jsonResponse(
          {
            error: "invalid_grant",
            error_description: "Invalid password.",
            send_access_error_type: "invalid_password",
            ErrorModel: {
              Message: "Invalid password.",
              Object: "error"
            }
          },
          400
        )
      };
    }
    if (rateLimit && sendPasswordLimitIpKey) {
      await rateLimit.clearLoginAttempts(sendPasswordLimitIpKey);
    }
  }
  const token = await createSendAccessToken(send.id, jwt.secret);
  return { token };
}
__name(issueSendAccessToken, "issueSendAccessToken");

// src/handlers/sync.ts
function buildSyncCacheRequest(request, userId, revisionDate, excludeDomains, excludeSends) {
  const url = new URL(request.url);
  const cacheUrl = new URL(
    `/__nodewarden/cache/sync/${encodeURIComponent(userId)}/${encodeURIComponent(revisionDate)}/${excludeDomains ? "1" : "0"}/${excludeSends ? "1" : "0"}`,
    url.origin
  );
  return new Request(cacheUrl.toString(), { method: "GET" });
}
__name(buildSyncCacheRequest, "buildSyncCacheRequest");
async function readSyncCache(cacheRequest) {
  const hit = await caches.default.match(cacheRequest);
  if (!hit) return null;
  return new Response(hit.body, hit);
}
__name(readSyncCache, "readSyncCache");
async function writeSyncCache(cacheRequest, response) {
  await caches.default.put(cacheRequest, response.clone());
}
__name(writeSyncCache, "writeSyncCache");
async function handleSync(request, env, userId) {
  const storage = new StorageService(env.DB);
  const url = new URL(request.url);
  const excludeDomainsParam = url.searchParams.get("excludeDomains");
  const excludeDomains = excludeDomainsParam !== null && /^(1|true|yes)$/i.test(excludeDomainsParam);
  const excludeSendsParam = url.searchParams.get("excludeSends");
  const excludeSends = excludeSendsParam !== null && /^(1|true|yes)$/i.test(excludeSendsParam);
  const user = await storage.getUserById(userId);
  if (!user) {
    return errorResponse("User not found", 404);
  }
  const revisionDate = await storage.getRevisionDate(userId);
  const cacheRequest = buildSyncCacheRequest(request, userId, revisionDate, excludeDomains, excludeSends);
  const cachedResponse = await readSyncCache(cacheRequest);
  if (cachedResponse) {
    return cachedResponse;
  }
  const [ciphers, folders, sends, attachmentsByCipher, domainSettings] = await Promise.all([
    storage.getAllCiphers(userId),
    storage.getAllFolders(userId),
    excludeSends ? Promise.resolve([]) : storage.getAllSends(userId),
    storage.getAttachmentsByUserId(userId),
    excludeDomains ? Promise.resolve(null) : storage.getUserDomainSettings(userId)
  ]);
  const accountKeys = buildAccountKeys(user);
  const userDecryptionOptions = buildUserDecryptionOptions(user);
  const profile = {
    id: user.id,
    name: user.name,
    email: user.email,
    emailVerified: true,
    premium: true,
    premiumFromOrganization: false,
    usesKeyConnector: false,
    masterPasswordHint: user.masterPasswordHint,
    culture: "en-US",
    twoFactorEnabled: !!user.totpSecret,
    key: user.key,
    privateKey: user.privateKey,
    accountKeys,
    securityStamp: user.securityStamp || user.id,
    organizations: [],
    providers: [],
    providerOrganizations: [],
    forcePasswordReset: false,
    avatarColor: null,
    creationDate: user.createdAt,
    verifyDevices: user.verifyDevices,
    object: "profile"
  };
  const cipherResponses = [];
  for (const cipher of ciphers) {
    const response2 = cipherToResponse(cipher, attachmentsByCipher.get(cipher.id) || []);
    if (isCipherResponseSyncCompatible(response2)) {
      cipherResponses.push(response2);
    }
  }
  const folderResponses = [];
  for (const folder of folders) {
    folderResponses.push({
      id: folder.id,
      name: folder.name,
      revisionDate: folder.updatedAt,
      creationDate: folder.createdAt,
      object: "folder"
    });
  }
  const sendResponses = sends.map(sendToResponse);
  const syncResponse = {
    profile,
    folders: folderResponses,
    collections: [],
    ciphers: cipherResponses,
    domains: excludeDomains ? null : buildDomainsResponse(
      domainSettings?.equivalentDomains || [],
      domainSettings?.customEquivalentDomains || [],
      domainSettings?.excludedGlobalEquivalentDomains || [],
      { omitExcludedGlobals: true }
    ),
    policies: [],
    sends: sendResponses,
    UserDecryption: {
      MasterPasswordUnlock: userDecryptionOptions.MasterPasswordUnlock,
      TrustedDeviceOption: null,
      KeyConnectorOption: null,
      Object: "userDecryption"
    },
    UserDecryptionOptions: userDecryptionOptions,
    userDecryption: buildUserDecryptionCompat(user),
    object: "sync"
  };
  const response = new Response(JSON.stringify(syncResponse), {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `private, max-age=${Math.max(1, Math.floor(LIMITS.cache.syncResponseTtlMs / 1e3))}`
    }
  });
  await writeSyncCache(cacheRequest, response);
  return response;
}
__name(handleSync, "handleSync");

// src/handlers/import.ts
function bindNull(v) {
  return v === void 0 ? null : v;
}
__name(bindNull, "bindNull");
function readAliasedImportProp(source, aliases) {
  if (!source || typeof source !== "object") return void 0;
  for (const key of aliases) {
    if (Object.prototype.hasOwnProperty.call(source, key)) {
      return source[key];
    }
  }
  return void 0;
}
__name(readAliasedImportProp, "readAliasedImportProp");
async function runBatchInChunks(db, statements, chunkSize) {
  for (let i = 0; i < statements.length; i += chunkSize) {
    const chunk = statements.slice(i, i + chunkSize);
    await db.batch(chunk);
  }
}
__name(runBatchInChunks, "runBatchInChunks");
async function handleCiphersImport(request, env, userId) {
  const storage = new StorageService(env.DB);
  const url = new URL(request.url);
  const returnCipherMap = url.searchParams.get("returnCipherMap") === "1";
  let importData;
  try {
    importData = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const folders = importData.folders || [];
  const ciphers = importData.ciphers || [];
  const folderRelationships = importData.folderRelationships || [];
  if (folders.length + ciphers.length > LIMITS.performance.importItemLimit) {
    return errorResponse(`Import exceeds maximum of ${LIMITS.performance.importItemLimit} items`, 400);
  }
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const batchChunkSize = LIMITS.performance.bulkMoveChunkSize;
  const folderIdMap = /* @__PURE__ */ new Map();
  const folderRows = [];
  for (let i = 0; i < folders.length; i++) {
    const folderId = generateUUID();
    folderIdMap.set(i, folderId);
    const folder = {
      id: folderId,
      userId,
      name: folders[i].name,
      createdAt: now,
      updatedAt: now
    };
    folderRows.push(folder);
  }
  if (folderRows.length > 0) {
    const folderStatements = folderRows.map(
      (folder) => env.DB.prepare(
        "INSERT INTO folders(id, user_id, name, created_at, updated_at) VALUES(?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET user_id=excluded.user_id, name=excluded.name, updated_at=excluded.updated_at"
      ).bind(folder.id, folder.userId, folder.name, folder.createdAt, folder.updatedAt)
    );
    await runBatchInChunks(env.DB, folderStatements, batchChunkSize);
  }
  const cipherFolderMap = /* @__PURE__ */ new Map();
  for (const rel of folderRelationships) {
    const folderId = folderIdMap.get(rel.value);
    if (folderId) {
      cipherFolderMap.set(rel.key, folderId);
    }
  }
  const cipherRows = [];
  const cipherMapRows = [];
  for (let i = 0; i < ciphers.length; i++) {
    const c = ciphers[i];
    const folderId = cipherFolderMap.get(i) || readAliasedImportProp(c, ["folderId", "FolderId"]) || null;
    const sourceIdRaw = String(c?.id ?? "").trim();
    const sourceId = sourceIdRaw || null;
    const login = readAliasedImportProp(c, ["login", "Login"]);
    const card = readAliasedImportProp(c, ["card", "Card"]);
    const identity = readAliasedImportProp(c, ["identity", "Identity"]);
    const secureNote = readAliasedImportProp(c, ["secureNote", "SecureNote"]);
    const fields = readAliasedImportProp(c, ["fields", "Fields"]);
    const passwordHistory = readAliasedImportProp(c, ["passwordHistory", "PasswordHistory"]);
    const key = readAliasedImportProp(c, ["key", "Key"]);
    const cipher = {
      ...c,
      id: generateUUID(),
      userId,
      type: c.type,
      folderId,
      name: c.name ?? "Untitled",
      notes: c.notes ?? null,
      favorite: c.favorite ?? false,
      login: login ? {
        ...login,
        username: login.username ?? null,
        password: login.password ?? null,
        uris: login.uris?.map((u) => ({
          ...u,
          uri: u.uri ?? null,
          uriChecksum: u.uriChecksum ?? null,
          match: u.match ?? null
        })) || null,
        totp: login.totp ?? null,
        autofillOnPageLoad: login.autofillOnPageLoad ?? null,
        fido2Credentials: Array.isArray(login.fido2Credentials) ? login.fido2Credentials : null,
        uri: login.uri ?? null,
        passwordRevisionDate: login.passwordRevisionDate ?? null
      } : null,
      card: card ? {
        ...card,
        cardholderName: card.cardholderName ?? null,
        brand: card.brand ?? null,
        number: card.number ?? null,
        expMonth: card.expMonth ?? null,
        expYear: card.expYear ?? null,
        code: card.code ?? null
      } : null,
      identity: identity ? {
        ...identity,
        title: identity.title ?? null,
        firstName: identity.firstName ?? null,
        middleName: identity.middleName ?? null,
        lastName: identity.lastName ?? null,
        address1: identity.address1 ?? null,
        address2: identity.address2 ?? null,
        address3: identity.address3 ?? null,
        city: identity.city ?? null,
        state: identity.state ?? null,
        postalCode: identity.postalCode ?? null,
        country: identity.country ?? null,
        company: identity.company ?? null,
        email: identity.email ?? null,
        phone: identity.phone ?? null,
        ssn: identity.ssn ?? null,
        username: identity.username ?? null,
        passportNumber: identity.passportNumber ?? null,
        licenseNumber: identity.licenseNumber ?? null
      } : null,
      secureNote: secureNote ?? null,
      fields: fields?.map((f) => ({
        ...f,
        name: f.name ?? null,
        value: f.value ?? null,
        type: f.type,
        linkedId: f.linkedId ?? null
      })) || null,
      passwordHistory: passwordHistory ?? null,
      reprompt: c.reprompt ?? 0,
      sshKey: normalizeCipherSshKeyForCompatibility(c.sshKey ?? null),
      key: key ?? null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null
    };
    cipher.login = normalizeCipherLoginForStorage(cipher.login);
    cipherRows.push(cipher);
    cipherMapRows.push({ index: i, sourceId, id: cipher.id });
  }
  if (cipherRows.length > 0) {
    const cipherStatements = cipherRows.map((cipher) => {
      const data = JSON.stringify(cipher);
      return env.DB.prepare(
        "INSERT INTO ciphers(id, user_id, type, folder_id, name, notes, favorite, data, reprompt, key, created_at, updated_at, archived_at, deleted_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET user_id=excluded.user_id, type=excluded.type, folder_id=excluded.folder_id, name=excluded.name, notes=excluded.notes, favorite=excluded.favorite, data=excluded.data, reprompt=excluded.reprompt, key=excluded.key, updated_at=excluded.updated_at, archived_at=excluded.archived_at, deleted_at=excluded.deleted_at"
      ).bind(
        cipher.id,
        cipher.userId,
        Number(cipher.type) || 1,
        bindNull(cipher.folderId),
        bindNull(cipher.name),
        bindNull(cipher.notes),
        cipher.favorite ? 1 : 0,
        data,
        bindNull(cipher.reprompt ?? 0),
        bindNull(cipher.key),
        cipher.createdAt,
        cipher.updatedAt,
        bindNull(cipher.archivedAt),
        bindNull(cipher.deletedAt)
      );
    });
    await runBatchInChunks(env.DB, cipherStatements, batchChunkSize);
  }
  const revisionDate = await storage.updateRevisionDate(userId);
  notifyUserVaultSync(env, userId, revisionDate, readActingDeviceIdentifier(request));
  if (returnCipherMap) {
    return jsonResponse({
      object: "import-result",
      cipherMap: cipherMapRows
    });
  }
  return new Response(null, { status: 200 });
}
__name(handleCiphersImport, "handleCiphersImport");

// src/handlers/devices.ts
var PERMANENT_TRUST_EXPIRES_AT_MS = Date.UTC(2099, 11, 31, 23, 59, 59);
function normalizeIdentifier(value) {
  return String(value || "").trim();
}
__name(normalizeIdentifier, "normalizeIdentifier");
function buildDevicePendingAuthRequest(value) {
  if (!value?.id || !value.creationDate) return null;
  return {
    id: String(value.id),
    creationDate: String(value.creationDate)
  };
}
__name(buildDevicePendingAuthRequest, "buildDevicePendingAuthRequest");
function isTrustedDevice(device) {
  return !!(device.encryptedUserKey && device.encryptedPublicKey);
}
__name(isTrustedDevice, "isTrustedDevice");
function buildDeviceResponse(device) {
  const displayName = String(device.deviceNote || "").trim() || device.name;
  const response = {
    Id: device.deviceIdentifier,
    id: device.deviceIdentifier,
    UserId: device.userId,
    userId: device.userId,
    Name: displayName,
    name: displayName,
    SystemName: device.name,
    systemName: device.name,
    DeviceNote: device.deviceNote,
    deviceNote: device.deviceNote,
    Identifier: device.deviceIdentifier,
    identifier: device.deviceIdentifier,
    Type: device.type,
    type: device.type,
    CreationDate: device.createdAt,
    creationDate: device.createdAt,
    RevisionDate: device.updatedAt,
    revisionDate: device.updatedAt,
    LastSeenAt: device.lastSeenAt,
    lastSeenAt: device.lastSeenAt,
    HasStoredDevice: true,
    hasStoredDevice: true,
    IsTrusted: isTrustedDevice(device),
    isTrusted: isTrustedDevice(device),
    EncryptedUserKey: device.encryptedUserKey,
    encryptedUserKey: device.encryptedUserKey,
    EncryptedPublicKey: device.encryptedPublicKey,
    encryptedPublicKey: device.encryptedPublicKey,
    DevicePendingAuthRequest: buildDevicePendingAuthRequest(device.devicePendingAuthRequest),
    devicePendingAuthRequest: buildDevicePendingAuthRequest(device.devicePendingAuthRequest),
    object: "device"
  };
  return response;
}
__name(buildDeviceResponse, "buildDeviceResponse");
function buildProtectedDeviceResponse(device) {
  const response = {
    Id: device.deviceIdentifier,
    id: device.deviceIdentifier,
    Name: String(device.deviceNote || "").trim() || device.name,
    name: String(device.deviceNote || "").trim() || device.name,
    SystemName: device.name,
    systemName: device.name,
    DeviceNote: device.deviceNote,
    deviceNote: device.deviceNote,
    Identifier: device.deviceIdentifier,
    identifier: device.deviceIdentifier,
    Type: device.type,
    type: device.type,
    CreationDate: device.createdAt,
    creationDate: device.createdAt,
    EncryptedUserKey: device.encryptedUserKey,
    encryptedUserKey: device.encryptedUserKey,
    EncryptedPublicKey: device.encryptedPublicKey,
    encryptedPublicKey: device.encryptedPublicKey,
    object: "protectedDevice"
  };
  return response;
}
__name(buildProtectedDeviceResponse, "buildProtectedDeviceResponse");
function parseKeysBody(body, fallback) {
  return {
    encryptedUserKey: Object.prototype.hasOwnProperty.call(body || {}, "encryptedUserKey") ? body?.encryptedUserKey ?? null : fallback?.encryptedUserKey ?? null,
    encryptedPublicKey: Object.prototype.hasOwnProperty.call(body || {}, "encryptedPublicKey") ? body?.encryptedPublicKey ?? null : fallback?.encryptedPublicKey ?? null,
    encryptedPrivateKey: Object.prototype.hasOwnProperty.call(body || {}, "encryptedPrivateKey") ? body?.encryptedPrivateKey ?? null : fallback?.encryptedPrivateKey ?? null
  };
}
__name(parseKeysBody, "parseKeysBody");
async function readJsonBody(request) {
  try {
    return await request.json();
  } catch {
    return null;
  }
}
__name(readJsonBody, "readJsonBody");
function parseDeviceName(value) {
  return String(value || "").trim().slice(0, 128);
}
__name(parseDeviceName, "parseDeviceName");
async function handleKnownDevice(request, env) {
  const storage = new StorageService(env.DB);
  const { email, deviceIdentifier } = readKnownDeviceProbe(request);
  if (!email || !deviceIdentifier) {
    return jsonResponse(false);
  }
  const known = await storage.isKnownDeviceByEmail(email, deviceIdentifier);
  return jsonResponse(known);
}
__name(handleKnownDevice, "handleKnownDevice");
async function handleGetDevices(request, env, userId) {
  void request;
  const storage = new StorageService(env.DB);
  const devices = await storage.getDevicesByUserId(userId);
  return jsonResponse({
    data: devices.map((device) => buildDeviceResponse(device)),
    object: "list",
    continuationToken: null
  });
}
__name(handleGetDevices, "handleGetDevices");
async function handleGetDeviceByIdentifier(request, env, userId, deviceIdentifier) {
  void request;
  const normalized = normalizeIdentifier(deviceIdentifier);
  if (!normalized) return errorResponse("Invalid device identifier", 400);
  const storage = new StorageService(env.DB);
  const device = await storage.getDevice(userId, normalized);
  if (!device) {
    return errorResponse("Device not found", 404);
  }
  return jsonResponse(buildDeviceResponse(device));
}
__name(handleGetDeviceByIdentifier, "handleGetDeviceByIdentifier");
async function handleGetDevice(request, env, userId, deviceIdentifier) {
  return handleGetDeviceByIdentifier(request, env, userId, deviceIdentifier);
}
__name(handleGetDevice, "handleGetDevice");
async function handleGetAuthorizedDevices(request, env, userId) {
  void request;
  const storage = new StorageService(env.DB);
  const [devices, trusted, onlineDeviceIdentifiers] = await Promise.all([
    storage.getDevicesByUserId(userId),
    storage.getTrustedDeviceTokenSummariesByUserId(userId),
    getOnlineUserDevices(env, userId)
  ]);
  const onlineSet = new Set(onlineDeviceIdentifiers);
  const trustedByIdentifier = /* @__PURE__ */ new Map();
  for (const row of trusted) {
    trustedByIdentifier.set(row.deviceIdentifier, { expiresAt: row.expiresAt, tokenCount: row.tokenCount });
  }
  const knownIdentifiers = /* @__PURE__ */ new Set();
  const data = devices.map((device) => {
    knownIdentifiers.add(device.deviceIdentifier);
    const trustedInfo = trustedByIdentifier.get(device.deviceIdentifier);
    return {
      ...buildDeviceResponse(device),
      online: onlineSet.has(device.deviceIdentifier),
      trusted: !!trustedInfo,
      trustedTokenCount: trustedInfo?.tokenCount || 0,
      trustedUntil: trustedInfo?.expiresAt ? new Date(trustedInfo.expiresAt).toISOString() : null,
      object: "device"
    };
  });
  for (const row of trusted) {
    if (knownIdentifiers.has(row.deviceIdentifier)) continue;
    const placeholderDevice = {
      userId,
      deviceIdentifier: row.deviceIdentifier,
      name: "Unknown device",
      type: 14,
      sessionStamp: "",
      encryptedUserKey: null,
      encryptedPublicKey: null,
      encryptedPrivateKey: null,
      devicePendingAuthRequest: null,
      deviceNote: null,
      lastSeenAt: null,
      createdAt: "",
      updatedAt: ""
    };
    data.push({
      ...buildDeviceResponse(placeholderDevice),
      isTrusted: true,
      hasStoredDevice: false,
      online: onlineSet.has(row.deviceIdentifier),
      trusted: true,
      trustedTokenCount: row.tokenCount,
      trustedUntil: row.expiresAt ? new Date(row.expiresAt).toISOString() : null,
      object: "device"
    });
  }
  return jsonResponse({
    data,
    object: "list",
    continuationToken: null
  });
}
__name(handleGetAuthorizedDevices, "handleGetAuthorizedDevices");
async function handleRevokeAllTrustedDevices(request, env, userId) {
  void request;
  const storage = new StorageService(env.DB);
  const removed = await storage.deleteTrustedTwoFactorTokensByUserId(userId);
  return jsonResponse({ success: true, removed });
}
__name(handleRevokeAllTrustedDevices, "handleRevokeAllTrustedDevices");
async function handleRevokeTrustedDevice(request, env, userId, deviceIdentifier) {
  void request;
  const normalized = String(deviceIdentifier || "").trim();
  if (!normalized) return errorResponse("Invalid device identifier", 400);
  const storage = new StorageService(env.DB);
  const removed = await storage.deleteTrustedTwoFactorTokensByDevice(userId, normalized);
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action: "device.trust.revoke",
    category: "device",
    level: "security",
    targetType: "device",
    targetId: normalized,
    metadata: { removed, ...auditRequestMetadata(request) }
  });
  return jsonResponse({ success: true, removed });
}
__name(handleRevokeTrustedDevice, "handleRevokeTrustedDevice");
async function handleTrustDevicePermanently(request, env, userId, deviceIdentifier) {
  void request;
  const normalized = String(deviceIdentifier || "").trim();
  if (!normalized) return errorResponse("Invalid device identifier", 400);
  const storage = new StorageService(env.DB);
  const updated = await storage.updateTrustedTwoFactorTokensExpiryByDevice(userId, normalized, PERMANENT_TRUST_EXPIRES_AT_MS);
  if (!updated) return errorResponse("Device is not currently trusted", 409);
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action: "device.trust.permanent",
    category: "device",
    level: "security",
    targetType: "device",
    targetId: normalized,
    metadata: { updated, ...auditRequestMetadata(request) }
  });
  return jsonResponse({
    success: true,
    updated,
    trustedUntil: new Date(PERMANENT_TRUST_EXPIRES_AT_MS).toISOString()
  });
}
__name(handleTrustDevicePermanently, "handleTrustDevicePermanently");
async function handleDeleteDevice(request, env, userId, deviceIdentifier) {
  void request;
  const normalized = String(deviceIdentifier || "").trim();
  if (!normalized) return errorResponse("Invalid device identifier", 400);
  const storage = new StorageService(env.DB);
  await storage.deleteTrustedTwoFactorTokensByDevice(userId, normalized);
  await storage.deleteRefreshTokensByDevice(userId, normalized);
  const deleted = await storage.deleteDevice(userId, normalized);
  if (deleted) {
    AuthService.invalidateDeviceCache(userId, normalized);
    notifyUserLogout(env, userId, normalized);
  }
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action: "device.delete",
    category: "device",
    level: "security",
    targetType: "device",
    targetId: normalized,
    metadata: { deleted, ...auditRequestMetadata(request) }
  });
  return jsonResponse({ success: deleted });
}
__name(handleDeleteDevice, "handleDeleteDevice");
async function handleUpdateDeviceName(request, env, userId, deviceIdentifier) {
  const normalized = String(deviceIdentifier || "").trim();
  if (!normalized) return errorResponse("Invalid device identifier", 400);
  const body = await readJsonBody(request);
  const name = parseDeviceName(body?.name);
  if (!name) return errorResponse("Device name is required", 400);
  const storage = new StorageService(env.DB);
  const updated = await storage.updateDeviceName(userId, normalized, name);
  if (!updated) return errorResponse("Device not found", 404);
  const device = await storage.getDevice(userId, normalized);
  if (!device) return errorResponse("Device not found", 404);
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action: "device.name.update",
    category: "device",
    level: "info",
    targetType: "device",
    targetId: normalized,
    metadata: { name, ...auditRequestMetadata(request) }
  });
  return jsonResponse(buildDeviceResponse(device));
}
__name(handleUpdateDeviceName, "handleUpdateDeviceName");
async function handleDeleteAllDevices(request, env, userId) {
  void request;
  const storage = new StorageService(env.DB);
  const user = await storage.getUserById(userId);
  if (!user) return errorResponse("User not found", 404);
  const [removedTrusted, removedSessions, removedDevices] = await Promise.all([
    storage.deleteTrustedTwoFactorTokensByUserId(userId),
    storage.deleteRefreshTokensByUserId(userId),
    storage.deleteDevicesByUserId(userId)
  ]);
  user.securityStamp = generateUUID();
  user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveUser(user);
  AuthService.invalidateUserCache(userId);
  notifyUserLogout(env, userId, null);
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action: "device.delete_all",
    category: "device",
    level: "security",
    targetType: "user",
    targetId: userId,
    metadata: { removedTrusted, removedSessions, removedDevices, ...auditRequestMetadata(request) }
  });
  return jsonResponse({ success: true, removedTrusted, removedSessions: removedSessions ?? 0, removedDevices });
}
__name(handleDeleteAllDevices, "handleDeleteAllDevices");
async function handleUpdateDeviceKeys(request, env, userId, deviceIdentifier) {
  const normalized = normalizeIdentifier(deviceIdentifier);
  if (!normalized) return errorResponse("Invalid device identifier", 400);
  const body = await readJsonBody(request);
  const storage = new StorageService(env.DB);
  const device = await storage.getDevice(userId, normalized);
  if (!device) {
    return errorResponse("Device not found", 404);
  }
  const updated = await storage.updateDeviceKeys(userId, normalized, parseKeysBody(body, device));
  if (!updated) {
    return errorResponse("Device not found", 404);
  }
  const nextDevice = await storage.getDevice(userId, normalized);
  return jsonResponse(buildDeviceResponse(nextDevice || device));
}
__name(handleUpdateDeviceKeys, "handleUpdateDeviceKeys");
async function handleUpdateDeviceTrust(request, env, userId) {
  const body = await readJsonBody(request);
  const storage = new StorageService(env.DB);
  const currentDeviceIdentifier = normalizeIdentifier(request.headers.get("Device-Identifier")) || normalizeIdentifier(request.headers.get("X-Device-Identifier"));
  const updates = [];
  if (currentDeviceIdentifier && body?.currentDevice) {
    updates.push({
      deviceIdentifier: currentDeviceIdentifier,
      keys: parseKeysBody(body.currentDevice, await storage.getDevice(userId, currentDeviceIdentifier) || void 0)
    });
  }
  if (Array.isArray(body?.otherDevices)) {
    for (const item of body.otherDevices) {
      const deviceIdentifier = normalizeIdentifier(item?.deviceId);
      if (!deviceIdentifier) continue;
      updates.push({
        deviceIdentifier,
        keys: parseKeysBody(item, await storage.getDevice(userId, deviceIdentifier) || void 0)
      });
    }
  }
  let updatedCount = 0;
  for (const update of updates) {
    const ok = await storage.updateDeviceKeys(userId, update.deviceIdentifier, update.keys);
    if (ok) updatedCount++;
  }
  return jsonResponse({ success: true, updated: updatedCount });
}
__name(handleUpdateDeviceTrust, "handleUpdateDeviceTrust");
async function handleUntrustDevices(request, env, userId) {
  const body = await readJsonBody(request);
  const storage = new StorageService(env.DB);
  const devices = Array.isArray(body?.devices) ? body.devices.map((id) => normalizeIdentifier(String(id))) : [];
  const removed = await storage.clearDeviceKeys(userId, devices);
  for (const deviceIdentifier of devices) {
    if (!deviceIdentifier) continue;
    await storage.deleteTrustedTwoFactorTokensByDevice(userId, deviceIdentifier);
  }
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action: "device.trust.revoke_batch",
    category: "device",
    level: "security",
    targetType: "user",
    targetId: userId,
    metadata: { requested: devices.length, removed, ...auditRequestMetadata(request) }
  });
  return jsonResponse({ success: true, removed });
}
__name(handleUntrustDevices, "handleUntrustDevices");
async function handleRetrieveDeviceKeys(request, env, userId, deviceIdentifier) {
  void request;
  const normalized = normalizeIdentifier(deviceIdentifier);
  if (!normalized) return errorResponse("Invalid device identifier", 400);
  const storage = new StorageService(env.DB);
  const device = await storage.getDevice(userId, normalized);
  if (!device) {
    return errorResponse("Device not found", 404);
  }
  return jsonResponse(buildProtectedDeviceResponse(device));
}
__name(handleRetrieveDeviceKeys, "handleRetrieveDeviceKeys");
async function handleDeactivateDevice(request, env, userId, deviceIdentifier) {
  void request;
  const normalized = normalizeIdentifier(deviceIdentifier);
  if (!normalized) return errorResponse("Invalid device identifier", 400);
  const storage = new StorageService(env.DB);
  await storage.deleteTrustedTwoFactorTokensByDevice(userId, normalized);
  await storage.deleteRefreshTokensByDevice(userId, normalized);
  const deleted = await storage.deleteDevice(userId, normalized);
  if (deleted) {
    AuthService.invalidateDeviceCache(userId, normalized);
    notifyUserLogout(env, userId, normalized);
  }
  await writeAuditEvent(storage, {
    actorUserId: userId,
    action: "device.deactivate",
    category: "device",
    level: "security",
    targetType: "device",
    targetId: normalized,
    metadata: { deleted, ...auditRequestMetadata(request) }
  });
  return jsonResponse({ success: deleted });
}
__name(handleDeactivateDevice, "handleDeactivateDevice");
async function handleUpdateDeviceToken(request, env, userId, deviceIdentifier) {
  void request;
  void env;
  void userId;
  void deviceIdentifier;
  return new Response(null, { status: 200 });
}
__name(handleUpdateDeviceToken, "handleUpdateDeviceToken");
async function handleUpdateDeviceWebPushAuth(request, env, userId, deviceIdentifier) {
  void request;
  void env;
  void userId;
  void deviceIdentifier;
  return new Response(null, { status: 200 });
}
__name(handleUpdateDeviceWebPushAuth, "handleUpdateDeviceWebPushAuth");
async function handleClearDeviceToken(request, env, userId, deviceIdentifier) {
  void request;
  void env;
  void userId;
  void deviceIdentifier;
  return new Response(null, { status: 200 });
}
__name(handleClearDeviceToken, "handleClearDeviceToken");

// src/router-devices.ts
async function handleAuthenticatedDeviceRoute(request, env, userId, path, method) {
  if (path === "/api/devices") {
    if (method === "GET") return handleGetDevices(request, env, userId);
    if (method === "DELETE") return handleDeleteAllDevices(request, env, userId);
    return null;
  }
  if (path === "/api/devices/authorized") {
    if (method === "GET") return handleGetAuthorizedDevices(request, env, userId);
    if (method === "DELETE") return handleRevokeAllTrustedDevices(request, env, userId);
    return null;
  }
  const authorizedDeviceMatch = path.match(/^\/api\/devices\/authorized\/([^/]+)$/i);
  if (authorizedDeviceMatch && method === "DELETE") {
    const deviceIdentifier = decodeURIComponent(authorizedDeviceMatch[1]);
    return handleRevokeTrustedDevice(request, env, userId, deviceIdentifier);
  }
  const permanentAuthorizedDeviceMatch = path.match(/^\/api\/devices\/authorized\/([^/]+)\/permanent$/i);
  if (permanentAuthorizedDeviceMatch && method === "POST") {
    const deviceIdentifier = decodeURIComponent(permanentAuthorizedDeviceMatch[1]);
    return handleTrustDevicePermanently(request, env, userId, deviceIdentifier);
  }
  const deleteDeviceMatch = path.match(/^\/api\/devices\/([^/]+)$/i);
  if (deleteDeviceMatch && method === "GET") {
    const deviceIdentifier = decodeURIComponent(deleteDeviceMatch[1]);
    return handleGetDevice(request, env, userId, deviceIdentifier);
  }
  if (deleteDeviceMatch && method === "DELETE") {
    const deviceIdentifier = decodeURIComponent(deleteDeviceMatch[1]);
    return handleDeleteDevice(request, env, userId, deviceIdentifier);
  }
  const updateDeviceNameMatch = path.match(/^\/api\/devices\/([^/]+)\/name$/i);
  if (updateDeviceNameMatch && method === "PUT") {
    const deviceIdentifier = decodeURIComponent(updateDeviceNameMatch[1]);
    return handleUpdateDeviceName(request, env, userId, deviceIdentifier);
  }
  const identifierMatch = path.match(/^\/api\/devices\/identifier\/([^/]+)$/i);
  if (identifierMatch && method === "GET") {
    const deviceIdentifier = decodeURIComponent(identifierMatch[1]);
    return handleGetDeviceByIdentifier(request, env, userId, deviceIdentifier);
  }
  const deviceKeysMatch = path.match(/^\/api\/devices\/([^/]+)\/keys$/i) || path.match(/^\/api\/devices\/identifier\/([^/]+)\/keys$/i);
  if (deviceKeysMatch && (method === "PUT" || method === "POST")) {
    const deviceIdentifier = decodeURIComponent(deviceKeysMatch[1]);
    return handleUpdateDeviceKeys(request, env, userId, deviceIdentifier);
  }
  const identifierTokenMatch = path.match(/^\/api\/devices\/identifier\/([^/]+)\/token$/i);
  if (identifierTokenMatch && (method === "PUT" || method === "POST")) {
    const deviceIdentifier = decodeURIComponent(identifierTokenMatch[1]);
    return handleUpdateDeviceToken(request, env, userId, deviceIdentifier);
  }
  const identifierWebPushMatch = path.match(/^\/api\/devices\/identifier\/([^/]+)\/web-push-auth$/i);
  if (identifierWebPushMatch && (method === "PUT" || method === "POST")) {
    const deviceIdentifier = decodeURIComponent(identifierWebPushMatch[1]);
    return handleUpdateDeviceWebPushAuth(request, env, userId, deviceIdentifier);
  }
  const identifierClearTokenMatch = path.match(/^\/api\/devices\/identifier\/([^/]+)\/clear-token$/i);
  if (identifierClearTokenMatch && (method === "PUT" || method === "POST")) {
    const deviceIdentifier = decodeURIComponent(identifierClearTokenMatch[1]);
    return handleClearDeviceToken(request, env, userId, deviceIdentifier);
  }
  const identifierRetrieveKeysMatch = path.match(/^\/api\/devices\/([^/]+)\/retrieve-keys$/i);
  if (identifierRetrieveKeysMatch && method === "POST") {
    const deviceIdentifier = decodeURIComponent(identifierRetrieveKeysMatch[1]);
    return handleRetrieveDeviceKeys(request, env, userId, deviceIdentifier);
  }
  const identifierDeactivateMatch = path.match(/^\/api\/devices\/([^/]+)\/deactivate$/i);
  if (identifierDeactivateMatch && (method === "POST" || method === "DELETE")) {
    const deviceIdentifier = decodeURIComponent(identifierDeactivateMatch[1]);
    return handleDeactivateDevice(request, env, userId, deviceIdentifier);
  }
  if (path === "/api/devices/update-trust" && method === "POST") {
    return handleUpdateDeviceTrust(request, env, userId);
  }
  if (path === "/api/devices/untrust" && method === "POST") {
    return handleUntrustDevices(request, env, userId);
  }
  return null;
}
__name(handleAuthenticatedDeviceRoute, "handleAuthenticatedDeviceRoute");

// src/handlers/admin.ts
function isAdmin(user) {
  return user.role === "admin" && user.status === "active";
}
__name(isAdmin, "isAdmin");
function randomHex(bytes) {
  const data = crypto.getRandomValues(new Uint8Array(bytes));
  return Array.from(data).map((v) => v.toString(16).padStart(2, "0")).join("");
}
__name(randomHex, "randomHex");
function buildInviteLink(request, code) {
  const url = new URL(request.url);
  return `${url.origin}/?invite=${encodeURIComponent(code)}`;
}
__name(buildInviteLink, "buildInviteLink");
async function writeAuditLog(storage, actorUserId, action, targetType, targetId, metadata, request) {
  await writeAuditEvent(storage, {
    actorUserId,
    action,
    targetType,
    targetId,
    category: action.startsWith("admin.user.") ? "security" : "system",
    level: action.startsWith("admin.user.") ? "security" : "info",
    metadata: {
      ...metadata || {},
      ...request ? auditRequestMetadata(request) : {}
    }
  });
}
__name(writeAuditLog, "writeAuditLog");
function toInviteResponse(request, invite) {
  return {
    code: invite.code,
    status: invite.status,
    createdBy: invite.createdBy,
    usedBy: invite.usedBy,
    createdAt: invite.createdAt,
    updatedAt: invite.updatedAt,
    expiresAt: invite.expiresAt,
    inviteLink: buildInviteLink(request, invite.code),
    object: "invite"
  };
}
__name(toInviteResponse, "toInviteResponse");
async function handleAdminListUsers(request, env, actorUser) {
  void request;
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  const storage = new StorageService(env.DB);
  const users = await storage.getAllUsers();
  return jsonResponse({
    data: users.map((user) => ({
      id: user.id,
      email: user.email,
      name: user.name,
      role: user.role,
      status: user.status,
      twoFactorEnabled: !!user.totpSecret,
      creationDate: user.createdAt,
      revisionDate: user.updatedAt,
      object: "user"
    })),
    object: "list",
    continuationToken: null
  });
}
__name(handleAdminListUsers, "handleAdminListUsers");
async function handleAdminListAuditLogs(request, env, actorUser) {
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  const url = new URL(request.url);
  const limit = Math.max(1, Math.min(200, Number(url.searchParams.get("limit") || 50)));
  const offset = Math.max(0, Number(url.searchParams.get("offset") || 0));
  const category = String(url.searchParams.get("category") || "").trim() || null;
  const level = String(url.searchParams.get("level") || "").trim() || null;
  const q = String(url.searchParams.get("q") || "").trim().toLowerCase() || null;
  const from = String(url.searchParams.get("from") || "").trim() || null;
  const to = String(url.searchParams.get("to") || "").trim() || null;
  const storage = new StorageService(env.DB);
  const result = await storage.listAuditLogs({ limit, offset, category, level, q, from, to });
  return jsonResponse({
    data: result.logs.map((log) => ({
      id: log.id,
      actorUserId: log.actorUserId,
      actorEmail: log.actorEmail,
      action: log.action,
      category: log.category,
      level: log.level,
      targetType: log.targetType,
      targetId: log.targetId,
      targetUserEmail: log.targetUserEmail,
      metadata: log.metadata,
      createdAt: log.createdAt,
      object: "auditLog"
    })),
    total: result.total,
    limit,
    offset,
    hasMore: result.hasMore,
    object: "list",
    continuationToken: result.hasMore ? String(offset + result.logs.length) : null
  });
}
__name(handleAdminListAuditLogs, "handleAdminListAuditLogs");
async function handleAdminGetAuditLogSettings(request, env, actorUser) {
  void request;
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  const storage = new StorageService(env.DB);
  return jsonResponse({
    object: "auditLogSettings",
    ...await getAuditLogSettings(storage)
  });
}
__name(handleAdminGetAuditLogSettings, "handleAdminGetAuditLogSettings");
async function handleAdminUpdateAuditLogSettings(request, env, actorUser) {
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const storage = new StorageService(env.DB);
  const settings = await saveAuditLogSettings(storage, normalizeAuditLogSettings(body));
  await writeAuditLog(storage, actorUser.id, "admin.audit.settings.update", "auditLog", null, { ...settings }, request);
  return jsonResponse({
    object: "auditLogSettings",
    ...settings
  });
}
__name(handleAdminUpdateAuditLogSettings, "handleAdminUpdateAuditLogSettings");
async function handleAdminClearAuditLogs(request, env, actorUser) {
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  const storage = new StorageService(env.DB);
  const deleted = await storage.clearAuditLogs();
  return jsonResponse({ object: "auditLogClear", deleted });
}
__name(handleAdminClearAuditLogs, "handleAdminClearAuditLogs");
async function handleAdminCreateInvite(request, env, actorUser) {
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  const storage = new StorageService(env.DB);
  let body = {};
  try {
    body = await request.json();
  } catch {
    body = {};
  }
  const expiresInHours = Number.isFinite(body.expiresInHours) ? Math.max(1, Math.min(24 * 30, Math.floor(Number(body.expiresInHours)))) : 24 * 7;
  const now = /* @__PURE__ */ new Date();
  const expiresAt = new Date(now.getTime() + expiresInHours * 60 * 60 * 1e3);
  const invite = {
    code: randomHex(20),
    createdBy: actorUser.id,
    usedBy: null,
    expiresAt: expiresAt.toISOString(),
    status: "active",
    createdAt: now.toISOString(),
    updatedAt: now.toISOString()
  };
  await storage.createInvite(invite);
  await writeAuditLog(storage, actorUser.id, "admin.invite.create", "invite", null, {
    expiresInHours
  }, request);
  return jsonResponse(toInviteResponse(request, invite), 201);
}
__name(handleAdminCreateInvite, "handleAdminCreateInvite");
async function handleAdminListInvites(request, env, actorUser) {
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  const storage = new StorageService(env.DB);
  const url = new URL(request.url);
  const includeInactive = url.searchParams.get("includeInactive") === "true";
  const invites = await storage.listInvites(includeInactive);
  return jsonResponse({
    data: invites.map((invite) => toInviteResponse(request, invite)),
    object: "list",
    continuationToken: null
  });
}
__name(handleAdminListInvites, "handleAdminListInvites");
async function handleAdminRevokeInvite(request, env, actorUser, code) {
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  const storage = new StorageService(env.DB);
  const revoked = await storage.revokeInvite(code);
  if (!revoked) {
    return errorResponse("Invite not found or already inactive", 404);
  }
  await writeAuditLog(storage, actorUser.id, "admin.invite.revoke", "invite", null, null, request);
  return new Response(null, { status: 204 });
}
__name(handleAdminRevokeInvite, "handleAdminRevokeInvite");
async function handleAdminDeleteAllInvites(request, env, actorUser) {
  void request;
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  const storage = new StorageService(env.DB);
  const deleted = await storage.deleteAllInvites();
  await writeAuditLog(storage, actorUser.id, "admin.invite.delete_all", "invite", null, {
    deleted
  }, request);
  return jsonResponse({ deleted }, 200);
}
__name(handleAdminDeleteAllInvites, "handleAdminDeleteAllInvites");
async function handleAdminSetUserStatus(request, env, actorUser, targetUserId) {
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const nextStatus = body.status === "banned" ? "banned" : body.status === "active" ? "active" : null;
  if (!nextStatus) {
    return errorResponse("status must be active or banned", 400);
  }
  if (targetUserId === actorUser.id && nextStatus !== "active") {
    return errorResponse("You cannot ban yourself", 400);
  }
  const storage = new StorageService(env.DB);
  const target = await storage.getUserById(targetUserId);
  if (!target) {
    return errorResponse("User not found", 404);
  }
  target.status = nextStatus;
  target.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
  await storage.saveUser(target);
  if (nextStatus === "banned") {
    await storage.deleteRefreshTokensByUserId(target.id);
  }
  AuthService.invalidateUserCache(target.id);
  await writeAuditLog(storage, actorUser.id, "admin.user.status", "user", target.id, {
    status: nextStatus
  }, request);
  return jsonResponse({
    id: target.id,
    email: target.email,
    role: target.role,
    status: target.status,
    object: "user"
  });
}
__name(handleAdminSetUserStatus, "handleAdminSetUserStatus");
async function handleAdminDeleteUser(request, env, actorUser, targetUserId) {
  void request;
  if (!isAdmin(actorUser)) {
    return errorResponse("Forbidden", 403);
  }
  if (targetUserId === actorUser.id) {
    return errorResponse("You cannot delete yourself", 400);
  }
  const storage = new StorageService(env.DB);
  const target = await storage.getUserById(targetUserId);
  if (!target) {
    return errorResponse("User not found", 404);
  }
  const attachmentMap = await storage.getAttachmentsByUserId(target.id);
  for (const [cipherId, attachments] of attachmentMap) {
    for (const att of attachments) {
      await deleteBlobObject(env, getAttachmentObjectKey(cipherId, att.id));
    }
  }
  const sends = await storage.getAllSends(target.id);
  for (const send of sends) {
    if (send.type === 1) {
      try {
        const parsed = JSON.parse(send.data);
        const fileId = typeof parsed.id === "string" ? parsed.id : null;
        if (fileId) {
          await deleteBlobObject(env, getSendFileObjectKey(send.id, fileId));
        }
      } catch {
      }
    }
  }
  await storage.deleteRefreshTokensByUserId(target.id);
  await storage.deleteUserById(target.id);
  AuthService.invalidateUserCache(target.id);
  await writeAuditLog(storage, actorUser.id, "admin.user.delete", "user", target.id, {
    targetEmail: target.email
  }, request);
  return new Response(null, { status: 204 });
}
__name(handleAdminDeleteUser, "handleAdminDeleteUser");

// node_modules/fflate/esm/browser.js
var u8 = Uint8Array;
var u16 = Uint16Array;
var i32 = Int32Array;
var fleb = new u8([
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  1,
  1,
  1,
  1,
  2,
  2,
  2,
  2,
  3,
  3,
  3,
  3,
  4,
  4,
  4,
  4,
  5,
  5,
  5,
  5,
  0,
  /* unused */
  0,
  0,
  /* impossible */
  0
]);
var fdeb = new u8([
  0,
  0,
  0,
  0,
  1,
  1,
  2,
  2,
  3,
  3,
  4,
  4,
  5,
  5,
  6,
  6,
  7,
  7,
  8,
  8,
  9,
  9,
  10,
  10,
  11,
  11,
  12,
  12,
  13,
  13,
  /* unused */
  0,
  0
]);
var clim = new u8([16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15]);
var freb = /* @__PURE__ */ __name(function(eb, start) {
  var b = new u16(31);
  for (var i = 0; i < 31; ++i) {
    b[i] = start += 1 << eb[i - 1];
  }
  var r = new i32(b[30]);
  for (var i = 1; i < 30; ++i) {
    for (var j = b[i]; j < b[i + 1]; ++j) {
      r[j] = j - b[i] << 5 | i;
    }
  }
  return { b, r };
}, "freb");
var _a = freb(fleb, 2);
var fl = _a.b;
var revfl = _a.r;
fl[28] = 258, revfl[258] = 28;
var _b = freb(fdeb, 0);
var fd = _b.b;
var revfd = _b.r;
var rev = new u16(32768);
for (i = 0; i < 32768; ++i) {
  x = (i & 43690) >> 1 | (i & 21845) << 1;
  x = (x & 52428) >> 2 | (x & 13107) << 2;
  x = (x & 61680) >> 4 | (x & 3855) << 4;
  rev[i] = ((x & 65280) >> 8 | (x & 255) << 8) >> 1;
}
var x;
var i;
var hMap = /* @__PURE__ */ __name((function(cd, mb, r) {
  var s = cd.length;
  var i = 0;
  var l = new u16(mb);
  for (; i < s; ++i) {
    if (cd[i])
      ++l[cd[i] - 1];
  }
  var le = new u16(mb);
  for (i = 1; i < mb; ++i) {
    le[i] = le[i - 1] + l[i - 1] << 1;
  }
  var co;
  if (r) {
    co = new u16(1 << mb);
    var rvb = 15 - mb;
    for (i = 0; i < s; ++i) {
      if (cd[i]) {
        var sv = i << 4 | cd[i];
        var r_1 = mb - cd[i];
        var v = le[cd[i] - 1]++ << r_1;
        for (var m = v | (1 << r_1) - 1; v <= m; ++v) {
          co[rev[v] >> rvb] = sv;
        }
      }
    }
  } else {
    co = new u16(s);
    for (i = 0; i < s; ++i) {
      if (cd[i]) {
        co[i] = rev[le[cd[i] - 1]++] >> 15 - cd[i];
      }
    }
  }
  return co;
}), "hMap");
var flt = new u8(288);
for (i = 0; i < 144; ++i)
  flt[i] = 8;
var i;
for (i = 144; i < 256; ++i)
  flt[i] = 9;
var i;
for (i = 256; i < 280; ++i)
  flt[i] = 7;
var i;
for (i = 280; i < 288; ++i)
  flt[i] = 8;
var i;
var fdt = new u8(32);
for (i = 0; i < 32; ++i)
  fdt[i] = 5;
var i;
var flm = /* @__PURE__ */ hMap(flt, 9, 0);
var flrm = /* @__PURE__ */ hMap(flt, 9, 1);
var fdm = /* @__PURE__ */ hMap(fdt, 5, 0);
var fdrm = /* @__PURE__ */ hMap(fdt, 5, 1);
var max = /* @__PURE__ */ __name(function(a) {
  var m = a[0];
  for (var i = 1; i < a.length; ++i) {
    if (a[i] > m)
      m = a[i];
  }
  return m;
}, "max");
var bits = /* @__PURE__ */ __name(function(d, p, m) {
  var o = p / 8 | 0;
  return (d[o] | d[o + 1] << 8) >> (p & 7) & m;
}, "bits");
var bits16 = /* @__PURE__ */ __name(function(d, p) {
  var o = p / 8 | 0;
  return (d[o] | d[o + 1] << 8 | d[o + 2] << 16) >> (p & 7);
}, "bits16");
var shft = /* @__PURE__ */ __name(function(p) {
  return (p + 7) / 8 | 0;
}, "shft");
var slc = /* @__PURE__ */ __name(function(v, s, e) {
  if (s == null || s < 0)
    s = 0;
  if (e == null || e > v.length)
    e = v.length;
  return new u8(v.subarray(s, e));
}, "slc");
var ec = [
  "unexpected EOF",
  "invalid block type",
  "invalid length/literal",
  "invalid distance",
  "stream finished",
  "no stream handler",
  ,
  "no callback",
  "invalid UTF-8 data",
  "extra field too long",
  "date not in range 1980-2099",
  "filename too long",
  "stream finishing",
  "invalid zip data"
  // determined by unknown compression method
];
var err = /* @__PURE__ */ __name(function(ind, msg, nt) {
  var e = new Error(msg || ec[ind]);
  e.code = ind;
  if (Error.captureStackTrace)
    Error.captureStackTrace(e, err);
  if (!nt)
    throw e;
  return e;
}, "err");
var inflt = /* @__PURE__ */ __name(function(dat, st, buf, dict) {
  var sl = dat.length, dl = dict ? dict.length : 0;
  if (!sl || st.f && !st.l)
    return buf || new u8(0);
  var noBuf = !buf;
  var resize = noBuf || st.i != 2;
  var noSt = st.i;
  if (noBuf)
    buf = new u8(sl * 3);
  var cbuf = /* @__PURE__ */ __name(function(l2) {
    var bl = buf.length;
    if (l2 > bl) {
      var nbuf = new u8(Math.max(bl * 2, l2));
      nbuf.set(buf);
      buf = nbuf;
    }
  }, "cbuf");
  var final = st.f || 0, pos = st.p || 0, bt = st.b || 0, lm = st.l, dm = st.d, lbt = st.m, dbt = st.n;
  var tbts = sl * 8;
  do {
    if (!lm) {
      final = bits(dat, pos, 1);
      var type = bits(dat, pos + 1, 3);
      pos += 3;
      if (!type) {
        var s = shft(pos) + 4, l = dat[s - 4] | dat[s - 3] << 8, t = s + l;
        if (t > sl) {
          if (noSt)
            err(0);
          break;
        }
        if (resize)
          cbuf(bt + l);
        buf.set(dat.subarray(s, t), bt);
        st.b = bt += l, st.p = pos = t * 8, st.f = final;
        continue;
      } else if (type == 1)
        lm = flrm, dm = fdrm, lbt = 9, dbt = 5;
      else if (type == 2) {
        var hLit = bits(dat, pos, 31) + 257, hcLen = bits(dat, pos + 10, 15) + 4;
        var tl = hLit + bits(dat, pos + 5, 31) + 1;
        pos += 14;
        var ldt = new u8(tl);
        var clt = new u8(19);
        for (var i = 0; i < hcLen; ++i) {
          clt[clim[i]] = bits(dat, pos + i * 3, 7);
        }
        pos += hcLen * 3;
        var clb = max(clt), clbmsk = (1 << clb) - 1;
        var clm = hMap(clt, clb, 1);
        for (var i = 0; i < tl; ) {
          var r = clm[bits(dat, pos, clbmsk)];
          pos += r & 15;
          var s = r >> 4;
          if (s < 16) {
            ldt[i++] = s;
          } else {
            var c = 0, n = 0;
            if (s == 16)
              n = 3 + bits(dat, pos, 3), pos += 2, c = ldt[i - 1];
            else if (s == 17)
              n = 3 + bits(dat, pos, 7), pos += 3;
            else if (s == 18)
              n = 11 + bits(dat, pos, 127), pos += 7;
            while (n--)
              ldt[i++] = c;
          }
        }
        var lt = ldt.subarray(0, hLit), dt = ldt.subarray(hLit);
        lbt = max(lt);
        dbt = max(dt);
        lm = hMap(lt, lbt, 1);
        dm = hMap(dt, dbt, 1);
      } else
        err(1);
      if (pos > tbts) {
        if (noSt)
          err(0);
        break;
      }
    }
    if (resize)
      cbuf(bt + 131072);
    var lms = (1 << lbt) - 1, dms = (1 << dbt) - 1;
    var lpos = pos;
    for (; ; lpos = pos) {
      var c = lm[bits16(dat, pos) & lms], sym = c >> 4;
      pos += c & 15;
      if (pos > tbts) {
        if (noSt)
          err(0);
        break;
      }
      if (!c)
        err(2);
      if (sym < 256)
        buf[bt++] = sym;
      else if (sym == 256) {
        lpos = pos, lm = null;
        break;
      } else {
        var add = sym - 254;
        if (sym > 264) {
          var i = sym - 257, b = fleb[i];
          add = bits(dat, pos, (1 << b) - 1) + fl[i];
          pos += b;
        }
        var d = dm[bits16(dat, pos) & dms], dsym = d >> 4;
        if (!d)
          err(3);
        pos += d & 15;
        var dt = fd[dsym];
        if (dsym > 3) {
          var b = fdeb[dsym];
          dt += bits16(dat, pos) & (1 << b) - 1, pos += b;
        }
        if (pos > tbts) {
          if (noSt)
            err(0);
          break;
        }
        if (resize)
          cbuf(bt + 131072);
        var end = bt + add;
        if (bt < dt) {
          var shift = dl - dt, dend = Math.min(dt, end);
          if (shift + bt < 0)
            err(3);
          for (; bt < dend; ++bt)
            buf[bt] = dict[shift + bt];
        }
        for (; bt < end; ++bt)
          buf[bt] = buf[bt - dt];
      }
    }
    st.l = lm, st.p = lpos, st.b = bt, st.f = final;
    if (lm)
      final = 1, st.m = lbt, st.d = dm, st.n = dbt;
  } while (!final);
  return bt != buf.length && noBuf ? slc(buf, 0, bt) : buf.subarray(0, bt);
}, "inflt");
var wbits = /* @__PURE__ */ __name(function(d, p, v) {
  v <<= p & 7;
  var o = p / 8 | 0;
  d[o] |= v;
  d[o + 1] |= v >> 8;
}, "wbits");
var wbits16 = /* @__PURE__ */ __name(function(d, p, v) {
  v <<= p & 7;
  var o = p / 8 | 0;
  d[o] |= v;
  d[o + 1] |= v >> 8;
  d[o + 2] |= v >> 16;
}, "wbits16");
var hTree = /* @__PURE__ */ __name(function(d, mb) {
  var t = [];
  for (var i = 0; i < d.length; ++i) {
    if (d[i])
      t.push({ s: i, f: d[i] });
  }
  var s = t.length;
  var t2 = t.slice();
  if (!s)
    return { t: et, l: 0 };
  if (s == 1) {
    var v = new u8(t[0].s + 1);
    v[t[0].s] = 1;
    return { t: v, l: 1 };
  }
  t.sort(function(a, b) {
    return a.f - b.f;
  });
  t.push({ s: -1, f: 25001 });
  var l = t[0], r = t[1], i0 = 0, i1 = 1, i2 = 2;
  t[0] = { s: -1, f: l.f + r.f, l, r };
  while (i1 != s - 1) {
    l = t[t[i0].f < t[i2].f ? i0++ : i2++];
    r = t[i0 != i1 && t[i0].f < t[i2].f ? i0++ : i2++];
    t[i1++] = { s: -1, f: l.f + r.f, l, r };
  }
  var maxSym = t2[0].s;
  for (var i = 1; i < s; ++i) {
    if (t2[i].s > maxSym)
      maxSym = t2[i].s;
  }
  var tr = new u16(maxSym + 1);
  var mbt = ln(t[i1 - 1], tr, 0);
  if (mbt > mb) {
    var i = 0, dt = 0;
    var lft = mbt - mb, cst = 1 << lft;
    t2.sort(function(a, b) {
      return tr[b.s] - tr[a.s] || a.f - b.f;
    });
    for (; i < s; ++i) {
      var i2_1 = t2[i].s;
      if (tr[i2_1] > mb) {
        dt += cst - (1 << mbt - tr[i2_1]);
        tr[i2_1] = mb;
      } else
        break;
    }
    dt >>= lft;
    while (dt > 0) {
      var i2_2 = t2[i].s;
      if (tr[i2_2] < mb)
        dt -= 1 << mb - tr[i2_2]++ - 1;
      else
        ++i;
    }
    for (; i >= 0 && dt; --i) {
      var i2_3 = t2[i].s;
      if (tr[i2_3] == mb) {
        --tr[i2_3];
        ++dt;
      }
    }
    mbt = mb;
  }
  return { t: new u8(tr), l: mbt };
}, "hTree");
var ln = /* @__PURE__ */ __name(function(n, l, d) {
  return n.s == -1 ? Math.max(ln(n.l, l, d + 1), ln(n.r, l, d + 1)) : l[n.s] = d;
}, "ln");
var lc = /* @__PURE__ */ __name(function(c) {
  var s = c.length;
  while (s && !c[--s])
    ;
  var cl = new u16(++s);
  var cli = 0, cln = c[0], cls = 1;
  var w = /* @__PURE__ */ __name(function(v) {
    cl[cli++] = v;
  }, "w");
  for (var i = 1; i <= s; ++i) {
    if (c[i] == cln && i != s)
      ++cls;
    else {
      if (!cln && cls > 2) {
        for (; cls > 138; cls -= 138)
          w(32754);
        if (cls > 2) {
          w(cls > 10 ? cls - 11 << 5 | 28690 : cls - 3 << 5 | 12305);
          cls = 0;
        }
      } else if (cls > 3) {
        w(cln), --cls;
        for (; cls > 6; cls -= 6)
          w(8304);
        if (cls > 2)
          w(cls - 3 << 5 | 8208), cls = 0;
      }
      while (cls--)
        w(cln);
      cls = 1;
      cln = c[i];
    }
  }
  return { c: cl.subarray(0, cli), n: s };
}, "lc");
var clen = /* @__PURE__ */ __name(function(cf, cl) {
  var l = 0;
  for (var i = 0; i < cl.length; ++i)
    l += cf[i] * cl[i];
  return l;
}, "clen");
var wfblk = /* @__PURE__ */ __name(function(out, pos, dat) {
  var s = dat.length;
  var o = shft(pos + 2);
  out[o] = s & 255;
  out[o + 1] = s >> 8;
  out[o + 2] = out[o] ^ 255;
  out[o + 3] = out[o + 1] ^ 255;
  for (var i = 0; i < s; ++i)
    out[o + i + 4] = dat[i];
  return (o + 4 + s) * 8;
}, "wfblk");
var wblk = /* @__PURE__ */ __name(function(dat, out, final, syms, lf, df, eb, li, bs, bl, p) {
  wbits(out, p++, final);
  ++lf[256];
  var _a2 = hTree(lf, 15), dlt = _a2.t, mlb = _a2.l;
  var _b2 = hTree(df, 15), ddt = _b2.t, mdb = _b2.l;
  var _c = lc(dlt), lclt = _c.c, nlc = _c.n;
  var _d = lc(ddt), lcdt = _d.c, ndc = _d.n;
  var lcfreq = new u16(19);
  for (var i = 0; i < lclt.length; ++i)
    ++lcfreq[lclt[i] & 31];
  for (var i = 0; i < lcdt.length; ++i)
    ++lcfreq[lcdt[i] & 31];
  var _e = hTree(lcfreq, 7), lct = _e.t, mlcb = _e.l;
  var nlcc = 19;
  for (; nlcc > 4 && !lct[clim[nlcc - 1]]; --nlcc)
    ;
  var flen = bl + 5 << 3;
  var ftlen = clen(lf, flt) + clen(df, fdt) + eb;
  var dtlen = clen(lf, dlt) + clen(df, ddt) + eb + 14 + 3 * nlcc + clen(lcfreq, lct) + 2 * lcfreq[16] + 3 * lcfreq[17] + 7 * lcfreq[18];
  if (bs >= 0 && flen <= ftlen && flen <= dtlen)
    return wfblk(out, p, dat.subarray(bs, bs + bl));
  var lm, ll, dm, dl;
  wbits(out, p, 1 + (dtlen < ftlen)), p += 2;
  if (dtlen < ftlen) {
    lm = hMap(dlt, mlb, 0), ll = dlt, dm = hMap(ddt, mdb, 0), dl = ddt;
    var llm = hMap(lct, mlcb, 0);
    wbits(out, p, nlc - 257);
    wbits(out, p + 5, ndc - 1);
    wbits(out, p + 10, nlcc - 4);
    p += 14;
    for (var i = 0; i < nlcc; ++i)
      wbits(out, p + 3 * i, lct[clim[i]]);
    p += 3 * nlcc;
    var lcts = [lclt, lcdt];
    for (var it = 0; it < 2; ++it) {
      var clct = lcts[it];
      for (var i = 0; i < clct.length; ++i) {
        var len = clct[i] & 31;
        wbits(out, p, llm[len]), p += lct[len];
        if (len > 15)
          wbits(out, p, clct[i] >> 5 & 127), p += clct[i] >> 12;
      }
    }
  } else {
    lm = flm, ll = flt, dm = fdm, dl = fdt;
  }
  for (var i = 0; i < li; ++i) {
    var sym = syms[i];
    if (sym > 255) {
      var len = sym >> 18 & 31;
      wbits16(out, p, lm[len + 257]), p += ll[len + 257];
      if (len > 7)
        wbits(out, p, sym >> 23 & 31), p += fleb[len];
      var dst = sym & 31;
      wbits16(out, p, dm[dst]), p += dl[dst];
      if (dst > 3)
        wbits16(out, p, sym >> 5 & 8191), p += fdeb[dst];
    } else {
      wbits16(out, p, lm[sym]), p += ll[sym];
    }
  }
  wbits16(out, p, lm[256]);
  return p + ll[256];
}, "wblk");
var deo = /* @__PURE__ */ new i32([65540, 131080, 131088, 131104, 262176, 1048704, 1048832, 2114560, 2117632]);
var et = /* @__PURE__ */ new u8(0);
var dflt = /* @__PURE__ */ __name(function(dat, lvl, plvl, pre, post, st) {
  var s = st.z || dat.length;
  var o = new u8(pre + s + 5 * (1 + Math.ceil(s / 7e3)) + post);
  var w = o.subarray(pre, o.length - post);
  var lst = st.l;
  var pos = (st.r || 0) & 7;
  if (lvl) {
    if (pos)
      w[0] = st.r >> 3;
    var opt = deo[lvl - 1];
    var n = opt >> 13, c = opt & 8191;
    var msk_1 = (1 << plvl) - 1;
    var prev = st.p || new u16(32768), head = st.h || new u16(msk_1 + 1);
    var bs1_1 = Math.ceil(plvl / 3), bs2_1 = 2 * bs1_1;
    var hsh = /* @__PURE__ */ __name(function(i2) {
      return (dat[i2] ^ dat[i2 + 1] << bs1_1 ^ dat[i2 + 2] << bs2_1) & msk_1;
    }, "hsh");
    var syms = new i32(25e3);
    var lf = new u16(288), df = new u16(32);
    var lc_1 = 0, eb = 0, i = st.i || 0, li = 0, wi = st.w || 0, bs = 0;
    for (; i + 2 < s; ++i) {
      var hv = hsh(i);
      var imod = i & 32767, pimod = head[hv];
      prev[imod] = pimod;
      head[hv] = imod;
      if (wi <= i) {
        var rem = s - i;
        if ((lc_1 > 7e3 || li > 24576) && (rem > 423 || !lst)) {
          pos = wblk(dat, w, 0, syms, lf, df, eb, li, bs, i - bs, pos);
          li = lc_1 = eb = 0, bs = i;
          for (var j = 0; j < 286; ++j)
            lf[j] = 0;
          for (var j = 0; j < 30; ++j)
            df[j] = 0;
        }
        var l = 2, d = 0, ch_1 = c, dif = imod - pimod & 32767;
        if (rem > 2 && hv == hsh(i - dif)) {
          var maxn = Math.min(n, rem) - 1;
          var maxd = Math.min(32767, i);
          var ml = Math.min(258, rem);
          while (dif <= maxd && --ch_1 && imod != pimod) {
            if (dat[i + l] == dat[i + l - dif]) {
              var nl = 0;
              for (; nl < ml && dat[i + nl] == dat[i + nl - dif]; ++nl)
                ;
              if (nl > l) {
                l = nl, d = dif;
                if (nl > maxn)
                  break;
                var mmd = Math.min(dif, nl - 2);
                var md = 0;
                for (var j = 0; j < mmd; ++j) {
                  var ti = i - dif + j & 32767;
                  var pti = prev[ti];
                  var cd = ti - pti & 32767;
                  if (cd > md)
                    md = cd, pimod = ti;
                }
              }
            }
            imod = pimod, pimod = prev[imod];
            dif += imod - pimod & 32767;
          }
        }
        if (d) {
          syms[li++] = 268435456 | revfl[l] << 18 | revfd[d];
          var lin = revfl[l] & 31, din = revfd[d] & 31;
          eb += fleb[lin] + fdeb[din];
          ++lf[257 + lin];
          ++df[din];
          wi = i + l;
          ++lc_1;
        } else {
          syms[li++] = dat[i];
          ++lf[dat[i]];
        }
      }
    }
    for (i = Math.max(i, wi); i < s; ++i) {
      syms[li++] = dat[i];
      ++lf[dat[i]];
    }
    pos = wblk(dat, w, lst, syms, lf, df, eb, li, bs, i - bs, pos);
    if (!lst) {
      st.r = pos & 7 | w[pos / 8 | 0] << 3;
      pos -= 7;
      st.h = head, st.p = prev, st.i = i, st.w = wi;
    }
  } else {
    for (var i = st.w || 0; i < s + lst; i += 65535) {
      var e = i + 65535;
      if (e >= s) {
        w[pos / 8 | 0] = lst;
        e = s;
      }
      pos = wfblk(w, pos + 1, dat.subarray(i, e));
    }
    st.i = s;
  }
  return slc(o, 0, pre + shft(pos) + post);
}, "dflt");
var crct = /* @__PURE__ */ (function() {
  var t = new Int32Array(256);
  for (var i = 0; i < 256; ++i) {
    var c = i, k = 9;
    while (--k)
      c = (c & 1 && -306674912) ^ c >>> 1;
    t[i] = c;
  }
  return t;
})();
var crc = /* @__PURE__ */ __name(function() {
  var c = -1;
  return {
    p: /* @__PURE__ */ __name(function(d) {
      var cr = c;
      for (var i = 0; i < d.length; ++i)
        cr = crct[cr & 255 ^ d[i]] ^ cr >>> 8;
      c = cr;
    }, "p"),
    d: /* @__PURE__ */ __name(function() {
      return ~c;
    }, "d")
  };
}, "crc");
var dopt = /* @__PURE__ */ __name(function(dat, opt, pre, post, st) {
  if (!st) {
    st = { l: 1 };
    if (opt.dictionary) {
      var dict = opt.dictionary.subarray(-32768);
      var newDat = new u8(dict.length + dat.length);
      newDat.set(dict);
      newDat.set(dat, dict.length);
      dat = newDat;
      st.w = dict.length;
    }
  }
  return dflt(dat, opt.level == null ? 6 : opt.level, opt.mem == null ? st.l ? Math.ceil(Math.max(8, Math.min(13, Math.log(dat.length))) * 1.5) : 20 : 12 + opt.mem, pre, post, st);
}, "dopt");
var mrg = /* @__PURE__ */ __name(function(a, b) {
  var o = {};
  for (var k in a)
    o[k] = a[k];
  for (var k in b)
    o[k] = b[k];
  return o;
}, "mrg");
var b2 = /* @__PURE__ */ __name(function(d, b) {
  return d[b] | d[b + 1] << 8;
}, "b2");
var b4 = /* @__PURE__ */ __name(function(d, b) {
  return (d[b] | d[b + 1] << 8 | d[b + 2] << 16 | d[b + 3] << 24) >>> 0;
}, "b4");
var b8 = /* @__PURE__ */ __name(function(d, b) {
  return b4(d, b) + b4(d, b + 4) * 4294967296;
}, "b8");
var wbytes = /* @__PURE__ */ __name(function(d, b, v) {
  for (; v; ++b)
    d[b] = v, v >>>= 8;
}, "wbytes");
function deflateSync(data, opts) {
  return dopt(data, opts || {}, 0, 0);
}
__name(deflateSync, "deflateSync");
function inflateSync(data, opts) {
  return inflt(data, { i: 2 }, opts && opts.out, opts && opts.dictionary);
}
__name(inflateSync, "inflateSync");
var fltn = /* @__PURE__ */ __name(function(d, p, t, o) {
  for (var k in d) {
    var val = d[k], n = p + k, op = o;
    if (Array.isArray(val))
      op = mrg(o, val[1]), val = val[0];
    if (val instanceof u8)
      t[n] = [val, op];
    else {
      t[n += "/"] = [new u8(0), op];
      fltn(val, n, t, o);
    }
  }
}, "fltn");
var te = typeof TextEncoder != "undefined" && /* @__PURE__ */ new TextEncoder();
var td = typeof TextDecoder != "undefined" && /* @__PURE__ */ new TextDecoder();
var tds = 0;
try {
  td.decode(et, { stream: true });
  tds = 1;
} catch (e) {
}
var dutf8 = /* @__PURE__ */ __name(function(d) {
  for (var r = "", i = 0; ; ) {
    var c = d[i++];
    var eb = (c > 127) + (c > 223) + (c > 239);
    if (i + eb > d.length)
      return { s: r, r: slc(d, i - 1) };
    if (!eb)
      r += String.fromCharCode(c);
    else if (eb == 3) {
      c = ((c & 15) << 18 | (d[i++] & 63) << 12 | (d[i++] & 63) << 6 | d[i++] & 63) - 65536, r += String.fromCharCode(55296 | c >> 10, 56320 | c & 1023);
    } else if (eb & 1)
      r += String.fromCharCode((c & 31) << 6 | d[i++] & 63);
    else
      r += String.fromCharCode((c & 15) << 12 | (d[i++] & 63) << 6 | d[i++] & 63);
  }
}, "dutf8");
function strToU8(str, latin1) {
  if (latin1) {
    var ar_1 = new u8(str.length);
    for (var i = 0; i < str.length; ++i)
      ar_1[i] = str.charCodeAt(i);
    return ar_1;
  }
  if (te)
    return te.encode(str);
  var l = str.length;
  var ar = new u8(str.length + (str.length >> 1));
  var ai = 0;
  var w = /* @__PURE__ */ __name(function(v) {
    ar[ai++] = v;
  }, "w");
  for (var i = 0; i < l; ++i) {
    if (ai + 5 > ar.length) {
      var n = new u8(ai + 8 + (l - i << 1));
      n.set(ar);
      ar = n;
    }
    var c = str.charCodeAt(i);
    if (c < 128 || latin1)
      w(c);
    else if (c < 2048)
      w(192 | c >> 6), w(128 | c & 63);
    else if (c > 55295 && c < 57344)
      c = 65536 + (c & 1023 << 10) | str.charCodeAt(++i) & 1023, w(240 | c >> 18), w(128 | c >> 12 & 63), w(128 | c >> 6 & 63), w(128 | c & 63);
    else
      w(224 | c >> 12), w(128 | c >> 6 & 63), w(128 | c & 63);
  }
  return slc(ar, 0, ai);
}
__name(strToU8, "strToU8");
function strFromU8(dat, latin1) {
  if (latin1) {
    var r = "";
    for (var i = 0; i < dat.length; i += 16384)
      r += String.fromCharCode.apply(null, dat.subarray(i, i + 16384));
    return r;
  } else if (td) {
    return td.decode(dat);
  } else {
    var _a2 = dutf8(dat), s = _a2.s, r = _a2.r;
    if (r.length)
      err(8);
    return s;
  }
}
__name(strFromU8, "strFromU8");
var slzh = /* @__PURE__ */ __name(function(d, b) {
  return b + 30 + b2(d, b + 26) + b2(d, b + 28);
}, "slzh");
var zh = /* @__PURE__ */ __name(function(d, b, z) {
  var fnl = b2(d, b + 28), fn = strFromU8(d.subarray(b + 46, b + 46 + fnl), !(b2(d, b + 8) & 2048)), es = b + 46 + fnl, bs = b4(d, b + 20);
  var _a2 = z && bs == 4294967295 ? z64e(d, es) : [bs, b4(d, b + 24), b4(d, b + 42)], sc = _a2[0], su = _a2[1], off = _a2[2];
  return [b2(d, b + 10), sc, su, fn, es + b2(d, b + 30) + b2(d, b + 32), off];
}, "zh");
var z64e = /* @__PURE__ */ __name(function(d, b) {
  for (; b2(d, b) != 1; b += 4 + b2(d, b + 2))
    ;
  return [b8(d, b + 12), b8(d, b + 4), b8(d, b + 20)];
}, "z64e");
var exfl = /* @__PURE__ */ __name(function(ex) {
  var le = 0;
  if (ex) {
    for (var k in ex) {
      var l = ex[k].length;
      if (l > 65535)
        err(9);
      le += l + 4;
    }
  }
  return le;
}, "exfl");
var wzh = /* @__PURE__ */ __name(function(d, b, f, fn, u, c, ce, co) {
  var fl2 = fn.length, ex = f.extra, col = co && co.length;
  var exl = exfl(ex);
  wbytes(d, b, ce != null ? 33639248 : 67324752), b += 4;
  if (ce != null)
    d[b++] = 20, d[b++] = f.os;
  d[b] = 20, b += 2;
  d[b++] = f.flag << 1 | (c < 0 && 8), d[b++] = u && 8;
  d[b++] = f.compression & 255, d[b++] = f.compression >> 8;
  var dt = new Date(f.mtime == null ? Date.now() : f.mtime), y = dt.getFullYear() - 1980;
  if (y < 0 || y > 119)
    err(10);
  wbytes(d, b, y << 25 | dt.getMonth() + 1 << 21 | dt.getDate() << 16 | dt.getHours() << 11 | dt.getMinutes() << 5 | dt.getSeconds() >> 1), b += 4;
  if (c != -1) {
    wbytes(d, b, f.crc);
    wbytes(d, b + 4, c < 0 ? -c - 2 : c);
    wbytes(d, b + 8, f.size);
  }
  wbytes(d, b + 12, fl2);
  wbytes(d, b + 14, exl), b += 16;
  if (ce != null) {
    wbytes(d, b, col);
    wbytes(d, b + 6, f.attrs);
    wbytes(d, b + 10, ce), b += 14;
  }
  d.set(fn, b);
  b += fl2;
  if (exl) {
    for (var k in ex) {
      var exf = ex[k], l = exf.length;
      wbytes(d, b, +k);
      wbytes(d, b + 2, l);
      d.set(exf, b + 4), b += 4 + l;
    }
  }
  if (col)
    d.set(co, b), b += col;
  return b;
}, "wzh");
var wzf = /* @__PURE__ */ __name(function(o, b, c, d, e) {
  wbytes(o, b, 101010256);
  wbytes(o, b + 8, c);
  wbytes(o, b + 10, c);
  wbytes(o, b + 12, d);
  wbytes(o, b + 16, e);
}, "wzf");
function zipSync(data, opts) {
  if (!opts)
    opts = {};
  var r = {};
  var files = [];
  fltn(data, "", r, opts);
  var o = 0;
  var tot = 0;
  for (var fn in r) {
    var _a2 = r[fn], file = _a2[0], p = _a2[1];
    var compression = p.level == 0 ? 0 : 8;
    var f = strToU8(fn), s = f.length;
    var com = p.comment, m = com && strToU8(com), ms = m && m.length;
    var exl = exfl(p.extra);
    if (s > 65535)
      err(11);
    var d = compression ? deflateSync(file, p) : file, l = d.length;
    var c = crc();
    c.p(file);
    files.push(mrg(p, {
      size: file.length,
      crc: c.d(),
      c: d,
      f,
      m,
      u: s != fn.length || m && com.length != ms,
      o,
      compression
    }));
    o += 30 + s + exl + l;
    tot += 76 + 2 * (s + exl) + (ms || 0) + l;
  }
  var out = new u8(tot + 22), oe = o, cdl = tot - o;
  for (var i = 0; i < files.length; ++i) {
    var f = files[i];
    wzh(out, f.o, f, f.f, f.u, f.c.length);
    var badd = 30 + f.f.length + exfl(f.extra);
    out.set(f.c, f.o + badd);
    wzh(out, o, f, f.f, f.u, f.c.length, f.o, f.m), o += 16 + badd + (f.m ? f.m.length : 0);
  }
  wzf(out, o, files.length, cdl, oe);
  return out;
}
__name(zipSync, "zipSync");
function unzipSync(data, opts) {
  var files = {};
  var e = data.length - 22;
  for (; b4(data, e) != 101010256; --e) {
    if (!e || data.length - e > 65558)
      err(13);
  }
  ;
  var c = b2(data, e + 8);
  if (!c)
    return {};
  var o = b4(data, e + 16);
  var z = o == 4294967295 || c == 65535;
  if (z) {
    var ze = b4(data, e - 12);
    z = b4(data, ze) == 101075792;
    if (z) {
      c = b4(data, ze + 32);
      o = b4(data, ze + 48);
    }
  }
  var fltr = opts && opts.filter;
  for (var i = 0; i < c; ++i) {
    var _a2 = zh(data, o, z), c_2 = _a2[0], sc = _a2[1], su = _a2[2], fn = _a2[3], no = _a2[4], off = _a2[5], b = slzh(data, off);
    o = no;
    if (!fltr || fltr({
      name: fn,
      size: sc,
      originalSize: su,
      compression: c_2
    })) {
      if (!c_2)
        files[fn] = slc(data, b, b + sc);
      else if (c_2 == 8)
        files[fn] = inflateSync(data.subarray(b, b + sc), { out: new u8(su) });
      else
        err(14, "unknown compression type " + c_2);
    }
  }
  return files;
}
__name(unzipSync, "unzipSync");

// shared/app-version.ts
var APP_VERSION = "1.5.2";

// src/services/backup-settings-crypto.ts
var RUNTIME_SALT = "nodewarden.backup-settings.runtime.v2";
var RUNTIME_INFO = "runtime";
var PORTABLE_ALGORITHM = "RSA-OAEP";
var PORTABLE_HASH = "SHA-1";
var AES_GCM_ALGORITHM = "AES-GCM";
var AES_GCM_IV_BYTES = 12;
var PORTABLE_DEK_BYTES = 32;
function bytesToBase64(bytes) {
  let text = "";
  for (let index = 0; index < bytes.length; index += 1) {
    text += String.fromCharCode(bytes[index]);
  }
  return btoa(text);
}
__name(bytesToBase64, "bytesToBase64");
function base64ToBytes(value) {
  const normalized = String(value || "").trim();
  const binary = atob(normalized);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}
__name(base64ToBytes, "base64ToBytes");
function isPlainObject(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
__name(isPlainObject, "isPlainObject");
async function deriveRuntimeKey(secret) {
  const encoder = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    "HKDF",
    false,
    ["deriveBits"]
  );
  const bits2 = await crypto.subtle.deriveBits(
    {
      name: "HKDF",
      hash: "SHA-256",
      salt: encoder.encode(RUNTIME_SALT),
      info: encoder.encode(RUNTIME_INFO)
    },
    keyMaterial,
    256
  );
  return crypto.subtle.importKey("raw", bits2, { name: AES_GCM_ALGORITHM }, false, ["encrypt", "decrypt"]);
}
__name(deriveRuntimeKey, "deriveRuntimeKey");
async function encryptAesGcm(plaintext, key) {
  const iv = crypto.getRandomValues(new Uint8Array(AES_GCM_IV_BYTES));
  const ciphertext = new Uint8Array(
    await crypto.subtle.encrypt(
      { name: AES_GCM_ALGORITHM, iv },
      key,
      plaintext
    )
  );
  return { iv, ciphertext };
}
__name(encryptAesGcm, "encryptAesGcm");
async function decryptAesGcm(ciphertext, iv, key) {
  return new Uint8Array(
    await crypto.subtle.decrypt(
      { name: AES_GCM_ALGORITHM, iv },
      key,
      ciphertext
    )
  );
}
__name(decryptAesGcm, "decryptAesGcm");
async function importPortablePublicKey(publicKeyBase64) {
  return crypto.subtle.importKey(
    "spki",
    base64ToBytes(publicKeyBase64),
    { name: PORTABLE_ALGORITHM, hash: PORTABLE_HASH },
    false,
    ["encrypt"]
  );
}
__name(importPortablePublicKey, "importPortablePublicKey");
function getEligiblePortableUsers(users) {
  return users.filter(
    (user) => user.role === "admin" && user.status === "active" && typeof user.publicKey === "string" && user.publicKey.trim().length > 0
  ).map((user) => ({
    id: user.id,
    publicKey: user.publicKey
  }));
}
__name(getEligiblePortableUsers, "getEligiblePortableUsers");
function parseBackupSettingsEnvelope(raw) {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (!isPlainObject(parsed) || Number(parsed.version) !== 2) return null;
    const runtime = parsed.runtime;
    const portable = parsed.portable;
    if (!isPlainObject(runtime) || !isPlainObject(portable)) return null;
    if (!Array.isArray(portable.wraps)) return null;
    if (typeof runtime.iv !== "string" || typeof runtime.ciphertext !== "string") return null;
    if (typeof portable.iv !== "string" || typeof portable.ciphertext !== "string") return null;
    return {
      version: 2,
      runtime: {
        iv: runtime.iv,
        ciphertext: runtime.ciphertext
      },
      portable: {
        iv: portable.iv,
        ciphertext: portable.ciphertext,
        wraps: portable.wraps.filter((entry) => isPlainObject(entry)).map((entry) => ({
          userId: String(entry.userId || "").trim(),
          wrappedKey: String(entry.wrappedKey || "").trim()
        })).filter((entry) => entry.userId && entry.wrappedKey)
      }
    };
  } catch {
    return null;
  }
}
__name(parseBackupSettingsEnvelope, "parseBackupSettingsEnvelope");
function exportPortableBackupSettingsEnvelope(raw) {
  const envelope = parseBackupSettingsEnvelope(raw);
  if (!envelope) return null;
  return JSON.stringify({
    version: 2,
    portableOnly: true,
    runtime: {
      iv: "",
      ciphertext: ""
    },
    portable: envelope.portable
  });
}
__name(exportPortableBackupSettingsEnvelope, "exportPortableBackupSettingsEnvelope");
async function encryptBackupSettingsEnvelope(plaintext, env, users) {
  const encoder = new TextEncoder();
  const eligibleUsers = getEligiblePortableUsers(users);
  if (!eligibleUsers.length) {
    throw new Error("No active administrator public keys are available for backup settings recovery");
  }
  const runtimeKey = await deriveRuntimeKey(env.JWT_SECRET);
  const runtime = await encryptAesGcm(encoder.encode(plaintext), runtimeKey);
  const portableDek = crypto.getRandomValues(new Uint8Array(PORTABLE_DEK_BYTES));
  const portableKey = await crypto.subtle.importKey(
    "raw",
    portableDek,
    { name: AES_GCM_ALGORITHM },
    false,
    ["encrypt"]
  );
  const portableCipher = await encryptAesGcm(encoder.encode(plaintext), portableKey);
  const wraps = [];
  for (const user of eligibleUsers) {
    const publicKey = await importPortablePublicKey(user.publicKey);
    const wrappedKey = new Uint8Array(
      await crypto.subtle.encrypt(
        { name: PORTABLE_ALGORITHM },
        publicKey,
        portableDek
      )
    );
    wraps.push({
      userId: user.id,
      wrappedKey: bytesToBase64(wrappedKey)
    });
  }
  const envelope = {
    version: 2,
    runtime: {
      iv: bytesToBase64(runtime.iv),
      ciphertext: bytesToBase64(runtime.ciphertext)
    },
    portable: {
      iv: bytesToBase64(portableCipher.iv),
      ciphertext: bytesToBase64(portableCipher.ciphertext),
      wraps
    }
  };
  return JSON.stringify(envelope);
}
__name(encryptBackupSettingsEnvelope, "encryptBackupSettingsEnvelope");
async function decryptBackupSettingsRuntime(raw, env) {
  const envelope = parseBackupSettingsEnvelope(raw);
  if (!envelope) {
    throw new Error("Backup settings envelope is invalid");
  }
  const runtimeKey = await deriveRuntimeKey(env.JWT_SECRET);
  const plaintext = await decryptAesGcm(
    base64ToBytes(envelope.runtime.ciphertext),
    base64ToBytes(envelope.runtime.iv),
    runtimeKey
  );
  return new TextDecoder().decode(plaintext);
}
__name(decryptBackupSettingsRuntime, "decryptBackupSettingsRuntime");

// shared/backup-schema.ts
var BACKUP_DEFAULT_TIMEZONE = "UTC";
var BACKUP_DEFAULT_RETENTION_COUNT = 30;
var BACKUP_DEFAULT_S3_REGION = "auto";
var BACKUP_DEFAULT_REMOTE_PATH = "nodewarden";
var BACKUP_DEFAULT_INTERVAL_HOURS = 24;
var BACKUP_DEFAULT_START_TIME = "03:00";
function createBackupRandomId() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `backup-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}
__name(createBackupRandomId, "createBackupRandomId");
function createDefaultBackupRuntimeState() {
  return {
    lastAttemptAt: null,
    lastAttemptLocalDate: null,
    lastSuccessAt: null,
    lastErrorAt: null,
    lastErrorMessage: null,
    lastUploadedFileName: null,
    lastUploadedSizeBytes: null,
    lastUploadedDestination: null
  };
}
__name(createDefaultBackupRuntimeState, "createDefaultBackupRuntimeState");
function createDefaultBackupScheduleConfig(timezone = BACKUP_DEFAULT_TIMEZONE) {
  return {
    enabled: false,
    intervalHours: BACKUP_DEFAULT_INTERVAL_HOURS,
    startTime: BACKUP_DEFAULT_START_TIME,
    timezone,
    retentionCount: BACKUP_DEFAULT_RETENTION_COUNT
  };
}
__name(createDefaultBackupScheduleConfig, "createDefaultBackupScheduleConfig");
function createDefaultBackupDestinationConfig(type) {
  if (type === "s3") {
    return {
      endpoint: "",
      bucket: "",
      region: BACKUP_DEFAULT_S3_REGION,
      accessKeyId: "",
      secretAccessKey: "",
      rootPath: BACKUP_DEFAULT_REMOTE_PATH
    };
  }
  return {
    baseUrl: "",
    username: "",
    password: "",
    remotePath: BACKUP_DEFAULT_REMOTE_PATH
  };
}
__name(createDefaultBackupDestinationConfig, "createDefaultBackupDestinationConfig");
function createDefaultBackupDestinationName(type, index) {
  if (type === "s3") return `S3 ${index}`;
  return `WebDAV ${index}`;
}
__name(createDefaultBackupDestinationName, "createDefaultBackupDestinationName");
function createBackupDestinationRecord(type, index, options = {}) {
  return {
    id: options.id || createBackupRandomId(),
    name: options.name || createDefaultBackupDestinationName(type, index),
    type,
    includeAttachments: false,
    destination: createDefaultBackupDestinationConfig(type),
    schedule: createDefaultBackupScheduleConfig(options.timezone || BACKUP_DEFAULT_TIMEZONE),
    runtime: createDefaultBackupRuntimeState()
  };
}
__name(createBackupDestinationRecord, "createBackupDestinationRecord");
function createDefaultBackupSettings(timezone = BACKUP_DEFAULT_TIMEZONE, options = {}) {
  return {
    destinations: [
      createBackupDestinationRecord("webdav", 1, {
        timezone,
        name: options.destinationName
      })
    ]
  };
}
__name(createDefaultBackupSettings, "createDefaultBackupSettings");

// src/services/backup-config.ts
var BACKUP_SETTINGS_CONFIG_KEY = "backup.settings.v1";
var BACKUP_SCHEDULER_WINDOW_MINUTES = 5;
var MAX_BACKUP_DESTINATIONS = 24;
function defaultScheduleConfig(timezone = "UTC") {
  return { ...createDefaultBackupScheduleConfig(assertValidTimeZone(timezone)) };
}
__name(defaultScheduleConfig, "defaultScheduleConfig");
function isPlainObject2(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
__name(isPlainObject2, "isPlainObject");
function asTrimmedString(value) {
  return String(value ?? "").trim();
}
__name(asTrimmedString, "asTrimmedString");
function normalizePath(value) {
  return asTrimmedString(value).replace(/\\/g, "/").replace(/^\/+|\/+$/g, "");
}
__name(normalizePath, "normalizePath");
function assertValidTimeZone(timezone) {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: timezone }).format(/* @__PURE__ */ new Date());
    return timezone;
  } catch {
    throw new Error("Invalid backup timezone");
  }
}
__name(assertValidTimeZone, "assertValidTimeZone");
function normalizeRetentionCount(value, fallback = 30) {
  if (value === void 0) return fallback;
  if (value === null || String(value).trim() === "") return null;
  const count = Number(value);
  if (!Number.isInteger(count) || count < 1 || count > 1e3) {
    throw new Error("Backup retention count must be between 1 and 1000");
  }
  return count;
}
__name(normalizeRetentionCount, "normalizeRetentionCount");
function normalizeIntervalHours(value, fallback = BACKUP_DEFAULT_INTERVAL_HOURS) {
  const raw = value === void 0 || value === null || value === "" ? fallback : Number(value);
  if (!Number.isInteger(raw) || raw < 1 || raw > 99) {
    throw new Error("Backup interval hours must be between 1 and 99");
  }
  return raw;
}
__name(normalizeIntervalHours, "normalizeIntervalHours");
function normalizeStartTime(value, fallback = BACKUP_DEFAULT_START_TIME) {
  const raw = asTrimmedString(value) || fallback;
  const match = raw.match(/^(\d{1,2})(?::(\d{1,2}))?$/);
  if (!match) {
    throw new Error("Backup start time must be in HH:mm format");
  }
  const hour = Number(match[1]);
  const minute = Number(match[2] ?? "0");
  if (!Number.isInteger(hour) || !Number.isInteger(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    throw new Error("Backup start time must be in HH:mm format");
  }
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}
__name(normalizeStartTime, "normalizeStartTime");
function normalizeS3Destination(value, allowIncomplete = false) {
  const source = isPlainObject2(value) ? value : {};
  const endpoint = asTrimmedString(source.endpoint);
  const bucket = asTrimmedString(source.bucket);
  const accessKeyId = asTrimmedString(source.accessKeyId);
  const secretAccessKey = asTrimmedString(source.secretAccessKey);
  const region = asTrimmedString(source.region) || "auto";
  const rootPath = normalizePath(source.rootPath);
  if (!allowIncomplete || endpoint) {
    if (!endpoint) throw new Error("S3 endpoint is required");
    if (!/^https?:\/\//i.test(endpoint)) throw new Error("S3 endpoint must start with http:// or https://");
  }
  if (!allowIncomplete || bucket) {
    if (!bucket) throw new Error("S3 bucket is required");
  }
  if (!allowIncomplete || accessKeyId) {
    if (!accessKeyId) throw new Error("S3 access key is required");
  }
  if (!allowIncomplete || secretAccessKey) {
    if (!secretAccessKey) throw new Error("S3 secret key is required");
  }
  return {
    endpoint: endpoint ? endpoint.replace(/\/+$/, "") : "",
    bucket,
    region,
    accessKeyId,
    secretAccessKey,
    rootPath
  };
}
__name(normalizeS3Destination, "normalizeS3Destination");
function normalizeWebDavDestination(value, allowIncomplete = false) {
  const source = isPlainObject2(value) ? value : {};
  const baseUrl = asTrimmedString(source.baseUrl);
  const username = asTrimmedString(source.username);
  const password = String(source.password ?? "");
  const remotePath = normalizePath(source.remotePath);
  if (!allowIncomplete || baseUrl) {
    if (!baseUrl) throw new Error("WebDAV server URL is required");
    if (!/^https?:\/\//i.test(baseUrl)) throw new Error("WebDAV server URL must start with http:// or https://");
  }
  if (!allowIncomplete || username) {
    if (!username) throw new Error("WebDAV username is required");
  }
  if (!allowIncomplete || password) {
    if (!password) throw new Error("WebDAV password is required");
  }
  return {
    baseUrl: baseUrl ? baseUrl.replace(/\/+$/, "") : "",
    username,
    password,
    remotePath
  };
}
__name(normalizeWebDavDestination, "normalizeWebDavDestination");
function normalizeDestination(destinationType, destination, allowIncomplete = false) {
  if (destinationType === "s3") return normalizeS3Destination(destination, allowIncomplete);
  return normalizeWebDavDestination(destination, allowIncomplete);
}
__name(normalizeDestination, "normalizeDestination");
function normalizeRuntime(value) {
  const source = isPlainObject2(value) ? value : {};
  const asIso = /* @__PURE__ */ __name((input) => {
    const raw = asTrimmedString(input);
    if (!raw) return null;
    const date = new Date(raw);
    return Number.isFinite(date.getTime()) ? date.toISOString() : null;
  }, "asIso");
  const asMaybeNumber = /* @__PURE__ */ __name((input) => {
    if (input === null || input === void 0 || input === "") return null;
    const n = Number(input);
    return Number.isFinite(n) && n >= 0 ? Math.floor(n) : null;
  }, "asMaybeNumber");
  return {
    lastAttemptAt: asIso(source.lastAttemptAt),
    lastAttemptLocalDate: asTrimmedString(source.lastAttemptLocalDate) || null,
    lastSuccessAt: asIso(source.lastSuccessAt),
    lastErrorAt: asIso(source.lastErrorAt),
    lastErrorMessage: asTrimmedString(source.lastErrorMessage) || null,
    lastUploadedFileName: asTrimmedString(source.lastUploadedFileName) || null,
    lastUploadedSizeBytes: asMaybeNumber(source.lastUploadedSizeBytes),
    lastUploadedDestination: asTrimmedString(source.lastUploadedDestination) || null
  };
}
__name(normalizeRuntime, "normalizeRuntime");
function defaultDestinationName(type, index) {
  return createDefaultBackupDestinationName(type, index);
}
__name(defaultDestinationName, "defaultDestinationName");
function getDestinationType(raw) {
  const value = asTrimmedString(raw);
  if (value === "e3") return "s3";
  if (value === "s3" || value === "webdav") return value;
  throw new Error("Backup destination type is invalid");
}
__name(getDestinationType, "getDestinationType");
function normalizeDestinationRecord(input, previousById, index, fallbackTimezone) {
  if (!isPlainObject2(input)) {
    throw new Error("Backup destination is invalid");
  }
  const id = asTrimmedString(input.id) || createBackupRandomId();
  const type = getDestinationType(input.type);
  const previous = previousById.get(id);
  const runtime = previous?.runtime ? normalizeRuntime(previous.runtime) : normalizeRuntime(input.runtime);
  const name = asTrimmedString(input.name) || previous?.name || defaultDestinationName(type, index + 1);
  const scheduleSource = isPlainObject2(input.schedule) ? input.schedule : {};
  const previousSchedule = previous?.schedule || defaultScheduleConfig(fallbackTimezone);
  const retentionSource = Object.prototype.hasOwnProperty.call(scheduleSource, "retentionCount") ? scheduleSource.retentionCount : previousSchedule.retentionCount;
  const schedule = {
    enabled: !!(scheduleSource.enabled ?? previousSchedule.enabled),
    intervalHours: normalizeIntervalHours(
      scheduleSource.intervalHours ?? previousSchedule.intervalHours,
      previousSchedule.intervalHours || BACKUP_DEFAULT_INTERVAL_HOURS
    ),
    startTime: normalizeStartTime(
      scheduleSource.startTime ?? previousSchedule.startTime,
      previousSchedule.startTime || BACKUP_DEFAULT_START_TIME
    ),
    timezone: assertValidTimeZone(asTrimmedString(scheduleSource.timezone ?? previousSchedule.timezone) || fallbackTimezone || BACKUP_DEFAULT_TIMEZONE),
    retentionCount: normalizeRetentionCount(retentionSource, previousSchedule.retentionCount)
  };
  const destination = normalizeDestination(type, input.destination, !schedule.enabled);
  return {
    id,
    name,
    type,
    includeAttachments: typeof input.includeAttachments === "boolean" ? input.includeAttachments : previous?.includeAttachments ?? false,
    destination,
    schedule,
    runtime
  };
}
__name(normalizeDestinationRecord, "normalizeDestinationRecord");
function parseLegacyBackupSettings(rawValue, fallbackTimezone) {
  const legacyFrequency = asTrimmedString(rawValue.frequency).toLowerCase();
  const intervalHours = legacyFrequency === "weekly" ? 24 * 7 : legacyFrequency === "monthly" ? 24 * 30 : BACKUP_DEFAULT_INTERVAL_HOURS;
  const destinationTypeRaw = asTrimmedString(rawValue.destinationType);
  const destinationType = destinationTypeRaw === "e3" || destinationTypeRaw === "s3" || destinationTypeRaw === "webdav" ? getDestinationType(destinationTypeRaw) : "webdav";
  const destination = {
    id: createBackupRandomId(),
    name: defaultDestinationName(destinationType, 1),
    type: destinationType,
    includeAttachments: false,
    destination: normalizeDestination(destinationType, rawValue.destination),
    schedule: {
      enabled: !!rawValue.enabled,
      intervalHours,
      startTime: BACKUP_DEFAULT_START_TIME,
      timezone: assertValidTimeZone(asTrimmedString(rawValue.timezone) || fallbackTimezone || BACKUP_DEFAULT_TIMEZONE),
      retentionCount: 30
    },
    runtime: normalizeRuntime(rawValue.runtime)
  };
  return {
    destinations: [destination]
  };
}
__name(parseLegacyBackupSettings, "parseLegacyBackupSettings");
function parseDestinations(rawDestinations, previousById, fallbackTimezone) {
  if (!Array.isArray(rawDestinations)) {
    throw new Error("Backup destinations are invalid");
  }
  if (rawDestinations.length > MAX_BACKUP_DESTINATIONS) {
    throw new Error(`You can save up to ${MAX_BACKUP_DESTINATIONS} backup destinations`);
  }
  const destinations = rawDestinations.map((entry, index) => normalizeDestinationRecord(entry, previousById, index, fallbackTimezone));
  const ids = /* @__PURE__ */ new Set();
  for (const destination of destinations) {
    if (ids.has(destination.id)) {
      throw new Error("Backup destination ids must be unique");
    }
    ids.add(destination.id);
  }
  return destinations;
}
__name(parseDestinations, "parseDestinations");
function mapDestinationsById(destinations) {
  return new Map(destinations.map((destination) => [destination.id, destination]));
}
__name(mapDestinationsById, "mapDestinationsById");
function getDefaultBackupSettings(timezone = "UTC") {
  return createDefaultBackupSettings(assertValidTimeZone(timezone));
}
__name(getDefaultBackupSettings, "getDefaultBackupSettings");
function parseBackupSettings(raw, fallbackTimezone = "UTC") {
  if (!raw) return getDefaultBackupSettings(fallbackTimezone);
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed.destinations)) {
      const globalTimezone = assertValidTimeZone(asTrimmedString(parsed.timezone) || fallbackTimezone || BACKUP_DEFAULT_TIMEZONE);
      const globalEnabled = !!parsed.enabled;
      const activeDestinationIdRaw = asTrimmedString(parsed.activeDestinationId);
      const globalFrequency = asTrimmedString(parsed.frequency).toLowerCase();
      const globalIntervalHours = globalFrequency === "weekly" ? 24 * 7 : globalFrequency === "monthly" ? 24 * 30 : BACKUP_DEFAULT_INTERVAL_HOURS;
      const previousById = /* @__PURE__ */ new Map();
      const normalizedEntries = parsed.destinations.map((entry) => {
        if (!isPlainObject2(entry)) return entry;
        if (isPlainObject2(entry.schedule)) return entry;
        const entryId = asTrimmedString(entry.id);
        const scheduleEnabled = globalEnabled && (!activeDestinationIdRaw || entryId === activeDestinationIdRaw);
        return {
          ...entry,
          schedule: {
            enabled: scheduleEnabled,
            intervalHours: globalIntervalHours,
            startTime: BACKUP_DEFAULT_START_TIME,
            timezone: globalTimezone,
            retentionCount: 30
          }
        };
      });
      return {
        destinations: parseDestinations(normalizedEntries, previousById, fallbackTimezone)
      };
    }
    return parseLegacyBackupSettings(parsed, fallbackTimezone);
  } catch {
    return getDefaultBackupSettings(fallbackTimezone);
  }
}
__name(parseBackupSettings, "parseBackupSettings");
function normalizeBackupSettingsInput(input, previous) {
  if (!isPlainObject2(input)) {
    throw new Error("Backup settings payload is invalid");
  }
  const previousById = mapDestinationsById(previous.destinations);
  const rawDestinations = input.destinations ?? previous.destinations;
  const destinations = parseDestinations(rawDestinations, previousById, BACKUP_DEFAULT_TIMEZONE);
  return {
    destinations
  };
}
__name(normalizeBackupSettingsInput, "normalizeBackupSettingsInput");
function serializeBackupSettings(settings) {
  return JSON.stringify(settings);
}
__name(serializeBackupSettings, "serializeBackupSettings");
async function loadBackupSettings(storage, env, fallbackTimezone = "UTC") {
  const raw = await storage.getConfigValue(BACKUP_SETTINGS_CONFIG_KEY);
  if (!raw) {
    const settings = getDefaultBackupSettings(fallbackTimezone);
    await saveBackupSettings(storage, env, settings);
    return settings;
  }
  const envelope = parseBackupSettingsEnvelope(raw);
  if (!envelope) {
    const settings = parseBackupSettings(raw, fallbackTimezone);
    await saveBackupSettings(storage, env, settings);
    return settings;
  }
  try {
    const decrypted = await decryptBackupSettingsRuntime(raw, env);
    return parseBackupSettings(decrypted, fallbackTimezone);
  } catch {
    throw new Error("Backup settings need administrator reactivation after restore");
  }
}
__name(loadBackupSettings, "loadBackupSettings");
async function saveBackupSettings(storage, env, settings) {
  const users = await storage.getAllUsers();
  const hasPortableAdmins = users.some(
    (user) => user.role === "admin" && user.status === "active" && typeof user.publicKey === "string" && user.publicKey.trim().length > 0
  );
  if (!hasPortableAdmins) {
    await storage.setConfigValue(BACKUP_SETTINGS_CONFIG_KEY, serializeBackupSettings(settings));
    return;
  }
  const encrypted = await encryptBackupSettingsEnvelope(serializeBackupSettings(settings), env, users);
  await storage.setConfigValue(BACKUP_SETTINGS_CONFIG_KEY, encrypted);
}
__name(saveBackupSettings, "saveBackupSettings");
async function normalizeImportedBackupSettingsValue(raw, env, users, fallbackTimezone = "UTC") {
  if (!raw) return null;
  const envelope = parseBackupSettingsEnvelope(raw);
  if (envelope) {
    try {
      const decrypted = await decryptBackupSettingsRuntime(raw, env);
      const settings2 = parseBackupSettings(decrypted, fallbackTimezone);
      const hasPortableAdmins2 = users.some(
        (user) => user.role === "admin" && user.status === "active" && typeof user.publicKey === "string" && user.publicKey.trim().length > 0
      );
      if (!hasPortableAdmins2) {
        return serializeBackupSettings(settings2);
      }
      return encryptBackupSettingsEnvelope(serializeBackupSettings(settings2), env, users);
    } catch {
      return raw;
    }
  }
  const settings = parseBackupSettings(raw, fallbackTimezone);
  const hasPortableAdmins = users.some(
    (user) => user.role === "admin" && user.status === "active" && typeof user.publicKey === "string" && user.publicKey.trim().length > 0
  );
  if (!hasPortableAdmins) {
    return serializeBackupSettings(settings);
  }
  return encryptBackupSettingsEnvelope(serializeBackupSettings(settings), env, users);
}
__name(normalizeImportedBackupSettingsValue, "normalizeImportedBackupSettingsValue");
async function getBackupSettingsRepairState(storage, env, fallbackTimezone = "UTC") {
  const raw = await storage.getConfigValue(BACKUP_SETTINGS_CONFIG_KEY);
  if (!raw) {
    const settings = getDefaultBackupSettings(fallbackTimezone);
    await saveBackupSettings(storage, env, settings);
    return { needsRepair: false, portable: null };
  }
  const envelope = parseBackupSettingsEnvelope(raw);
  if (!envelope) {
    const settings = parseBackupSettings(raw, fallbackTimezone);
    await saveBackupSettings(storage, env, settings);
    return { needsRepair: false, portable: null };
  }
  try {
    await decryptBackupSettingsRuntime(raw, env);
    return { needsRepair: false, portable: null };
  } catch {
    return {
      needsRepair: true,
      portable: envelope.portable
    };
  }
}
__name(getBackupSettingsRepairState, "getBackupSettingsRepairState");
async function repairBackupSettings(storage, env, settings) {
  await saveBackupSettings(storage, env, settings);
}
__name(repairBackupSettings, "repairBackupSettings");
function findBackupDestination(settings, destinationId) {
  const normalizedId = asTrimmedString(destinationId);
  if (!normalizedId) return null;
  return settings.destinations.find((destination) => destination.id === normalizedId) || null;
}
__name(findBackupDestination, "findBackupDestination");
function requireBackupDestination(settings, destinationId) {
  const destination = destinationId ? findBackupDestination(settings, destinationId) : settings.destinations[0] || null;
  if (!destination) {
    throw new Error("Backup destination not found");
  }
  return destination;
}
__name(requireBackupDestination, "requireBackupDestination");
function getDateTimeParts(date, timezone) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23"
  });
  const parts = formatter.formatToParts(date);
  const pick = /* @__PURE__ */ __name((type) => parts.find((part) => part.type === type)?.value || "", "pick");
  return {
    year: pick("year"),
    month: pick("month"),
    day: pick("day"),
    hour: pick("hour"),
    minute: pick("minute")
  };
}
__name(getDateTimeParts, "getDateTimeParts");
function getBackupLocalDateKey(date, timezone) {
  const parts = getDateTimeParts(date, timezone);
  return `${parts.year}-${parts.month}-${parts.day}`;
}
__name(getBackupLocalDateKey, "getBackupLocalDateKey");
function parseLocalDateKey(dateKey) {
  const match = String(dateKey || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
  return { year, month, day };
}
__name(parseLocalDateKey, "parseLocalDateKey");
function getUtcDateForLocalTime(timezone, year, month, day, hour, minute) {
  const utcGuess = Date.UTC(year, month - 1, day, hour, minute, 0, 0);
  const actual = getDateTimeParts(new Date(utcGuess), timezone);
  const actualUtc = Date.UTC(
    Number(actual.year),
    Number(actual.month) - 1,
    Number(actual.day),
    Number(actual.hour),
    Number(actual.minute),
    0,
    0
  );
  const desiredUtc = Date.UTC(year, month - 1, day, hour, minute, 0, 0);
  return new Date(utcGuess - (actualUtc - desiredUtc));
}
__name(getUtcDateForLocalTime, "getUtcDateForLocalTime");
function getBackupSlotStartsForLocalDay(dateKey, timezone, startTime, intervalHours) {
  const parsedDate = parseLocalDateKey(dateKey);
  const parsedTime = normalizeStartTime(startTime).split(":").map((value) => Number(value));
  if (!parsedDate || parsedTime.length !== 2) return [];
  const [hour, minute] = parsedTime;
  const firstSlot = getUtcDateForLocalTime(timezone, parsedDate.year, parsedDate.month, parsedDate.day, hour, minute);
  const nextLocalDay = new Date(Date.UTC(parsedDate.year, parsedDate.month - 1, parsedDate.day, 0, 0, 0, 0));
  nextLocalDay.setUTCDate(nextLocalDay.getUTCDate() + 1);
  const nextDay = getUtcDateForLocalTime(
    timezone,
    nextLocalDay.getUTCFullYear(),
    nextLocalDay.getUTCMonth() + 1,
    nextLocalDay.getUTCDate(),
    0,
    0
  );
  const intervalMs = intervalHours * 60 * 60 * 1e3;
  const slots = [];
  for (let slotMs = firstSlot.getTime(); slotMs < nextDay.getTime(); slotMs += intervalMs) {
    slots.push(new Date(slotMs));
  }
  return slots;
}
__name(getBackupSlotStartsForLocalDay, "getBackupSlotStartsForLocalDay");
function hasBackupSlotBetween(destination, startInclusive, endExclusive) {
  if (!destination.schedule.enabled) return false;
  const startMs = startInclusive.getTime();
  const endMs = endExclusive.getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return false;
  const lastAttemptAt = destination.runtime.lastAttemptAt ? new Date(destination.runtime.lastAttemptAt) : null;
  const lastAttemptMs = lastAttemptAt && Number.isFinite(lastAttemptAt.getTime()) ? lastAttemptAt.getTime() : Number.NEGATIVE_INFINITY;
  const dayCursor = new Date(startMs);
  dayCursor.setUTCHours(0, 0, 0, 0);
  const endDay = new Date(endMs);
  endDay.setUTCHours(0, 0, 0, 0);
  const checkedLocalDateKeys = /* @__PURE__ */ new Set();
  while (dayCursor.getTime() <= endDay.getTime() + 24 * 60 * 60 * 1e3) {
    const localDateKey = getBackupLocalDateKey(dayCursor, destination.schedule.timezone);
    if (!checkedLocalDateKeys.has(localDateKey)) {
      checkedLocalDateKeys.add(localDateKey);
      const slotStarts = getBackupSlotStartsForLocalDay(
        localDateKey,
        destination.schedule.timezone,
        destination.schedule.startTime,
        destination.schedule.intervalHours
      );
      for (const slotStart of slotStarts) {
        const slotStartMs = slotStart.getTime();
        if (slotStartMs < startMs || slotStartMs >= endMs) continue;
        if (lastAttemptMs >= slotStartMs) continue;
        return true;
      }
    }
    dayCursor.setUTCDate(dayCursor.getUTCDate() + 1);
  }
  return false;
}
__name(hasBackupSlotBetween, "hasBackupSlotBetween");
function isBackupDueNow(destination, now, windowMinutes = BACKUP_SCHEDULER_WINDOW_MINUTES) {
  if (!destination.schedule.enabled) return false;
  const toleranceMs = Math.max(1, windowMinutes) * 60 * 1e3;
  const lastAttemptAt = destination.runtime.lastAttemptAt ? new Date(destination.runtime.lastAttemptAt) : null;
  const lastAttemptMs = lastAttemptAt && Number.isFinite(lastAttemptAt.getTime()) ? lastAttemptAt.getTime() : Number.NEGATIVE_INFINITY;
  const localDateKey = getBackupLocalDateKey(now, destination.schedule.timezone);
  const slotStarts = getBackupSlotStartsForLocalDay(
    localDateKey,
    destination.schedule.timezone,
    destination.schedule.startTime,
    destination.schedule.intervalHours
  );
  for (const slotStart of slotStarts) {
    const slotStartMs = slotStart.getTime();
    if (now.getTime() < slotStartMs || now.getTime() >= slotStartMs + toleranceMs) continue;
    if (lastAttemptMs >= slotStartMs) return false;
    return true;
  }
  return false;
}
__name(isBackupDueNow, "isBackupDueNow");

// src/services/backup-archive.ts
var BACKUP_FORMAT_VERSION = 1;
var BACKUP_RUNNER_LOCK_CONFIG_KEY = "backup.runner.lock.v1";
var BACKUP_FILE_HASH_PREFIX_LENGTH = 5;
var BACKUP_TEXT_COMPRESSION_LEVEL = 0;
var BACKUP_JSON_INDENT = 2;
var MAX_BACKUP_ARCHIVE_BYTES = 64 * 1024 * 1024;
var MAX_BACKUP_ARCHIVE_ENTRY_COUNT = 1e4;
var MAX_BACKUP_EXTRACTED_BYTES = 64 * 1024 * 1024;
var MAX_BACKUP_DB_JSON_BYTES = 32 * 1024 * 1024;
async function queryRows(db, sql, ...values) {
  const result = await db.prepare(sql).bind(...values).all();
  return (result.results || []).map((row) => ({ ...row }));
}
__name(queryRows, "queryRows");
function sanitizeConfigRowsForExport(rows) {
  const sanitized = [];
  for (const row of rows) {
    const key = String(row.key || "").trim();
    if (!key || key === BACKUP_RUNNER_LOCK_CONFIG_KEY) continue;
    if (key === BACKUP_SETTINGS_CONFIG_KEY) {
      const portableOnly = exportPortableBackupSettingsEnvelope(typeof row.value === "string" ? row.value : null);
      if (portableOnly) sanitized.push({ ...row, value: portableOnly });
      continue;
    }
    sanitized.push({ ...row });
  }
  return sanitized;
}
__name(sanitizeConfigRowsForExport, "sanitizeConfigRowsForExport");
async function sha256Hex(bytes) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
__name(sha256Hex, "sha256Hex");
function getDateParts(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23"
  });
  const parts = formatter.formatToParts(date);
  const pick = /* @__PURE__ */ __name((type) => parts.find((part) => part.type === type)?.value || "", "pick");
  return `${pick("year")}${pick("month")}${pick("day")}_${pick("hour")}${pick("minute")}${pick("second")}`;
}
__name(getDateParts, "getDateParts");
function buildBackupFileNameInTimeZone(date = /* @__PURE__ */ new Date(), checksumPrefix = null, timeZone = "UTC") {
  const parts = getDateParts(date, timeZone);
  const suffix = checksumPrefix ? `_${checksumPrefix}` : "";
  return `nodewarden_backup_${parts}${suffix}.zip`;
}
__name(buildBackupFileNameInTimeZone, "buildBackupFileNameInTimeZone");
function extractBackupFileChecksumPrefix(fileName) {
  const normalized = String(fileName || "").trim();
  const match = normalized.match(/_([0-9a-f]{5})\.zip$/i);
  return match ? match[1].toLowerCase() : null;
}
__name(extractBackupFileChecksumPrefix, "extractBackupFileChecksumPrefix");
async function inspectBackupArchiveFileNameChecksum(bytes, fileName) {
  const expectedPrefix = extractBackupFileChecksumPrefix(fileName);
  const actualHash = await sha256Hex(bytes);
  const actualPrefix = actualHash.slice(0, BACKUP_FILE_HASH_PREFIX_LENGTH);
  return {
    hasChecksumPrefix: !!expectedPrefix,
    expectedPrefix,
    actualPrefix,
    matches: !expectedPrefix || actualPrefix === expectedPrefix
  };
}
__name(inspectBackupArchiveFileNameChecksum, "inspectBackupArchiveFileNameChecksum");
async function verifyBackupArchiveFileNameChecksum(bytes, fileName) {
  const result = await inspectBackupArchiveFileNameChecksum(bytes, fileName);
  return result.matches;
}
__name(verifyBackupArchiveFileNameChecksum, "verifyBackupArchiveFileNameChecksum");
function validateArchiveSize(bytes) {
  if (bytes.byteLength > MAX_BACKUP_ARCHIVE_BYTES) {
    throw new Error(`Backup archive is too large. The current restore limit is ${Math.floor(MAX_BACKUP_ARCHIVE_BYTES / (1024 * 1024))} MiB`);
  }
}
__name(validateArchiveSize, "validateArchiveSize");
function getRequiredZipEntries(db) {
  const entries = [];
  for (const row of db.attachments) {
    const cipherId = String(row.cipher_id || "").trim();
    const attachmentId = String(row.id || "").trim();
    if (!cipherId || !attachmentId) continue;
    entries.push(`attachments/${cipherId}/${attachmentId}.bin`);
  }
  return entries;
}
__name(getRequiredZipEntries, "getRequiredZipEntries");
function ensureRowArray(value, table) {
  if (!Array.isArray(value)) {
    throw new Error(`Backup archive table ${table} is invalid`);
  }
  return value;
}
__name(ensureRowArray, "ensureRowArray");
function createZipEntries(files) {
  const entries = {};
  for (const [path, bytes] of Object.entries(files)) {
    entries[path] = [bytes, { level: BACKUP_TEXT_COMPRESSION_LEVEL }];
  }
  return entries;
}
__name(createZipEntries, "createZipEntries");
function parseBackupArchive(bytes, options = {}) {
  validateArchiveSize(bytes);
  let zipped;
  try {
    zipped = unzipSync(bytes);
  } catch {
    throw new Error("Invalid backup archive");
  }
  const entryNames = Object.keys(zipped);
  if (entryNames.length > MAX_BACKUP_ARCHIVE_ENTRY_COUNT) {
    throw new Error("Backup archive contains too many files");
  }
  let totalExtractedBytes = 0;
  for (const entry of entryNames) {
    const entryBytes = zipped[entry];
    totalExtractedBytes += entryBytes.byteLength;
    if (entry === "db.json" && entryBytes.byteLength > MAX_BACKUP_DB_JSON_BYTES) {
      throw new Error("Backup archive database payload is too large");
    }
    if (totalExtractedBytes > MAX_BACKUP_EXTRACTED_BYTES) {
      throw new Error("Backup archive expands beyond the current restore limit");
    }
  }
  const manifestBytes = zipped["manifest.json"];
  const dbBytes = zipped["db.json"];
  if (!manifestBytes || !dbBytes) {
    throw new Error("Backup archive is missing manifest.json or db.json");
  }
  const decoder = new TextDecoder();
  let manifest;
  let db;
  try {
    manifest = JSON.parse(decoder.decode(manifestBytes));
    db = JSON.parse(decoder.decode(dbBytes));
  } catch {
    throw new Error("Backup archive contains invalid JSON metadata");
  }
  if (manifest?.formatVersion !== BACKUP_FORMAT_VERSION) {
    throw new Error("Unsupported backup format version");
  }
  if (!db || typeof db !== "object") {
    throw new Error("Backup archive database payload is invalid");
  }
  const externalAttachmentKeys = new Set(
    options.allowExternalAttachmentBlobs ? (manifest.attachmentBlobs || []).map((item) => `attachments/${String(item.cipherId || "").trim()}/${String(item.attachmentId || "").trim()}.bin`) : []
  );
  const requiredEntries = getRequiredZipEntries(db).filter((entry) => !externalAttachmentKeys.has(entry));
  for (const entry of requiredEntries) {
    if (!zipped[entry]) {
      throw new Error(`Backup archive is missing required file: ${entry}`);
    }
  }
  return {
    payload: { manifest, db },
    files: zipped
  };
}
__name(parseBackupArchive, "parseBackupArchive");
function validateBackupPayloadContents(payload, files, options = {}) {
  const configRows = ensureRowArray(payload.db.config, "config");
  const userRows = ensureRowArray(payload.db.users, "users");
  const revisionRows = ensureRowArray(payload.db.user_revisions, "user_revisions");
  const domainSettingsRows = ensureRowArray(payload.db.domain_settings || [], "domain_settings");
  const folderRows = ensureRowArray(payload.db.folders, "folders");
  const cipherRows = ensureRowArray(payload.db.ciphers, "ciphers");
  const attachmentRows = ensureRowArray(payload.db.attachments, "attachments");
  const externalAttachmentKeys = new Set(
    options.allowExternalAttachmentBlobs ? (payload.manifest.attachmentBlobs || []).map((item) => `attachments/${String(item.cipherId || "").trim()}/${String(item.attachmentId || "").trim()}.bin`) : []
  );
  const userIds = /* @__PURE__ */ new Set();
  for (const row of userRows) {
    const id = String(row.id || "").trim();
    const email = String(row.email || "").trim();
    if (!id || !email) throw new Error("Backup archive contains an invalid user row");
    if (userIds.has(id)) throw new Error(`Backup archive contains duplicate user id: ${id}`);
    userIds.add(id);
  }
  for (const row of configRows) {
    const key = String(row.key || "").trim();
    if (!key) throw new Error("Backup archive contains an invalid config row");
  }
  for (const row of revisionRows) {
    const userId = String(row.user_id || "").trim();
    if (!userId || !userIds.has(userId)) {
      throw new Error(`Backup archive contains a revision for an unknown user: ${userId || "(empty)"}`);
    }
  }
  const domainSettingUserIds = /* @__PURE__ */ new Set();
  for (const row of domainSettingsRows) {
    const userId = String(row.user_id || "").trim();
    if (!userId || !userIds.has(userId)) {
      throw new Error(`Backup archive contains domain settings for an unknown user: ${userId || "(empty)"}`);
    }
    if (domainSettingUserIds.has(userId)) {
      throw new Error(`Backup archive contains duplicate domain settings for user: ${userId}`);
    }
    domainSettingUserIds.add(userId);
  }
  const folderIds = /* @__PURE__ */ new Set();
  for (const row of folderRows) {
    const id = String(row.id || "").trim();
    const userId = String(row.user_id || "").trim();
    if (!id || !userIds.has(userId)) throw new Error("Backup archive contains an invalid folder row");
    if (folderIds.has(id)) throw new Error(`Backup archive contains duplicate folder id: ${id}`);
    folderIds.add(id);
  }
  const cipherIds = /* @__PURE__ */ new Set();
  for (const row of cipherRows) {
    const id = String(row.id || "").trim();
    const userId = String(row.user_id || "").trim();
    const folderId = String(row.folder_id || "").trim();
    if (!id || !userIds.has(userId)) throw new Error("Backup archive contains an invalid cipher row");
    if (folderId && !folderIds.has(folderId)) {
      throw new Error(`Backup archive contains a cipher for an unknown folder: ${folderId}`);
    }
    if (cipherIds.has(id)) throw new Error(`Backup archive contains duplicate cipher id: ${id}`);
    cipherIds.add(id);
  }
  for (const row of attachmentRows) {
    const id = String(row.id || "").trim();
    const cipherId = String(row.cipher_id || "").trim();
    if (!id || !cipherId || !cipherIds.has(cipherId)) {
      throw new Error("Backup archive contains an invalid attachment row");
    }
    const attachmentPath = `attachments/${cipherId}/${id}.bin`;
    if (!files[attachmentPath] && !externalAttachmentKeys.has(attachmentPath)) {
      throw new Error(`Backup archive is missing required file: attachments/${cipherId}/${id}.bin`);
    }
  }
}
__name(validateBackupPayloadContents, "validateBackupPayloadContents");
async function buildBackupArchive(env, date = /* @__PURE__ */ new Date(), options = {}) {
  const includeAttachments = options.includeAttachments !== false;
  await options.progress?.({
    step: "collect_data",
    fileName: "",
    stageTitle: "txt_backup_archive_progress_collect_title",
    stageDetail: includeAttachments ? "txt_backup_archive_progress_collect_with_attachments_detail" : "txt_backup_archive_progress_collect_detail",
    includeAttachments
  });
  const encoder = new TextEncoder();
  const [configRows, userRows, domainSettingsRows, revisionRows, folderRows, cipherRows, attachmentRows] = await Promise.all([
    queryRows(env.DB, "SELECT key, value FROM config ORDER BY key ASC"),
    queryRows(env.DB, "SELECT id, email, name, master_password_hint, master_password_hash, key, private_key, public_key, kdf_type, kdf_iterations, kdf_memory, kdf_parallelism, security_stamp, role, status, verify_devices, totp_secret, totp_recovery_code, created_at, updated_at FROM users ORDER BY created_at ASC"),
    queryRows(env.DB, "SELECT user_id, equivalent_domains, custom_equivalent_domains, excluded_global_equivalent_domains, updated_at FROM domain_settings ORDER BY user_id ASC"),
    queryRows(env.DB, "SELECT user_id, revision_date FROM user_revisions ORDER BY user_id ASC"),
    queryRows(env.DB, "SELECT id, user_id, name, created_at, updated_at FROM folders ORDER BY created_at ASC"),
    queryRows(env.DB, "SELECT id, user_id, type, folder_id, name, notes, favorite, data, reprompt, key, created_at, updated_at, archived_at, deleted_at FROM ciphers ORDER BY created_at ASC"),
    queryRows(env.DB, "SELECT id, cipher_id, file_name, size, size_name, key FROM attachments ORDER BY cipher_id ASC, id ASC")
  ]);
  const exportedConfigRows = sanitizeConfigRowsForExport(configRows);
  const exportedAttachmentRows = includeAttachments ? attachmentRows : [];
  const attachmentBlobs = exportedAttachmentRows.map((row) => {
    const cipherId = String(row.cipher_id || "").trim();
    const attachmentId = String(row.id || "").trim();
    return {
      cipherId,
      attachmentId,
      blobName: getAttachmentObjectKey(cipherId, attachmentId),
      sizeBytes: Number(row.size || 0) || 0
    };
  });
  const manifestBase = {
    formatVersion: BACKUP_FORMAT_VERSION,
    exportedAt: date.toISOString(),
    appVersion: APP_VERSION,
    storageKind: getBlobStorageKind(env),
    tableCounts: {
      config: exportedConfigRows.length,
      users: userRows.length,
      domain_settings: domainSettingsRows.length,
      user_revisions: revisionRows.length,
      folders: folderRows.length,
      ciphers: cipherRows.length,
      attachments: exportedAttachmentRows.length
    },
    includes: {
      attachments: includeAttachments
    },
    blobSummary: {
      attachmentFiles: attachmentBlobs.length,
      totalBytes: attachmentBlobs.reduce((sum, item) => sum + item.sizeBytes, 0),
      largestObjectBytes: attachmentBlobs.reduce((max2, item) => Math.max(max2, item.sizeBytes), 0)
    },
    attachmentBlobs: includeAttachments ? attachmentBlobs : []
  };
  const files = {
    "manifest.json": encoder.encode(JSON.stringify(manifestBase, null, BACKUP_JSON_INDENT)),
    "db.json": encoder.encode(JSON.stringify({
      config: exportedConfigRows,
      users: userRows,
      domain_settings: domainSettingsRows,
      user_revisions: revisionRows,
      folders: folderRows,
      ciphers: cipherRows,
      attachments: exportedAttachmentRows
    }, null, BACKUP_JSON_INDENT))
  };
  await options.progress?.({
    step: "package_archive",
    fileName: "",
    stageTitle: "txt_backup_archive_progress_package_title",
    stageDetail: includeAttachments ? "txt_backup_archive_progress_package_with_attachments_detail" : "txt_backup_archive_progress_package_detail",
    includeAttachments
  });
  const bytes = zipSync(createZipEntries(files));
  const fileHashPrefix = (await sha256Hex(bytes)).slice(0, BACKUP_FILE_HASH_PREFIX_LENGTH);
  const backupTimeZone = options.timeZone || "UTC";
  const fileName = buildBackupFileNameInTimeZone(date, fileHashPrefix, backupTimeZone);
  await options.progress?.({
    step: "archive_ready",
    fileName,
    stageTitle: "txt_backup_archive_progress_ready_title",
    stageDetail: "txt_backup_archive_progress_ready_detail",
    includeAttachments
  });
  return {
    bytes,
    fileName,
    manifest: manifestBase
  };
}
__name(buildBackupArchive, "buildBackupArchive");

// src/services/backup-import.ts
var BACKUP_TABLES = [
  "config",
  "users",
  "domain_settings",
  "user_revisions",
  "folders",
  "ciphers",
  "attachments"
];
function shadowTableName(table) {
  return `${table}__restore`;
}
__name(shadowTableName, "shadowTableName");
async function queryRows2(db, sql, ...values) {
  const response = await db.prepare(sql).bind(...values).all();
  return (response.results || []).map((row) => ({ ...row }));
}
__name(queryRows2, "queryRows");
async function getTableCreateSql(db, table) {
  const row = await db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?").bind(table).first();
  const sql = String(row?.sql || "").trim();
  if (!sql) {
    throw new Error(`Restore shadow schema is missing table definition for ${table}`);
  }
  return sql;
}
__name(getTableCreateSql, "getTableCreateSql");
function buildShadowTableCreateSql(createSql, table) {
  const tablePattern = new RegExp(`^CREATE TABLE(?:\\s+IF NOT EXISTS)?\\s+(?:"${table}"|${table})(?=\\s*\\()`, "i");
  let next = createSql.replace(tablePattern, `CREATE TABLE "${shadowTableName(table)}"`);
  if (next === createSql) {
    throw new Error(`Restore shadow schema could not rewrite CREATE TABLE statement for ${table}`);
  }
  for (const currentTable of BACKUP_TABLES) {
    const referencePattern = new RegExp(`\\bREFERENCES\\s+(?:"${currentTable}"|${currentTable})(?=\\s*\\()`, "gi");
    next = next.replace(
      referencePattern,
      `REFERENCES "${shadowTableName(currentTable)}"`
    );
  }
  return next;
}
__name(buildShadowTableCreateSql, "buildShadowTableCreateSql");
async function resetRestoreArtifacts(db) {
  const dropStatements = BACKUP_TABLES.slice().reverse().map((table) => db.prepare(`DROP TABLE IF EXISTS ${shadowTableName(table)}`));
  if (dropStatements.length) {
    await db.batch(dropStatements);
  }
}
__name(resetRestoreArtifacts, "resetRestoreArtifacts");
async function createShadowTables(db) {
  const createStatements = [];
  for (const table of BACKUP_TABLES) {
    const createSql = await getTableCreateSql(db, table);
    createStatements.push(db.prepare(buildShadowTableCreateSql(createSql, table)));
  }
  await db.batch(createStatements);
}
__name(createShadowTables, "createShadowTables");
async function validateShadowTableCounts(db, expectedCounts) {
  await Promise.all(BACKUP_TABLES.map(async (table) => {
    const expected = expectedCounts[table] ?? 0;
    const row = await db.prepare(`SELECT COUNT(*) AS count FROM ${shadowTableName(table)}`).first();
    const actual = Number(row?.count || 0);
    if (actual !== expected) {
      throw new Error(`Restore shadow validation failed for ${table}: expected ${expected}, received ${actual}`);
    }
  }));
}
__name(validateShadowTableCounts, "validateShadowTableCounts");
async function swapShadowTablesIntoPlace(db) {
  const statements = [];
  for (const sql of buildResetImportTargetStatements(db)) {
    statements.push(sql);
  }
  for (const table of BACKUP_TABLES) {
    statements.push(db.prepare(`INSERT INTO ${table} SELECT * FROM ${shadowTableName(table)}`));
  }
  await db.batch(statements);
}
__name(swapShadowTablesIntoPlace, "swapShadowTablesIntoPlace");
async function ensureImportTargetIsFresh(db) {
  const counts = await Promise.all([
    db.prepare("SELECT COUNT(*) AS count FROM ciphers").first(),
    db.prepare("SELECT COUNT(*) AS count FROM folders").first(),
    db.prepare("SELECT COUNT(*) AS count FROM attachments").first(),
    db.prepare("SELECT COUNT(*) AS count FROM sends").first()
  ]);
  const total = counts.reduce((sum, row) => sum + Number(row?.count || 0), 0);
  if (total > 0) {
    throw new Error("Backup import requires a fresh instance with no vault or send data");
  }
}
__name(ensureImportTargetIsFresh, "ensureImportTargetIsFresh");
function buildResetImportTargetStatements(db) {
  return [
    "DELETE FROM attachments",
    "DELETE FROM ciphers",
    "DELETE FROM folders",
    "DELETE FROM domain_settings",
    "DELETE FROM user_revisions",
    "DELETE FROM users",
    "DELETE FROM config"
  ].map((sql) => db.prepare(sql));
}
__name(buildResetImportTargetStatements, "buildResetImportTargetStatements");
async function collectCurrentBlobKeys(db) {
  const keys = /* @__PURE__ */ new Set();
  const attachmentRows = await queryRows2(
    db,
    `SELECT a.id, a.cipher_id
     FROM attachments a
     INNER JOIN ciphers c ON c.id = a.cipher_id`
  );
  for (const row of attachmentRows) {
    const cipherId = String(row.cipher_id || "").trim();
    const attachmentId = String(row.id || "").trim();
    if (!cipherId || !attachmentId) continue;
    keys.add(getAttachmentObjectKey(cipherId, attachmentId));
  }
  return keys;
}
__name(collectCurrentBlobKeys, "collectCurrentBlobKeys");
var KV_BLOB_SKIP_REASON = "Cloudflare KV object size limit (25 MB)";
var BLOB_STORAGE_UNAVAILABLE_SKIP_REASON = "Attachment storage is not configured";
var ATTACHMENT_RESTORE_FAILED_REASON = "Some attachments could not be restored and were skipped";
function attachmentRowKey(row) {
  const attachmentId = String(row.id || "").trim();
  const cipherId = String(row.cipher_id || "").trim();
  return `${cipherId}/${attachmentId}`;
}
__name(attachmentRowKey, "attachmentRowKey");
function cloneRows(rows) {
  return rows.map((row) => ({ ...row }));
}
__name(cloneRows, "cloneRows");
function upsertConfigRow(rows, key, value) {
  let replaced = false;
  const nextRows = rows.map((row) => {
    if (String(row.key || "").trim() !== key) return { ...row };
    replaced = true;
    return { ...row, key, value };
  });
  if (!replaced) {
    nextRows.push({ key, value });
  }
  return nextRows;
}
__name(upsertConfigRow, "upsertConfigRow");
async function prepareImportedConfigRows(env, configRows, userRows) {
  let nextConfigRows = cloneRows(configRows || []);
  const rawBackupSettings = nextConfigRows.find((row) => String(row.key || "").trim() === BACKUP_SETTINGS_CONFIG_KEY);
  const normalizedBackupSettings = await normalizeImportedBackupSettingsValue(
    typeof rawBackupSettings?.value === "string" ? rawBackupSettings.value : null,
    env,
    userRows.map((row) => ({
      id: String(row.id || "").trim(),
      publicKey: typeof row.public_key === "string" ? row.public_key : null,
      role: String(row.role || "").trim(),
      status: String(row.status || "").trim()
    })),
    "UTC"
  );
  if (normalizedBackupSettings !== null) {
    nextConfigRows = upsertConfigRow(nextConfigRows, BACKUP_SETTINGS_CONFIG_KEY, normalizedBackupSettings);
  }
  nextConfigRows = upsertConfigRow(nextConfigRows, "registered", "true");
  return nextConfigRows;
}
__name(prepareImportedConfigRows, "prepareImportedConfigRows");
async function importPreparedBackupRows(db, payload, env) {
  const preparedDb = {
    config: await prepareImportedConfigRows(env, payload.config || [], payload.users || []),
    users: cloneRows(payload.users || []).map((row) => ({
      ...row,
      verify_devices: row.verify_devices ?? 1
    })),
    domain_settings: cloneRows(payload.domain_settings || []),
    user_revisions: cloneRows(payload.user_revisions || []),
    folders: cloneRows(payload.folders || []),
    ciphers: cloneRows(payload.ciphers || []).map((row) => ({
      ...row,
      archived_at: row.archived_at ?? null
    })),
    attachments: cloneRows(payload.attachments || [])
  };
  await importBackupRows(db, preparedDb, true);
  return preparedDb;
}
__name(importPreparedBackupRows, "importPreparedBackupRows");
function prepareImportPayloadForTarget(env, payload, files) {
  const storageKind = getBlobStorageKind(env);
  if (storageKind === "r2") {
    return {
      payload,
      skipped: {
        reason: null,
        attachments: 0,
        items: []
      }
    };
  }
  if (storageKind === null) {
    const skippedItems2 = (payload.db.attachments || []).map((row) => {
      const cipherId = String(row.cipher_id || "").trim();
      const attachmentId = String(row.id || "").trim();
      return {
        kind: "attachment",
        path: `attachments/${cipherId}/${attachmentId}.bin`,
        sizeBytes: Number(row.size || 0) || 0
      };
    });
    const result2 = {
      payload: {
        ...payload,
        db: {
          ...payload.db,
          attachments: []
        }
      },
      skipped: {
        reason: skippedItems2.length ? BLOB_STORAGE_UNAVAILABLE_SKIP_REASON : null,
        attachments: skippedItems2.length,
        items: skippedItems2
      }
    };
    return result2;
  }
  const oversizedAttachmentPaths = /* @__PURE__ */ new Set();
  const skippedItems = [];
  for (const entry of Object.keys(files)) {
    if (!entry.endsWith(".bin")) continue;
    const sizeBytes = files[entry].byteLength;
    if (sizeBytes <= KV_MAX_OBJECT_BYTES) continue;
    if (entry.startsWith("attachments/")) {
      oversizedAttachmentPaths.add(entry);
      skippedItems.push({ kind: "attachment", path: entry, sizeBytes });
    }
  }
  const nextAttachments = (payload.db.attachments || []).filter((row) => {
    const cipherId = String(row.cipher_id || "").trim();
    const attachmentId = String(row.id || "").trim();
    if (!cipherId || !attachmentId) return false;
    return !oversizedAttachmentPaths.has(`attachments/${cipherId}/${attachmentId}.bin`);
  });
  const nextPayload = {
    ...payload,
    db: {
      ...payload.db,
      attachments: nextAttachments
    }
  };
  const needsKvBlobStorage = nextAttachments.length > 0;
  if (needsKvBlobStorage && !env.ATTACHMENTS_KV) {
    throw new Error("Backup restore requires ATTACHMENTS_KV when using KV blob storage");
  }
  const result = {
    payload: nextPayload,
    skipped: {
      reason: skippedItems.length ? KV_BLOB_SKIP_REASON : null,
      attachments: skippedItems.length,
      items: skippedItems
    }
  };
  return result;
}
__name(prepareImportPayloadForTarget, "prepareImportPayloadForTarget");
function buildInsertStatements(db, table, columns, rows, upsert = false) {
  if (!rows.length) return [];
  const placeholders = `(${columns.map(() => "?").join(", ")})`;
  const sql = `INSERT ${upsert ? "OR REPLACE " : ""}INTO ${table} (${columns.join(", ")}) VALUES ${placeholders}`;
  return rows.map((row) => db.prepare(sql).bind(...columns.map((column) => row[column] ?? null)));
}
__name(buildInsertStatements, "buildInsertStatements");
async function runInsertBatch(db, table, statements) {
  if (!statements.length) return;
  try {
    await db.batch(statements);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Restore insert failed for ${table}: ${message}`);
  }
}
__name(runInsertBatch, "runInsertBatch");
async function restoreBlobFiles(env, db, files) {
  const restoredAttachments = [];
  const skippedItems = [];
  for (const row of db.attachments || []) {
    const cipherId = String(row.cipher_id || "").trim();
    const attachmentId = String(row.id || "").trim();
    if (!cipherId || !attachmentId) continue;
    const key = `attachments/${cipherId}/${attachmentId}.bin`;
    const bytes = files[key];
    if (!bytes) {
      skippedItems.push({
        kind: "attachment",
        path: key,
        sizeBytes: Number(row.size || 0) || 0
      });
      continue;
    }
    try {
      await putBlobObject(env, getAttachmentObjectKey(cipherId, attachmentId), bytes, {
        size: bytes.byteLength,
        contentType: "application/octet-stream"
      });
      restoredAttachments.push(row);
    } catch {
      skippedItems.push({
        kind: "attachment",
        path: key,
        sizeBytes: bytes.byteLength
      });
    }
  }
  return {
    imported: restoredAttachments.length,
    restoredAttachments,
    skipped: {
      reason: skippedItems.length ? ATTACHMENT_RESTORE_FAILED_REASON : null,
      attachments: skippedItems.length,
      items: skippedItems
    }
  };
}
__name(restoreBlobFiles, "restoreBlobFiles");
function buildAttachmentBlobLookup(manifest) {
  return new Map(
    (manifest.attachmentBlobs || []).map((item) => [`${item.cipherId}/${item.attachmentId}`, item])
  );
}
__name(buildAttachmentBlobLookup, "buildAttachmentBlobLookup");
async function prepareRemoteAttachmentPayload(env, payload, files, source) {
  const manifestLookup = buildAttachmentBlobLookup(payload.manifest);
  const storageKind = getBlobStorageKind(env);
  const nextAttachments = [];
  const skippedItems = [];
  for (const row of payload.db.attachments || []) {
    const cipherId = String(row.cipher_id || "").trim();
    const attachmentId = String(row.id || "").trim();
    const lookupKey = `${cipherId}/${attachmentId}`;
    const ref = manifestLookup.get(lookupKey);
    const sizeBytes = ref?.sizeBytes || Number(row.size || 0) || 0;
    const path = ref ? `attachments/${ref.blobName}` : `attachments/${lookupKey}`;
    const inlinePath = `attachments/${cipherId}/${attachmentId}.bin`;
    if (files[inlinePath]) {
      nextAttachments.push(row);
      continue;
    }
    if (!ref) {
      skippedItems.push({ kind: "attachment", path, sizeBytes });
      continue;
    }
    if (storageKind === "kv" && sizeBytes > KV_MAX_OBJECT_BYTES) {
      skippedItems.push({ kind: "attachment", path, sizeBytes });
      continue;
    }
    if (storageKind === null) {
      skippedItems.push({ kind: "attachment", path, sizeBytes });
      continue;
    }
    nextAttachments.push(row);
  }
  const result = {
    payload: {
      ...payload,
      db: {
        ...payload.db,
        attachments: nextAttachments
      }
    },
    skipped: {
      reason: skippedItems.length ? "Some remote attachments were unavailable and were skipped" : null,
      attachments: skippedItems.length,
      items: skippedItems
    }
  };
  return result;
}
__name(prepareRemoteAttachmentPayload, "prepareRemoteAttachmentPayload");
async function removeAttachmentRows(db, attachmentRows, useShadowTable = false) {
  if (!attachmentRows.length) return;
  const tableName = useShadowTable ? shadowTableName("attachments") : "attachments";
  const statements = attachmentRows.map((row) => {
    const attachmentId = String(row.id || "").trim();
    const cipherId = String(row.cipher_id || "").trim();
    if (!attachmentId || !cipherId) return null;
    return db.prepare(`DELETE FROM ${tableName} WHERE id = ? AND cipher_id = ?`).bind(attachmentId, cipherId);
  }).filter((statement) => !!statement);
  if (!statements.length) return;
  await db.batch(statements);
}
__name(removeAttachmentRows, "removeAttachmentRows");
async function restoreRemoteAttachmentFiles(env, payload, files, source) {
  const manifestLookup = buildAttachmentBlobLookup(payload.manifest);
  const restoredAttachments = [];
  const skippedItems = [];
  for (const row of payload.db.attachments || []) {
    const cipherId = String(row.cipher_id || "").trim();
    const attachmentId = String(row.id || "").trim();
    const inlinePath = `attachments/${cipherId}/${attachmentId}.bin`;
    const ref = manifestLookup.get(`${cipherId}/${attachmentId}`);
    if (!ref && !files[inlinePath]) {
      skippedItems.push({
        kind: "attachment",
        path: `attachments/${cipherId}/${attachmentId}`,
        sizeBytes: Number(row.size || 0) || 0
      });
      continue;
    }
    const bytes = files[inlinePath] || (ref ? await source.loadAttachment(ref.blobName) : null);
    if (!bytes) {
      skippedItems.push({
        kind: "attachment",
        path: ref ? `attachments/${ref.blobName}` : inlinePath,
        sizeBytes: ref?.sizeBytes || Number(row.size || 0) || 0
      });
      continue;
    }
    try {
      await putBlobObject(env, getAttachmentObjectKey(cipherId, attachmentId), bytes, {
        size: bytes.byteLength,
        contentType: "application/octet-stream"
      });
      restoredAttachments.push(row);
    } catch {
      skippedItems.push({
        kind: "attachment",
        path: ref ? `attachments/${ref.blobName}` : inlinePath,
        sizeBytes: bytes.byteLength
      });
    }
  }
  return {
    imported: restoredAttachments.length,
    restoredAttachments,
    skipped: {
      reason: skippedItems.length ? ATTACHMENT_RESTORE_FAILED_REASON : null,
      attachments: skippedItems.length,
      items: skippedItems
    }
  };
}
__name(restoreRemoteAttachmentFiles, "restoreRemoteAttachmentFiles");
async function cleanupOrphanedBlobFiles(env, beforeKeys, afterKeys) {
  const staleKeys = Array.from(beforeKeys).filter((key) => !afterKeys.has(key));
  for (const key of staleKeys) {
    await deleteBlobObject(env, key);
  }
}
__name(cleanupOrphanedBlobFiles, "cleanupOrphanedBlobFiles");
async function importBackupRows(db, payload, useShadowTables = false) {
  const tableName = /* @__PURE__ */ __name((table) => useShadowTables ? shadowTableName(table) : table, "tableName");
  await runInsertBatch(
    db,
    tableName("config"),
    buildInsertStatements(db, tableName("config"), ["key", "value"], payload.config || [], true)
  );
  await runInsertBatch(
    db,
    tableName("users"),
    buildInsertStatements(
      db,
      tableName("users"),
      ["id", "email", "name", "master_password_hint", "master_password_hash", "key", "private_key", "public_key", "kdf_type", "kdf_iterations", "kdf_memory", "kdf_parallelism", "security_stamp", "role", "status", "verify_devices", "totp_secret", "totp_recovery_code", "created_at", "updated_at"],
      payload.users || []
    )
  );
  await runInsertBatch(
    db,
    tableName("user_revisions"),
    buildInsertStatements(db, tableName("user_revisions"), ["user_id", "revision_date"], payload.user_revisions || [], true)
  );
  await runInsertBatch(
    db,
    tableName("domain_settings"),
    buildInsertStatements(
      db,
      tableName("domain_settings"),
      ["user_id", "equivalent_domains", "custom_equivalent_domains", "excluded_global_equivalent_domains", "updated_at"],
      payload.domain_settings || [],
      true
    )
  );
  await runInsertBatch(
    db,
    tableName("folders"),
    buildInsertStatements(db, tableName("folders"), ["id", "user_id", "name", "created_at", "updated_at"], payload.folders || [])
  );
  await runInsertBatch(
    db,
    tableName("ciphers"),
    buildInsertStatements(
      db,
      tableName("ciphers"),
      ["id", "user_id", "type", "folder_id", "name", "notes", "favorite", "data", "reprompt", "key", "created_at", "updated_at", "archived_at", "deleted_at"],
      payload.ciphers || []
    )
  );
  await runInsertBatch(
    db,
    tableName("attachments"),
    buildInsertStatements(db, tableName("attachments"), ["id", "cipher_id", "file_name", "size", "size_name", "key"], payload.attachments || [])
  );
}
__name(importBackupRows, "importBackupRows");
async function importBackupArchiveBytes(archiveBytes, env, actorUserId, replaceExisting, progress, fileName = "nodewarden_backup.zip") {
  const parsed = parseBackupArchive(archiveBytes);
  validateBackupPayloadContents(parsed.payload, parsed.files);
  const prepared = prepareImportPayloadForTarget(env, parsed.payload, parsed.files);
  try {
    await ensureImportTargetIsFresh(env.DB);
  } catch (error) {
    if (!replaceExisting) {
      throw error instanceof Error ? error : new Error("Backup import requires a fresh instance");
    }
  }
  await resetRestoreArtifacts(env.DB);
  const previousBlobKeys = replaceExisting ? await collectCurrentBlobKeys(env.DB) : /* @__PURE__ */ new Set();
  try {
    await progress?.({
      source: "local",
      step: "local_create_shadow",
      fileName,
      stageTitle: "txt_backup_restore_progress_local_shadow_title",
      stageDetail: "txt_backup_restore_progress_local_shadow_detail",
      replaceExisting
    });
    await createShadowTables(env.DB);
    await progress?.({
      source: "local",
      step: "local_import_data",
      fileName,
      stageTitle: "txt_backup_restore_progress_local_data_title",
      stageDetail: "txt_backup_restore_progress_local_data_detail",
      replaceExisting
    });
    const db = await importPreparedBackupRows(env.DB, prepared.payload.db, env);
    await validateShadowTableCounts(env.DB, {
      config: (db.config || []).length,
      users: (db.users || []).length,
      domain_settings: (db.domain_settings || []).length,
      user_revisions: (db.user_revisions || []).length,
      folders: (db.folders || []).length,
      ciphers: (db.ciphers || []).length,
      attachments: (db.attachments || []).length
    });
    await progress?.({
      source: "local",
      step: "local_restore_files",
      fileName,
      stageTitle: "txt_backup_restore_progress_local_files_title",
      stageDetail: "txt_backup_restore_progress_local_files_detail",
      replaceExisting
    });
    const restored = await restoreBlobFiles(env, db, parsed.files);
    const restoredAttachmentKeys = new Set((restored.restoredAttachments || []).map(attachmentRowKey));
    const failedRestoreRows = (db.attachments || []).filter((row) => !restoredAttachmentKeys.has(attachmentRowKey(row)));
    await removeAttachmentRows(env.DB, failedRestoreRows, true).catch(() => void 0);
    await validateShadowTableCounts(env.DB, {
      config: (db.config || []).length,
      users: (db.users || []).length,
      domain_settings: (db.domain_settings || []).length,
      user_revisions: (db.user_revisions || []).length,
      folders: (db.folders || []).length,
      ciphers: (db.ciphers || []).length,
      attachments: restored.restoredAttachments.length
    });
    await progress?.({
      source: "local",
      step: "local_finalize",
      fileName,
      stageTitle: "txt_backup_restore_progress_local_finalize_title",
      stageDetail: "txt_backup_restore_progress_local_finalize_detail",
      replaceExisting
    });
    await swapShadowTablesIntoPlace(env.DB);
    await resetRestoreArtifacts(env.DB).catch(() => void 0);
    if (replaceExisting && previousBlobKeys.size) {
      const nextBlobKeys = await collectCurrentBlobKeys(env.DB).catch(() => null);
      if (nextBlobKeys) {
        await cleanupOrphanedBlobFiles(env, previousBlobKeys, nextBlobKeys).catch(() => void 0);
      }
    }
    await progress?.({
      source: "local",
      step: "local_complete",
      fileName,
      stageTitle: "txt_backup_restore_progress_local_finalize_title",
      stageDetail: "txt_backup_restore_progress_local_finalize_detail",
      replaceExisting,
      done: true,
      ok: true
    });
    return {
      auditActorUserId: (db.users || []).some((row) => String(row.id || "").trim() === actorUserId) ? actorUserId : null,
      result: {
        object: "instance-backup-import",
        imported: {
          config: (db.config || []).length,
          users: (db.users || []).length,
          domainSettings: (db.domain_settings || []).length,
          userRevisions: (db.user_revisions || []).length,
          folders: (db.folders || []).length,
          ciphers: (db.ciphers || []).length,
          attachments: restored.restoredAttachments.length,
          attachmentFiles: restored.imported
        },
        skipped: {
          reason: restored.skipped.reason || prepared.skipped.reason,
          attachments: prepared.skipped.attachments + restored.skipped.attachments,
          items: [...prepared.skipped.items, ...restored.skipped.items]
        }
      }
    };
  } catch (error) {
    await progress?.({
      source: "local",
      step: "local_failed",
      fileName,
      stageTitle: "txt_backup_restore_progress_local_finalize_title",
      stageDetail: "txt_backup_restore_progress_local_finalize_detail",
      replaceExisting,
      done: true,
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
    await resetRestoreArtifacts(env.DB).catch(() => void 0);
    throw error;
  }
}
__name(importBackupArchiveBytes, "importBackupArchiveBytes");
async function importRemoteBackupArchiveBytes(archiveBytes, env, actorUserId, replaceExisting, source, progress, fileName = "nodewarden_backup.zip") {
  const parsed = parseBackupArchive(archiveBytes, { allowExternalAttachmentBlobs: true });
  const preparedRemote = await prepareRemoteAttachmentPayload(env, parsed.payload, parsed.files, source);
  validateBackupPayloadContents(preparedRemote.payload, parsed.files, { allowExternalAttachmentBlobs: true });
  try {
    await ensureImportTargetIsFresh(env.DB);
  } catch (error) {
    if (!replaceExisting) {
      throw error instanceof Error ? error : new Error("Backup import requires a fresh instance");
    }
  }
  await resetRestoreArtifacts(env.DB);
  const previousBlobKeys = replaceExisting ? await collectCurrentBlobKeys(env.DB) : /* @__PURE__ */ new Set();
  try {
    await progress?.({
      source: "remote",
      step: "remote_create_shadow",
      fileName,
      stageTitle: "txt_backup_restore_progress_remote_shadow_title",
      stageDetail: "txt_backup_restore_progress_remote_shadow_detail",
      replaceExisting
    });
    await createShadowTables(env.DB);
    await progress?.({
      source: "remote",
      step: "remote_import_data",
      fileName,
      stageTitle: "txt_backup_restore_progress_remote_data_title",
      stageDetail: "txt_backup_restore_progress_remote_data_detail",
      replaceExisting
    });
    const db = await importPreparedBackupRows(env.DB, preparedRemote.payload.db, env);
    await validateShadowTableCounts(env.DB, {
      config: (db.config || []).length,
      users: (db.users || []).length,
      domain_settings: (db.domain_settings || []).length,
      user_revisions: (db.user_revisions || []).length,
      folders: (db.folders || []).length,
      ciphers: (db.ciphers || []).length,
      attachments: (db.attachments || []).length
    });
    await progress?.({
      source: "remote",
      step: "remote_restore_files",
      fileName,
      stageTitle: "txt_backup_restore_progress_remote_files_title",
      stageDetail: "txt_backup_restore_progress_remote_files_detail",
      replaceExisting
    });
    const restored = await restoreRemoteAttachmentFiles(env, preparedRemote.payload, parsed.files, source);
    const restoredAttachmentKeys = new Set((restored.restoredAttachments || []).map(attachmentRowKey));
    const failedRestoreRows = (db.attachments || []).filter((row) => !restoredAttachmentKeys.has(attachmentRowKey(row)));
    await removeAttachmentRows(env.DB, failedRestoreRows, true).catch(() => void 0);
    await validateShadowTableCounts(env.DB, {
      config: (db.config || []).length,
      users: (db.users || []).length,
      domain_settings: (db.domain_settings || []).length,
      user_revisions: (db.user_revisions || []).length,
      folders: (db.folders || []).length,
      ciphers: (db.ciphers || []).length,
      attachments: restored.restoredAttachments.length
    });
    await progress?.({
      source: "remote",
      step: "remote_finalize",
      fileName,
      stageTitle: "txt_backup_restore_progress_remote_finalize_title",
      stageDetail: "txt_backup_restore_progress_remote_finalize_detail",
      replaceExisting
    });
    await swapShadowTablesIntoPlace(env.DB);
    await resetRestoreArtifacts(env.DB).catch(() => void 0);
    if (replaceExisting && previousBlobKeys.size) {
      const nextBlobKeys = await collectCurrentBlobKeys(env.DB).catch(() => null);
      if (nextBlobKeys) {
        await cleanupOrphanedBlobFiles(env, previousBlobKeys, nextBlobKeys).catch(() => void 0);
      }
    }
    await progress?.({
      source: "remote",
      step: "remote_complete",
      fileName,
      stageTitle: "txt_backup_restore_progress_remote_finalize_title",
      stageDetail: "txt_backup_restore_progress_remote_finalize_detail",
      replaceExisting,
      done: true,
      ok: true
    });
    const finalSkippedItems = [...preparedRemote.skipped.items, ...restored.skipped.items];
    const finalSkippedReason = finalSkippedItems.length ? restored.skipped.reason || preparedRemote.skipped.reason : null;
    return {
      auditActorUserId: (db.users || []).some((row) => String(row.id || "").trim() === actorUserId) ? actorUserId : null,
      result: {
        object: "instance-backup-import",
        imported: {
          config: (db.config || []).length,
          users: (db.users || []).length,
          domainSettings: (db.domain_settings || []).length,
          userRevisions: (db.user_revisions || []).length,
          folders: (db.folders || []).length,
          ciphers: (db.ciphers || []).length,
          attachments: restored.restoredAttachments.length,
          attachmentFiles: restored.imported
        },
        skipped: {
          reason: finalSkippedReason,
          attachments: finalSkippedItems.length,
          items: finalSkippedItems
        }
      }
    };
  } catch (error) {
    await progress?.({
      source: "remote",
      step: "remote_failed",
      fileName,
      stageTitle: "txt_backup_restore_progress_remote_finalize_title",
      stageDetail: "txt_backup_restore_progress_remote_finalize_detail",
      replaceExisting,
      done: true,
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
    await resetRestoreArtifacts(env.DB).catch(() => void 0);
    throw error;
  }
}
__name(importRemoteBackupArchiveBytes, "importRemoteBackupArchiveBytes");

// src/services/backup-uploader.ts
function isBackupArchiveName(name) {
  return /\.zip$/i.test(String(name || "").trim());
}
__name(isBackupArchiveName, "isBackupArchiveName");
function encodePathSegments(path) {
  return path.split("/").filter(Boolean).map((segment) => encodeURIComponent(segment)).join("/");
}
__name(encodePathSegments, "encodePathSegments");
function trimSlashes(value) {
  let next = String(value || "");
  while (next.startsWith("/")) next = next.slice(1);
  while (next.endsWith("/")) next = next.slice(0, -1);
  return next;
}
__name(trimSlashes, "trimSlashes");
function buildJoinedPath(...segments) {
  return segments.map(trimSlashes).filter(Boolean).join("/");
}
__name(buildJoinedPath, "buildJoinedPath");
function normalizeRelativePath(path) {
  const normalized = trimSlashes(path).replace(/\\/g, "/");
  if (!normalized) return "";
  const parts = normalized.split("/").filter(Boolean);
  if (parts.some((part) => part === "." || part === "..")) {
    throw new Error("Invalid remote backup path");
  }
  return parts.join("/");
}
__name(normalizeRelativePath, "normalizeRelativePath");
function basename(path) {
  const normalized = trimSlashes(path);
  if (!normalized) return "";
  const parts = normalized.split("/").filter(Boolean);
  return parts[parts.length - 1] || "";
}
__name(basename, "basename");
function parentPath(path) {
  const normalized = normalizeRelativePath(path);
  if (!normalized) return null;
  const parts = normalized.split("/");
  parts.pop();
  return parts.length ? parts.join("/") : "";
}
__name(parentPath, "parentPath");
function sortRemoteItems(items) {
  return items.slice().sort((a, b) => {
    const aIsAttachmentsDir = a.isDirectory && a.name === "attachments";
    const bIsAttachmentsDir = b.isDirectory && b.name === "attachments";
    if (aIsAttachmentsDir !== bIsAttachmentsDir) return aIsAttachmentsDir ? -1 : 1;
    if (a.isDirectory !== b.isDirectory) return a.isDirectory ? -1 : 1;
    return a.name.localeCompare(b.name, "en");
  });
}
__name(sortRemoteItems, "sortRemoteItems");
function decodeXmlText(value) {
  return value.replace(/&(amp|lt|gt|quot|#39);/g, (_match, entity) => {
    switch (entity) {
      case "amp":
        return "&";
      case "lt":
        return "<";
      case "gt":
        return ">";
      case "quot":
        return '"';
      case "#39":
        return "'";
      default:
        return _match;
    }
  });
}
__name(decodeXmlText, "decodeXmlText");
function parseHttpDate(value) {
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : null;
}
__name(parseHttpDate, "parseHttpDate");
function extractXmlBlocks(xml, tagName) {
  const pattern = new RegExp(`<(?:[^:>]+:)?${tagName}\\b[^>]*>([\\s\\S]*?)</(?:[^:>]+:)?${tagName}>`, "gi");
  const blocks = [];
  let match;
  while (match = pattern.exec(xml)) {
    blocks.push(match[1]);
  }
  return blocks;
}
__name(extractXmlBlocks, "extractXmlBlocks");
function extractXmlFirst(xml, tagName) {
  const pattern = new RegExp(`<(?:[^:>]+:)?${tagName}\\b[^>]*>([\\s\\S]*?)</(?:[^:>]+:)?${tagName}>`, "i");
  const match = xml.match(pattern);
  return match?.[1] ? decodeXmlText(match[1].trim()) : null;
}
__name(extractXmlFirst, "extractXmlFirst");
async function sha256Hex2(value) {
  const bytes = typeof value === "string" ? new TextEncoder().encode(value) : value;
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
__name(sha256Hex2, "sha256Hex");
async function hmacSha256Raw(keyBytes, message) {
  const key = await crypto.subtle.importKey("raw", keyBytes, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(message));
  return new Uint8Array(signature);
}
__name(hmacSha256Raw, "hmacSha256Raw");
function toBasicAuthHeader(username, password) {
  const token = btoa(`${username}:${password}`);
  return `Basic ${token}`;
}
__name(toBasicAuthHeader, "toBasicAuthHeader");
function buildCanonicalQueryString(url) {
  const params = Array.from(url.searchParams.entries()).sort(([aKey, aValue], [bKey, bValue]) => {
    if (aKey === bKey) return aValue.localeCompare(bValue);
    return aKey.localeCompare(bKey);
  });
  return params.map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`).join("&");
}
__name(buildCanonicalQueryString, "buildCanonicalQueryString");
async function buildAwsV4Authorization(method, url, headers, payloadHashHex, accessKeyId, secretAccessKey, region) {
  const amzDate = headers["x-amz-date"];
  const shortDate = amzDate.slice(0, 8);
  const headerEntries = Object.entries(headers).map(([name, value]) => [name.toLowerCase(), value]).sort(([a], [b]) => a.localeCompare(b));
  const canonicalHeaders = headerEntries.map(([name, value]) => `${name}:${String(value).trim().replace(/\s+/g, " ")}`).join("\n");
  const signedHeaders = headerEntries.map(([name]) => name).join(";");
  const canonicalRequest = [
    method.toUpperCase(),
    url.pathname || "/",
    buildCanonicalQueryString(url),
    `${canonicalHeaders}
`,
    signedHeaders,
    payloadHashHex
  ].join("\n");
  const credentialScope = `${shortDate}/${region}/s3/aws4_request`;
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    credentialScope,
    await sha256Hex2(canonicalRequest)
  ].join("\n");
  const kDate = await hmacSha256Raw(new TextEncoder().encode(`AWS4${secretAccessKey}`), shortDate);
  const kRegion = await hmacSha256Raw(kDate, region);
  const kService = await hmacSha256Raw(kRegion, "s3");
  const kSigning = await hmacSha256Raw(kService, "aws4_request");
  const signatureBytes = await hmacSha256Raw(kSigning, stringToSign);
  const signature = Array.from(signatureBytes).map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
}
__name(buildAwsV4Authorization, "buildAwsV4Authorization");
function ensureDestinationConfigReady(destination) {
  if (destination.type === "webdav") {
    const config = destination.destination;
    if (!String(config.baseUrl || "").trim()) throw new Error("WebDAV server URL is required");
    if (!/^https?:\/\//i.test(String(config.baseUrl || "").trim())) throw new Error("WebDAV server URL must start with http:// or https://");
    if (!String(config.username || "").trim()) throw new Error("WebDAV username is required");
    if (!String(config.password || "")) throw new Error("WebDAV password is required");
    return;
  }
  if (destination.type === "s3") {
    const config = destination.destination;
    if (!String(config.endpoint || "").trim()) throw new Error("S3 endpoint is required");
    if (!/^https?:\/\//i.test(String(config.endpoint || "").trim())) throw new Error("S3 endpoint must start with http:// or https://");
    if (!String(config.bucket || "").trim()) throw new Error("S3 bucket is required");
    if (!String(config.accessKeyId || "").trim()) throw new Error("S3 access key is required");
    if (!String(config.secretAccessKey || "")) throw new Error("S3 secret key is required");
  }
}
__name(ensureDestinationConfigReady, "ensureDestinationConfigReady");
function buildWebDavUrl(baseUrl, relativePath) {
  const trimmedBase = baseUrl.replace(/\/+$/, "");
  const normalized = normalizeRelativePath(relativePath);
  return normalized ? `${trimmedBase}/${encodePathSegments(normalized)}` : trimmedBase;
}
__name(buildWebDavUrl, "buildWebDavUrl");
function webDavFullPath(config, relativePath) {
  return buildJoinedPath(config.remotePath, normalizeRelativePath(relativePath));
}
__name(webDavFullPath, "webDavFullPath");
async function ensureWebDavDirectory(baseUrl, directoryPath, authHeader) {
  const segments = trimSlashes(directoryPath).split("/").filter(Boolean);
  let current = "";
  for (const segment of segments) {
    current = buildJoinedPath(current, segment);
    const url = buildWebDavUrl(baseUrl, current);
    const response = await fetch(url, {
      method: "MKCOL",
      headers: {
        Authorization: authHeader
      }
    });
    if ([200, 201, 204, 301, 302, 405].includes(response.status)) continue;
    throw new Error(`WebDAV directory creation failed: ${response.status}`);
  }
}
__name(ensureWebDavDirectory, "ensureWebDavDirectory");
async function ensureWebDavDirectoryCached(baseUrl, directoryPath, authHeader, ensuredDirectories) {
  const segments = trimSlashes(directoryPath).split("/").filter(Boolean);
  let current = "";
  for (const segment of segments) {
    current = buildJoinedPath(current, segment);
    if (ensuredDirectories.has(current)) continue;
    const url = buildWebDavUrl(baseUrl, current);
    const response = await fetch(url, {
      method: "MKCOL",
      headers: {
        Authorization: authHeader
      }
    });
    if ([200, 201, 204, 301, 302, 405].includes(response.status)) {
      ensuredDirectories.add(current);
      continue;
    }
    throw new Error(`WebDAV directory creation failed: ${response.status}`);
  }
}
__name(ensureWebDavDirectoryCached, "ensureWebDavDirectoryCached");
async function putToWebDav(config, relativePath, bytes, options = {}, ensuredDirectories) {
  const authHeader = toBasicAuthHeader(config.username, config.password);
  const remoteFilePath = buildJoinedPath(config.remotePath, relativePath);
  const remoteDir = parentPath(remoteFilePath);
  if (remoteDir) {
    if (ensuredDirectories) {
      await ensureWebDavDirectoryCached(config.baseUrl, remoteDir, authHeader, ensuredDirectories);
    } else {
      await ensureWebDavDirectory(config.baseUrl, remoteDir, authHeader);
    }
  }
  const response = await fetch(buildWebDavUrl(config.baseUrl, remoteFilePath), {
    method: "PUT",
    headers: {
      Authorization: authHeader,
      "Content-Type": options.contentType || "application/octet-stream",
      "Content-Length": String(bytes.byteLength)
    },
    body: bytes
  });
  if (!response.ok) {
    throw new Error(`WebDAV upload failed: ${response.status}`);
  }
}
__name(putToWebDav, "putToWebDav");
async function uploadToWebDav(config, archive, fileName) {
  await putToWebDav(config, fileName, archive, { contentType: "application/zip" });
  return {
    provider: "webdav",
    remotePath: buildJoinedPath(config.remotePath, fileName)
  };
}
__name(uploadToWebDav, "uploadToWebDav");
function parseWebDavResponsePath(baseUrl, href) {
  const base = new URL(baseUrl);
  const target = new URL(href, base);
  const basePath = trimSlashes(decodeURIComponent(base.pathname));
  const entryPath = trimSlashes(decodeURIComponent(target.pathname));
  if (!basePath) return entryPath;
  if (entryPath === basePath) return "";
  return entryPath.startsWith(`${basePath}/`) ? entryPath.slice(basePath.length + 1) : entryPath;
}
__name(parseWebDavResponsePath, "parseWebDavResponsePath");
async function listWebDavEntries(config, relativePath) {
  const currentPath = normalizeRelativePath(relativePath);
  const targetFullPath = webDavFullPath(config, currentPath);
  const authHeader = toBasicAuthHeader(config.username, config.password);
  const response = await fetch(buildWebDavUrl(config.baseUrl, targetFullPath), {
    method: "PROPFIND",
    headers: {
      Authorization: authHeader,
      Depth: "1",
      "Content-Type": "application/xml; charset=utf-8"
    },
    body: `<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><prop><resourcetype/><getcontentlength/><getlastmodified/></prop></propfind>`
  });
  if (response.status === 404) {
    return {
      provider: "webdav",
      currentPath,
      parentPath: parentPath(currentPath),
      items: []
    };
  }
  if (!response.ok) {
    throw new Error(`WebDAV listing failed: ${response.status}`);
  }
  const xml = await response.text();
  const rootFullPath = trimSlashes(config.remotePath);
  const items = [];
  for (const block of extractXmlBlocks(xml, "response")) {
    const href = extractXmlFirst(block, "href");
    if (!href) continue;
    const fullPath = trimSlashes(parseWebDavResponsePath(config.baseUrl, href));
    if (!fullPath) continue;
    if (fullPath === targetFullPath) continue;
    if (rootFullPath && !(fullPath === rootFullPath || fullPath.startsWith(`${rootFullPath}/`))) continue;
    const relative = rootFullPath ? fullPath === rootFullPath ? "" : fullPath.slice(rootFullPath.length + 1) : fullPath;
    if (!relative) continue;
    const directParent = parentPath(relative);
    if ((directParent || "") !== currentPath) continue;
    const resourceTypeBlock = extractXmlFirst(block, "resourcetype") || "";
    const isDirectory = /<(?:[^:>]+:)?collection\b/i.test(resourceTypeBlock);
    const sizeRaw = extractXmlFirst(block, "getcontentlength");
    const modifiedAtRaw = extractXmlFirst(block, "getlastmodified");
    items.push({
      path: relative,
      name: basename(relative) || relative,
      isDirectory,
      size: !isDirectory && sizeRaw && Number.isFinite(Number(sizeRaw)) ? Number(sizeRaw) : null,
      modifiedAt: modifiedAtRaw ? parseHttpDate(modifiedAtRaw) : null
    });
  }
  return {
    provider: "webdav",
    currentPath,
    parentPath: parentPath(currentPath),
    items: sortRemoteItems(items)
  };
}
__name(listWebDavEntries, "listWebDavEntries");
async function downloadFromWebDav(config, relativePath) {
  const normalized = normalizeRelativePath(relativePath);
  if (!normalized || normalized.endsWith("/")) {
    throw new Error("Please select a backup file");
  }
  const authHeader = toBasicAuthHeader(config.username, config.password);
  const remotePath = webDavFullPath(config, normalized);
  const response = await fetch(buildWebDavUrl(config.baseUrl, remotePath), {
    method: "GET",
    headers: {
      Authorization: authHeader
    }
  });
  if (!response.ok) {
    throw new Error(`WebDAV download failed: ${response.status}`);
  }
  return {
    provider: "webdav",
    remotePath: normalized,
    fileName: basename(normalized) || "backup.zip",
    contentType: String(response.headers.get("Content-Type") || "application/zip").trim() || "application/zip",
    bytes: new Uint8Array(await response.arrayBuffer())
  };
}
__name(downloadFromWebDav, "downloadFromWebDav");
async function deleteFromWebDav(config, relativePath) {
  const authHeader = toBasicAuthHeader(config.username, config.password);
  const remotePath = webDavFullPath(config, relativePath);
  const response = await fetch(buildWebDavUrl(config.baseUrl, remotePath), {
    method: "DELETE",
    headers: {
      Authorization: authHeader
    }
  });
  if (!response.ok && response.status !== 404) {
    throw new Error(`WebDAV delete failed: ${response.status}`);
  }
}
__name(deleteFromWebDav, "deleteFromWebDav");
async function existsInWebDav(config, relativePath) {
  const authHeader = toBasicAuthHeader(config.username, config.password);
  const remotePath = webDavFullPath(config, relativePath);
  const response = await fetch(buildWebDavUrl(config.baseUrl, remotePath), {
    method: "HEAD",
    headers: {
      Authorization: authHeader
    }
  });
  if (response.status === 404) return false;
  if (!response.ok) {
    throw new Error(`WebDAV existence check failed: ${response.status}`);
  }
  return true;
}
__name(existsInWebDav, "existsInWebDav");
function s3BucketBaseUrl(config) {
  return new URL(`${config.endpoint.replace(/\/+$/, "")}/${encodeURIComponent(config.bucket)}`);
}
__name(s3BucketBaseUrl, "s3BucketBaseUrl");
function normalizeS3ObjectKey(config, relativePath) {
  return buildJoinedPath(config.rootPath, normalizeRelativePath(relativePath));
}
__name(normalizeS3ObjectKey, "normalizeS3ObjectKey");
async function signedS3Request(config, method, url, body, contentType) {
  const payloadHashHex = await sha256Hex2(body || new Uint8Array());
  const amzDate = (/* @__PURE__ */ new Date()).toISOString().replace(/[:-]|\.\d{3}/g, "");
  const headers = {
    host: url.host,
    "x-amz-content-sha256": payloadHashHex,
    "x-amz-date": amzDate
  };
  if (method === "PUT") headers["content-type"] = contentType || "application/octet-stream";
  const authorization = await buildAwsV4Authorization(
    method,
    url,
    headers,
    payloadHashHex,
    config.accessKeyId,
    config.secretAccessKey,
    config.region || "auto"
  );
  return fetch(url.toString(), {
    method,
    headers: {
      Authorization: authorization,
      "X-Amz-Content-Sha256": headers["x-amz-content-sha256"],
      "X-Amz-Date": headers["x-amz-date"],
      ...method === "PUT" ? { "Content-Type": headers["content-type"] } : {}
    },
    body
  });
}
__name(signedS3Request, "signedS3Request");
async function putToS3(config, relativePath, bytes, options = {}) {
  const objectKey = normalizeS3ObjectKey(config, relativePath);
  const url = new URL(`${s3BucketBaseUrl(config).toString()}/${encodePathSegments(objectKey)}`);
  const response = await signedS3Request(config, "PUT", url, bytes, options.contentType);
  if (!response.ok) {
    throw new Error(`S3 upload failed: ${response.status}`);
  }
}
__name(putToS3, "putToS3");
async function uploadToS3(config, archive, fileName) {
  await putToS3(config, fileName, archive, { contentType: "application/zip" });
  return {
    provider: "s3",
    remotePath: normalizeS3ObjectKey(config, fileName)
  };
}
__name(uploadToS3, "uploadToS3");
async function listS3Entries(config, relativePath) {
  const currentPath = normalizeRelativePath(relativePath);
  const targetPrefixBase = normalizeS3ObjectKey(config, currentPath);
  const targetPrefix = trimSlashes(targetPrefixBase) ? `${trimSlashes(targetPrefixBase)}/` : "";
  const url = s3BucketBaseUrl(config);
  url.searchParams.set("list-type", "2");
  url.searchParams.set("delimiter", "/");
  if (targetPrefix) url.searchParams.set("prefix", targetPrefix);
  const response = await signedS3Request(config, "GET", url);
  if (!response.ok) {
    throw new Error(`S3 listing failed: ${response.status}`);
  }
  const xml = await response.text();
  const rootPrefix = trimSlashes(config.rootPath);
  const items = [];
  for (const prefix of extractXmlBlocks(xml, "CommonPrefixes")) {
    const fullPrefix = trimSlashes(extractXmlFirst(prefix, "Prefix") || "");
    if (!fullPrefix) continue;
    const relative = rootPrefix ? fullPrefix === rootPrefix ? "" : fullPrefix.startsWith(`${rootPrefix}/`) ? fullPrefix.slice(rootPrefix.length + 1) : "" : fullPrefix;
    const normalizedRelative = trimSlashes(relative);
    if (!normalizedRelative) continue;
    const itemPath = normalizedRelative.replace(/\/+$/, "");
    if ((parentPath(itemPath) || "") !== currentPath) continue;
    items.push({
      path: itemPath,
      name: basename(itemPath) || itemPath,
      isDirectory: true,
      size: null,
      modifiedAt: null
    });
  }
  for (const content of extractXmlBlocks(xml, "Contents")) {
    const fullKey = trimSlashes(extractXmlFirst(content, "Key") || "");
    if (!fullKey || targetPrefix && fullKey === trimSlashes(targetPrefix)) continue;
    const relative = rootPrefix ? fullKey.startsWith(`${rootPrefix}/`) ? fullKey.slice(rootPrefix.length + 1) : "" : fullKey;
    const normalizedRelative = trimSlashes(relative);
    if (!normalizedRelative || (parentPath(normalizedRelative) || "") !== currentPath) continue;
    items.push({
      path: normalizedRelative,
      name: basename(normalizedRelative) || normalizedRelative,
      isDirectory: false,
      size: Number(extractXmlFirst(content, "Size") || 0) || null,
      modifiedAt: parseHttpDate(extractXmlFirst(content, "LastModified") || "") || null
    });
  }
  const deduped = /* @__PURE__ */ new Map();
  for (const item of items) deduped.set(`${item.isDirectory ? "d" : "f"}:${item.path}`, item);
  return {
    provider: "s3",
    currentPath,
    parentPath: parentPath(currentPath),
    items: sortRemoteItems(Array.from(deduped.values()))
  };
}
__name(listS3Entries, "listS3Entries");
async function downloadFromS3(config, relativePath) {
  const normalized = normalizeRelativePath(relativePath);
  if (!normalized || normalized.endsWith("/")) {
    throw new Error("Please select a backup file");
  }
  const objectKey = normalizeS3ObjectKey(config, normalized);
  const url = new URL(`${s3BucketBaseUrl(config).toString()}/${encodePathSegments(objectKey)}`);
  const response = await signedS3Request(config, "GET", url);
  if (!response.ok) {
    throw new Error(`S3 download failed: ${response.status}`);
  }
  return {
    provider: "s3",
    remotePath: normalized,
    fileName: basename(normalized) || "backup.zip",
    contentType: String(response.headers.get("Content-Type") || "application/zip").trim() || "application/zip",
    bytes: new Uint8Array(await response.arrayBuffer())
  };
}
__name(downloadFromS3, "downloadFromS3");
async function deleteFromS3(config, relativePath) {
  const objectKey = normalizeS3ObjectKey(config, relativePath);
  const url = new URL(`${s3BucketBaseUrl(config).toString()}/${encodePathSegments(objectKey)}`);
  const response = await signedS3Request(config, "DELETE", url);
  if (!response.ok && response.status !== 404) {
    throw new Error(`S3 delete failed: ${response.status}`);
  }
}
__name(deleteFromS3, "deleteFromS3");
async function existsInS3(config, relativePath) {
  const objectKey = normalizeS3ObjectKey(config, relativePath);
  const url = new URL(`${s3BucketBaseUrl(config).toString()}/${encodePathSegments(objectKey)}`);
  const response = await signedS3Request(config, "HEAD", url);
  if (response.status === 404) return false;
  if (!response.ok) {
    throw new Error(`S3 existence check failed: ${response.status}`);
  }
  return true;
}
__name(existsInS3, "existsInS3");
function resolveConfiguredDestinationAdapter(destination) {
  ensureDestinationConfigReady(destination);
  if (destination.type === "webdav") {
    return {
      provider: "webdav",
      config: destination.destination,
      upload: /* @__PURE__ */ __name((config, archive, fileName) => uploadToWebDav(config, archive, fileName), "upload"),
      putFile: /* @__PURE__ */ __name((config, relativePath, bytes, options) => putToWebDav(config, relativePath, bytes, options), "putFile"),
      list: /* @__PURE__ */ __name((config, relativePath) => listWebDavEntries(config, relativePath), "list"),
      download: /* @__PURE__ */ __name((config, relativePath) => downloadFromWebDav(config, relativePath), "download"),
      deleteFile: /* @__PURE__ */ __name((config, relativePath) => deleteFromWebDav(config, relativePath), "deleteFile"),
      exists: /* @__PURE__ */ __name((config, relativePath) => existsInWebDav(config, relativePath), "exists")
    };
  }
  if (destination.type === "s3") {
    return {
      provider: "s3",
      config: destination.destination,
      upload: /* @__PURE__ */ __name((config, archive, fileName) => uploadToS3(config, archive, fileName), "upload"),
      putFile: /* @__PURE__ */ __name((config, relativePath, bytes, options) => putToS3(config, relativePath, bytes, options), "putFile"),
      list: /* @__PURE__ */ __name((config, relativePath) => listS3Entries(config, relativePath), "list"),
      download: /* @__PURE__ */ __name((config, relativePath) => downloadFromS3(config, relativePath), "download"),
      deleteFile: /* @__PURE__ */ __name((config, relativePath) => deleteFromS3(config, relativePath), "deleteFile"),
      exists: /* @__PURE__ */ __name((config, relativePath) => existsInS3(config, relativePath), "exists")
    };
  }
  throw new Error("Unsupported backup destination type");
}
__name(resolveConfiguredDestinationAdapter, "resolveConfiguredDestinationAdapter");
function createRemoteBackupTransferSession(destination) {
  const adapter = resolveConfiguredDestinationAdapter(destination);
  const ensuredDirectories = adapter.provider === "webdav" ? /* @__PURE__ */ new Set() : null;
  const putFile = /* @__PURE__ */ __name(async (relativePath, bytes, options = {}) => {
    const normalized = normalizeRelativePath(relativePath);
    if (adapter.provider === "webdav" && ensuredDirectories) {
      await putToWebDav(adapter.config, normalized, bytes, options, ensuredDirectories);
      return;
    }
    await adapter.putFile(adapter.config, normalized, bytes, options);
  }, "putFile");
  return {
    provider: adapter.provider,
    uploadArchive: /* @__PURE__ */ __name(async (archive, fileName) => {
      await putFile(fileName, archive, { contentType: "application/zip" });
      return {
        provider: adapter.provider,
        remotePath: adapter.provider === "webdav" ? buildJoinedPath(adapter.config.remotePath, fileName) : normalizeS3ObjectKey(adapter.config, fileName)
      };
    }, "uploadArchive"),
    putFile,
    list: /* @__PURE__ */ __name(async (relativePath) => adapter.list(adapter.config, relativePath), "list"),
    download: /* @__PURE__ */ __name(async (relativePath) => adapter.download(adapter.config, relativePath), "download"),
    deleteFile: /* @__PURE__ */ __name(async (relativePath) => adapter.deleteFile(adapter.config, normalizeRelativePath(relativePath)), "deleteFile"),
    exists: /* @__PURE__ */ __name(async (relativePath) => adapter.exists(adapter.config, normalizeRelativePath(relativePath)), "exists")
  };
}
__name(createRemoteBackupTransferSession, "createRemoteBackupTransferSession");
async function listRemoteBackupEntries(destination, relativePath) {
  return createRemoteBackupTransferSession(destination).list(relativePath);
}
__name(listRemoteBackupEntries, "listRemoteBackupEntries");
async function downloadRemoteBackupFile(destination, relativePath) {
  return createRemoteBackupTransferSession(destination).download(relativePath);
}
__name(downloadRemoteBackupFile, "downloadRemoteBackupFile");
async function deleteRemoteBackupFile(destination, relativePath) {
  const normalized = ensureRemoteRestoreCandidate(relativePath);
  await createRemoteBackupTransferSession(destination).deleteFile(normalized);
}
__name(deleteRemoteBackupFile, "deleteRemoteBackupFile");
function compareBackupItemsByRecency(a, b, preferredFileName) {
  if (preferredFileName) {
    const aPreferred = a.name === preferredFileName ? 1 : 0;
    const bPreferred = b.name === preferredFileName ? 1 : 0;
    if (aPreferred !== bPreferred) return bPreferred - aPreferred;
  }
  const aTime = a.modifiedAt ? new Date(a.modifiedAt).getTime() : 0;
  const bTime = b.modifiedAt ? new Date(b.modifiedAt).getTime() : 0;
  if (aTime !== bTime) return bTime - aTime;
  return b.name.localeCompare(a.name, "en");
}
__name(compareBackupItemsByRecency, "compareBackupItemsByRecency");
async function pruneRemoteBackupArchives(destination, retentionCount, preferredFileName) {
  if (retentionCount === null) return 0;
  const adapter = resolveConfiguredDestinationAdapter(destination);
  const listing = await adapter.list(adapter.config, "");
  const backupFiles = listing.items.filter((item) => !item.isDirectory && isBackupArchiveName(item.name)).sort((a, b) => compareBackupItemsByRecency(a, b, preferredFileName));
  if (backupFiles.length <= retentionCount) return 0;
  for (const item of backupFiles.slice(retentionCount)) {
    await adapter.deleteFile(adapter.config, item.path);
  }
  return backupFiles.length - retentionCount;
}
__name(pruneRemoteBackupArchives, "pruneRemoteBackupArchives");
function ensureRemoteRestoreCandidate(relativePath) {
  const normalized = normalizeRelativePath(relativePath);
  if (!normalized || !/\.zip$/i.test(normalized)) {
    throw new Error("Please select a backup ZIP file");
  }
  return normalized;
}
__name(ensureRemoteRestoreCandidate, "ensureRemoteRestoreCandidate");

// src/handlers/backup.ts
function isAdmin2(user) {
  return user.role === "admin" && user.status === "active";
}
__name(isAdmin2, "isAdmin");
async function writeAuditLog2(storage, actorUserId, action, targetType, targetId, metadata, request) {
  await writeAuditEvent(storage, {
    actorUserId,
    action,
    targetType,
    targetId,
    category: "data",
    level: action.endsWith(".failed") ? "error" : "info",
    metadata: {
      ...metadata || {},
      ...request ? auditRequestMetadata(request) : {}
    }
  });
}
__name(writeAuditLog2, "writeAuditLog");
function getBackupDestinationSummary(destination) {
  if (!destination) {
    return {
      destinationId: null,
      destinationName: null,
      destinationType: null
    };
  }
  return {
    destinationId: destination.id,
    destinationName: destination.name,
    destinationType: destination.type
  };
}
__name(getBackupDestinationSummary, "getBackupDestinationSummary");
var BACKUP_RUNNER_LOCK_KEY = "backup.runner.lock.v1";
var BACKUP_RUNNER_LEASE_MS = 10 * 60 * 1e3;
var BACKUP_RUNNER_HEARTBEAT_MS = 30 * 1e3;
async function acquireBackupRunnerLease(env, reason) {
  const token = generateUUID();
  const nowMs = Date.now();
  const expiresAtMs = nowMs + BACKUP_RUNNER_LEASE_MS;
  const value = JSON.stringify({
    token,
    reason,
    acquiredAt: new Date(nowMs).toISOString(),
    touchedAt: new Date(nowMs).toISOString(),
    expiresAtMs
  });
  const result = await env.DB.prepare(
    `INSERT INTO config(key, value) VALUES(?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value
       WHERE COALESCE(CAST(json_extract(config.value, '$.expiresAtMs') AS INTEGER), 0) <= ?`
  ).bind(BACKUP_RUNNER_LOCK_KEY, value, nowMs).run();
  if ((result.meta?.changes || 0) < 1) {
    return null;
  }
  return {
    token,
    touch: /* @__PURE__ */ __name(async () => {
      const nextNowMs = Date.now();
      const nextValue = JSON.stringify({
        token,
        reason,
        acquiredAt: new Date(nowMs).toISOString(),
        touchedAt: new Date(nextNowMs).toISOString(),
        expiresAtMs: nextNowMs + BACKUP_RUNNER_LEASE_MS
      });
      await env.DB.prepare(
        `UPDATE config
           SET value = ?
           WHERE key = ?
             AND json_extract(value, '$.token') = ?`
      ).bind(nextValue, BACKUP_RUNNER_LOCK_KEY, token).run();
    }, "touch"),
    release: /* @__PURE__ */ __name(async () => {
      await env.DB.prepare(
        `DELETE FROM config
           WHERE key = ?
             AND json_extract(value, '$.token') = ?`
      ).bind(BACKUP_RUNNER_LOCK_KEY, token).run();
    }, "release")
  };
}
__name(acquireBackupRunnerLease, "acquireBackupRunnerLease");
async function withBackupRunnerLease(env, reason, task) {
  const lease = await acquireBackupRunnerLease(env, reason);
  if (!lease) return null;
  let lastHeartbeatAt = 0;
  const keepAlive = /* @__PURE__ */ __name(async () => {
    const nowMs = Date.now();
    if (nowMs - lastHeartbeatAt < BACKUP_RUNNER_HEARTBEAT_MS) return;
    lastHeartbeatAt = nowMs;
    await lease.touch();
  }, "keepAlive");
  try {
    await keepAlive();
    return await task(keepAlive);
  } finally {
    await lease.release();
  }
}
__name(withBackupRunnerLease, "withBackupRunnerLease");
function ensureBackupBlobName(value) {
  const normalized = String(value || "").trim().replace(/\\/g, "/").replace(/^\/+|\/+$/g, "");
  if (!normalized) {
    throw new Error("Backup attachment blob is required");
  }
  const parts = normalized.split("/").filter(Boolean);
  if (!parts.length || parts.some((part) => part === "." || part === "..")) {
    throw new Error("Backup attachment blob is invalid");
  }
  return parts.join("/");
}
__name(ensureBackupBlobName, "ensureBackupBlobName");
var REMOTE_ATTACHMENT_INDEX_PATH = "attachments/.nodewarden-attachment-index.v1.json";
async function loadRemoteAttachmentIndex(session) {
  try {
    const file = await session.download(REMOTE_ATTACHMENT_INDEX_PATH);
    const payload = JSON.parse(new TextDecoder().decode(file.bytes));
    if (payload?.version !== 1 || !payload.blobs || typeof payload.blobs !== "object") {
      return /* @__PURE__ */ new Map();
    }
    return new Map(
      Object.entries(payload.blobs).filter(([key, value]) => !!String(key || "").trim() && Number.isFinite(Number(value?.sizeBytes || 0))).map(([key, value]) => [key, Number(value.sizeBytes || 0)])
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const normalized = message.toLowerCase();
    if (normalized.includes("404") || normalized.includes("403") || normalized.includes("530") || normalized.includes("not found") || normalized.includes("file not found") || normalized.includes("does not exist") || normalized.includes("please select a backup file")) {
      return /* @__PURE__ */ new Map();
    }
    throw error;
  }
}
__name(loadRemoteAttachmentIndex, "loadRemoteAttachmentIndex");
async function saveRemoteAttachmentIndex(session, index) {
  const payload = {
    version: 1,
    blobs: Object.fromEntries(
      Array.from(index.entries()).map(([blobName, sizeBytes]) => [
        blobName,
        {
          sizeBytes,
          updatedAt: (/* @__PURE__ */ new Date()).toISOString()
        }
      ])
    )
  };
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  await session.putFile(REMOTE_ATTACHMENT_INDEX_PATH, bytes, {
    contentType: "application/json; charset=utf-8"
  });
}
__name(saveRemoteAttachmentIndex, "saveRemoteAttachmentIndex");
async function executeConfiguredBackup(env, storage, actorUserId, trigger, destinationId, keepAlive, progress, auditMetadata) {
  const maxArchiveUploadAttempts = 3;
  const touchLease = /* @__PURE__ */ __name(async () => {
    await keepAlive?.();
  }, "touchLease");
  const currentSettings = await loadBackupSettings(storage, env, "UTC");
  const destination = requireBackupDestination(currentSettings, destinationId);
  const now = /* @__PURE__ */ new Date();
  destination.runtime.lastAttemptAt = now.toISOString();
  destination.runtime.lastAttemptLocalDate = getBackupLocalDateKey(now, destination.schedule.timezone);
  destination.runtime.lastErrorAt = null;
  destination.runtime.lastErrorMessage = null;
  await touchLease();
  await saveBackupSettings(storage, env, currentSettings);
  try {
    await touchLease();
    await progress?.({
      operation: "backup-remote-run",
      step: "remote_run_prepare",
      fileName: "",
      stageTitle: "txt_backup_remote_run_progress_prepare_title",
      stageDetail: "txt_backup_remote_run_progress_prepare_detail"
    });
    await touchLease();
    const archive = await buildBackupArchive(env, now, {
      includeAttachments: destination.includeAttachments,
      timeZone: destination.schedule.timezone,
      progress: progress ? async (event) => {
        if (event.step === "archive_ready") {
          return;
        }
        await progress({
          operation: "backup-remote-run",
          step: `remote_run_${event.step}`,
          fileName: event.fileName || "",
          stageTitle: event.stageTitle,
          stageDetail: event.stageDetail
        });
      } : void 0
    });
    await progress?.({
      operation: "backup-remote-run",
      step: "remote_run_sync_attachments",
      fileName: archive.fileName,
      stageTitle: "txt_backup_remote_run_progress_sync_attachments_title",
      stageDetail: destination.includeAttachments ? "txt_backup_remote_run_progress_sync_attachments_detail" : "txt_backup_remote_run_progress_sync_attachments_skipped_detail"
    });
    const remoteSession = createRemoteBackupTransferSession(destination);
    if (destination.includeAttachments) {
      await touchLease();
      const remoteAttachmentIndex = await loadRemoteAttachmentIndex(remoteSession);
      let attachmentIndexChanged = false;
      for (const attachment of archive.manifest.attachmentBlobs || []) {
        await touchLease();
        if (remoteAttachmentIndex.get(attachment.blobName) === attachment.sizeBytes) {
          continue;
        }
        const remotePath = `attachments/${attachment.blobName}`;
        const object = await getBlobObject(env, attachment.blobName);
        if (!object) {
          throw new Error(`Attachment blob missing for ${attachment.blobName}`);
        }
        const bytes = new Uint8Array(await new Response(object.body).arrayBuffer());
        await remoteSession.putFile(remotePath, bytes, {
          contentType: object.contentType
        });
        remoteAttachmentIndex.set(attachment.blobName, attachment.sizeBytes);
        attachmentIndexChanged = true;
      }
      if (attachmentIndexChanged) {
        await touchLease();
        await saveRemoteAttachmentIndex(remoteSession, remoteAttachmentIndex);
      }
    }
    let upload = null;
    for (let attempt = 1; attempt <= maxArchiveUploadAttempts; attempt++) {
      await touchLease();
      await progress?.({
        operation: "backup-remote-run",
        step: "remote_run_upload_archive",
        fileName: archive.fileName,
        stageTitle: "txt_backup_remote_run_progress_upload_title",
        stageDetail: "txt_backup_remote_run_progress_upload_detail"
      });
      upload = await remoteSession.uploadArchive(archive.bytes, archive.fileName);
      try {
        await touchLease();
        await progress?.({
          operation: "backup-remote-run",
          step: "remote_run_verify_archive",
          fileName: archive.fileName,
          stageTitle: "txt_backup_remote_run_progress_verify_title",
          stageDetail: "txt_backup_remote_run_progress_verify_detail"
        });
        const remoteFile = await remoteSession.download(archive.fileName);
        const checksumOk = await verifyBackupArchiveFileNameChecksum(remoteFile.bytes, archive.fileName);
        if (!checksumOk) {
          throw new Error("Remote backup ZIP checksum verification failed");
        }
        if (remoteFile.bytes.byteLength !== archive.bytes.byteLength) {
          throw new Error("Remote backup ZIP size verification failed");
        }
        break;
      } catch (error) {
        await remoteSession.deleteFile(archive.fileName).catch(() => void 0);
        if (attempt === maxArchiveUploadAttempts) {
          const message = error instanceof Error ? error.message : "Remote backup ZIP verification failed";
          throw new Error(`Backup archive upload verification failed after ${maxArchiveUploadAttempts} attempts: ${message}`);
        }
      }
    }
    if (!upload) {
      throw new Error("Backup archive upload failed");
    }
    let prunedFileCount = 0;
    let pruneErrorMessage = null;
    try {
      await touchLease();
      await progress?.({
        operation: "backup-remote-run",
        step: "remote_run_cleanup",
        fileName: archive.fileName,
        stageTitle: "txt_backup_remote_run_progress_cleanup_title",
        stageDetail: "txt_backup_remote_run_progress_cleanup_detail"
      });
      prunedFileCount = await pruneRemoteBackupArchives(destination, destination.schedule.retentionCount, archive.fileName);
    } catch (error) {
      pruneErrorMessage = error instanceof Error ? error.message : "Old backup cleanup failed";
    }
    destination.runtime.lastSuccessAt = (/* @__PURE__ */ new Date()).toISOString();
    destination.runtime.lastErrorAt = null;
    destination.runtime.lastErrorMessage = null;
    destination.runtime.lastUploadedFileName = archive.fileName;
    destination.runtime.lastUploadedSizeBytes = archive.bytes.byteLength;
    destination.runtime.lastUploadedDestination = upload.remotePath;
    await touchLease();
    await saveBackupSettings(storage, env, currentSettings);
    await touchLease();
    await writeAuditLog2(storage, actorUserId, `admin.backup.remote.${trigger}`, "backup", null, {
      ...getBackupDestinationSummary(destination),
      provider: upload.provider,
      remotePath: upload.remotePath,
      fileName: archive.fileName,
      fileBytes: archive.bytes.byteLength,
      uploadVerificationAttempts: maxArchiveUploadAttempts,
      prunedFileCount,
      pruneError: pruneErrorMessage,
      ...auditMetadata || {}
    });
    await progress?.({
      operation: "backup-remote-run",
      step: "remote_run_complete",
      fileName: archive.fileName,
      stageTitle: "txt_backup_remote_run_progress_complete_title",
      stageDetail: "txt_backup_remote_run_progress_complete_detail",
      done: true,
      ok: true
    });
    return {
      fileName: archive.fileName,
      fileSize: archive.bytes.byteLength,
      remotePath: upload.remotePath,
      provider: upload.provider
    };
  } catch (error) {
    destination.runtime.lastErrorAt = (/* @__PURE__ */ new Date()).toISOString();
    destination.runtime.lastErrorMessage = error instanceof Error ? error.message : "Backup upload failed";
    await touchLease();
    await saveBackupSettings(storage, env, currentSettings);
    await touchLease();
    await writeAuditLog2(storage, actorUserId, `admin.backup.remote.${trigger}.failed`, "backup", null, {
      ...getBackupDestinationSummary(destination),
      error: destination.runtime.lastErrorMessage,
      ...auditMetadata || {}
    });
    await progress?.({
      operation: "backup-remote-run",
      step: "remote_run_failed",
      fileName: "",
      stageTitle: "txt_backup_remote_run_progress_failed_title",
      stageDetail: "txt_backup_remote_run_progress_failed_detail",
      done: true,
      ok: false,
      error: destination.runtime.lastErrorMessage
    });
    throw error;
  }
}
__name(executeConfiguredBackup, "executeConfiguredBackup");
function toImportStatusCode(message) {
  const lower = message.toLowerCase();
  if (lower.includes("invalid backup") || lower.includes("invalid json")) return 400;
  if (lower.includes("fresh instance")) return 409;
  if (lower.includes("not configured") || lower.includes("kv")) return 409;
  return 500;
}
__name(toImportStatusCode, "toImportStatusCode");
async function runImportAndAudit(env, request, actorUser, archiveBytes, fileName, replaceExisting, metadata) {
  const storage = new StorageService(env.DB);
  const targetDeviceIdentifier = String(request.headers.get("X-NodeWarden-Acting-Device-Id") || "").trim() || null;
  const progress = /* @__PURE__ */ __name(async (event) => {
    await notifyUserBackupRestoreProgress(
      env,
      actorUser.id,
      {
        operation: "backup-restore",
        ...event
      },
      targetDeviceIdentifier
    );
  }, "progress");
  await progress({
    source: "local",
    step: "local_upload_received",
    fileName,
    stageTitle: "txt_backup_restore_progress_local_upload_title",
    stageDetail: "txt_backup_restore_progress_local_upload_detail",
    replaceExisting
  });
  const imported = await importBackupArchiveBytes(archiveBytes, env, actorUser.id, replaceExisting, progress, fileName);
  await writeAuditLog2(storage, imported.auditActorUserId, "admin.backup.import", "backup", null, {
    users: imported.result.imported.users,
    ciphers: imported.result.imported.ciphers,
    attachments: imported.result.imported.attachmentFiles,
    skippedAttachments: imported.result.skipped.attachments,
    skippedReason: imported.result.skipped.reason,
    replaceExisting,
    ...metadata
  }, request);
  return imported;
}
__name(runImportAndAudit, "runImportAndAudit");
async function runScheduledBackupIfDue(env) {
  await withBackupRunnerLease(env, "scheduled", async (keepAlive) => {
    const storage = new StorageService(env.DB);
    let scanStartMs = Date.now();
    while (true) {
      await keepAlive();
      const settings = await loadBackupSettings(storage, env, "UTC");
      const now = /* @__PURE__ */ new Date();
      const dueDestinations = settings.destinations.filter(
        (destination) => isBackupDueNow(destination, now, BACKUP_SCHEDULER_WINDOW_MINUTES) || hasBackupSlotBetween(destination, new Date(scanStartMs), now)
      );
      if (!dueDestinations.length) {
        return;
      }
      scanStartMs = now.getTime();
      for (const destination of dueDestinations) {
        await keepAlive();
        await executeConfiguredBackup(env, storage, null, "scheduled", destination.id, keepAlive);
      }
    }
  });
}
__name(runScheduledBackupIfDue, "runScheduledBackupIfDue");
async function handleGetAdminBackupSettings(request, env, actorUser) {
  void request;
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  const storage = new StorageService(env.DB);
  try {
    const settings = await loadBackupSettings(storage, env, "UTC");
    return jsonResponse(settings);
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Backup settings could not be loaded", 409);
  }
}
__name(handleGetAdminBackupSettings, "handleGetAdminBackupSettings");
async function handleUpdateAdminBackupSettings(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Backup settings payload is invalid", 400);
  }
  const storage = new StorageService(env.DB);
  let previous;
  try {
    previous = await loadBackupSettings(storage, env, "UTC");
  } catch {
    previous = getDefaultBackupSettings("UTC");
  }
  let next;
  try {
    next = normalizeBackupSettingsInput(body, previous);
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Backup settings are invalid", 400);
  }
  await saveBackupSettings(storage, env, next);
  await writeAuditLog2(storage, actorUser.id, "admin.backup.settings.update", "backup", null, {
    destinationCount: next.destinations.length,
    scheduledDestinationCount: next.destinations.filter((destination) => destination.schedule.enabled).length
  }, request);
  return jsonResponse(next);
}
__name(handleUpdateAdminBackupSettings, "handleUpdateAdminBackupSettings");
async function handleGetAdminBackupSettingsRepairState(request, env, actorUser) {
  void request;
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  const storage = new StorageService(env.DB);
  try {
    const state = await getBackupSettingsRepairState(storage, env, "UTC");
    return jsonResponse({
      object: "backup-settings-repair",
      needsRepair: state.needsRepair,
      portable: state.portable
    });
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Backup settings repair state could not be loaded", 409);
  }
}
__name(handleGetAdminBackupSettingsRepairState, "handleGetAdminBackupSettingsRepairState");
async function handleRepairAdminBackupSettings(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Backup settings repair payload is invalid", 400);
  }
  const storage = new StorageService(env.DB);
  let previous;
  try {
    previous = await loadBackupSettings(storage, env, "UTC");
  } catch {
    previous = getDefaultBackupSettings("UTC");
  }
  let next;
  try {
    next = normalizeBackupSettingsInput(body, previous);
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Backup settings repair payload is invalid", 400);
  }
  await repairBackupSettings(storage, env, next);
  await writeAuditLog2(storage, actorUser.id, "admin.backup.settings.repair", "backup", null, {
    destinationCount: next.destinations.length,
    scheduledDestinationCount: next.destinations.filter((destination) => destination.schedule.enabled).length
  }, request);
  return jsonResponse(next);
}
__name(handleRepairAdminBackupSettings, "handleRepairAdminBackupSettings");
async function handleRunAdminConfiguredBackup(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  try {
    let body = null;
    try {
      if ((request.headers.get("Content-Type") || "").includes("application/json")) {
        body = await request.json();
      }
    } catch {
      return errorResponse("Backup run payload is invalid", 400);
    }
    const targetDeviceIdentifier = String(request.headers.get("X-NodeWarden-Acting-Device-Id") || "").trim() || null;
    const progress = /* @__PURE__ */ __name(async (event) => {
      await notifyUserBackupProgress(env, actorUser.id, event, targetDeviceIdentifier);
    }, "progress");
    const outcome = await withBackupRunnerLease(env, `manual:${actorUser.id}`, async (keepAlive) => {
      const storage = new StorageService(env.DB);
      const result = await executeConfiguredBackup(
        env,
        storage,
        actorUser.id,
        "manual",
        body?.destinationId || null,
        keepAlive,
        progress,
        auditRequestMetadata(request)
      );
      const settings = await loadBackupSettings(storage, env, "UTC");
      return { result, settings };
    });
    if (!outcome) {
      return errorResponse("Another backup run is already in progress", 409);
    }
    return jsonResponse({
      object: "backup-run",
      result: {
        fileName: outcome.result.fileName,
        fileSize: outcome.result.fileSize,
        provider: outcome.result.provider,
        remotePath: outcome.result.remotePath
      },
      settings: outcome.settings
    });
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Backup run failed", 500);
  }
}
__name(handleRunAdminConfiguredBackup, "handleRunAdminConfiguredBackup");
async function handleListAdminRemoteBackups(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  const storage = new StorageService(env.DB);
  try {
    const settings = await loadBackupSettings(storage, env, "UTC");
    const url = new URL(request.url);
    const destination = requireBackupDestination(settings, url.searchParams.get("destinationId") || null);
    const listing = await listRemoteBackupEntries(destination, url.searchParams.get("path") || "");
    return jsonResponse({
      object: "backup-remote-browser",
      destinationId: destination.id,
      destinationName: destination.name,
      ...listing
    });
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Remote backup listing failed", 409);
  }
}
__name(handleListAdminRemoteBackups, "handleListAdminRemoteBackups");
async function handleDownloadAdminRemoteBackup(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  const storage = new StorageService(env.DB);
  try {
    const settings = await loadBackupSettings(storage, env, "UTC");
    const url = new URL(request.url);
    const path = ensureRemoteRestoreCandidate(url.searchParams.get("path") || "");
    const destination = requireBackupDestination(settings, url.searchParams.get("destinationId") || null);
    const remoteFile = await downloadRemoteBackupFile(destination, path);
    return new Response(remoteFile.bytes, {
      status: 200,
      headers: {
        "Content-Type": remoteFile.contentType || "application/zip",
        "Content-Disposition": `attachment; filename="${remoteFile.fileName}"`,
        "Cache-Control": "no-store"
      }
    });
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Remote backup download failed", 409);
  }
}
__name(handleDownloadAdminRemoteBackup, "handleDownloadAdminRemoteBackup");
async function handleInspectAdminRemoteBackup(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  const storage = new StorageService(env.DB);
  try {
    const settings = await loadBackupSettings(storage, env, "UTC");
    const url = new URL(request.url);
    const path = ensureRemoteRestoreCandidate(url.searchParams.get("path") || "");
    const destination = requireBackupDestination(settings, url.searchParams.get("destinationId") || null);
    const remoteFile = await downloadRemoteBackupFile(destination, path);
    const integrity = await inspectBackupArchiveFileNameChecksum(remoteFile.bytes, remoteFile.fileName || path);
    return jsonResponse({
      object: "backup-remote-integrity",
      destinationId: destination.id,
      path,
      fileName: remoteFile.fileName || path.split("/").pop() || path,
      integrity
    });
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Remote backup integrity inspection failed", 409);
  }
}
__name(handleInspectAdminRemoteBackup, "handleInspectAdminRemoteBackup");
async function handleDeleteAdminRemoteBackup(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  const storage = new StorageService(env.DB);
  try {
    const settings = await loadBackupSettings(storage, env, "UTC");
    const url = new URL(request.url);
    const path = ensureRemoteRestoreCandidate(url.searchParams.get("path") || "");
    const destination = requireBackupDestination(settings, url.searchParams.get("destinationId") || null);
    await deleteRemoteBackupFile(destination, path);
    await writeAuditLog2(storage, actorUser.id, "admin.backup.remote.delete", "backup", null, {
      ...getBackupDestinationSummary(destination),
      remotePath: path
    }, request);
    return jsonResponse({ object: "backup-remote-delete", deleted: true, path });
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Remote backup delete failed", 409);
  }
}
__name(handleDeleteAdminRemoteBackup, "handleDeleteAdminRemoteBackup");
async function handleRestoreAdminRemoteBackup(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Remote restore payload is invalid", 400);
  }
  const storage = new StorageService(env.DB);
  try {
    const settings = await loadBackupSettings(storage, env, "UTC");
    const destination = requireBackupDestination(settings, body.destinationId || null);
    const path = ensureRemoteRestoreCandidate(String(body.path || ""));
    const targetDeviceIdentifier = String(request.headers.get("X-NodeWarden-Acting-Device-Id") || "").trim() || null;
    const restoreFileNameFromPath = path.split("/").pop() || path;
    await notifyUserBackupRestoreProgress(
      env,
      actorUser.id,
      {
        operation: "backup-restore",
        source: "remote",
        step: "remote_fetch_archive",
        fileName: restoreFileNameFromPath,
        stageTitle: "txt_backup_restore_progress_remote_fetch_title",
        stageDetail: "txt_backup_restore_progress_remote_fetch_detail",
        replaceExisting: !!body.replaceExisting
      },
      targetDeviceIdentifier
    );
    const remoteFile = await downloadRemoteBackupFile(destination, path);
    const checksumOk = await verifyBackupArchiveFileNameChecksum(remoteFile.bytes, remoteFile.fileName || path);
    if (!checksumOk && !body.allowChecksumMismatch) {
      return errorResponse("Remote backup file checksum does not match its filename", 400);
    }
    const restoreFileName = remoteFile.fileName || path.split("/").pop() || path;
    const progress = /* @__PURE__ */ __name(async (event) => {
      await notifyUserBackupRestoreProgress(
        env,
        actorUser.id,
        {
          operation: "backup-restore",
          ...event
        },
        targetDeviceIdentifier
      );
    }, "progress");
    const imported = await (async () => {
      const storage2 = new StorageService(env.DB);
      const result = await importRemoteBackupArchiveBytes(
        remoteFile.bytes,
        env,
        actorUser.id,
        !!body.replaceExisting,
        {
          loadAttachment: /* @__PURE__ */ __name(async (blobName) => {
            const file = await downloadRemoteBackupFile(destination, `attachments/${blobName}`).catch(() => null);
            return file?.bytes || null;
          }, "loadAttachment")
        },
        progress,
        restoreFileName
      );
      await writeAuditLog2(storage2, result.auditActorUserId, "admin.backup.import", "backup", null, {
        users: result.result.imported.users,
        ciphers: result.result.imported.ciphers,
        attachments: result.result.imported.attachmentFiles,
        skippedAttachments: result.result.skipped.attachments,
        skippedReason: result.result.skipped.reason,
        replaceExisting: !!body.replaceExisting,
        ...getBackupDestinationSummary(destination),
        remotePath: path,
        bytes: remoteFile.bytes.byteLength,
        trigger: "remote",
        checksumMismatchAccepted: !checksumOk
      }, request);
      return result;
    })();
    return jsonResponse(imported.result);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Remote backup restore failed";
    return errorResponse(message, toImportStatusCode(message));
  }
}
__name(handleRestoreAdminRemoteBackup, "handleRestoreAdminRemoteBackup");
async function handleAdminExportBackup(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  const storage = new StorageService(env.DB);
  const targetDeviceIdentifier = String(request.headers.get("X-NodeWarden-Acting-Device-Id") || "").trim() || null;
  let body = null;
  try {
    if ((request.headers.get("Content-Type") || "").includes("application/json")) {
      body = await request.json();
    }
  } catch {
    return errorResponse("Backup export payload is invalid", 400);
  }
  let archive;
  try {
    const progress = /* @__PURE__ */ __name(async (event) => {
      await notifyUserBackupProgress(
        env,
        actorUser.id,
        {
          operation: "backup-export",
          source: "local",
          step: `export_${event.step}`,
          fileName: event.fileName || "",
          stageTitle: event.stageTitle,
          stageDetail: event.stageDetail
        },
        targetDeviceIdentifier
      );
    }, "progress");
    archive = await buildBackupArchive(env, /* @__PURE__ */ new Date(), {
      includeAttachments: !!body?.includeAttachments,
      progress
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Backup export failed";
    await notifyUserBackupProgress(
      env,
      actorUser.id,
      {
        operation: "backup-export",
        source: "local",
        step: "export_failed",
        fileName: "",
        stageTitle: "txt_backup_export_progress_failed_title",
        stageDetail: "txt_backup_export_progress_failed_detail",
        done: true,
        ok: false,
        error: message
      },
      targetDeviceIdentifier
    );
    return errorResponse(message, message.includes("blob missing") ? 409 : 500);
  }
  await writeAuditLog2(storage, actorUser.id, "admin.backup.export", "backup", null, {
    users: archive.manifest.tableCounts.users,
    ciphers: archive.manifest.tableCounts.ciphers,
    attachments: archive.manifest.tableCounts.attachments,
    compressedBytes: archive.bytes.byteLength,
    includesAttachments: archive.manifest.includes.attachments
  }, request);
  return new Response(archive.bytes, {
    status: 200,
    headers: {
      "Content-Type": "application/zip",
      "Content-Disposition": `attachment; filename="${archive.fileName}"`,
      "Cache-Control": "no-store"
    }
  });
}
__name(handleAdminExportBackup, "handleAdminExportBackup");
async function handleDownloadAdminBackupAttachment(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  try {
    const url = new URL(request.url);
    const blobName = ensureBackupBlobName(url.searchParams.get("blobName") || "");
    const object = await getBlobObject(env, blobName);
    if (!object) {
      return errorResponse("Backup attachment blob not found", 404);
    }
    return new Response(object.body, {
      status: 200,
      headers: {
        "Content-Type": object.contentType || "application/octet-stream",
        "Content-Length": String(object.size),
        "Cache-Control": "no-store"
      }
    });
  } catch (error) {
    return errorResponse(error instanceof Error ? error.message : "Backup attachment download failed", 400);
  }
}
__name(handleDownloadAdminBackupAttachment, "handleDownloadAdminBackupAttachment");
async function handleAdminImportBackup(request, env, actorUser) {
  if (!isAdmin2(actorUser)) return errorResponse("Forbidden", 403);
  let formData;
  try {
    formData = await request.formData();
  } catch {
    return errorResponse("Content-Type must be multipart/form-data", 400);
  }
  const file = formData.get("file");
  if (!file || typeof file !== "object" || !("arrayBuffer" in file)) {
    return errorResponse("Backup file is required", 400);
  }
  const replaceExisting = String(formData.get("replaceExisting") || "").trim() === "1";
  const allowChecksumMismatch = String(formData.get("allowChecksumMismatch") || "").trim() === "1";
  let archiveBytes;
  try {
    archiveBytes = new Uint8Array(await file.arrayBuffer());
  } catch {
    return errorResponse("Unable to read backup file", 400);
  }
  try {
    const fileName = "name" in file ? String(file.name || "") : "";
    const checksumOk = await verifyBackupArchiveFileNameChecksum(archiveBytes, fileName);
    if (!checksumOk && !allowChecksumMismatch) {
      return errorResponse("Backup file checksum does not match its filename", 400);
    }
    const imported = await runImportAndAudit(env, request, actorUser, archiveBytes, fileName || "nodewarden_backup.zip", replaceExisting, {
      trigger: "local",
      bytes: archiveBytes.byteLength,
      checksumMismatchAccepted: !checksumOk
    });
    return jsonResponse(imported.result);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Backup import failed";
    return errorResponse(message, toImportStatusCode(message));
  }
}
__name(handleAdminImportBackup, "handleAdminImportBackup");

// src/router-admin-backup.ts
async function handleAdminBackupRoute(request, env, actorUser, path, method) {
  if (path === "/api/admin/backup/export" && method === "POST") {
    return handleAdminExportBackup(request, env, actorUser);
  }
  if (path === "/api/admin/backup/blob" && method === "GET") {
    return handleDownloadAdminBackupAttachment(request, env, actorUser);
  }
  if (path === "/api/admin/backup/settings") {
    if (method === "GET") return handleGetAdminBackupSettings(request, env, actorUser);
    if (method === "PUT") return handleUpdateAdminBackupSettings(request, env, actorUser);
    return null;
  }
  if (path === "/api/admin/backup/settings/repair") {
    if (method === "GET") return handleGetAdminBackupSettingsRepairState(request, env, actorUser);
    if (method === "POST") return handleRepairAdminBackupSettings(request, env, actorUser);
    return null;
  }
  if (path === "/api/admin/backup/run" && method === "POST") {
    return handleRunAdminConfiguredBackup(request, env, actorUser);
  }
  if (path === "/api/admin/backup/remote" && method === "GET") {
    return handleListAdminRemoteBackups(request, env, actorUser);
  }
  if (path === "/api/admin/backup/remote/download" && method === "GET") {
    return handleDownloadAdminRemoteBackup(request, env, actorUser);
  }
  if (path === "/api/admin/backup/remote/integrity" && method === "GET") {
    return handleInspectAdminRemoteBackup(request, env, actorUser);
  }
  if (path === "/api/admin/backup/remote/file" && method === "DELETE") {
    return handleDeleteAdminRemoteBackup(request, env, actorUser);
  }
  if (path === "/api/admin/backup/remote/restore" && method === "POST") {
    return handleRestoreAdminRemoteBackup(request, env, actorUser);
  }
  if (path === "/api/admin/backup/import" && method === "POST") {
    return handleAdminImportBackup(request, env, actorUser);
  }
  return null;
}
__name(handleAdminBackupRoute, "handleAdminBackupRoute");

// src/router-admin.ts
async function handleAdminRoute(request, env, actorUser, path, method) {
  if (path === "/api/admin/users" && method === "GET") {
    return handleAdminListUsers(request, env, actorUser);
  }
  if (path === "/api/admin/logs" && method === "GET") {
    return handleAdminListAuditLogs(request, env, actorUser);
  }
  if (path === "/api/admin/logs" && method === "DELETE") {
    return handleAdminClearAuditLogs(request, env, actorUser);
  }
  if (path === "/api/admin/logs/settings") {
    if (method === "GET") return handleAdminGetAuditLogSettings(request, env, actorUser);
    if (method === "PUT" || method === "POST") return handleAdminUpdateAuditLogSettings(request, env, actorUser);
    return null;
  }
  const adminBackupResponse = await handleAdminBackupRoute(request, env, actorUser, path, method);
  if (adminBackupResponse) return adminBackupResponse;
  if (path === "/api/admin/invites") {
    if (method === "GET") return handleAdminListInvites(request, env, actorUser);
    if (method === "POST") return handleAdminCreateInvite(request, env, actorUser);
    if (method === "DELETE") return handleAdminDeleteAllInvites(request, env, actorUser);
    return null;
  }
  const adminInviteMatch = path.match(/^\/api\/admin\/invites\/([^/]+)$/i);
  if (adminInviteMatch && method === "DELETE") {
    const inviteCode = decodeURIComponent(adminInviteMatch[1]);
    return handleAdminRevokeInvite(request, env, actorUser, inviteCode);
  }
  const adminUserStatusMatch = path.match(/^\/api\/admin\/users\/([a-f0-9-]+)\/status$/i);
  if (adminUserStatusMatch && (method === "PUT" || method === "POST")) {
    return handleAdminSetUserStatus(request, env, actorUser, adminUserStatusMatch[1]);
  }
  const adminUserDeleteMatch = path.match(/^\/api\/admin\/users\/([a-f0-9-]+)$/i);
  if (adminUserDeleteMatch && method === "DELETE") {
    return handleAdminDeleteUser(request, env, actorUser, adminUserDeleteMatch[1]);
  }
  return null;
}
__name(handleAdminRoute, "handleAdminRoute");

// src/handlers/domains.ts
function firstPresent(payload, keys) {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(payload, key)) return payload[key];
  }
  return void 0;
}
__name(firstPresent, "firstPresent");
async function readPayload(request) {
  try {
    const parsed = await request.json();
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}
__name(readPayload, "readPayload");
async function handleGetDomains(env, userId) {
  const storage = new StorageService(env.DB);
  const settings = await storage.getUserDomainSettings(userId);
  return jsonResponse(buildDomainsResponse(
    settings.equivalentDomains,
    settings.customEquivalentDomains,
    settings.excludedGlobalEquivalentDomains
  ));
}
__name(handleGetDomains, "handleGetDomains");
async function handleUpdateDomains(request, env, userId) {
  const storage = new StorageService(env.DB);
  const payload = await readPayload(request);
  const current = await storage.getUserDomainSettings(userId);
  const equivalentDomainsRaw = firstPresent(payload, [
    "equivalentDomains",
    "EquivalentDomains"
  ]);
  const customEquivalentDomainsRaw = firstPresent(payload, [
    "customEquivalentDomains",
    "CustomEquivalentDomains"
  ]);
  const excludedGlobalEquivalentDomainsRaw = firstPresent(payload, [
    "excludedGlobalEquivalentDomains",
    "ExcludedGlobalEquivalentDomains",
    // Some older compatible clients send the excluded type list under this key.
    "globalEquivalentDomains",
    "GlobalEquivalentDomains"
  ]);
  const customEquivalentDomains = customEquivalentDomainsRaw === void 0 ? equivalentDomainsRaw === void 0 ? current.customEquivalentDomains : normalizeCustomEquivalentDomains(normalizeEquivalentDomains(equivalentDomainsRaw)) : normalizeCustomEquivalentDomains(customEquivalentDomainsRaw);
  const equivalentDomains = customRulesToActiveEquivalentDomains(customEquivalentDomains);
  const excludedGlobalEquivalentDomains = excludedGlobalEquivalentDomainsRaw === void 0 ? current.excludedGlobalEquivalentDomains : normalizeExcludedGlobalTypes(excludedGlobalEquivalentDomainsRaw);
  await storage.saveUserDomainSettings(userId, equivalentDomains, customEquivalentDomains, excludedGlobalEquivalentDomains);
  const settings = await storage.getUserDomainSettings(userId);
  if (!settings) {
    return errorResponse("Domain settings unavailable", 500);
  }
  return jsonResponse(buildDomainsResponse(
    settings.equivalentDomains,
    settings.customEquivalentDomains,
    settings.excludedGlobalEquivalentDomains
  ));
}
__name(handleUpdateDomains, "handleUpdateDomains");

// src/router-authenticated.ts
async function handleAuthenticatedRoute(request, env, userId, currentUser, path, method) {
  if (method === "POST" || method === "PUT" || method === "DELETE") {
    const blockedAccountPaths = /* @__PURE__ */ new Set([
      "/api/accounts/set-password",
      "/api/accounts/delete",
      "/api/accounts/delete-account",
      "/api/accounts/delete-vault"
    ]);
    if (blockedAccountPaths.has(path)) {
      return errorResponse("Not implemented", 501);
    }
  }
  if (path === "/api/accounts/profile") {
    if (method === "GET") return handleGetProfile(request, env, userId);
    if (method === "PUT") return handleUpdateProfile(request, env, userId);
    return errorResponse("Method not allowed", 405);
  }
  if ((path === "/api/accounts/password" || path === "/api/accounts/change-password") && (method === "POST" || method === "PUT")) {
    return handleChangePassword(request, env, userId);
  }
  if (path === "/api/accounts/keys" && method === "POST") {
    return handleSetKeys(request, env, userId);
  }
  if (path === "/api/accounts/totp") {
    if (method === "GET") return handleGetTotpStatus(request, env, userId);
    if (method === "PUT" || method === "POST") return handleSetTotpStatus(request, env, userId);
    return null;
  }
  if ((path === "/api/accounts/totp/recovery-code" || path === "/api/two-factor/get-recover") && method === "POST") {
    return handleGetTotpRecoveryCode(request, env, userId);
  }
  if (path === "/api/accounts/revision-date" && method === "GET") {
    return handleGetRevisionDate(request, env, userId);
  }
  if (path === "/api/accounts/verify-password" && method === "POST") {
    return handleVerifyPassword(request, env, userId);
  }
  if (path === "/api/accounts/verify-devices" && (method === "PUT" || method === "POST")) {
    return handleSetVerifyDevices(request, env, userId);
  }
  if ((path === "/api/accounts/api-key" || path === "/api/accounts/api_key") && method === "POST") {
    return handleGetApiKey(request, env, userId);
  }
  if ((path === "/api/accounts/rotate-api-key" || path === "/api/accounts/rotate_api_key") && method === "POST") {
    return handleRotateApiKey(request, env, userId);
  }
  if (path === "/api/sync" && method === "GET") {
    return handleSync(request, env, userId);
  }
  if (path.startsWith("/notifications/")) {
    return errorResponse("Not found", 404);
  }
  if (path === "/api/ciphers" || path === "/api/ciphers/create") {
    if (method === "GET") return handleGetCiphers(request, env, userId);
    if (method === "POST") return handleCreateCipher(request, env, userId);
    return null;
  }
  if (path === "/api/ciphers/import" && method === "POST") {
    return handleCiphersImport(request, env, userId);
  }
  if (path === "/api/ciphers/delete" && method === "POST") {
    return handleBulkDeleteCiphers(request, env, userId);
  }
  if (path === "/api/ciphers/delete-permanent" && method === "POST") {
    return handleBulkPermanentDeleteCiphers(request, env, userId);
  }
  if (path === "/api/ciphers/restore" && method === "POST") {
    return handleBulkRestoreCiphers(request, env, userId);
  }
  if (path === "/api/ciphers/archive" && (method === "PUT" || method === "POST")) {
    return handleBulkArchiveCiphers(request, env, userId);
  }
  if (path === "/api/ciphers/unarchive" && (method === "PUT" || method === "POST")) {
    return handleBulkUnarchiveCiphers(request, env, userId);
  }
  if (path === "/api/ciphers/move" && (method === "POST" || method === "PUT")) {
    return handleBulkMoveCiphers(request, env, userId);
  }
  const cipherMatch = path.match(/^\/api\/ciphers\/([a-f0-9-]+)(\/.*)?$/i);
  if (cipherMatch) {
    const cipherId = cipherMatch[1];
    const subPath = cipherMatch[2] || "";
    if (subPath === "" || subPath === "/") {
      if (method === "GET") return handleGetCipher(request, env, userId, cipherId);
      if (method === "PUT" || method === "POST") return handleUpdateCipher(request, env, userId, cipherId);
      if (method === "DELETE") return handleDeleteCipherCompat(request, env, userId, cipherId);
    }
    if (subPath === "/delete" && method === "PUT") return handleDeleteCipher(request, env, userId, cipherId);
    if (subPath === "/delete" && method === "DELETE") return handlePermanentDeleteCipher(request, env, userId, cipherId);
    if (subPath === "/restore" && method === "PUT") return handleRestoreCipher(request, env, userId, cipherId);
    if (subPath === "/archive" && (method === "PUT" || method === "POST")) return handleArchiveCipher(request, env, userId, cipherId);
    if (subPath === "/unarchive" && (method === "PUT" || method === "POST")) return handleUnarchiveCipher(request, env, userId, cipherId);
    if (subPath === "/partial" && (method === "PUT" || method === "POST")) return handlePartialUpdateCipher(request, env, userId, cipherId);
    if (subPath === "/share" && method === "POST") return handleGetCipher(request, env, userId, cipherId);
    if (subPath === "/details" && method === "GET") return handleGetCipher(request, env, userId, cipherId);
    if (subPath === "/attachment/v2" && method === "POST") return handleCreateAttachment(request, env, userId, cipherId);
    if (subPath === "/attachment" && method === "POST") return handleCreateAttachment(request, env, userId, cipherId);
    const attachmentMatch = subPath.match(/^\/attachment\/([a-f0-9-]+)$/i);
    if (attachmentMatch) {
      const attachmentId = attachmentMatch[1];
      if (method === "POST" || method === "PUT") return handleUploadAttachment(request, env, userId, cipherId, attachmentId);
      if (method === "GET") return handleGetAttachment(request, env, userId, cipherId, attachmentId);
      if (method === "DELETE") return handleDeleteAttachment(request, env, userId, cipherId, attachmentId);
    }
    const attachmentMetadataMatch = subPath.match(/^\/attachment\/([a-f0-9-]+)\/metadata$/i);
    if (attachmentMetadataMatch && (method === "POST" || method === "PUT")) {
      return handleUpdateAttachmentMetadata(request, env, userId, cipherId, attachmentMetadataMatch[1]);
    }
    const attachmentDeleteMatch = subPath.match(/^\/attachment\/([a-f0-9-]+)\/delete$/i);
    if (attachmentDeleteMatch && method === "POST") {
      return handleDeleteAttachment(request, env, userId, cipherId, attachmentDeleteMatch[1]);
    }
  }
  if (path === "/api/folders") {
    if (method === "GET") return handleGetFolders(request, env, userId);
    if (method === "POST") return handleCreateFolder(request, env, userId);
    return null;
  }
  if (path === "/api/folders/delete" && method === "POST") {
    return handleBulkDeleteFolders(request, env, userId);
  }
  const folderMatch = path.match(/^\/api\/folders\/([a-f0-9-]+)$/i);
  if (folderMatch) {
    const folderId = folderMatch[1];
    if (method === "GET") return handleGetFolder(request, env, userId, folderId);
    if (method === "PUT") return handleUpdateFolder(request, env, userId, folderId);
    if (method === "DELETE") return handleDeleteFolder(request, env, userId, folderId);
  }
  if (path.startsWith("/api/auth-requests")) {
    return jsonResponse({ data: [], object: "list", continuationToken: null });
  }
  if (path === "/api/collections" || path.startsWith("/api/collections/")) {
    if (method === "GET") {
      return jsonResponse({ data: [], object: "list", continuationToken: null });
    }
    return null;
  }
  if (path === "/api/organizations" || path.startsWith("/api/organizations/")) {
    if (method === "GET") {
      return jsonResponse({ data: [], object: "list", continuationToken: null });
    }
    return null;
  }
  if (path === "/api/sends") {
    if (method === "GET") return handleGetSends(request, env, userId);
    if (method === "POST") return handleCreateSend(request, env, userId);
    return null;
  }
  if (path === "/api/sends/file/v2" && method === "POST") {
    return handleCreateFileSendV2(request, env, userId);
  }
  if (path === "/api/sends/delete" && method === "POST") {
    return handleBulkDeleteSends(request, env, userId);
  }
  const sendMatch = path.match(/^\/api\/sends\/([^/]+)(\/.*)?$/i);
  if (sendMatch) {
    const sendId = sendMatch[1];
    const subPath = sendMatch[2] || "";
    if (subPath === "" || subPath === "/") {
      if (method === "GET") return handleGetSend(request, env, userId, sendId);
      if (method === "PUT") return handleUpdateSend(request, env, userId, sendId);
      if (method === "DELETE") return handleDeleteSend(request, env, userId, sendId);
    }
    if (subPath === "/remove-password" && (method === "PUT" || method === "POST")) {
      return handleRemoveSendPassword(request, env, userId, sendId);
    }
    if (subPath === "/remove-auth" && (method === "PUT" || method === "POST")) {
      return handleRemoveSendAuth(request, env, userId, sendId);
    }
    const sendFileUploadMatch = subPath.match(/^\/file\/([^/]+)\/?$/i);
    if (sendFileUploadMatch) {
      const fileId = sendFileUploadMatch[1];
      if (method === "GET") return handleGetSendFileUpload(request, env, userId, sendId, fileId);
      if (method === "POST" || method === "PUT") return handleUploadSendFile(request, env, userId, sendId, fileId);
    }
  }
  if (path === "/api/policies" || path.startsWith("/api/policies/")) {
    if (method === "GET") {
      return jsonResponse({ data: [], object: "list", continuationToken: null });
    }
    return null;
  }
  if (path === "/api/settings/domains" || path === "/settings/domains") {
    if (method === "GET") return handleGetDomains(env, userId);
    if (method === "PUT" || method === "POST") return handleUpdateDomains(request, env, userId);
    return null;
  }
  const authenticatedDeviceResponse = await handleAuthenticatedDeviceRoute(request, env, userId, path, method);
  if (authenticatedDeviceResponse) return authenticatedDeviceResponse;
  const adminResponse = await handleAdminRoute(request, env, currentUser, path, method);
  if (adminResponse) return adminResponse;
  return null;
}
__name(handleAuthenticatedRoute, "handleAuthenticatedRoute");

// src/handlers/identity.ts
var TWO_FACTOR_REMEMBER_TTL_MS2 = 30 * 24 * 60 * 60 * 1e3;
var TWO_FACTOR_PROVIDER_AUTHENTICATOR = 0;
var TWO_FACTOR_PROVIDER_REMEMBER = 5;
var WEB_REFRESH_COOKIE = "nodewarden_web_refresh";
var TWO_FACTOR_PROVIDER_RECOVERY_CODE_RESPONSE = "-1";
var TWO_FACTOR_PROVIDER_RECOVERY_CODE_LEGACY = 8;
var TWO_FACTOR_PROVIDER_RECOVERY_CODE_ANDROID_REQUEST = 100;
function resolveTotpSecret(userSecret) {
  if (userSecret && isTotpEnabled(userSecret)) {
    return userSecret;
  }
  return null;
}
__name(resolveTotpSecret, "resolveTotpSecret");
async function resolveDeviceSession(storage, userId, deviceInfo) {
  if (!deviceInfo.deviceIdentifier) return null;
  const existingDevice = await storage.getDevice(userId, deviceInfo.deviceIdentifier);
  const sessionStamp = String(existingDevice?.sessionStamp || "").trim() || generateUUID();
  return { identifier: deviceInfo.deviceIdentifier, sessionStamp };
}
__name(resolveDeviceSession, "resolveDeviceSession");
function shouldUseWebSession(request) {
  return String(request.headers.get("X-NodeWarden-Web-Session") || "").trim() === "1";
}
__name(shouldUseWebSession, "shouldUseWebSession");
function parseCookieValue(request, name) {
  const rawCookie = String(request.headers.get("Cookie") || "").trim();
  if (!rawCookie) return null;
  for (const part of rawCookie.split(";")) {
    const [key, ...rest] = part.trim().split("=");
    if (key !== name) continue;
    const value = rest.join("=").trim();
    return value ? decodeURIComponent(value) : null;
  }
  return null;
}
__name(parseCookieValue, "parseCookieValue");
function constantTimeEquals(a, b) {
  const encA = new TextEncoder().encode(a);
  const encB = new TextEncoder().encode(b);
  if (encA.length !== encB.length) return false;
  let diff = 0;
  for (let i = 0; i < encA.length; i++) {
    diff |= encA[i] ^ encB[i];
  }
  return diff === 0;
}
__name(constantTimeEquals, "constantTimeEquals");
function buildRefreshCookie(request, refreshToken, maxAgeSeconds) {
  const isHttps = new URL(request.url).protocol === "https:";
  const parts = [
    `${WEB_REFRESH_COOKIE}=${encodeURIComponent(refreshToken)}`,
    "Path=/identity/connect",
    "HttpOnly",
    "SameSite=Strict",
    `Max-Age=${Math.max(0, Math.floor(maxAgeSeconds))}`
  ];
  if (isHttps) parts.push("Secure");
  return parts.join("; ");
}
__name(buildRefreshCookie, "buildRefreshCookie");
function buildClearedRefreshCookie(request) {
  return buildRefreshCookie(request, "", 0);
}
__name(buildClearedRefreshCookie, "buildClearedRefreshCookie");
function withWebRefreshCookie(request, response, refreshToken) {
  const headers = new Headers(response.headers);
  headers.append(
    "Set-Cookie",
    refreshToken ? buildRefreshCookie(request, refreshToken, Math.floor(LIMITS.auth.refreshTokenTtlMs / 1e3)) : buildClearedRefreshCookie(request)
  );
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}
__name(withWebRefreshCookie, "withWebRefreshCookie");
function buildPreloginResponse(email, kdfType, kdfIterations, kdfMemory, kdfParallelism) {
  return {
    kdf: kdfType,
    kdfIterations,
    kdfMemory,
    kdfParallelism,
    KdfSettings: {
      KdfType: kdfType,
      Iterations: kdfIterations,
      Memory: kdfMemory,
      Parallelism: kdfParallelism
    },
    Salt: email.toLowerCase()
  };
}
__name(buildPreloginResponse, "buildPreloginResponse");
function twoFactorRequiredResponse(message = "Two factor required.", includeRecoveryCode = false) {
  const providers = includeRecoveryCode ? [String(TWO_FACTOR_PROVIDER_AUTHENTICATOR), TWO_FACTOR_PROVIDER_RECOVERY_CODE_RESPONSE] : [String(TWO_FACTOR_PROVIDER_AUTHENTICATOR)];
  const providers2 = {};
  for (const provider of providers) providers2[provider] = null;
  const customResponse = {
    TwoFactorProviders: providers,
    TwoFactorProviders2: providers2,
    SsoEmail2faSessionToken: null,
    MasterPasswordPolicy: {
      Object: "masterPasswordPolicy"
    }
  };
  return jsonResponse(
    {
      error: "invalid_grant",
      error_description: message,
      Error: "invalid_grant",
      ErrorDescription: message,
      ErrorMessage: message,
      TwoFactorProviders: customResponse.TwoFactorProviders,
      TwoFactorProviders2: customResponse.TwoFactorProviders2,
      // Required by current Android parser (nullable value is acceptable).
      SsoEmail2faSessionToken: customResponse.SsoEmail2faSessionToken,
      MasterPasswordPolicy: customResponse.MasterPasswordPolicy,
      CustomResponse: customResponse,
      ErrorModel: {
        Message: message,
        Object: "error"
      }
    },
    400
  );
}
__name(twoFactorRequiredResponse, "twoFactorRequiredResponse");
async function recordFailedLoginAndBuildResponse(rateLimit, loginIdentifier, message) {
  const result = await rateLimit.recordFailedLogin(loginIdentifier);
  if (result.locked) {
    return identityErrorResponse(
      `Too many failed login attempts. Account locked for ${Math.ceil(result.retryAfterSeconds / 60)} minutes.`,
      "TooManyRequests",
      429
    );
  }
  return identityErrorResponse(message, "invalid_grant", 400);
}
__name(recordFailedLoginAndBuildResponse, "recordFailedLoginAndBuildResponse");
async function recordFailedTwoFactorAndBuildResponse(rateLimit, loginIdentifier) {
  const failed = await rateLimit.recordFailedLogin(loginIdentifier);
  if (failed.locked) {
    return identityErrorResponse(
      `Too many failed login attempts. Account locked for ${Math.ceil(failed.retryAfterSeconds / 60)} minutes.`,
      "TooManyRequests",
      429
    );
  }
  return identityErrorResponse("Two-step token is invalid. Try again.", "invalid_grant", 400);
}
__name(recordFailedTwoFactorAndBuildResponse, "recordFailedTwoFactorAndBuildResponse");
async function handleToken(request, env) {
  const storage = new StorageService(env.DB);
  const auth = new AuthService(env);
  const rateLimit = new RateLimitService(env.DB);
  let body;
  const contentType = request.headers.get("content-type") || "";
  try {
    if (contentType.includes("application/x-www-form-urlencoded")) {
      const formData = await request.formData();
      body = Object.fromEntries(formData.entries());
    } else {
      body = await request.json();
    }
  } catch {
    return identityErrorResponse("Invalid request payload", "invalid_request", 400);
  }
  const grantType = body.grant_type;
  const clientIdentifier = getClientIdentifier(request);
  if (!clientIdentifier) {
    return identityErrorResponse("Client IP is required", "invalid_request", 403);
  }
  if (grantType === "password") {
    const email = body.username?.toLowerCase();
    const passwordHash = body.password;
    const twoFactorToken = body.twoFactorToken;
    const twoFactorProvider = body.twoFactorProvider;
    const twoFactorRemember = body.twoFactorRemember;
    const loginIdentifier = `${clientIdentifier}:${email}`;
    const deviceInfo = readAuthRequestDeviceInfo(body, request);
    if (!email || !passwordHash) {
      return identityErrorResponse("Email and password are required", "invalid_request", 400);
    }
    const loginCheck = await rateLimit.checkLoginAttempt(loginIdentifier);
    if (!loginCheck.allowed) {
      return identityErrorResponse(
        `Too many failed login attempts. Try again in ${Math.ceil(loginCheck.retryAfterSeconds / 60)} minutes.`,
        "TooManyRequests",
        429
      );
    }
    const user = await storage.getUser(email);
    if (!user) {
      await rateLimit.recordFailedLogin(loginIdentifier);
      return identityErrorResponse("Username or password is incorrect. Try again", "invalid_grant", 400);
    }
    if (user.status !== "active") {
      await rateLimit.recordFailedLogin(loginIdentifier);
      await safeWriteAuditEvent(env, {
        actorUserId: user.id,
        action: "auth.login.failed.user_inactive",
        category: "auth",
        level: "warn",
        targetType: "user",
        targetId: user.id,
        metadata: {
          grantType,
          deviceIdentifier: deviceInfo.deviceIdentifier,
          ...auditRequestMetadata(request)
        }
      });
      return identityErrorResponse("Account is disabled", "invalid_grant", 400);
    }
    const valid = await auth.verifyPassword(passwordHash, user.masterPasswordHash, user.email);
    if (!valid) {
      await safeWriteAuditEvent(env, {
        actorUserId: user.id,
        action: "auth.login.failed.bad_password",
        category: "auth",
        level: "warn",
        targetType: "user",
        targetId: user.id,
        metadata: {
          grantType,
          deviceIdentifier: deviceInfo.deviceIdentifier,
          ...auditRequestMetadata(request)
        }
      });
      return recordFailedLoginAndBuildResponse(
        rateLimit,
        loginIdentifier,
        "Username or password is incorrect. Try again"
      );
    }
    let trustedTwoFactorTokenToReturn;
    const effectiveTotpSecret = resolveTotpSecret(user.totpSecret);
    if (effectiveTotpSecret) {
      const canUseRecoveryCode = !!user.totpRecoveryCode;
      const normalizedTwoFactorProvider = String(twoFactorProvider ?? "").trim();
      const normalizedTwoFactorToken = String(twoFactorToken ?? "").trim();
      let rememberRequested = ["1", "true", "True", "TRUE", "on", "yes", "Yes", "YES"].includes(String(twoFactorRemember || "").trim());
      const hasProvider = normalizedTwoFactorProvider.length > 0;
      const hasToken = normalizedTwoFactorToken.length > 0;
      if (!hasProvider || !hasToken) {
        return twoFactorRequiredResponse("Two factor required.", canUseRecoveryCode);
      }
      let passedByRememberToken = false;
      if (normalizedTwoFactorProvider === String(TWO_FACTOR_PROVIDER_REMEMBER)) {
        if (deviceInfo.deviceIdentifier) {
          const trustedUserId = await storage.getTrustedTwoFactorDeviceTokenUserId(
            normalizedTwoFactorToken,
            deviceInfo.deviceIdentifier
          );
          passedByRememberToken = trustedUserId === user.id;
        }
        if (!passedByRememberToken) {
          return twoFactorRequiredResponse("Two factor required.", canUseRecoveryCode);
        }
      } else if (normalizedTwoFactorProvider === String(TWO_FACTOR_PROVIDER_AUTHENTICATOR)) {
        const totpOk = await verifyTotpToken(effectiveTotpSecret, normalizedTwoFactorToken);
        if (!totpOk) {
          return recordFailedTwoFactorAndBuildResponse(rateLimit, loginIdentifier);
        }
      } else if (normalizedTwoFactorProvider === TWO_FACTOR_PROVIDER_RECOVERY_CODE_RESPONSE || normalizedTwoFactorProvider === String(TWO_FACTOR_PROVIDER_RECOVERY_CODE_LEGACY) || normalizedTwoFactorProvider === String(TWO_FACTOR_PROVIDER_RECOVERY_CODE_ANDROID_REQUEST)) {
        if (!recoveryCodeEquals(normalizedTwoFactorToken, user.totpRecoveryCode)) {
          return recordFailedTwoFactorAndBuildResponse(rateLimit, loginIdentifier);
        }
        user.totpSecret = null;
        user.totpRecoveryCode = createRecoveryCode();
        user.updatedAt = (/* @__PURE__ */ new Date()).toISOString();
        await storage.saveUser(user);
        await storage.deleteRefreshTokensByUserId(user.id);
        rememberRequested = false;
      } else {
        return recordFailedTwoFactorAndBuildResponse(rateLimit, loginIdentifier);
      }
      if (rememberRequested && !passedByRememberToken && deviceInfo.deviceIdentifier) {
        trustedTwoFactorTokenToReturn = createRefreshToken();
        await storage.saveTrustedTwoFactorDeviceToken(
          trustedTwoFactorTokenToReturn,
          user.id,
          deviceInfo.deviceIdentifier,
          Date.now() + TWO_FACTOR_REMEMBER_TTL_MS2
        );
      }
    }
    const deviceSession = await resolveDeviceSession(storage, user.id, deviceInfo);
    if (deviceSession) {
      await storage.upsertDevice(
        user.id,
        deviceSession.identifier,
        deviceInfo.deviceName,
        deviceInfo.deviceType,
        deviceSession.sessionStamp
      );
    }
    await rateLimit.clearLoginAttempts(loginIdentifier);
    const accessToken = await auth.generateAccessToken(user, deviceSession);
    const refreshToken = await auth.generateRefreshToken(user.id, deviceSession);
    const accountKeys = buildAccountKeys(user);
    const userDecryptionOptions = buildUserDecryptionOptions(user);
    await safeWriteAuditEvent(env, {
      actorUserId: user.id,
      action: "auth.login.success",
      category: "auth",
      level: "info",
      targetType: "user",
      targetId: user.id,
      metadata: {
        grantType,
        webSession: shouldUseWebSession(request),
        deviceIdentifier: deviceSession?.identifier ?? deviceInfo.deviceIdentifier,
        deviceType: deviceInfo.deviceType,
        ...auditRequestMetadata(request)
      }
    });
    const response = {
      access_token: accessToken,
      expires_in: LIMITS.auth.accessTokenTtlSeconds,
      token_type: "Bearer",
      ...shouldUseWebSession(request) ? { web_session: true } : { refresh_token: refreshToken },
      ...trustedTwoFactorTokenToReturn ? { TwoFactorToken: trustedTwoFactorTokenToReturn } : {},
      Key: user.key,
      PrivateKey: user.privateKey,
      AccountKeys: accountKeys,
      accountKeys,
      Kdf: user.kdfType,
      KdfIterations: user.kdfIterations,
      KdfMemory: user.kdfMemory,
      KdfParallelism: user.kdfParallelism,
      ForcePasswordReset: false,
      ResetMasterPassword: false,
      MasterPasswordPolicy: {
        Object: "masterPasswordPolicy"
      },
      ApiUseKeyConnector: false,
      scope: "api offline_access",
      unofficialServer: true,
      UserDecryptionOptions: userDecryptionOptions,
      userDecryptionOptions
    };
    const baseResponse = jsonResponse(response);
    return shouldUseWebSession(request) ? withWebRefreshCookie(request, baseResponse, refreshToken) : baseResponse;
  } else if (grantType === "client_credentials") {
    const clientId = body.client_id;
    const clientSecret = body.client_secret;
    const scope = body.scope;
    const deviceInfo = readAuthRequestDeviceInfo(body, request);
    const loginIdentifier = `${clientIdentifier}:${clientId}`;
    const parmValid = checkClientCredentialsParam(clientId, clientSecret, scope);
    if (!parmValid) {
      return identityErrorResponse("Parameter error", "invalid_request", 400);
    }
    const loginCheck = await rateLimit.checkLoginAttempt(loginIdentifier);
    if (!loginCheck.allowed) {
      return identityErrorResponse(
        `Too many failed login attempts. Try again in ${Math.ceil(loginCheck.retryAfterSeconds / 60)} minutes.`,
        "TooManyRequests",
        429
      );
    }
    const uid = clientId.slice(5);
    const user = await storage.getUserById(uid);
    if (!user) {
      await rateLimit.recordFailedLogin(loginIdentifier);
      return identityErrorResponse("ClientId or clientSecret is incorrect. Try again", "invalid_grant", 400);
    }
    if (user.status !== "active") {
      await rateLimit.recordFailedLogin(loginIdentifier);
      await safeWriteAuditEvent(env, {
        actorUserId: user.id,
        action: "auth.login.failed.user_inactive",
        category: "auth",
        level: "warn",
        targetType: "user",
        targetId: user.id,
        metadata: {
          grantType,
          deviceIdentifier: deviceInfo.deviceIdentifier,
          ...auditRequestMetadata(request)
        }
      });
      return identityErrorResponse("Account is disabled", "invalid_grant", 400);
    }
    if (!user.apiKey || !constantTimeEquals(clientSecret, user.apiKey)) {
      await rateLimit.recordFailedLogin(loginIdentifier);
      await safeWriteAuditEvent(env, {
        actorUserId: user.id,
        action: "auth.login.failed.bad_api_key",
        category: "auth",
        level: "warn",
        targetType: "user",
        targetId: user.id,
        metadata: {
          grantType,
          deviceIdentifier: deviceInfo.deviceIdentifier,
          ...auditRequestMetadata(request)
        }
      });
      return identityErrorResponse("ClientId or clientSecret is incorrect. Try again", "invalid_grant", 400);
    }
    const deviceSession = await resolveDeviceSession(storage, user.id, deviceInfo);
    if (deviceSession) {
      await storage.upsertDevice(
        user.id,
        deviceSession.identifier,
        deviceInfo.deviceName,
        deviceInfo.deviceType,
        deviceSession.sessionStamp
      );
    }
    await rateLimit.clearLoginAttempts(loginIdentifier);
    const accessToken = await auth.generateAccessToken(user, deviceSession);
    const refreshToken = await auth.generateRefreshToken(user.id, deviceSession);
    const accountKeys = buildAccountKeys(user);
    const userDecryptionOptions = buildUserDecryptionOptions(user);
    await safeWriteAuditEvent(env, {
      actorUserId: user.id,
      action: "auth.login.success",
      category: "auth",
      level: "info",
      targetType: "user",
      targetId: user.id,
      metadata: {
        grantType,
        webSession: shouldUseWebSession(request),
        deviceIdentifier: deviceSession?.identifier ?? deviceInfo.deviceIdentifier,
        deviceType: deviceInfo.deviceType,
        ...auditRequestMetadata(request)
      }
    });
    const response = {
      access_token: accessToken,
      expires_in: LIMITS.auth.accessTokenTtlSeconds,
      token_type: "Bearer",
      ...shouldUseWebSession(request) ? { web_session: true } : { refresh_token: refreshToken },
      Key: user.key,
      PrivateKey: user.privateKey,
      AccountKeys: accountKeys,
      accountKeys,
      Kdf: user.kdfType,
      KdfIterations: user.kdfIterations,
      KdfMemory: user.kdfMemory,
      KdfParallelism: user.kdfParallelism,
      ForcePasswordReset: false,
      ResetMasterPassword: false,
      MasterPasswordPolicy: {
        Object: "masterPasswordPolicy"
      },
      ApiUseKeyConnector: false,
      scope: "api offline_access",
      unofficialServer: true,
      UserDecryptionOptions: userDecryptionOptions,
      userDecryptionOptions
    };
    const baseResponse = jsonResponse(response);
    return shouldUseWebSession(request) ? withWebRefreshCookie(request, baseResponse, refreshToken) : baseResponse;
  } else if (grantType === "send_access") {
    const sendAccessLimit = await rateLimit.consumeBudget(`${clientIdentifier}:public`, LIMITS.rateLimit.publicRequestsPerMinute);
    if (!sendAccessLimit.allowed) {
      return identityErrorResponse(
        `Rate limit exceeded. Try again in ${sendAccessLimit.retryAfterSeconds} seconds.`,
        "TooManyRequests",
        429
      );
    }
    const sendId = String(body.send_id || body.sendId || "").trim();
    if (!sendId) {
      return jsonResponse(
        {
          error: "invalid_request",
          error_description: "send_id is required",
          send_access_error_type: "invalid_send_id",
          ErrorModel: {
            Message: "send_id is required",
            Object: "error"
          }
        },
        400
      );
    }
    const passwordHashB64 = String(
      body.password_hash_b64 || body.passwordHashB64 || body.passwordHash || body.password_hash || ""
    ).trim() || null;
    const password = String(body.password || "").trim() || null;
    const result = await issueSendAccessToken(
      env,
      sendId,
      passwordHashB64,
      password,
      rateLimit,
      `${clientIdentifier}:send-password`
    );
    if ("error" in result) {
      return result.error;
    }
    return jsonResponse({
      access_token: result.token,
      expires_in: LIMITS.auth.sendAccessTokenTtlSeconds,
      token_type: "Bearer",
      scope: "api.send",
      unofficialServer: true
    });
  } else if (grantType === "refresh_token") {
    const refreshLimit = await rateLimit.consumeBudget(
      `${clientIdentifier}:identity-refresh`,
      LIMITS.rateLimit.refreshTokenRequestsPerMinute
    );
    if (!refreshLimit.allowed) {
      return identityErrorResponse(
        `Rate limit exceeded. Try again in ${refreshLimit.retryAfterSeconds} seconds.`,
        "TooManyRequests",
        429
      );
    }
    const refreshToken = String(body.refresh_token || "").trim() || (shouldUseWebSession(request) ? parseCookieValue(request, WEB_REFRESH_COOKIE) : null);
    if (!refreshToken) {
      return identityErrorResponse("Refresh token is required", "invalid_request", 400);
    }
    const result = await auth.refreshAccessTokenDetailed(refreshToken);
    if (!result.ok) {
      await safeWriteAuditEvent(env, {
        actorUserId: result.userId ?? null,
        action: `auth.refresh.failed.${result.reason}`,
        category: "auth",
        level: "warn",
        targetType: result.deviceIdentifier ? "device" : "refreshToken",
        targetId: result.deviceIdentifier ?? null,
        metadata: {
          grantType,
          reason: result.reason,
          webSession: shouldUseWebSession(request),
          ...auditRequestMetadata(request)
        }
      });
      const invalidResponse = identityErrorResponse("Invalid refresh token", "invalid_grant", 400);
      return shouldUseWebSession(request) ? withWebRefreshCookie(request, invalidResponse, null) : invalidResponse;
    }
    await storage.constrainRefreshTokenExpiry(
      refreshToken,
      Date.now() + LIMITS.auth.refreshTokenOverlapGraceMs
    );
    const { accessToken, user, device } = result;
    if (device?.identifier) {
      await storage.touchDeviceLastSeen(user.id, device.identifier);
    }
    const newRefreshToken = await auth.generateRefreshToken(user.id, device);
    const accountKeys = buildAccountKeys(user);
    const userDecryptionOptions = buildUserDecryptionOptions(user);
    const response = {
      access_token: accessToken,
      expires_in: LIMITS.auth.accessTokenTtlSeconds,
      token_type: "Bearer",
      ...shouldUseWebSession(request) ? { web_session: true } : { refresh_token: newRefreshToken },
      Key: user.key,
      PrivateKey: user.privateKey,
      AccountKeys: accountKeys,
      accountKeys,
      Kdf: user.kdfType,
      KdfIterations: user.kdfIterations,
      KdfMemory: user.kdfMemory,
      KdfParallelism: user.kdfParallelism,
      ForcePasswordReset: false,
      ResetMasterPassword: false,
      MasterPasswordPolicy: {
        Object: "masterPasswordPolicy"
      },
      ApiUseKeyConnector: false,
      scope: "api offline_access",
      unofficialServer: true,
      UserDecryptionOptions: userDecryptionOptions,
      userDecryptionOptions
    };
    const baseResponse = jsonResponse(response);
    return shouldUseWebSession(request) ? withWebRefreshCookie(request, baseResponse, newRefreshToken) : baseResponse;
  }
  return identityErrorResponse("Unsupported grant type", "unsupported_grant_type", 400);
}
__name(handleToken, "handleToken");
async function handlePrelogin(request, env) {
  const storage = new StorageService(env.DB);
  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON", 400);
  }
  const email = body.email?.toLowerCase();
  if (!email) {
    return errorResponse("Email is required", 400);
  }
  const user = await storage.getUser(email);
  const kdfType = user?.kdfType ?? 0;
  const kdfIterations = user?.kdfIterations ?? LIMITS.auth.defaultKdfIterations;
  const kdfMemory = user?.kdfMemory ?? null;
  const kdfParallelism = user?.kdfParallelism ?? null;
  return jsonResponse(buildPreloginResponse(email, kdfType, kdfIterations, kdfMemory, kdfParallelism));
}
__name(handlePrelogin, "handlePrelogin");
async function handleRevocation(request, env) {
  const storage = new StorageService(env.DB);
  let body;
  const contentType = request.headers.get("content-type") || "";
  try {
    if (contentType.includes("application/x-www-form-urlencoded")) {
      const formData = await request.formData();
      body = Object.fromEntries(formData.entries());
    } else {
      body = await request.json();
    }
  } catch {
    return new Response(null, { status: 200 });
  }
  const token = String(body.token || "").trim() || (shouldUseWebSession(request) ? parseCookieValue(request, WEB_REFRESH_COOKIE) || "" : "");
  if (token) {
    await storage.deleteRefreshToken(token);
  }
  const baseResponse = new Response(null, { status: 200 });
  return shouldUseWebSession(request) ? withWebRefreshCookie(request, baseResponse, null) : baseResponse;
}
__name(handleRevocation, "handleRevocation");
function checkClientCredentialsParam(clientId, clientSecret, scope) {
  if (scope !== "api") {
    return false;
  }
  if (!clientId.startsWith("user.")) {
    return false;
  }
  if (!clientSecret) {
    return false;
  }
  return true;
}
__name(checkClientCredentialsParam, "checkClientCredentialsParam");

// src/handlers/notifications.ts
function extractAccessToken(request) {
  const url = new URL(request.url);
  const queryToken = String(url.searchParams.get("access_token") || "").trim();
  if (queryToken) return queryToken;
  const authHeader = String(request.headers.get("Authorization") || "").trim();
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match?.[1]?.trim() || null;
}
__name(extractAccessToken, "extractAccessToken");
async function authenticateNotificationsRequest(request, env) {
  const accessToken = extractAccessToken(request);
  if (!accessToken) return null;
  const auth = new AuthService(env);
  return auth.verifyAccessToken(`Bearer ${accessToken}`);
}
__name(authenticateNotificationsRequest, "authenticateNotificationsRequest");
async function handleNotificationsNegotiate(request, env) {
  const payload = await authenticateNotificationsRequest(request, env);
  if (!payload?.sub) return errorResponse("Unauthorized", 401);
  const connectionId = generateUUID();
  return jsonResponse({
    connectionId,
    connectionToken: connectionId,
    negotiateVersion: 1,
    availableTransports: [
      {
        transport: "WebSockets",
        transferFormats: ["Text", "Binary"]
      }
    ]
  });
}
__name(handleNotificationsNegotiate, "handleNotificationsNegotiate");
async function handleNotificationsHub(request, env) {
  const payload = await authenticateNotificationsRequest(request, env);
  if (!payload?.sub) return errorResponse("Unauthorized", 401);
  if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
    return errorResponse("Expected websocket", 426);
  }
  const userId = payload.sub;
  const id = env.NOTIFICATIONS_HUB.idFromName(userId);
  const stub = env.NOTIFICATIONS_HUB.get(id);
  const forwardedUrl = new URL(request.url);
  forwardedUrl.searchParams.set("nw_uid", userId);
  if (payload.did) {
    forwardedUrl.searchParams.set("nw_did", payload.did);
  }
  return stub.fetch(new Request(forwardedUrl.toString(), request));
}
__name(handleNotificationsHub, "handleNotificationsHub");

// src/router-public.ts
function isSameOriginWriteRequest(request) {
  const targetOrigin = new URL(request.url).origin;
  const origin = request.headers.get("Origin");
  if (origin) {
    return origin === targetOrigin;
  }
  const referer = request.headers.get("Referer");
  if (referer) {
    try {
      return new URL(referer).origin === targetOrigin;
    } catch {
      return false;
    }
  }
  return false;
}
__name(isSameOriginWriteRequest, "isSameOriginWriteRequest");
function getDefaultWebsiteIconSvg() {
  return `<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96" role="img" aria-label="Globe icon"><circle cx="48" cy="48" r="34" fill="none" stroke="#8ea9c7" stroke-width="6"/><path d="M14 48h68M48 14c10 10 16 21.5 16 34s-6 24-16 34c-10-10-16-21.5-16-34s6-24 16-34zm-24 10c8 5 17 8 24 8s16-3 24-8m-48 48c8-5 17-8 24-8s16 3 24 8" fill="none" stroke="#8ea9c7" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
}
__name(getDefaultWebsiteIconSvg, "getDefaultWebsiteIconSvg");
function handleNwFavicon() {
  return new Response(getDefaultWebsiteIconSvg(), {
    status: 200,
    headers: {
      "Content-Type": "image/svg+xml; charset=utf-8",
      "Cache-Control": `public, max-age=${LIMITS.cache.iconTtlSeconds}, immutable`
    }
  });
}
__name(handleNwFavicon, "handleNwFavicon");
function handleMissingWebsiteIcon() {
  return new Response(null, {
    status: 404,
    headers: {
      "Cache-Control": "public, max-age=300"
    }
  });
}
__name(handleMissingWebsiteIcon, "handleMissingWebsiteIcon");
function isPrivateIpv4(hostname) {
  const parts = hostname.split(".").map((part) => Number(part));
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return false;
  const [a, b] = parts;
  return a === 10 || a === 127 || a === 169 && b === 254 || a === 172 && b >= 16 && b <= 31 || a === 192 && b === 168 || a === 0;
}
__name(isPrivateIpv4, "isPrivateIpv4");
function isBlockedChangePasswordHost(hostname) {
  const normalized = hostname.toLowerCase().replace(/\.+$/, "");
  return normalized === "localhost" || normalized.endsWith(".localhost") || normalized.endsWith(".local") || normalized === "::1" || normalized.startsWith("[") || isPrivateIpv4(normalized);
}
__name(isBlockedChangePasswordHost, "isBlockedChangePasswordHost");
function parsePublicHttpUrl(rawUri) {
  if (!rawUri) return null;
  try {
    const url = new URL(rawUri);
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    if (isBlockedChangePasswordHost(url.hostname)) return null;
    return url;
  } catch {
    return null;
  }
}
__name(parsePublicHttpUrl, "parsePublicHttpUrl");
async function handleChangePasswordUri(request) {
  const sourceUrl = parsePublicHttpUrl(new URL(request.url).searchParams.get("uri"));
  if (!sourceUrl) {
    return jsonResponse({ uri: null });
  }
  const wellKnownUrl = new URL("/.well-known/change-password", sourceUrl.origin);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), ICON_UPSTREAM_TIMEOUT_MS);
  try {
    const response = await fetch(wellKnownUrl.toString(), {
      method: "GET",
      redirect: "manual",
      signal: controller.signal,
      cf: {
        cacheEverything: true,
        cacheTtl: LIMITS.cache.iconTtlSeconds
      }
    });
    if (response.status < 300 || response.status >= 400) {
      return jsonResponse({ uri: null });
    }
    const location = response.headers.get("Location");
    if (!location) return jsonResponse({ uri: null });
    const targetUrl = parsePublicHttpUrl(new URL(location, wellKnownUrl).toString());
    if (!targetUrl) return jsonResponse({ uri: null });
    return jsonResponse({ uri: targetUrl.toString() });
  } catch {
    return jsonResponse({ uri: null });
  } finally {
    clearTimeout(timeout);
  }
}
__name(handleChangePasswordUri, "handleChangePasswordUri");
function buildIconServiceBase(origin) {
  return `${origin}/icons`;
}
__name(buildIconServiceBase, "buildIconServiceBase");
function buildIconServiceTemplate(origin) {
  return `${buildIconServiceBase(origin)}/{}/icon.png`;
}
__name(buildIconServiceTemplate, "buildIconServiceTemplate");
function buildIconServiceCsp(origin) {
  return `img-src 'self' data: ${origin}`;
}
__name(buildIconServiceCsp, "buildIconServiceCsp");
function buildConfigResponse(origin) {
  return {
    version: LIMITS.compatibility.bitwardenServerVersion,
    gitHash: "nodewarden",
    server: null,
    environment: {
      cloudRegion: "self-hosted",
      vault: origin,
      api: origin + "/api",
      identity: origin + "/identity",
      notifications: origin + "/notifications",
      icons: origin,
      sso: "",
      fillAssistRules: null
    },
    push: {
      pushTechnology: 0,
      vapidPublicKey: null
    },
    communication: null,
    settings: {
      disableUserRegistration: false
    },
    _icon_service_url: buildIconServiceTemplate(origin),
    _icon_service_csp: buildIconServiceCsp(origin),
    featureStates: {
      "cipher-key-encryption": true,
      "duo-redirect": true,
      "email-verification": true,
      "pm-19051-send-email-verification": false,
      "pm-19148-innovation-archive": true,
      "unauth-ui-refresh": true,
      "web-push": false
    },
    object: "config"
  };
}
__name(buildConfigResponse, "buildConfigResponse");
function normalizeIconHost(rawHost) {
  let decoded;
  try {
    decoded = decodeURIComponent(String(rawHost || "").trim()).toLowerCase().replace(/\.+$/, "");
  } catch {
    return null;
  }
  if (!decoded || decoded.includes("/") || decoded.includes("\\")) return null;
  try {
    const parsed = new URL(`https://${decoded}`);
    return parsed.hostname === decoded ? decoded : null;
  } catch {
    return null;
  }
}
__name(normalizeIconHost, "normalizeIconHost");
var ICON_UPSTREAM_TIMEOUT_MS = 2500;
var BITWARDEN_DEFAULT_GLOBE_ICON_BYTES = 500;
var BITWARDEN_DEFAULT_GLOBE_ICON_SHA256 = "aaa64871332ad5b7d28fe8874efb19c2d9cc2f1e6de75d52b080b438225a0783";
async function fetchIconSource(source) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), ICON_UPSTREAM_TIMEOUT_MS);
  try {
    return await fetch(source.url, {
      headers: source.headers,
      redirect: "follow",
      signal: controller.signal,
      cf: {
        cacheEverything: true,
        cacheTtl: LIMITS.cache.iconTtlSeconds
      }
    });
  } finally {
    clearTimeout(timeout);
  }
}
__name(fetchIconSource, "fetchIconSource");
async function sha256Hex3(bytes) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
__name(sha256Hex3, "sha256Hex");
function iconResponse(body, contentType) {
  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": contentType || "image/png",
      "Cache-Control": `public, max-age=${LIMITS.cache.iconTtlSeconds}, immutable`
    }
  });
}
__name(iconResponse, "iconResponse");
async function handleWebsiteIcon(host, fallbackMode = "default") {
  const normalizedHost = normalizeIconHost(host);
  if (!normalizedHost) return fallbackMode === "not-found" ? handleMissingWebsiteIcon() : handleNwFavicon();
  const encodedHost = encodeURIComponent(normalizedHost);
  const requestHeaders = { "User-Agent": "NodeWarden/1.0" };
  const upstreamSources = [
    {
      url: `https://favicon.im/zh/${encodedHost}?larger=true&throw-error-on-404=true`,
      headers: requestHeaders
    },
    {
      url: `https://icons.bitwarden.net/${encodedHost}/icon.png`,
      rejectImage: {
        byteLength: BITWARDEN_DEFAULT_GLOBE_ICON_BYTES,
        sha256: BITWARDEN_DEFAULT_GLOBE_ICON_SHA256
      },
      headers: requestHeaders
    }
  ];
  for (const source of upstreamSources) {
    try {
      const resp = await fetchIconSource(source);
      if (!resp.ok) continue;
      const contentType = String(resp.headers.get("Content-Type") || "").toLowerCase();
      if (!contentType.startsWith("image/")) continue;
      if (!source.rejectImage) {
        return iconResponse(resp.body, resp.headers.get("Content-Type"));
      }
      const contentLength = Number(resp.headers.get("Content-Length") || "");
      if (Number.isFinite(contentLength) && contentLength > 0 && contentLength !== source.rejectImage.byteLength) {
        return iconResponse(resp.body, resp.headers.get("Content-Type"));
      }
      const bytes = await resp.arrayBuffer();
      if (bytes.byteLength === 0) continue;
      if (bytes.byteLength === source.rejectImage.byteLength && await sha256Hex3(bytes) === source.rejectImage.sha256) continue;
      return iconResponse(bytes, resp.headers.get("Content-Type"));
    } catch {
      continue;
    }
  }
  return fallbackMode === "not-found" ? handleMissingWebsiteIcon() : handleNwFavicon();
}
__name(handleWebsiteIcon, "handleWebsiteIcon");
async function buildWebBootstrapResponse(env) {
  const secret = (env.JWT_SECRET || "").trim();
  const jwtUnsafeReason = !secret ? "missing" : secret === DEFAULT_DEV_SECRET ? "default" : secret.length < LIMITS.auth.jwtSecretMinLength ? "too_short" : null;
  const storage = new StorageService(env.DB);
  const userCount = await storage.getUserCount();
  return {
    defaultKdfIterations: LIMITS.auth.defaultKdfIterations,
    jwtUnsafeReason,
    jwtSecretMinLength: LIMITS.auth.jwtSecretMinLength,
    registrationInviteRequired: userCount > 0
  };
}
__name(buildWebBootstrapResponse, "buildWebBootstrapResponse");
async function handlePublicRoute(request, env, path, method, enforcePublicRateLimit) {
  if (path === "/.well-known/appspecific/com.chrome.devtools.json" && method === "GET") {
    return new Response("{}", {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store"
      }
    });
  }
  if ((path === "/api/web-bootstrap" || path === "/web-bootstrap") && method === "GET") {
    const blocked = await enforcePublicRateLimit("public-read", LIMITS.rateLimit.publicReadRequestsPerMinute);
    if (blocked) return blocked;
    return jsonResponse(await buildWebBootstrapResponse(env));
  }
  if (path === "/icons/change-password-uri" && method === "GET") {
    const blocked = await enforcePublicRateLimit("public-read", LIMITS.rateLimit.publicReadRequestsPerMinute);
    if (blocked) return blocked;
    return handleChangePasswordUri(request);
  }
  const iconMatch = path.match(/^\/icons\/([^/]+)\/icon\.png$/i);
  if (iconMatch && method === "GET") {
    const fallbackMode = new URL(request.url).searchParams.get("fallback") === "404" ? "not-found" : "default";
    return handleWebsiteIcon(iconMatch[1], fallbackMode);
  }
  const publicAttachmentMatch = path.match(/^\/api\/attachments\/([a-f0-9-]+)\/([a-f0-9-]+)$/i);
  if (publicAttachmentMatch && method === "GET") {
    return handlePublicDownloadAttachment(request, env, publicAttachmentMatch[1], publicAttachmentMatch[2]);
  }
  const publicAttachmentUploadMatch = path.match(/^\/api\/ciphers\/([a-f0-9-]+)\/attachment\/([a-f0-9-]+)$/i);
  if (publicAttachmentUploadMatch && (method === "POST" || method === "PUT") && new URL(request.url).searchParams.has("token")) {
    return handlePublicUploadAttachment(request, env, publicAttachmentUploadMatch[1], publicAttachmentUploadMatch[2]);
  }
  const publicSendUploadMatch = path.match(/^\/api\/sends\/([^/]+)\/file\/([^/]+)\/?$/i);
  if (publicSendUploadMatch && (method === "POST" || method === "PUT") && new URL(request.url).searchParams.has("token")) {
    return handlePublicUploadSendFile(request, env, publicSendUploadMatch[1], publicSendUploadMatch[2]);
  }
  const sendAccessMatch = path.match(/^\/api\/sends\/access\/([^/]+)$/i);
  if (sendAccessMatch && method === "POST") {
    const blocked = await enforcePublicRateLimit();
    if (blocked) return blocked;
    return handleAccessSend(request, env, sendAccessMatch[1]);
  }
  if (path === "/api/sends/access" && method === "POST") {
    const blocked = await enforcePublicRateLimit();
    if (blocked) return blocked;
    return handleAccessSendV2(request, env);
  }
  const sendAccessFileV2Match = path.match(/^\/api\/sends\/access\/file\/([^/]+)\/?$/i);
  if (sendAccessFileV2Match && method === "POST") {
    const blocked = await enforcePublicRateLimit();
    if (blocked) return blocked;
    return handleAccessSendFileV2(request, env, sendAccessFileV2Match[1]);
  }
  const sendAccessFileMatch = path.match(/^\/api\/sends\/([^/]+)\/access\/file\/([^/]+)\/?$/i);
  if (sendAccessFileMatch && method === "POST") {
    const blocked = await enforcePublicRateLimit();
    if (blocked) return blocked;
    return handleAccessSendFile(request, env, sendAccessFileMatch[1], sendAccessFileMatch[2]);
  }
  const sendDownloadMatch = path.match(/^\/api\/sends\/([^/]+)\/([^/]+)\/?$/i);
  if (sendDownloadMatch && method === "GET") {
    return handleDownloadSendFile(request, env, sendDownloadMatch[1], sendDownloadMatch[2]);
  }
  if (path === "/identity/connect/token" && method === "POST") {
    return handleToken(request, env);
  }
  if (path === "/api/devices/knowndevice" && method === "GET") {
    const blocked = await enforcePublicRateLimit();
    if (blocked) return jsonResponse(false);
    return handleKnownDevice(request, env);
  }
  const clearDeviceTokenMatch = path.match(/^\/api\/devices\/identifier\/([^/]+)\/clear-token$/i);
  if (clearDeviceTokenMatch && (method === "PUT" || method === "POST")) {
    return new Response(null, { status: 200 });
  }
  if ((path === "/identity/connect/revocation" || path === "/identity/connect/revoke") && method === "POST") {
    const blocked = await enforcePublicRateLimit("public-sensitive", LIMITS.rateLimit.sensitivePublicRequestsPerMinute);
    if (blocked) return blocked;
    return handleRevocation(request, env);
  }
  if (path === "/identity/accounts/prelogin" && method === "POST") {
    const blocked = await enforcePublicRateLimit("public-sensitive", LIMITS.rateLimit.sensitivePublicRequestsPerMinute);
    if (blocked) return blocked;
    return handlePrelogin(request, env);
  }
  if (path === "/identity/accounts/prelogin/password" && method === "POST") {
    const blocked = await enforcePublicRateLimit("public-sensitive", LIMITS.rateLimit.sensitivePublicRequestsPerMinute);
    if (blocked) return blocked;
    return handlePrelogin(request, env);
  }
  if ((path === "/identity/accounts/recover-2fa" || path === "/api/accounts/recover-2fa") && method === "POST") {
    return handleRecoverTwoFactor(request, env);
  }
  if (path === "/api/accounts/password-hint" && method === "POST") {
    const blocked = await enforcePublicRateLimit("public-sensitive", LIMITS.rateLimit.sensitivePublicRequestsPerMinute);
    if (blocked) return blocked;
    if (!isSameOriginWriteRequest(request)) {
      return new Response(JSON.stringify({ error: "Forbidden origin" }), {
        status: 403,
        headers: { "Content-Type": "application/json" }
      });
    }
    return handleGetPasswordHint(request, env);
  }
  if ((path === "/config" || path === "/api/config") && method === "GET") {
    const blocked = await enforcePublicRateLimit("public-read", LIMITS.rateLimit.publicReadRequestsPerMinute);
    if (blocked) return blocked;
    const origin = new URL(request.url).origin;
    return jsonResponse(buildConfigResponse(origin));
  }
  if (path === "/api/version" && method === "GET") {
    const blocked = await enforcePublicRateLimit("public-read", LIMITS.rateLimit.publicReadRequestsPerMinute);
    if (blocked) return blocked;
    return jsonResponse(LIMITS.compatibility.bitwardenServerVersion);
  }
  if (path === "/api/accounts/register" && method === "POST") {
    const blocked = await enforcePublicRateLimit("register", LIMITS.rateLimit.registerRequestsPerMinute);
    if (blocked) return blocked;
    if (!isSameOriginWriteRequest(request)) {
      return new Response(JSON.stringify({ error: "Forbidden origin" }), {
        status: 403,
        headers: { "Content-Type": "application/json" }
      });
    }
    return handleRegister(request, env);
  }
  if (path === "/notifications/hub/negotiate" && method === "POST") {
    return handleNotificationsNegotiate(request, env);
  }
  if (path === "/notifications/hub" && method === "GET") {
    return handleNotificationsHub(request, env);
  }
  return null;
}
__name(handlePublicRoute, "handlePublicRoute");

// src/router.ts
function jwtSecretUnsafeReason2(env) {
  const secret = (env.JWT_SECRET || "").trim();
  if (!secret) return "missing";
  if (secret === DEFAULT_DEV_SECRET) return "default";
  if (secret.length < LIMITS.auth.jwtSecretMinLength) return "too_short";
  return null;
}
__name(jwtSecretUnsafeReason2, "jwtSecretUnsafeReason");
function isImportBypassRequest(request, path, method) {
  if (request.headers.get("X-NodeWarden-Import") !== "1") return false;
  if (method === "POST") {
    if (path === "/api/ciphers/import") return true;
    if (/^\/api\/ciphers\/[a-f0-9-]+\/attachment\/v2$/i.test(path)) return true;
    if (/^\/api\/ciphers\/[a-f0-9-]+\/attachment\/[a-f0-9-]+$/i.test(path)) return true;
  }
  return false;
}
__name(isImportBypassRequest, "isImportBypassRequest");
async function handleRequest(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;
  const clientId = getClientIdentifier(request);
  async function enforcePublicRateLimit(category = "public", maxRequests = LIMITS.rateLimit.publicRequestsPerMinute) {
    if (!clientId) {
      return new Response(
        JSON.stringify({
          error: "Forbidden",
          error_description: "Client IP is required"
        }),
        {
          status: 403,
          headers: { "Content-Type": "application/json" }
        }
      );
    }
    const rateLimit = new RateLimitService(env.DB);
    const check = await rateLimit.consumeBudget(`${clientId}:${category}`, maxRequests);
    if (check.allowed) return null;
    return new Response(
      JSON.stringify({
        error: "Too many requests",
        error_description: `Rate limit exceeded. Try again in ${check.retryAfterSeconds} seconds.`
      }),
      {
        status: 429,
        headers: {
          "Content-Type": "application/json",
          "Retry-After": String(check.retryAfterSeconds || 60),
          "X-RateLimit-Remaining": "0"
        }
      }
    );
  }
  __name(enforcePublicRateLimit, "enforcePublicRateLimit");
  if (method === "OPTIONS") {
    return handleCors(request);
  }
  try {
    const isLargeUploadPath = /^\/api\/ciphers\/[a-f0-9-]+\/attachment\/[a-f0-9-]+$/i.test(path) || /^\/api\/sends\/[a-f0-9-]+\/file\/[a-f0-9-]+$/i.test(path) || path === "/api/admin/backup/import";
    if (!isLargeUploadPath) {
      const contentLength = parseInt(request.headers.get("Content-Length") || "0", 10);
      if (contentLength > LIMITS.request.maxBodyBytes) {
        return errorResponse("Request body too large", 413);
      }
    }
    const publicResponse = await handlePublicRoute(request, env, path, method, enforcePublicRateLimit);
    if (publicResponse) return publicResponse;
    const secretIssue = jwtSecretUnsafeReason2(env);
    if (secretIssue) {
      return errorResponse("Server configuration error: JWT_SECRET is not set or too weak", 500);
    }
    const auth = new AuthService(env);
    const authHeader = request.headers.get("Authorization");
    const verified = await auth.verifyAccessTokenWithUser(authHeader);
    if (!verified) {
      return errorResponse("Unauthorized", 401);
    }
    const { payload, user: currentUser } = verified;
    const actingDeviceId = String(payload.did || "").trim();
    if (actingDeviceId) {
      const nextHeaders = new Headers(request.headers);
      nextHeaders.set("X-NodeWarden-Acting-Device-Id", actingDeviceId);
      request = new Request(request, { headers: nextHeaders });
    }
    const userId = payload.sub;
    if (currentUser.status !== "active") {
      return errorResponse("Account is disabled", 403);
    }
    if (!isImportBypassRequest(request, path, method)) {
      const rateLimit = new RateLimitService(env.DB);
      const rateLimitCheck = await rateLimit.consumeBudget(`${userId}:api`, LIMITS.rateLimit.apiRequestsPerMinute);
      if (!rateLimitCheck.allowed) {
        return new Response(
          JSON.stringify({
            error: "Too many requests",
            error_description: `Rate limit exceeded. Try again in ${rateLimitCheck.retryAfterSeconds} seconds.`
          }),
          {
            status: 429,
            headers: {
              "Content-Type": "application/json",
              "Retry-After": String(rateLimitCheck.retryAfterSeconds || 60),
              "X-RateLimit-Remaining": "0"
            }
          }
        );
      }
    }
    const authenticatedResponse = await handleAuthenticatedRoute(request, env, userId, currentUser, path, method);
    if (authenticatedResponse) return authenticatedResponse;
    return errorResponse("Not found", 404);
  } catch (error) {
    console.error("Request error:", error);
    return errorResponse("Internal server error", 500);
  }
}
__name(handleRequest, "handleRequest");

// src/index.ts
var dbInitialized = false;
var dbInitError = null;
var dbInitPromise = null;
function normalizeRequestUrl(request) {
  const url = new URL(request.url);
  const normalizedPathname = url.pathname.length <= 1 ? url.pathname : url.pathname.replace(/\/+$/, "");
  if (normalizedPathname === url.pathname) return request;
  url.pathname = normalizedPathname;
  return new Request(url.toString(), request);
}
__name(normalizeRequestUrl, "normalizeRequestUrl");
function isWorkerHandledPath(path) {
  return path.startsWith("/api/") || path.startsWith("/identity/") || path.startsWith("/icons/") || path.startsWith("/notifications/") || path.startsWith("/.well-known/") || path === "/config" || path === "/api/config" || path === "/api/version";
}
__name(isWorkerHandledPath, "isWorkerHandledPath");
function addSearchIndexHeaders(request, response) {
  const url = new URL(request.url);
  const contentType = String(response.headers.get("Content-Type") || "").toLowerCase();
  const shouldNoIndex = url.pathname === "/robots.txt" || contentType.includes("text/html");
  if (!shouldNoIndex) return response;
  const headers = new Headers(response.headers);
  headers.set("X-Robots-Tag", "noindex, nofollow, noarchive, nosnippet");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}
__name(addSearchIndexHeaders, "addSearchIndexHeaders");
async function maybeServeAsset(request, env) {
  if (!env.ASSETS) return null;
  if (request.method !== "GET" && request.method !== "HEAD") return null;
  const url = new URL(request.url);
  if (isWorkerHandledPath(url.pathname)) return null;
  const response = await env.ASSETS.fetch(request);
  return addSearchIndexHeaders(request, response);
}
__name(maybeServeAsset, "maybeServeAsset");
async function ensureDatabaseInitialized(env) {
  if (dbInitialized) return;
  if (!dbInitPromise) {
    dbInitPromise = (async () => {
      const storage = new StorageService(env.DB);
      await storage.initializeDatabase();
      dbInitialized = true;
      dbInitError = null;
    })().catch((error) => {
      console.error("Failed to initialize database:", error);
      dbInitError = error instanceof Error ? error.message : "Unknown database initialization error";
    }).finally(() => {
      dbInitPromise = null;
    });
  }
  await dbInitPromise;
}
__name(ensureDatabaseInitialized, "ensureDatabaseInitialized");
var index_default = {
  async fetch(request, env, ctx) {
    void ctx;
    const normalizedRequest = normalizeRequestUrl(request);
    const assetResponse = await maybeServeAsset(normalizedRequest, env);
    if (assetResponse) {
      return applyCors(normalizedRequest, assetResponse);
    }
    await ensureDatabaseInitialized(env);
    if (dbInitError) {
      console.error("DB init error (not forwarded to client):", dbInitError);
      const resp2 = jsonResponse(
        {
          error: "Database not initialized",
          error_description: "Database initialization failed. Check server logs for details.",
          ErrorModel: {
            Message: "Service temporarily unavailable",
            Object: "error"
          }
        },
        500
      );
      return applyCors(normalizedRequest, resp2);
    }
    const resp = await handleRequest(normalizedRequest, env);
    return applyCors(normalizedRequest, resp);
  },
  async scheduled(controller, env, ctx) {
    void controller;
    await ensureDatabaseInitialized(env);
    if (dbInitError) {
      console.error("Skipping scheduled backup because DB init failed:", dbInitError);
      return;
    }
    ctx.waitUntil(runScheduledBackupIfDue(env).catch((error) => {
      console.error("Scheduled backup failed:", error);
    }));
  }
};
export {
  NotificationsHub,
  index_default as default
};
//# sourceMappingURL=index.js.map
