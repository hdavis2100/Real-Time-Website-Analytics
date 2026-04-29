import { createHash, randomBytes, scrypt as scryptCallback, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";

const scrypt = promisify(scryptCallback);

const PASSWORD_HASH_ALGO = "scrypt";
const PASSWORD_HASH_N = 16384;
const PASSWORD_HASH_R = 8;
const PASSWORD_HASH_P = 1;
const PASSWORD_HASH_BYTES = 64;
const DEFAULT_SESSION_COOKIE_NAME = "poker_app_session";
const DEFAULT_SESSION_TTL_DAYS = 14;

function base64UrlToBuffer(value) {
  return Buffer.from(String(value || ""), "base64url");
}

function shouldUseSecureCookies(req) {
  const forced = String(process.env.APP_SESSION_COOKIE_SECURE || "").trim().toLowerCase();
  if (forced === "true") {
    return true;
  }
  if (forced === "false") {
    return false;
  }
  const forwardedProto = String(req?.get?.("x-forwarded-proto") || "").trim().toLowerCase();
  return Boolean(req?.secure || forwardedProto === "https");
}

function sessionCookieName() {
  return (
    String(process.env.APP_SESSION_COOKIE_NAME || DEFAULT_SESSION_COOKIE_NAME).trim() ||
    DEFAULT_SESSION_COOKIE_NAME
  );
}

export function normalizeEmail(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized || null;
}

export function validatePasswordStrength(password) {
  const normalized = String(password || "");
  if (normalized.length < 8) {
    return "Password must be at least 8 characters.";
  }
  if (!/[a-z]/i.test(normalized) || !/\d/.test(normalized)) {
    return "Password must include at least one letter and one number.";
  }
  return null;
}

export async function hashPassword(password) {
  const validationMessage = validatePasswordStrength(password);
  if (validationMessage) {
    throw new Error(validationMessage);
  }

  const salt = randomBytes(16).toString("base64url");
  const derivedKey = await scrypt(password, salt, PASSWORD_HASH_BYTES, {
    N: PASSWORD_HASH_N,
    r: PASSWORD_HASH_R,
    p: PASSWORD_HASH_P
  });
  return [
    PASSWORD_HASH_ALGO,
    PASSWORD_HASH_N,
    PASSWORD_HASH_R,
    PASSWORD_HASH_P,
    salt,
    Buffer.from(derivedKey).toString("base64url")
  ].join("$");
}

export async function verifyPassword(password, storedHash) {
  const [algorithm, n, r, p, salt, encodedKey] = String(storedHash || "").split("$");
  if (
    algorithm !== PASSWORD_HASH_ALGO ||
    !n ||
    !r ||
    !p ||
    !salt ||
    !encodedKey
  ) {
    return false;
  }

  const derivedKey = await scrypt(password, salt, base64UrlToBuffer(encodedKey).length, {
    N: Number(n),
    r: Number(r),
    p: Number(p)
  });
  const expectedKey = base64UrlToBuffer(encodedKey);
  const actualKey = Buffer.from(derivedKey);

  if (expectedKey.length !== actualKey.length) {
    return false;
  }

  return timingSafeEqual(expectedKey, actualKey);
}

export function createSessionToken() {
  return randomBytes(32).toString("base64url");
}

export function hashSessionToken(sessionToken) {
  return createHash("sha256").update(String(sessionToken || "")).digest("hex");
}

export function getSessionTtlMs() {
  const configuredDays = Number(process.env.APP_SESSION_TTL_DAYS || DEFAULT_SESSION_TTL_DAYS);
  const ttlDays = Number.isFinite(configuredDays) && configuredDays > 0
    ? configuredDays
    : DEFAULT_SESSION_TTL_DAYS;
  return ttlDays * 24 * 60 * 60 * 1000;
}

export function buildSessionExpiry(now = new Date()) {
  return new Date(now.getTime() + getSessionTtlMs());
}

export function parseCookies(headerValue) {
  return String(headerValue || "")
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean)
    .reduce((cookies, entry) => {
      const separatorIndex = entry.indexOf("=");
      if (separatorIndex <= 0) {
        return cookies;
      }
      const key = entry.slice(0, separatorIndex).trim();
      const value = entry.slice(separatorIndex + 1).trim();
      cookies[key] = decodeURIComponent(value);
      return cookies;
    }, {});
}

export function getSessionTokenFromRequest(req) {
  const cookies = parseCookies(req?.headers?.cookie);
  return cookies[sessionCookieName()] || null;
}

function buildCookieString(token, { maxAgeSeconds, expiresAt = null, req } = {}) {
  const cookieParts = [
    `${sessionCookieName()}=${encodeURIComponent(token)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax"
  ];
  if (typeof maxAgeSeconds === "number") {
    cookieParts.push(`Max-Age=${Math.max(0, Math.floor(maxAgeSeconds))}`);
  }
  if (expiresAt instanceof Date && !Number.isNaN(expiresAt.getTime())) {
    cookieParts.push(`Expires=${expiresAt.toUTCString()}`);
  }
  if (shouldUseSecureCookies(req)) {
    cookieParts.push("Secure");
  }
  return cookieParts.join("; ");
}

export function setSessionCookie(res, token, expiresAt, req) {
  const maxAgeSeconds = Math.max(
    0,
    Math.floor((new Date(expiresAt).getTime() - Date.now()) / 1000)
  );
  res.append("Set-Cookie", buildCookieString(token, { maxAgeSeconds, expiresAt, req }));
}

export function clearSessionCookie(res, req) {
  res.append(
    "Set-Cookie",
    buildCookieString("", {
      maxAgeSeconds: 0,
      expiresAt: new Date(0),
      req
    })
  );
}
