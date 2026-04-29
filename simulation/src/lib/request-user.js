function firstNonEmpty(...values) {
  for (const value of values) {
    if (value === null || value === undefined) {
      continue;
    }
    const normalized = String(value).trim();
    if (normalized) {
      return normalized;
    }
  }
  return null;
}

export function resolveRequestUser(req, { allowBody = false } = {}) {
  const authenticatedUser =
    req?.authenticatedUser && typeof req.authenticatedUser === "object"
      ? req.authenticatedUser
      : null;
  if (authenticatedUser?.userId) {
    return {
      userId: authenticatedUser.userId,
      email: authenticatedUser.email || null,
      displayName: authenticatedUser.displayName || null,
      authMode: authenticatedUser.authMode || "password"
    };
  }

  const body = allowBody && req?.body && typeof req.body === "object" ? req.body : {};
  const query = req?.query && typeof req.query === "object" ? req.query : {};
  const headers = req?.headers && typeof req.headers === "object" ? req.headers : {};

  const userId = firstNonEmpty(
    headers["x-user-id"],
    query.user_id,
    body.user_id,
    process.env.APP_DEV_USER_ID
  );
  const email = firstNonEmpty(
    headers["x-user-email"],
    query.user_email,
    body.user_email,
    process.env.APP_DEV_USER_EMAIL
  );
  const displayName = firstNonEmpty(
    headers["x-user-display-name"],
    query.user_display_name,
    body.user_display_name,
    process.env.APP_DEV_USER_DISPLAY_NAME
  );

  if (!userId && !email && !displayName) {
    return null;
  }

  return {
    userId,
    email,
    displayName,
    authMode: "dev"
  };
}
