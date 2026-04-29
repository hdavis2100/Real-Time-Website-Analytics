import { createHash, randomUUID } from "node:crypto";

import {
  getAppDatabase,
  hasAppDatabaseConfig,
  query,
  withTransaction
} from "./app-db.js";
import { hashSessionToken, normalizeEmail } from "./auth.js";

export class AppStoreConflictError extends Error {
  constructor(message) {
    super(message);
    this.name = "AppStoreConflictError";
  }
}

function toIsoString(value) {
  if (!value) {
    return null;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  return String(value);
}

function normalizedDecisionBackend(value) {
  if (value === "heuristic") {
    return "heuristic";
  }
  return "llm";
}

const RUN_STATUS_RANK = {
  queued: 0,
  running: 1,
  finalizing: 2,
  completed: 3,
  failed: 3
};

function normalizedRunStatus(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(RUN_STATUS_RANK, normalized)
    ? normalized
    : null;
}

function runStatusRank(value) {
  const normalized = normalizedRunStatus(value);
  return normalized == null ? -1 : RUN_STATUS_RANK[normalized];
}

function parseTimestampMaybe(value) {
  if (!value) {
    return null;
  }
  const parsed = value instanceof Date ? value : new Date(String(value));
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function earlierTimestamp(left, right) {
  const leftDate = parseTimestampMaybe(left);
  const rightDate = parseTimestampMaybe(right);
  if (leftDate && rightDate) {
    return leftDate <= rightDate ? leftDate.toISOString() : rightDate.toISOString();
  }
  return leftDate?.toISOString() || rightDate?.toISOString() || null;
}

function laterTimestamp(left, right) {
  const leftDate = parseTimestampMaybe(left);
  const rightDate = parseTimestampMaybe(right);
  if (leftDate && rightDate) {
    return leftDate >= rightDate ? leftDate.toISOString() : rightDate.toISOString();
  }
  return leftDate?.toISOString() || rightDate?.toISOString() || null;
}

function numericMax(left, right) {
  const leftNumber = left == null ? null : Number(left);
  const rightNumber = right == null ? null : Number(right);
  const usableLeft = Number.isFinite(leftNumber) ? leftNumber : null;
  const usableRight = Number.isFinite(rightNumber) ? rightNumber : null;
  if (usableLeft == null) {
    return usableRight;
  }
  if (usableRight == null) {
    return usableLeft;
  }
  return Math.max(usableLeft, usableRight);
}

function normalizeHeroProfitTimeseries(value) {
  const rows = Array.isArray(value) ? value : [];
  const byHandNumber = new Map();
  for (const row of rows) {
    if (!row || typeof row !== "object") {
      continue;
    }
    const handNumber = Number(row.hand_number);
    const cumulative = Number(row.cumulative_bb_won);
    if (!Number.isFinite(handNumber) || handNumber < 0 || !Number.isFinite(cumulative)) {
      continue;
    }
    byHandNumber.set(String(handNumber), {
      simulation_run_id: row.simulation_run_id || null,
      hand_number: Math.floor(handNumber),
      cumulative_bb_won: cumulative,
      event_ts: row.event_ts || null
    });
  }
  return [...byHandNumber.values()].sort((left, right) => left.hand_number - right.hand_number);
}

function mergeHeroProfitTimeseries(existingRows, nextRows) {
  const merged = normalizeHeroProfitTimeseries(existingRows);
  const incoming = normalizeHeroProfitTimeseries(nextRows);
  if (!incoming.length) {
    return merged;
  }
  return normalizeHeroProfitTimeseries([...merged, ...incoming]);
}

function contextHash(contextText) {
  const normalized = String(contextText || "").trim();
  if (!normalized) {
    return null;
  }
  return createHash("sha256").update(normalized).digest("hex");
}

function serializeUser(row) {
  if (!row) {
    return null;
  }
  return {
    user_id: row.id,
    email: row.email,
    display_name: row.display_name,
    created_at: toIsoString(row.created_at),
    updated_at: toIsoString(row.updated_at)
  };
}

function serializeAgentVersion(row) {
  if (!row) {
    return null;
  }
  return {
    agent_version_id: row.id,
    id: row.id,
    agent_definition_id: row.agent_definition_id,
    version_number: Number(row.version_number),
    context_text: row.context_text,
    context_hash: row.context_hash,
    decision_backend_default: row.decision_backend_default,
    metadata: row.metadata || {},
    created_at: toIsoString(row.created_at)
  };
}

function serializeAgent(row) {
  if (!row) {
    return null;
  }
  return {
    agent_definition_id: row.id,
    id: row.id,
    user_id: row.user_id,
    name: row.name,
    description: row.description,
    is_archived: Boolean(row.is_archived),
    created_at: toIsoString(row.created_at),
    updated_at: toIsoString(row.updated_at),
    latest_version: row.latest_version_id
      ? {
          agent_version_id: row.latest_version_id,
          id: row.latest_version_id,
          version_number: Number(row.latest_version_number),
          context_hash: row.latest_context_hash,
          decision_backend_default: row.latest_decision_backend_default,
          created_at: toIsoString(row.latest_version_created_at)
        }
      : null
  };
}

function serializeSimulationRun(row) {
  if (!row) {
    return null;
  }
  return {
    simulation_run_id: row.id,
    user_id: row.user_id,
    hero_agent_definition_id: row.hero_agent_definition_id,
    hero_agent_version_id: row.hero_agent_version_id,
    status: row.status,
    decision_backend: row.decision_backend,
    backend_type: row.backend_type,
    model_name: row.model_name,
    request_mode: row.request_mode,
    hand_count: row.hand_count == null ? null : Number(row.hand_count),
    seed: row.seed == null ? null : Number(row.seed),
    hero_seat: row.hero_seat == null ? null : Number(row.hero_seat),
    hero_context_hash: row.hero_context_hash,
    hero_context_preview: row.hero_context_preview,
    request_payload: row.request_payload || {},
    published_actions:
      row.published_actions == null ? 0 : Number(row.published_actions),
    published_hand_summaries:
      row.published_hand_summaries == null
        ? 0
        : Number(row.published_hand_summaries),
    player_summaries: Array.isArray(row.player_summaries)
      ? row.player_summaries
      : [],
    profile_session_features: Array.isArray(row.profile_session_features)
      ? row.profile_session_features
      : [],
    hero_profit_timeseries: normalizeHeroProfitTimeseries(row.hero_profit_timeseries),
    error_message: row.error_message,
    requested_at: toIsoString(row.requested_at),
    started_at: toIsoString(row.started_at),
    completed_at: toIsoString(row.completed_at),
    finished_at: toIsoString(row.completed_at),
    updated_at: toIsoString(row.updated_at)
  };
}

function serializeSavedQuery(row) {
  if (!row) {
    return null;
  }
  return {
    saved_query_id: row.id,
    id: row.id,
    user_id: row.user_id,
    name: row.name,
    query_type: row.query_type,
    filters: row.filters || {},
    created_at: toIsoString(row.created_at),
    updated_at: toIsoString(row.updated_at)
  };
}

function serializeProfileJob(row) {
  if (!row) {
    return null;
  }
  return {
    profile_job_id: row.id,
    id: row.id,
    simulation_run_id: row.simulation_run_id,
    user_id: row.user_id,
    agent_definition_id: row.agent_definition_id,
    agent_version_id: row.agent_version_id,
    player_id: row.player_id,
    agent_id: row.agent_id,
    status: row.status,
    refresh_requested: Boolean(row.refresh_requested),
    requested_at: toIsoString(row.requested_at),
    completed_at: toIsoString(row.completed_at),
    error_message: row.error_message
  };
}

function serializeAppSession(row) {
  if (!row) {
    return null;
  }
  return {
    session_id: row.session_id || row.id,
    user_id: row.user_id,
    created_at: toIsoString(row.created_at || row.session_created_at),
    expires_at: toIsoString(row.expires_at)
  };
}

function handleWriteError(error, message) {
  if (error?.code === "23505") {
    throw new AppStoreConflictError(message);
  }
  throw error;
}

async function upsertUserRecord(client, { userId, email = null, displayName = null }) {
  if (!userId) {
    return null;
  }

  const normalizedEmail = normalizeEmail(email);

  const result = await client.query(
    `
      insert into users (id, email, display_name)
      values ($1, $2, $3)
      on conflict (id) do update
      set email = coalesce(excluded.email, users.email),
          display_name = coalesce(excluded.display_name, users.display_name),
          updated_at = now()
      returning *
    `,
    [userId, normalizedEmail, displayName]
  );
  return serializeUser(result.rows[0]);
}

export function createAppStore() {
  return {
    isConfigured() {
      return Boolean(getAppDatabase());
    },

    async ensureUser({ userId, email = null, displayName = null }) {
      if (!userId) {
        return null;
      }
      return withTransaction((client) =>
        upsertUserRecord(client, { userId, email, displayName })
      );
    },

    async listUsersByIds(userIds = []) {
      const ids = [
        ...new Set(
          (Array.isArray(userIds) ? userIds : [])
            .map((userId) => String(userId || "").trim())
            .filter(Boolean)
        )
      ];
      if (!ids.length) {
        return [];
      }

      const result = await query(
        `
          select *
          from users
          where id::text = any($1::text[])
        `,
        [ids]
      );
      return result.rows.map(serializeUser);
    },

    async createPasswordUser({ email, displayName = null, passwordHash }) {
      const normalized = normalizeEmail(email);
      if (!normalized || !passwordHash) {
        return null;
      }

      try {
        return await withTransaction(async (client) => {
          const userId = randomUUID();
          const authIdentityId = randomUUID();
          const userResult = await client.query(
            `
              insert into users (id, email, display_name)
              values ($1, $2, $3)
              returning *
            `,
            [userId, normalized, displayName]
          );

          await client.query(
            `
              insert into auth_identities (
                id,
                user_id,
                provider,
                provider_subject,
                password_hash
              )
              values ($1, $2, 'password', $3, $4)
            `,
            [authIdentityId, userId, normalized, passwordHash]
          );

          return serializeUser(userResult.rows[0]);
        });
      } catch (error) {
        handleWriteError(error, "A user with that email already exists");
      }
    },

    async registerPasswordUser({
      email,
      displayName = null,
      passwordHash,
      sessionToken,
      expiresAt,
      ipAddress = null,
      userAgent = null
    }) {
      const normalized = normalizeEmail(email);
      if (
        !normalized ||
        !passwordHash ||
        !sessionToken ||
        !expiresAt
      ) {
        return null;
      }

      try {
        return await withTransaction(async (client) => {
          const userId = randomUUID();
          const authIdentityId = randomUUID();
          const sessionId = randomUUID();
          const userResult = await client.query(
            `
              insert into users (id, email, display_name)
              values ($1, $2, $3)
              returning *
            `,
            [userId, normalized, displayName]
          );

          await client.query(
            `
              insert into auth_identities (
                id,
                user_id,
                provider,
                provider_subject,
                password_hash
              )
              values ($1, $2, 'password', $3, $4)
            `,
            [authIdentityId, userId, normalized, passwordHash]
          );

          const sessionResult = await client.query(
            `
              insert into app_sessions (
                id,
                user_id,
                session_token_hash,
                ip_address,
                user_agent,
                expires_at
              )
              values ($1, $2, $3, $4, $5, $6::timestamptz)
              returning *
            `,
            [
              sessionId,
              userId,
              hashSessionToken(sessionToken),
              ipAddress,
              userAgent,
              expiresAt
            ]
          );

          return {
            user: serializeUser(userResult.rows[0]),
            session: serializeAppSession(sessionResult.rows[0])
          };
        });
      } catch (error) {
        handleWriteError(error, "A user with that email already exists");
      }
    },

    async getPasswordAuthIdentityByEmail(email) {
      const normalized = normalizeEmail(email);
      if (!normalized) {
        return null;
      }

      const result = await query(
        `
          select
            u.*,
            ai.id as auth_identity_id,
            ai.password_hash
          from auth_identities ai
          join users u on u.id = ai.user_id
          where ai.provider = 'password'
            and ai.provider_subject = $1
            and ai.password_hash is not null
          limit 1
        `,
        [normalized]
      );

      if (result.rowCount === 0) {
        return null;
      }

      return {
        auth_identity_id: result.rows[0].auth_identity_id,
        password_hash: result.rows[0].password_hash,
        user: serializeUser(result.rows[0])
      };
    },

    async createSession({
      userId,
      sessionToken,
      expiresAt,
      ipAddress = null,
      userAgent = null
    }) {
      if (!userId || !sessionToken || !expiresAt) {
        return null;
      }

      const result = await query(
        `
          insert into app_sessions (
            id,
            user_id,
            session_token_hash,
            ip_address,
            user_agent,
            expires_at
          )
          values ($1, $2, $3, $4, $5, $6::timestamptz)
          returning *
        `,
        [
          randomUUID(),
          userId,
          hashSessionToken(sessionToken),
          ipAddress,
          userAgent,
          expiresAt
        ]
      );

      return serializeAppSession(result.rows[0]);
    },

    async getSessionByToken(sessionToken) {
      if (!sessionToken) {
        return null;
      }

      const tokenHash = hashSessionToken(sessionToken);
      const result = await query(
        `
          select
            s.id as session_id,
            s.user_id,
            s.created_at as session_created_at,
            s.expires_at,
            u.id,
            u.email,
            u.display_name,
            u.created_at,
            u.updated_at
          from app_sessions s
          join users u on u.id = s.user_id
          where s.session_token_hash = $1
            and s.expires_at > now()
          limit 1
        `,
        [tokenHash]
      );

      if (result.rowCount === 0) {
        await query(
          `
            delete from app_sessions
            where session_token_hash = $1
              and expires_at <= now()
          `,
          [tokenHash]
        ).catch(() => null);
        return null;
      }

      return {
        session: serializeAppSession(result.rows[0]),
        user: serializeUser(result.rows[0])
      };
    },

    async deleteSessionByToken(sessionToken) {
      if (!sessionToken) {
        return false;
      }

      const result = await query(
        `
          delete from app_sessions
          where session_token_hash = $1
        `,
        [hashSessionToken(sessionToken)]
      );

      return result.rowCount > 0;
    },

    async createAgent({
      userId,
      name,
      description = null,
      contextText,
      decisionBackendDefault = "llm",
      metadata = {}
    }) {
      const normalizedBackend = normalizedDecisionBackend(decisionBackendDefault);
      const hashedContext = contextHash(contextText);

      return withTransaction(async (client) => {
        const agentDefinitionId = randomUUID();
        const agentVersionId = randomUUID();

        const definitionResult = await client.query(
          `
            insert into agent_definitions (id, user_id, name, description)
            values ($1, $2, $3, $4)
            returning *
          `,
          [agentDefinitionId, userId, name, description]
        );

        const versionResult = await client.query(
          `
            insert into agent_versions (
              id,
              agent_definition_id,
              version_number,
              context_text,
              context_hash,
              decision_backend_default,
              metadata
            )
            values ($1, $2, 1, $3, $4, $5, $6::jsonb)
            returning *
          `,
          [
            agentVersionId,
            agentDefinitionId,
            contextText,
            hashedContext,
            normalizedBackend,
            JSON.stringify(metadata || {})
          ]
        );

        return {
          agent: {
            ...serializeAgent(definitionResult.rows[0]),
            latest_version: serializeAgentVersion(versionResult.rows[0])
          },
          version: serializeAgentVersion(versionResult.rows[0])
        };
      });
    },

    async listAgents({ userId, includeArchived = false }) {
      const params = [];
      const where = [];

      if (userId) {
        params.push(userId);
        where.push(`d.user_id = $${params.length}`);
      }
      if (!includeArchived) {
        where.push("d.is_archived = false");
      }

      const result = await query(
        `
          select
            d.*,
            lv.id as latest_version_id,
            lv.version_number as latest_version_number,
            lv.context_hash as latest_context_hash,
            lv.decision_backend_default as latest_decision_backend_default,
            lv.created_at as latest_version_created_at
          from agent_definitions d
          left join lateral (
            select *
            from agent_versions v
            where v.agent_definition_id = d.id
            order by v.version_number desc
            limit 1
          ) lv on true
          ${where.length ? `where ${where.join(" and ")}` : ""}
          order by d.updated_at desc, d.created_at desc
        `,
        params
      );

      return result.rows.map(serializeAgent);
    },

    async getAgent(agentDefinitionId, { userId = null } = {}) {
      const definitionResult = await query(
        `
          select
            d.*,
            lv.id as latest_version_id,
            lv.version_number as latest_version_number,
            lv.context_hash as latest_context_hash,
            lv.decision_backend_default as latest_decision_backend_default,
            lv.created_at as latest_version_created_at
          from agent_definitions d
          left join lateral (
            select *
            from agent_versions v
            where v.agent_definition_id = d.id
            order by v.version_number desc
            limit 1
          ) lv on true
          where d.id = $1
          ${userId ? "and d.user_id = $2" : ""}
          limit 1
        `,
        userId ? [agentDefinitionId, userId] : [agentDefinitionId]
      );

      if (definitionResult.rowCount === 0) {
        return null;
      }

      const versionsResult = await query(
        `
          select *
          from agent_versions
          where agent_definition_id = $1
          order by version_number desc
        `,
        [agentDefinitionId]
      );

      return {
        ...serializeAgent(definitionResult.rows[0]),
        versions: versionsResult.rows.map(serializeAgentVersion)
      };
    },

    async deleteAgent(agentDefinitionId, { userId = null } = {}) {
      const params = [agentDefinitionId];
      const where = ["id = $1"];

      if (userId) {
        params.push(userId);
        where.push(`user_id = $${params.length}`);
      }

      const result = await query(
        `
          delete from agent_definitions
          where ${where.join(" and ")}
          returning *
        `,
        params
      );

      if (result.rowCount === 0) {
        return null;
      }

      return serializeAgent(result.rows[0]);
    },

    async createAgentVersion({
      agentDefinitionId,
      userId = null,
      contextText,
      decisionBackendDefault = "llm",
      metadata = {}
    }) {
      const normalizedBackend = normalizedDecisionBackend(decisionBackendDefault);
      const hashedContext = contextHash(contextText);

      return withTransaction(async (client) => {
        const definitionResult = await client.query(
          `
            select *
            from agent_definitions
            where id = $1
            ${userId ? "and user_id = $2" : ""}
            limit 1
          `,
          userId ? [agentDefinitionId, userId] : [agentDefinitionId]
        );

        if (definitionResult.rowCount === 0) {
          return null;
        }

        const nextVersionResult = await client.query(
          `
            select coalesce(max(version_number), 0) + 1 as next_version_number
            from agent_versions
            where agent_definition_id = $1
          `,
          [agentDefinitionId]
        );
        const nextVersionNumber = Number(
          nextVersionResult.rows[0]?.next_version_number || 1
        );

        const versionResult = await client.query(
          `
            insert into agent_versions (
              id,
              agent_definition_id,
              version_number,
              context_text,
              context_hash,
              decision_backend_default,
              metadata
            )
            values ($1, $2, $3, $4, $5, $6, $7::jsonb)
            returning *
          `,
          [
            randomUUID(),
            agentDefinitionId,
            nextVersionNumber,
            contextText,
            hashedContext,
            normalizedBackend,
            JSON.stringify(metadata || {})
          ]
        );

        await client.query(
          `
            update agent_definitions
            set updated_at = now()
            where id = $1
          `,
          [agentDefinitionId]
        );

        return serializeAgentVersion(versionResult.rows[0]);
      });
    },

    async getAgentVersion(agentVersionId, { userId = null } = {}) {
      const result = await query(
        `
          select v.*, d.user_id
          from agent_versions v
          join agent_definitions d on d.id = v.agent_definition_id
          where v.id = $1
          ${userId ? "and d.user_id = $2" : ""}
          limit 1
        `,
        userId ? [agentVersionId, userId] : [agentVersionId]
      );

      if (result.rowCount === 0) {
        return null;
      }

      return {
        ...serializeAgentVersion(result.rows[0]),
        user_id: result.rows[0].user_id
      };
    },

    async createSimulationRun({
      simulationRunId,
      userId = null,
      heroAgentDefinitionId = null,
      heroAgentVersionId = null,
      status = "queued",
      decisionBackend = "llm",
      backendType = null,
      modelName = null,
      requestMode = "context",
      handCount,
      seed,
      heroSeat,
      heroContextHash = null,
      heroContextPreview = null,
      requestPayload = {},
      publishedActions = 0,
      publishedHandSummaries = 0,
      playerSummaries = [],
      profileSessionFeatures = [],
      heroProfitTimeseries = [],
      errorMessage = null,
      requestedAt = new Date().toISOString(),
      startedAt = null,
      completedAt = null
    }) {
      try {
        const result = await query(
          `
            insert into simulation_runs (
              id,
              user_id,
              hero_agent_definition_id,
              hero_agent_version_id,
              status,
              decision_backend,
              backend_type,
              model_name,
              request_mode,
              hand_count,
              seed,
              hero_seat,
              hero_context_hash,
              hero_context_preview,
              request_payload,
              published_actions,
              published_hand_summaries,
              player_summaries,
              profile_session_features,
              hero_profit_timeseries,
              error_message,
              requested_at,
              started_at,
              completed_at
            )
            values (
              $1, $2, $3, $4, $5, $6, $7, $8, $9,
              $10, $11, $12, $13, $14, $15::jsonb, $16, $17, $18::jsonb, $19::jsonb, $20::jsonb, $21, $22, $23, $24
            )
            returning *
          `,
          [
            simulationRunId,
            userId,
            heroAgentDefinitionId,
            heroAgentVersionId,
            status,
            normalizedDecisionBackend(decisionBackend),
            backendType,
            modelName,
            requestMode,
            handCount,
            seed,
            heroSeat,
            heroContextHash,
            heroContextPreview,
            JSON.stringify(requestPayload || {}),
            publishedActions == null ? 0 : Number(publishedActions),
            publishedHandSummaries == null ? 0 : Number(publishedHandSummaries),
            JSON.stringify(Array.isArray(playerSummaries) ? playerSummaries : []),
            JSON.stringify(
              Array.isArray(profileSessionFeatures) ? profileSessionFeatures : []
            ),
            JSON.stringify(normalizeHeroProfitTimeseries(heroProfitTimeseries)),
            errorMessage,
            requestedAt,
            startedAt,
            completedAt
          ]
        );
        return serializeSimulationRun(result.rows[0]);
      } catch (error) {
        handleWriteError(
          error,
          `simulation_run_id already exists: ${simulationRunId}`
        );
      }
    },

    async createSavedQuery({
      userId,
      name,
      queryType = "hero_performance",
      filters = {}
    }) {
      try {
        const result = await query(
          `
            insert into saved_queries (
              id,
              user_id,
              name,
              query_type,
              filters
            )
            values ($1, $2, $3, $4, $5::jsonb)
            returning *
          `,
          [
            randomUUID(),
            userId,
            name,
            queryType,
            JSON.stringify(filters || {})
          ]
        );
        return serializeSavedQuery(result.rows[0]);
      } catch (error) {
        handleWriteError(error, `saved query already exists: ${name}`);
      }
    },

    async listSavedQueries({ userId, queryType = null, limit = 100 } = {}) {
      const params = [];
      const where = [];

      if (userId) {
        params.push(userId);
        where.push(`user_id = $${params.length}`);
      }
      if (queryType) {
        params.push(queryType);
        where.push(`query_type = $${params.length}`);
      }

      params.push(Math.max(1, Math.min(Number(limit) || 100, 500)));
      const limitPlaceholder = `$${params.length}`;

      const result = await query(
        `
          select *
          from saved_queries
          ${where.length ? `where ${where.join(" and ")}` : ""}
          order by updated_at desc, created_at desc
          limit ${limitPlaceholder}
        `,
        params
      );

      return result.rows.map(serializeSavedQuery);
    },

    async getSavedQuery(savedQueryId, { userId = null } = {}) {
      const result = await query(
        `
          select *
          from saved_queries
          where id = $1
          ${userId ? "and user_id = $2" : ""}
          limit 1
        `,
        userId ? [savedQueryId, userId] : [savedQueryId]
      );

      if (result.rowCount === 0) {
        return null;
      }

      return serializeSavedQuery(result.rows[0]);
    },

    async createProfileJob({
      simulationRunId,
      userId = null,
      agentDefinitionId = null,
      agentVersionId = null,
      playerId = null,
      agentId = null,
      status = "running",
      refreshRequested = false,
      requestedAt = new Date().toISOString(),
      completedAt = null,
      errorMessage = null
    }) {
      try {
        const result = await query(
          `
            insert into profile_jobs (
              id,
              simulation_run_id,
              user_id,
              agent_definition_id,
              agent_version_id,
              player_id,
              agent_id,
              status,
              refresh_requested,
              requested_at,
              completed_at,
              error_message
            )
            values (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
            )
            returning *
          `,
          [
            randomUUID(),
            simulationRunId,
            userId,
            agentDefinitionId,
            agentVersionId,
            playerId,
            agentId,
            status,
            Boolean(refreshRequested),
            requestedAt,
            completedAt,
            errorMessage
          ]
        );
        return serializeProfileJob(result.rows[0]);
      } catch (error) {
        handleWriteError(error, `profile job already exists for: ${simulationRunId}`);
      }
    },

    async updateProfileJob(
      profileJobId,
      {
        status,
        completedAt = new Date().toISOString(),
        errorMessage = null
      } = {}
    ) {
      const result = await query(
        `
          update profile_jobs
          set status = coalesce($2, status),
              completed_at = $3,
              error_message = $4
          where id = $1
          returning *
        `,
        [profileJobId, status || null, completedAt, errorMessage]
      );

      if (result.rowCount === 0) {
        return null;
      }

      return serializeProfileJob(result.rows[0]);
    },

    async listProfileJobs({
      simulationRunId = null,
      userId = null,
      limit = 50
    } = {}) {
      const params = [];
      const where = [];

      if (simulationRunId) {
        params.push(simulationRunId);
        where.push(`simulation_run_id = $${params.length}`);
      }
      if (userId) {
        params.push(userId);
        where.push(`user_id = $${params.length}`);
      }

      params.push(Math.max(1, Math.min(Number(limit) || 50, 500)));
      const limitPlaceholder = `$${params.length}`;

      const result = await query(
        `
          select *
          from profile_jobs
          ${where.length ? `where ${where.join(" and ")}` : ""}
          order by requested_at desc, completed_at desc nulls last
          limit ${limitPlaceholder}
        `,
        params
      );

      return result.rows.map(serializeProfileJob);
    },

    async listSimulationRuns({
      userId = null,
      status = null,
      decisionBackend = null,
      heroAgentDefinitionId = null,
      heroAgentVersionId = null,
      limit = 50,
      offset = 0
    } = {}) {
      const params = [];
      const where = [];

      if (userId) {
        params.push(userId);
        where.push(`user_id = $${params.length}`);
      }
      if (status) {
        params.push(status);
        where.push(`status = $${params.length}`);
      }
      if (decisionBackend) {
        params.push(normalizedDecisionBackend(decisionBackend));
        where.push(`decision_backend = $${params.length}`);
      }
      if (heroAgentDefinitionId) {
        params.push(heroAgentDefinitionId);
        where.push(`hero_agent_definition_id = $${params.length}`);
      }
      if (heroAgentVersionId) {
        params.push(heroAgentVersionId);
        where.push(`hero_agent_version_id = $${params.length}`);
      }

      params.push(Math.max(1, Math.min(Number(limit) || 50, 5000)));
      const limitPlaceholder = `$${params.length}`;
      params.push(Math.max(0, Number(offset) || 0));
      const offsetPlaceholder = `$${params.length}`;

      const result = await query(
        `
          select *
          from simulation_runs
          ${where.length ? `where ${where.join(" and ")}` : ""}
          order by requested_at desc, updated_at desc
          limit ${limitPlaceholder}
          offset ${offsetPlaceholder}
        `,
        params
      );

      return result.rows.map(serializeSimulationRun);
    },

    async listLiveSimulationRuns({
      userId = null,
      decisionBackend = null,
      limit = 50
    } = {}) {
      const params = [];
      const where = ["status in ('running', 'finalizing')"];

      if (userId) {
        params.push(userId);
        where.push(`user_id = $${params.length}`);
      }
      if (decisionBackend) {
        params.push(normalizedDecisionBackend(decisionBackend));
        where.push(`decision_backend = $${params.length}`);
      }

      params.push(Math.max(1, Math.min(Number(limit) || 50, 500)));
      const limitPlaceholder = `$${params.length}`;

      const result = await query(
        `
          select *
          from simulation_runs
          where ${where.join(" and ")}
          order by started_at desc nulls last, updated_at desc, requested_at desc
          limit ${limitPlaceholder}
        `,
        params
      );

      return result.rows.map(serializeSimulationRun);
    },

    async getSimulationRun(simulationRunId, { userId = null } = {}) {
      const result = await query(
        `
          select *
          from simulation_runs
          where id = $1
          ${userId ? "and user_id = $2" : ""}
          limit 1
        `,
        userId ? [simulationRunId, userId] : [simulationRunId]
      );

      if (result.rowCount === 0) {
        return null;
      }

      return serializeSimulationRun(result.rows[0]);
    },

    async deleteSimulationRun(simulationRunId, { userId = null } = {}) {
      const params = [simulationRunId];
      const where = ["id = $1"];

      if (userId) {
        params.push(userId);
        where.push(`user_id = $${params.length}`);
      }

      const result = await query(
        `
          delete from simulation_runs
          where ${where.join(" and ")}
          returning *
        `,
        params
      );

      if (result.rowCount === 0) {
        return null;
      }

      return serializeSimulationRun(result.rows[0]);
    },

    async updateSimulationRunLifecycle(
      simulationRunId,
      {
        userId = null,
        heroAgentDefinitionId = null,
        heroAgentVersionId = null,
        status = null,
        decisionBackend = null,
        backendType = null,
        modelName = null,
        requestMode = null,
        handCount = null,
        seed = null,
        heroSeat = null,
        heroContextHash = null,
        heroContextPreview = null,
        requestedAt = null,
        startedAt = null,
        completedAt = null,
        clearCompletedAt = false,
        publishedActions = null,
        publishedHandSummaries = null,
        playerSummaries = null,
        profileSessionFeatures = null,
        heroProfitTimeseries = null,
        errorMessage = null,
        clearErrorMessage = false
      } = {}
    ) {
      return withTransaction(async (client) => {
        const existingResult = await client.query(
          `
            select *
            from simulation_runs
            where id = $1
            for update
          `,
          [simulationRunId]
        );

        if (existingResult.rowCount === 0) {
          return null;
        }

        const existing = existingResult.rows[0];
        if (userId) {
          await upsertUserRecord(client, { userId });
        }

        const existingStatus = normalizedRunStatus(existing.status);
        let nextStatus = normalizedRunStatus(status);
        const statusRegression =
          nextStatus != null &&
          existingStatus != null &&
          runStatusRank(nextStatus) < runStatusRank(existingStatus);

        if (statusRegression) {
          nextStatus = null;
          clearCompletedAt = false;
          clearErrorMessage = false;
          completedAt = null;
          errorMessage = null;
          playerSummaries = null;
          profileSessionFeatures = null;
          heroProfitTimeseries = null;
        }

        const nextStartedAt = earlierTimestamp(existing.started_at, startedAt);
        const nextCompletedAt = laterTimestamp(existing.completed_at, completedAt);
        const nextPublishedActions = numericMax(
          existing.published_actions,
          publishedActions
        );
        const nextPublishedHandSummaries = numericMax(
          existing.published_hand_summaries,
          publishedHandSummaries
        );
        const nextHeroProfitTimeseries = mergeHeroProfitTimeseries(
          existing.hero_profit_timeseries,
          heroProfitTimeseries
        );

        const result = await client.query(
          `
            update simulation_runs
            set user_id = coalesce($2, user_id),
                hero_agent_definition_id = coalesce($3, hero_agent_definition_id),
                hero_agent_version_id = coalesce($4, hero_agent_version_id),
                status = coalesce($5, status),
                decision_backend = coalesce($6, decision_backend),
                backend_type = coalesce($7, backend_type),
                model_name = coalesce($8, model_name),
                request_mode = coalesce($9, request_mode),
                hand_count = coalesce($10, hand_count),
                seed = coalesce($11, seed),
                hero_seat = coalesce($12, hero_seat),
                hero_context_hash = coalesce($13, hero_context_hash),
                hero_context_preview = coalesce($14, hero_context_preview),
                error_message = case
                  when $15 then null
                  when $16::text is not null then $16::text
                  else error_message
                end,
                requested_at = coalesce($17::timestamptz, requested_at),
                started_at = coalesce($18::timestamptz, started_at),
                completed_at = case
                  when $19 then null
                  when $20::timestamptz is not null then $20::timestamptz
                  else completed_at
                end,
                published_actions = coalesce($21, published_actions),
                published_hand_summaries = coalesce($22, published_hand_summaries),
                player_summaries = coalesce($23::jsonb, player_summaries),
                profile_session_features = coalesce($24::jsonb, profile_session_features),
                hero_profit_timeseries = $25::jsonb,
                updated_at = now()
            where id = $1
            returning *
          `,
          [
            simulationRunId,
            userId,
            heroAgentDefinitionId,
            heroAgentVersionId,
            nextStatus,
            decisionBackend ? normalizedDecisionBackend(decisionBackend) : null,
            backendType,
            modelName,
            requestMode,
            handCount == null ? null : Number(handCount),
            seed == null ? null : Number(seed),
            heroSeat == null ? null : Number(heroSeat),
            heroContextHash,
            heroContextPreview,
            Boolean(clearErrorMessage),
            errorMessage,
            requestedAt,
            nextStartedAt,
            Boolean(clearCompletedAt),
            nextCompletedAt,
            nextPublishedActions,
            nextPublishedHandSummaries,
            playerSummaries == null
              ? null
              : JSON.stringify(Array.isArray(playerSummaries) ? playerSummaries : []),
            profileSessionFeatures == null
              ? null
              : JSON.stringify(
                  Array.isArray(profileSessionFeatures)
                    ? profileSessionFeatures
                    : []
                ),
            JSON.stringify(nextHeroProfitTimeseries)
          ]
        );

        return serializeSimulationRun(result.rows[0]);
      });
    },

    async markSimulationRunFailed(simulationRunId, errorMessage) {
      const result = await query(
        `
          update simulation_runs
          set status = 'failed',
              error_message = $2,
              completed_at = coalesce(completed_at, now()),
              updated_at = now()
          where id = $1
          returning *
        `,
        [simulationRunId, errorMessage]
      );

      if (result.rowCount === 0) {
        return null;
      }

      return serializeSimulationRun(result.rows[0]);
    }
  };
}

let appStoreSingleton = null;

export function getAppStore() {
  if (!hasAppDatabaseConfig()) {
    return null;
  }
  if (!appStoreSingleton) {
    appStoreSingleton = createAppStore();
  }
  return appStoreSingleton;
}
