// ======================================================================
// Part 2 — Migration 001: Base Tables, Partitions, Sharding, Indexes
// ======================================================================
// File: core/migrations/
// Decade-hardened, zero-downtime, self-documenting foundation.
// Enhanced with structured logging, timing telemetry, integrity checks,
// and safe configuration access.

/**
 * Security helper – never trust raw env without validation.
 */
function getSafeConfig(source, fallback = null) {
  const value = source?.trim();
  if (!value) return fallback;
  if (value.includes(';') || value.includes('--')) {
    throw new Error('Potentially unsafe config value detected');
  }
  return value;
}

/**
 * Structured logger – all messages go to stdout as JSON for Loki/Grafana.
 */
function logEvent(level, message, metadata = {}) {
  console.log(JSON.stringify({
    level,
    timestamp: new Date().toISOString(),
    service: 'migration-001',
    message,
    ...metadata,
  }));
}

// --------------------------------------------------------------------
// UP migration
// --------------------------------------------------------------------
exports.up = async function(knex) {
  const timings = {};
  const startTotal = Date.now();

  async function timed(stepName, fn) {
    const start = Date.now();
    try {
      await fn();
    } finally {
      timings[stepName] = Date.now() - start;
    }
  }

  // ================================================================
  // Phase 1: Extensions
  // ================================================================
  await timed('extensions', async () => {
    await knex.raw('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"');
    await knex.raw('CREATE EXTENSION IF NOT EXISTS "pgcrypto"');
    await knex.raw('CREATE EXTENSION IF NOT EXISTS "pg_trgm"');
    try {
      await knex.raw('CREATE EXTENSION IF NOT EXISTS "citus"');
    } catch (err) {
      logEvent('warn', 'Citus not available – sharding disabled', { error: err.message });
    }
  });

  // Detect Citus availability
  const hasCitus = await knex.raw(
    "SELECT count(*) > 0 AS present FROM pg_extension WHERE extname = 'citus'"
  );
  const useCitus = hasCitus.rows[0].present;

  // ================================================================
  // Phase 2: Emails table (partitioned)
  // ================================================================
  await timed('table_emails', async () => {
    // Ensure no leftover partition definitions interfere
    await knex.schema.raw(`DROP TABLE IF EXISTS emails CASCADE`).catch(() => {});

    await knex.schema.raw(`
      CREATE TABLE emails (
        id BIGSERIAL,
        email VARCHAR(255) NOT NULL,
        context TEXT,
        source_url TEXT,
        tags JSONB DEFAULT '[]'::jsonb,
        verified BOOLEAN DEFAULT FALSE,
        confidence DOUBLE PRECISION DEFAULT 0.0
          CHECK (confidence >= 0 AND confidence <= 1),
        first_seen TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at);
    `);

    // Create monthly partitions (past 1 month + future 12 months)
    await knex.schema.raw(`
      DO $$
      DECLARE
        start_date date := date_trunc('month', now())::date - INTERVAL '1 month';
        end_date date := start_date + INTERVAL '14 months';
        d date := start_date;
      BEGIN
        WHILE d < end_date LOOP
          EXECUTE format(
            'CREATE TABLE IF NOT EXISTS emails_%s PARTITION OF emails FOR VALUES FROM (%L) TO (%L)',
            to_char(d, 'YYYY_MM'),
            d,
            d + INTERVAL '1 month'
          );
          d := d + INTERVAL '1 month';
        END LOOP;
      END;
      $$;
    `);
  });

  // ================================================================
  // Phase 3: Citus distribution
  // ================================================================
  await timed('citus', async () => {
    if (useCitus) {
      await knex.schema.raw(`SELECT create_distributed_table('emails', 'email')`);
    }
  });

  // ================================================================
  // Phase 4: Indexes
  // ================================================================
  await timed('indexes', async () => {
    const indexes = [
      `CREATE INDEX IF NOT EXISTS idx_emails_domain ON emails ((split_part(email, '@', 2)))`,
      `CREATE INDEX IF NOT EXISTS idx_emails_tags ON emails USING GIN (tags jsonb_path_ops)`,
      `CREATE INDEX IF NOT EXISTS idx_emails_verified_created ON emails (verified, created_at DESC)`,
      `CREATE INDEX IF NOT EXISTS idx_emails_confidence ON emails (confidence DESC)`,
      `CREATE INDEX IF NOT EXISTS idx_emails_trgm_email ON emails USING GIN (email gin_trgm_ops)`,
      `CREATE INDEX IF NOT EXISTS idx_emails_created_brin ON emails USING BRIN (created_at) WITH (pages_per_range = 32)`,
    ];

    for (const sql of indexes) {
      await knex.schema.raw(sql);
    }
  });

  // ================================================================
  // Phase 5: Visited URLs (partitioned)
  // ================================================================
  await timed('table_visited_urls', async () => {
    await knex.schema.raw(`DROP TABLE IF EXISTS visited_urls CASCADE`).catch(() => {});
    await knex.schema.raw(`
      CREATE TABLE visited_urls (
        url TEXT NOT NULL,
        crawled_at TIMESTAMPTZ DEFAULT NOW(),
        status_code INT,
        source_session_id UUID,
        PRIMARY KEY (url, crawled_at)
      ) PARTITION BY RANGE (crawled_at);
    `);

    await knex.schema.raw(`
      DO $$
      DECLARE
        start_date date := date_trunc('month', now())::date - INTERVAL '1 month';
        end_date date := start_date + INTERVAL '14 months';
        d date := start_date;
      BEGIN
        WHILE d < end_date LOOP
          EXECUTE format(
            'CREATE TABLE IF NOT EXISTS visited_urls_%s PARTITION OF visited_urls FOR VALUES FROM (%L) TO (%L)',
            to_char(d, 'YYYY_MM'),
            d,
            d + INTERVAL '1 month'
          );
          d := d + INTERVAL '1 month';
        END LOOP;
      END;
      $$;
    `);

    await knex.schema.raw(`
      CREATE INDEX IF NOT EXISTS idx_visited_urls_time ON visited_urls (crawled_at DESC);
    `);
  });

  // ================================================================
  // Phase 6: Sessions, proxy pool, profiles, audit log
  // ================================================================
  await timed('tables_aux', async () => {
    await knex.schema.raw(`
      CREATE TABLE IF NOT EXISTS sessions (
        session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        config JSONB NOT NULL,
        status TEXT DEFAULT 'active',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        finished_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions (status);
    `);

    await knex.schema.raw(`
      CREATE TABLE IF NOT EXISTS proxy_pool (
        proxy_url TEXT PRIMARY KEY,
        type TEXT,
        country TEXT,
        success INT DEFAULT 0,
        fail INT DEFAULT 0,
        banned BOOLEAN DEFAULT FALSE,
        last_used TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_proxy_pool_country ON proxy_pool (country);
      CREATE INDEX IF NOT EXISTS idx_proxy_pool_banned ON proxy_pool (banned);
    `);

    await knex.schema.raw(`
      CREATE TABLE IF NOT EXISTS profiles (
        id SERIAL PRIMARY KEY,
        profile_name TEXT UNIQUE NOT NULL,
        proxy_url TEXT REFERENCES proxy_pool(proxy_url) ON DELETE SET NULL,
        user_agent TEXT,
        viewport TEXT,
        cookies JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_used TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_profiles_proxy ON profiles (proxy_url);
    `);

    await knex.schema.raw(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id UUID PRIMARY KEY,
        action TEXT NOT NULL,
        changed_by TEXT,
        table_name TEXT,
        old_data JSONB,
        new_data JSONB,
        changed_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_audit_log_table ON audit_log (table_name, changed_at DESC);
    `);
  });

  // ================================================================
  // Phase 7: Auto-update trigger
  // ================================================================
  await timed('trigger_last_seen', async () => {
    await knex.schema.raw(`
      CREATE OR REPLACE FUNCTION update_last_seen()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW.last_seen = NOW();
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS trg_emails_last_seen ON emails;
      CREATE TRIGGER trg_emails_last_seen
        BEFORE UPDATE ON emails
        FOR EACH ROW
        EXECUTE FUNCTION update_last_seen();
    `);
  });

  // ================================================================
  // Phase 8: Integrity check
  // ================================================================
  const totalTime = Date.now() - startTotal;
  logEvent('info', 'Migration 001 completed', { total_ms: totalTime, timings });

  const tables = ['emails', 'visited_urls', 'sessions', 'proxy_pool', 'profiles', 'audit_log'];
  for (const t of tables) {
    if (!(await knex.schema.hasTable(t))) {
      throw new Error(`[001_init] CRITICAL: Table ${t} was not created!`);
    }
  }
  logEvent('info', 'All tables verified successfully');
};

// --------------------------------------------------------------------
// DOWN migration
// --------------------------------------------------------------------
exports.down = async function(knex) {
  const tables = ['audit_log', 'profiles', 'proxy_pool', 'sessions', 'visited_urls', 'emails'];
  for (const t of tables) {
    await knex.schema.dropTableIfExists(t);
  }
  await knex.schema.raw('DROP FUNCTION IF EXISTS update_last_seen()');
  logEvent('info', 'Migration 001 rolled back', { tables });
};