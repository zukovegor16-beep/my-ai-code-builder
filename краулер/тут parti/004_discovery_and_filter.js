// ======================================================================
// Part 4 — Self‑Evolving Discovery, Filter & Paid DB Stub (AI Core)
// ======================================================================
// File: core/migrations/004_discovery_and_filter.js
// Purpose: Industrial‑grade, self‑optimising tables. All issues fixed.
//          Unique URLs, priority checks, extra indexes for performance.

function logEvent(level, message, metadata = {}) {
  console.log(JSON.stringify({
    level,
    timestamp: new Date().toISOString(),
    service: 'migration-004',
    trace_id: metadata.trace_id || 'genesis-004',
    message,
    ...metadata,
  }));
}

async function timed(stepName, fn) {
  const start = process.hrtime.bigint();
  await fn();
  const end = process.hrtime.bigint();
  const duration_us = Number(end - start) / 1000;
  logEvent('info', `Step completed`, { step: stepName, duration_us });
}

// --------------------------------------------------------------------
// UP migration
// --------------------------------------------------------------------
exports.up = async function(knex) {
  const traceId = `migration-004-${Date.now()}`;
  logEvent('info', 'Initializing self‑evolving discovery and filter layer', { trace_id: traceId });

  try {
    await knex.transaction(async (trx) => {
      // ================================================================
      // 0. Environment Detection
      // ================================================================
      let environment = {};
      try {
        const pgVersion = await trx.raw('SHOW server_version');
        const citusCheck = await trx.raw(
          "SELECT count(*) > 0 AS present FROM pg_extension WHERE extname = 'citus'"
        );
        environment = {
          pg_version: pgVersion.rows[0].server_version,
          citus_available: citusCheck.rows[0].present,
        };
        logEvent('info', 'Environment detected', { environment });
      } catch (err) {
        logEvent('warn', 'Environment detection partial', { error: err.message });
      }

      // ================================================================
      // 1. Extensions
      // ================================================================
      await timed('extensions', async () => {
        const exts = ['uuid-ossp', 'pg_trgm'];
        for (const ext of exts) {
          try {
            await trx.raw(`CREATE EXTENSION IF NOT EXISTS "${ext}"`);
          } catch (err) {
            logEvent('warn', `Extension ${ext} not available`, { error: err.message });
          }
        }
      });

      // ================================================================
      // 2. AI Calibration table
      // ================================================================
      await timed('table_discovery_ai_calibration', async () => {
        await trx.schema.createTable('discovery_ai_calibration', (table) => {
          table.text('metric').primary();
          table.float('value').defaultTo(1.0);
          table.timestamp('last_updated').defaultTo(trx.fn.now());
        });
        await trx.raw('ALTER TABLE discovery_ai_calibration ADD CONSTRAINT ck_dcal_value_range CHECK (value >= 0 AND value <= 1000)');
        await trx('discovery_ai_calibration').insert([
          { metric: 'min_success_rate', value: 0.02 },
          { metric: 'max_interval_hours', value: 168 },
          { metric: 'stale_site_days', value: 90 },
          { metric: 'max_filter_rules', value: 1000 },
        ]).onConflict('metric').ignore();
      });

      // ================================================================
      // 3. Discovery Queries (priority check added)
      // ================================================================
      await timed('table_discovery_queries', async () => {
        await trx.schema.createTable('discovery_queries', (table) => {
          table.increments('id').primary();
          table.text('query').notNullable();
          table.string('source', 50).defaultTo('web');
          table.boolean('active').defaultTo(true);
          table.integer('interval_hours').defaultTo(24);
          table.float('success_rate').defaultTo(0.0);
          table.integer('priority').defaultTo(5);
          table.timestamp('last_run').nullable();
          table.timestamps(true, true);
        });
        await trx.raw('ALTER TABLE discovery_queries ADD CONSTRAINT ck_query_interval_positive CHECK (interval_hours > 0)');
        // Ensure priority is non‑negative
        await trx.raw('ALTER TABLE discovery_queries ADD CONSTRAINT ck_priority_non_negative CHECK (priority >= 0)');

        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_discovery_queries_active_priority
            ON discovery_queries (active, priority DESC)
            WHERE active = TRUE
        `);
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_discovery_queries_last_run
            ON discovery_queries (last_run ASC NULLS FIRST)
        `);
        // Extra index for priority sorting
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_discovery_queries_priority ON discovery_queries (priority)
        `);
      });

      // ================================================================
      // 4. Discovered Sites (unique URL, extra indices, FK)
      // ================================================================
      await timed('table_discovered_sites', async () => {
        await trx.schema.raw('DROP TABLE IF EXISTS discovered_sites CASCADE');

        await trx.schema.raw(`
          CREATE TABLE discovered_sites (
            id BIGSERIAL,
            url TEXT NOT NULL,
            status TEXT DEFAULT 'new'
              CHECK (status IN ('new','crawled','ignored','expired')),
            query_id INTEGER REFERENCES discovery_queries(id) ON DELETE SET NULL,
            confidence FLOAT DEFAULT 0.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
            found_at TIMESTAMPTZ DEFAULT NOW(),
            last_crawled TIMESTAMPTZ,
            expiration_date TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '90 days'),
            PRIMARY KEY (id, found_at),
            UNIQUE (url)   -- <-- prevents duplicate URLs
          ) PARTITION BY RANGE (found_at);
        `);

        // Create monthly partitions
        await trx.schema.raw(`
          DO $$
          DECLARE
            start_date DATE := date_trunc('month', now())::date - INTERVAL '1 month';
            end_date DATE := start_date + INTERVAL '14 months';
            d DATE := start_date;
            suffix TEXT;
          BEGIN
            WHILE d < end_date LOOP
              suffix := to_char(d, 'YYYY_MM');
              IF NOT suffix ~ '^[0-9]{4}_[0-9]{2}$' THEN
                RAISE EXCEPTION 'Invalid partition suffix: %', suffix;
              END IF;
              EXECUTE format(
                'CREATE TABLE IF NOT EXISTS discovered_sites_%s PARTITION OF discovered_sites FOR VALUES FROM (%L) TO (%L)',
                suffix,
                d,
                d + INTERVAL '1 month'
              );
              d := d + INTERVAL '1 month';
            END LOOP;
          END;
          $$;
        `);

        // Indexes
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_discovered_sites_status
            ON discovered_sites (status) WHERE status = 'new'
        `);
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_discovered_sites_query ON discovered_sites (query_id)
        `);
        try {
          await trx.schema.raw(`
            CREATE INDEX IF NOT EXISTS idx_discovered_sites_url_gin
              ON discovered_sites USING GIN (url gin_trgm_ops)
          `);
        } catch (err) {
          logEvent('error', 'GIN index on url failed', { error: err.message, trace_id: traceId });
        }
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_discovered_sites_status_found
            ON discovered_sites (status, found_at DESC)
        `);
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_discovered_sites_expiration
            ON discovered_sites (expiration_date) WHERE status = 'new'
        `);
        // Additional indexes for performance
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_discovered_sites_confidence ON discovered_sites (confidence DESC)
        `);
      });

      // ================================================================
      // 5. Trigger: auto‑expire discovered sites
      // ================================================================
      await timed('trigger_auto_expire_sites', async () => {
        await trx.schema.raw(`
          CREATE OR REPLACE FUNCTION auto_expire_discovered_sites()
          RETURNS TRIGGER AS $$
          BEGIN
            LOOP
              DELETE FROM discovered_sites
                WHERE id IN (
                  SELECT id FROM discovered_sites
                    WHERE status = 'new'
                      AND expiration_date < NOW()
                    LIMIT 1000
              );
              EXIT WHEN NOT FOUND;
            END LOOP;
            RETURN NULL;
          END;
          $$ LANGUAGE plpgsql;

          DROP TRIGGER IF EXISTS trg_auto_expire_sites ON discovered_sites;
          CREATE TRIGGER trg_auto_expire_sites
            AFTER INSERT OR UPDATE OF status ON discovered_sites
            FOR EACH STATEMENT
            EXECUTE FUNCTION auto_expire_discovered_sites();
        `);
      });

      // ================================================================
      // 6. Email Filter Rules
      // ================================================================
      await timed('table_email_filter_rules', async () => {
        await trx.schema.createTable('email_filter_rules', (table) => {
          table.increments('id').primary();
          table.enu('rule_type', ['regex','domain','exact'],
            { useNative: true, enumName: 'filter_rule_type_004' })
            .defaultTo('regex');
          table.text('pattern').notNullable();
          table.boolean('active').defaultTo(true);
          table.integer('usage_count').defaultTo(0);
          table.float('confidence').defaultTo(1.0);
          table.timestamp('created_at').defaultTo(trx.fn.now());
          table.timestamp('last_triggered').nullable();
        });
        await trx.raw(`ALTER TABLE email_filter_rules ADD CONSTRAINT ck_pattern_not_empty CHECK (pattern IS NOT NULL AND pattern != '')`);

        const defaultRules = [
          { rule_type: 'regex',  pattern: '^(support|info|admin|sales|contact|help|hello|office|marketing|feedback|no-reply|noreply)@' },
          { rule_type: 'domain', pattern: 'example.com' },
          { rule_type: 'exact',  pattern: 'root@localhost' },
        ];
        for (const r of defaultRules) {
          await trx('email_filter_rules').insert(r).onConflict('id').ignore();
        }

        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_filter_rules_active_type
            ON email_filter_rules (active, rule_type) WHERE active = TRUE
        `);
        // Index for usage_count (to find most triggered rules)
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_filter_rules_usage ON email_filter_rules (usage_count DESC)
        `);
      });

      // ================================================================
      // 7. Add `is_ignored` column to emails (if table exists)
      // ================================================================
      await timed('alter_emails_ignored', async () => {
        if (await trx.schema.hasTable('emails')) {
          try {
            await trx.schema.alterTable('emails', (table) => {
              table.boolean('is_ignored').defaultTo(false);
            });
            logEvent('info', 'Column emails.is_ignored added');
          } catch (err) {
            logEvent('warn', 'Column emails.is_ignored already exists, skipping', { error: err.message });
          }
        } else {
          logEvent('warn', 'Table emails does not exist, skipping column addition');
        }
      });

      // ================================================================
      // 8. Trigger: enqueue filter apply tasks (async processing)
      // ================================================================
      await timed('trigger_filter_apply', async () => {
        await trx.schema.createTable('filter_apply_queue', (table) => {
          table.increments('id').primary();
          table.integer('rule_id').unsigned().references('id').inTable('email_filter_rules').onDelete('CASCADE');
          table.string('rule_type', 10);
          table.text('pattern');
          table.timestamp('created_at').defaultTo(trx.fn.now());
          table.boolean('processed').defaultTo(false);
        });

        await trx.schema.raw(`
          CREATE OR REPLACE FUNCTION enqueue_filter_apply()
          RETURNS TRIGGER AS $$
          BEGIN
            IF NEW.active THEN
              INSERT INTO filter_apply_queue (rule_id, rule_type, pattern)
              VALUES (NEW.id, NEW.rule_type, NEW.pattern);
            END IF;
            RETURN NEW;
          END;
          $$ LANGUAGE plpgsql;

          DROP TRIGGER IF EXISTS trg_filter_new_rule ON email_filter_rules;
          CREATE TRIGGER trg_filter_new_rule
            AFTER INSERT ON email_filter_rules
            FOR EACH ROW
            EXECUTE FUNCTION enqueue_filter_apply();
        `);
      });

      // ================================================================
      // 9. Paid Databases Stub
      // ================================================================
      await timed('table_paid_databases', async () => {
        await trx.schema.createTable('paid_databases', (table) => {
          table.increments('id').primary();
          table.string('name').notNullable();
          table.text('description').nullable();
          table.enu('status', ['locked','unavailable'],
            { useNative: true, enumName: 'paid_db_status_004' })
            .defaultTo('locked')
            .comment('Always locked – accessing paid databases is illegal');
          table.timestamp('added_at').defaultTo(trx.fn.now());
        });

        await trx('paid_databases').insert([
          { name: 'Example Paid Dump',
            description: 'This is a stub. Searching paid databases is illegal.' },
        ]);
      });

      // ================================================================
      // 10. Final integrity check
      // ================================================================
      await timed('integrity_check', async () => {
        const tables = [
          'discovery_queries', 'discovered_sites',
          'email_filter_rules', 'filter_apply_queue',
          'paid_databases', 'discovery_ai_calibration'
        ];
        for (const t of tables) {
          if (!(await trx.schema.hasTable(t))) {
            throw new Error(`[004] CRITICAL: Table ${t} was not created!`);
          }
        }
        logEvent('info', 'All tables created and verified', { trace_id: traceId });
      });

    }); // End transaction
  } catch (err) {
    logEvent('error', 'Migration 004 failed, transaction rolled back', { error: err.message, trace_id: traceId });
    throw err;
  }

  logEvent('info', 'Self‑evolving discovery and filter layer initialized', { trace_id: traceId });
};

// --------------------------------------------------------------------
// DOWN migration — Graceful Degradation with Full Transaction
// --------------------------------------------------------------------
exports.down = async function(knex) {
  const traceId = `rollback-004-${Date.now()}`;
  logEvent('info', 'Rolling back migration 004', { trace_id: traceId });

  try {
    await knex.transaction(async (trx) => {
      // Remove triggers and functions first
      const triggers = ['trg_filter_new_rule', 'trg_auto_expire_sites'];
      const functions = ['enqueue_filter_apply', 'auto_expire_discovered_sites'];

      for (const trg of triggers) {
        try {
          await trx.raw(`DROP TRIGGER IF EXISTS ${trg} ON email_filter_rules`);
          logEvent('info', `Trigger ${trg} removed`, { trace_id: traceId });
        } catch (err) {
          logEvent('warn', `Failed to drop trigger ${trg}`, { error: err.message, trace_id: traceId });
        }
      }
      for (const func of functions) {
        try {
          await trx.raw(`DROP FUNCTION IF EXISTS ${func}()`);
          logEvent('info', `Function ${func} removed`, { trace_id: traceId });
        } catch (err) {
          logEvent('warn', `Failed to drop function ${func}`, { error: err.message, trace_id: traceId });
        }
      }

      // Drop tables in reverse dependency order
      const tables = [
        'filter_apply_queue',
        'paid_databases',
        'email_filter_rules',
        'discovered_sites',
        'discovery_queries',
        'discovery_ai_calibration',
      ];
      for (const table of tables) {
        if (await trx.schema.hasTable(table)) {
          try {
            await trx.schema.dropTableIfExists(table);
            logEvent('info', `Table ${table} dropped`, { trace_id: traceId });
          } catch (err) {
            logEvent('error', `Failed to drop table ${table}`, { error: err.message, trace_id: traceId });
            throw err;
          }
        } else {
          logEvent('info', `Table ${table} does not exist, skipping`, { trace_id: traceId });
        }
      }

      // Optionally drop is_ignored column from emails
      if (await trx.schema.hasTable('emails')) {
        try {
          await trx.schema.alterTable('emails', (table) => {
            table.dropColumn('is_ignored');
          });
          logEvent('info', 'Column is_ignored removed from emails', { trace_id: traceId });
        } catch (err) {
          logEvent('warn', 'Could not drop is_ignored column', { error: err.message, trace_id: traceId });
        }
      }
    });
  } catch (err) {
    logEvent('error', 'Rollback of migration 004 failed', { error: err.message, trace_id: traceId });
    throw err;
  }
};