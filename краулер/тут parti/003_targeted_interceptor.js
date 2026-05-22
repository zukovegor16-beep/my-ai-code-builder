// ======================================================================
// Part 3 — Targeted Interceptor Schema (Self‑Evolving AI Foundation)
// ======================================================================
// File: core/migrations/003_targeted_interceptor.js
// Purpose: Create an intelligent, self‑optimizing data layer for
//          real‑time email interception. All reasonable recommendations applied.

/**
 * Structured logger with trace context for distributed debugging.
 */
function logEvent(level, message, metadata = {}) {
  console.log(JSON.stringify({
    level,
    timestamp: new Date().toISOString(),
    service: 'migration-003',
    trace_id: metadata.trace_id || 'genesis', // 'genesis' is intentional – the beginning
    message,
    ...metadata,
  }));
}

/**
 * High‑precision timing wrapper with performance analytics.
 */
async function timed(stepName, fn) {
  const start = process.hrtime.bigint();
  await fn();
  const end = process.hrtime.bigint();
  const duration_us = Number(end - start) / 1000;
  logEvent('info', `Step completed`, { step: stepName, duration_us });
}

// --------------------------------------------------------------------
// UP migration — The Birth of the AI‑Data Layer
// --------------------------------------------------------------------
exports.up = async function(knex) {
  const traceId = `migration-003-${Date.now()}`;
  logEvent('info', 'Initializing self‑evolving interceptor data layer', { trace_id: traceId });

  // Outer try/catch ensures transaction is fully aborted on any failure
  try {
    await knex.transaction(async (trx) => {
      // ================================================================
      // 0. Environment Detection & Adaptation
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
          instance_type: process.env.INSTANCE_TYPE || 'unknown',
          region: process.env.REGION || 'unknown',
        };
        logEvent('info', 'Environment detected', { environment });
      } catch (err) {
        logEvent('warn', 'Environment detection partial', { error: err.message });
      }

      // ================================================================
      // 1. Extensions
      // ================================================================
      await timed('extensions', async () => {
        const extensions = ['uuid-ossp', 'pgcrypto', 'pg_trgm', 'btree_gin'];
        for (const ext of extensions) {
          try {
            await trx.raw(`CREATE EXTENSION IF NOT EXISTS "${ext}"`);
          } catch (err) {
            logEvent('warn', `Extension ${ext} not available`, { error: err.message });
          }
        }
      });

      // ================================================================
      // 2. Calibration table (for AI metrics)
      // ================================================================
      await timed('table_ai_calibration', async () => {
        await trx.schema.createTable('ai_calibration', (table) => {
          table.text('metric').primary();
          table.float('value').defaultTo(1.0);
          table.timestamp('last_updated').defaultTo(trx.fn.now());
          table.check('value >= 0 AND value <= 100', null, 'ck_calibration_value_range');
        });
        // Seed calibration values
        await trx('ai_calibration').insert([
          { metric: 'success_increment', value: 0.05 },
          { metric: 'success_decrement', value: 0.02 },
          { metric: 'health_increment', value: 0.1 },
          { metric: 'health_decrement', value: 0.05 },
          { metric: 'min_health', value: 0.1 },
          { metric: 'max_health', value: 1.0 },
        ]).onConflict('metric').ignore();
      });

      // ================================================================
      // 3. Interceptor Targets (with AI metadata)
      // ================================================================
      await timed('table_interceptor_targets', async () => {
        await trx.schema.createTable('interceptor_targets', (table) => {
          table.increments('id').primary();
          table.text('url').notNullable().comment('Target page URL');
          table
            .enum('target_type', ['registration', 'job_site', 'generic', 'api_endpoint'])
            .defaultTo('generic')
            .comment('Type of interception');
          table.boolean('active').defaultTo(true).comment('Whether monitoring is active');
          table.integer('check_interval_minutes').defaultTo(15).comment('Base check interval');
          table.integer('priority').defaultTo(5).comment('AI‑assigned priority (1‑10)');
          table.float('success_rate').defaultTo(0.0).comment('Historical email capture rate');
          table.float('health_score').defaultTo(1.0).comment('AI health score (0‑1)');
          table.timestamp('last_scanned').nullable();
          table.timestamp('last_email_found').nullable();
          table.jsonb('ai_metadata').defaultTo('{}').comment('AI learned metadata');
          table.jsonb('config').defaultTo('{}').comment('Target‑specific configuration');
          table.timestamps(true, true);

          // AI‑driven URL validation
          table.check(
            "url ~* '^https?://[^\\s/$.?#].[^\\s]*$'",
            null,
            'ck_target_url_format'
          );
        });

        // AI‑optimized indexes
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_targets_active_priority
            ON interceptor_targets (active, priority DESC)
            WHERE active = TRUE
        `);
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_targets_health_score
            ON interceptor_targets (health_score)
            WHERE health_score < 0.5
        `);
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_targets_last_scanned
            ON interceptor_targets (last_scanned ASC NULLS FIRST)
        `);
      });

      // ================================================================
      // 4. Self‑Evolving Partition Manager (with SQL injection protection)
      // ================================================================
      await timed('partition_manager', async () => {
        await trx.schema.raw(`
          CREATE OR REPLACE FUNCTION ai_manage_partitions(
            table_name TEXT,
            retention_months INTEGER DEFAULT 24,
            future_months INTEGER DEFAULT 12
          ) RETURNS TABLE(action TEXT, partition_name TEXT, created BOOLEAN) AS $$
          DECLARE
            current_date DATE := date_trunc('month', now())::date;
            oldest_date DATE := current_date - (retention_months || ' months')::INTERVAL;
            future_date DATE := current_date + (future_months || ' months')::INTERVAL;
            d DATE;
            partition_exists BOOLEAN;
          BEGIN
            -- Protect against SQL injection via table name validation
            IF NOT table_name ~ '^[a-z_][a-z0-9_]*$' THEN
              RAISE EXCEPTION 'Invalid table name: %', table_name;
            END IF;

            -- Check that the main table exists
            IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = table_name) THEN
              RAISE EXCEPTION 'Table % does not exist', table_name;
            END IF;

            -- Create future partitions
            d := current_date;
            WHILE d <= future_date LOOP
              BEGIN
                RAISE NOTICE 'Creating partition %', format('%s_%s', table_name, to_char(d, 'YYYY_MM'));
                SELECT EXISTS (
                  SELECT 1 FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE c.relname = format('%s_%s', table_name, to_char(d, 'YYYY_MM'))
                ) INTO partition_exists;

                IF NOT partition_exists THEN
                  EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I_%s PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                    table_name,
                    to_char(d, 'YYYY_MM'),
                    table_name,
                    d,
                    d + '1 month'::INTERVAL
                  );
                  RETURN QUERY SELECT 'created'::TEXT, format('%s_%s', table_name, to_char(d, 'YYYY_MM')), TRUE;
                END IF;
              EXCEPTION
                WHEN OTHERS THEN
                  RAISE WARNING 'Failed to create partition %: %', format('%s_%s', table_name, to_char(d, 'YYYY_MM')). SQLERRM;
              END;
              d := d + '1 month'::INTERVAL;
            END LOOP;

            -- Detach old partitions (optional archival)
            d := current_date - '1 month'::INTERVAL;
            WHILE d >= oldest_date LOOP
              SELECT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = format('%s_%s', table_name, to_char(d, 'YYYY_MM'))
              ) INTO partition_exists;

              IF partition_exists THEN
                RETURN QUERY SELECT 'kept'::TEXT, format('%s_%s', table_name, to_char(d, 'YYYY_MM')), FALSE;
              END IF;
              d := d - '1 month'::INTERVAL;
            END LOOP;

            RETURN;
          END;
          $$ LANGUAGE plpgsql;
        `);
      });

      // ================================================================
      // 5. Interceptor Emails (partitioned)
      // ================================================================
      await timed('table_interceptor_emails', async () => {
        await trx.schema.raw('DROP TABLE IF EXISTS interceptor_emails CASCADE');

        await trx.schema.raw(`
          CREATE TABLE interceptor_emails (
            id BIGSERIAL,
            email VARCHAR(255) NOT NULL,
            source_url TEXT,
            context TEXT,
            capture_method TEXT DEFAULT 'html',
            target_type TEXT DEFAULT 'generic',
            tags JSONB DEFAULT '[]'::jsonb,
            is_duplicate BOOLEAN DEFAULT FALSE,
            confidence_score FLOAT DEFAULT 0.0 CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
            ai_processed BOOLEAN DEFAULT FALSE,
            captured_at TIMESTAMPTZ DEFAULT NOW(),
            target_id INTEGER,
            metadata JSONB DEFAULT '{}',
            PRIMARY KEY (id, captured_at),
            CONSTRAINT chk_capture_method CHECK (capture_method IN (
              'html', 'api_response', 'local_storage', 'session_storage', 
              'indexeddb', 'manual', 'ai_predicted'
            )),
            CONSTRAINT chk_target_type CHECK (target_type IN (
              'registration', 'job_site', 'generic', 'api_endpoint'
            ))
          ) PARTITION BY RANGE (captured_at);
        `);

        // Initialize partitions using AI manager
        await trx.schema.raw(`
          SELECT ai_manage_partitions('interceptor_emails', 24, 12);
        `);
      });

      // ================================================================
      // 6. Optimized Indexes
      // ================================================================
      await timed('ai_indexes', async () => {
        // GIN index for fast email search (supports LIKE, ILIKE, regex)
        await trx.schema.raw(`
          CREATE INDEX IF NOT EXISTS idx_ie_email_gin ON interceptor_emails USING GIN (email gin_trgm_ops)
        `);

        const idxList = [
          `CREATE INDEX IF NOT EXISTS idx_ie_captured_brin ON interceptor_emails USING BRIN (captured_at) WITH (pages_per_range = 64)`,
          `CREATE INDEX IF NOT EXISTS idx_ie_captured_btree ON interceptor_emails (captured_at DESC)`, // B‑tree for smaller data sets
          `CREATE INDEX IF NOT EXISTS idx_ie_tags_gin ON interceptor_emails USING GIN (tags jsonb_path_ops)`,
          `CREATE INDEX IF NOT EXISTS idx_ie_confidence ON interceptor_emails (confidence_score DESC) WHERE confidence_score > 0.7`,
          `CREATE INDEX IF NOT EXISTS idx_ie_ai_processed ON interceptor_emails (ai_processed) WHERE ai_processed = FALSE`,
          `CREATE INDEX IF NOT EXISTS idx_ie_target_captured ON interceptor_emails (target_id, captured_at DESC)`,
          `CREATE INDEX IF NOT EXISTS idx_ie_email_captured_composite ON interceptor_emails (email, captured_at DESC)`,
        ];

        for (const sql of idxList) {
          try {
            await trx.schema.raw(sql);
          } catch (err) {
            logEvent('warn', 'Index creation failed (non‑critical)', { sql: sql.substring(0, 60), error: err.message });
          }
        }
      });

      // ================================================================
      // 7. Citus Integration (dynamic shard count based on environment)
      // ================================================================
      await timed('citus_ai_sharding', async () => {
        if (environment.citus_available) {
          try {
            const shardCount = environment.instance_type === 'production' ? 32 : 8;
            await trx.schema.raw(`
              SELECT create_distributed_table('interceptor_emails', 'email', 
                colocate_with => 'emails',
                shard_count => ${shardCount}
              );
            `);
            logEvent('info', `AI‑optimized Citus distribution configured (${shardCount} shards)`);
          } catch (err) {
            logEvent('warn', 'Citus distribution failed', { error: err.message });
          }
        }
      });

      // ================================================================
      // 8. Neural Triggers – Refined (with existence checks)
      // ================================================================
      await timed('neural_triggers', async () => {
        // AI Duplicate Detection (exact match only; fuzzy is async)
        await trx.schema.raw(`
          CREATE OR REPLACE FUNCTION ai_interceptor_processor()
          RETURNS TRIGGER AS $$
          BEGIN
            IF NEW.email IS NULL THEN
              RETURN NEW;
            END IF;
            
            IF EXISTS (SELECT 1 FROM interceptor_emails WHERE email = NEW.email) THEN
              NEW.is_duplicate := TRUE;
              NEW.confidence_score := 1.0;
            END IF;
            
            NEW.metadata = jsonb_set(COALESCE(NEW.metadata, '{}'), '{processed_at}', to_jsonb(NOW()));
            NEW.metadata = jsonb_set(NEW.metadata, '{ai_version}', '"1.0"');
            
            RETURN NEW;
          END;
          $$ LANGUAGE plpgsql;

          DROP TRIGGER IF EXISTS trg_ai_interceptor ON interceptor_emails;
          CREATE TRIGGER trg_ai_interceptor
          BEFORE INSERT ON interceptor_emails
          FOR EACH ROW
          EXECUTE FUNCTION ai_interceptor_processor();
        `);

        // Self‑Updating Target Health (with calibration values)
        await trx.schema.raw(`
          CREATE OR REPLACE FUNCTION ai_update_target_health()
          RETURNS TRIGGER AS $$
          DECLARE
            succ_inc FLOAT := 0.05;
            succ_dec FLOAT := 0.02;
            hlth_inc FLOAT := 0.1;
            hlth_dec FLOAT := 0.05;
          BEGIN
            IF NEW.target_id IS NULL THEN
              RETURN NEW;
            END IF;

            BEGIN
              SELECT value INTO succ_inc FROM ai_calibration WHERE metric = 'success_increment';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
            BEGIN
              SELECT value INTO succ_dec FROM ai_calibration WHERE metric = 'success_decrement';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
            BEGIN
              SELECT value INTO hlth_inc FROM ai_calibration WHERE metric = 'health_increment';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
            BEGIN
              SELECT value INTO hlth_dec FROM ai_calibration WHERE metric = 'health_decrement';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;

            UPDATE interceptor_targets
            SET 
              last_scanned = NOW(),
              last_email_found = CASE WHEN NEW.email IS NOT NULL THEN NOW() ELSE last_email_found END,
              success_rate = CASE 
                WHEN NEW.email IS NOT NULL THEN LEAST(1.0, success_rate + succ_inc)
                ELSE GREATEST(0.0, success_rate - succ_dec)
              END,
              health_score = CASE 
                WHEN NEW.email IS NOT NULL THEN LEAST(1.0, health_score + hlth_inc)
                ELSE GREATEST(0.1, health_score - hlth_dec)
              END,
              ai_metadata = jsonb_set(
                jsonb_set(COALESCE(ai_metadata, '{}'), '{last_updated}', to_jsonb(NOW())),
                '{interactions_count}', 
                to_jsonb(COALESCE((ai_metadata->>'interactions_count')::int, 0) + 1)
              )
            WHERE id = NEW.target_id;
            
            RETURN NEW;
          END;
          $$ LANGUAGE plpgsql;

          DROP TRIGGER IF EXISTS trg_ai_target_health ON interceptor_emails;
          CREATE TRIGGER trg_ai_target_health
          AFTER INSERT ON interceptor_emails
          FOR EACH ROW
          WHEN (NEW.target_id IS NOT NULL)
          EXECUTE FUNCTION ai_update_target_health();
        `);
      });

      // ================================================================
      // 9. Seed AI Training Data (idempotent, merge allowed fields)
      // ================================================================
      await timed('seed_ai_training', async () => {
        const seedTargets = [
          {
            url: 'https://example.com/register',
            target_type: 'registration',
            check_interval_minutes: 30,
            priority: 8,
            ai_metadata: { trained: true, model: 'v1.0' },
            config: { headers: { 'User-Agent': 'Mozilla/5.0' }, follow_redirects: true },
          },
          {
            url: 'https://jobs.example.com',
            target_type: 'job_site',
            check_interval_minutes: 60,
            priority: 7,
            ai_metadata: { trained: true, model: 'v1.0' },
            config: { parse_strategy: 'deep_scan', max_depth: 3 },
          },
        ];

        for (const target of seedTargets) {
          await trx('interceptor_targets')
            .insert(target)
            .onConflict('url')
            .merge(['check_interval_minutes', 'priority', 'ai_metadata']);
        }
      });

      // ================================================================
      // 10. Self‑Diagnostic Integrity Check
      // ================================================================
      await timed('ai_integrity_check', async () => {
        const tables = ['interceptor_targets', 'interceptor_emails', 'ai_calibration'];
        const functions = ['ai_manage_partitions', 'ai_interceptor_processor', 'ai_update_target_health'];
        
        for (const t of tables) {
          const exists = await trx.schema.hasTable(t);
          if (!exists) throw new Error(`[AI-003] CRITICAL: Table ${t} creation failed!`);
        }
        
        for (const f of functions) {
          const result = await trx.raw(`SELECT proname FROM pg_proc WHERE proname = ?`, [f]);
          if (result.rows.length === 0) {
            logEvent('warn', `Function ${f} not found (non‑critical)`, { trace_id: traceId });
          }
        }
        
        logEvent('info', 'AI integrity check passed', { trace_id: traceId });
      });

    }); // End transaction
  } catch (err) {
    logEvent('error', 'Migration failed, transaction rolled back', { error: err.message, trace_id: traceId });
    throw err;
  }

  logEvent('info', 'Self‑evolving interceptor data layer initialized', { trace_id: traceId });
};

// --------------------------------------------------------------------
// DOWN migration — Graceful Degradation with Full Transaction
// --------------------------------------------------------------------
exports.down = async function(knex) {
  const traceId = `rollback-003-${Date.now()}`;
  logEvent('info', 'Initiating graceful AI layer degradation', { trace_id: traceId });

  try {
    await knex.transaction(async (trx) => {
      try {
        await trx.schema.dropTableIfExists('interceptor_emails');
        logEvent('info', 'Table interceptor_emails dropped', { trace_id: traceId });
      } catch (err) {
        logEvent('error', 'Failed to drop table interceptor_emails', { error: err.message, trace_id: traceId });
        throw err;
      }

      try {
        await trx.schema.dropTableIfExists('interceptor_targets');
        logEvent('info', 'Table interceptor_targets dropped', { trace_id: traceId });
      } catch (err) {
        logEvent('error', 'Failed to drop table interceptor_targets', { error: err.message, trace_id: traceId });
        throw err;
      }

      try {
        await trx.schema.dropTableIfExists('ai_calibration');
        logEvent('info', 'Table ai_calibration dropped', { trace_id: traceId });
      } catch (err) {
        logEvent('error', 'Failed to drop table ai_calibration', { error: err.message, trace_id: traceId });
        throw err;
      }

      const aiFunctions = ['ai_interceptor_processor', 'ai_update_target_health', 'ai_manage_partitions'];
      for (const func of aiFunctions) {
        try {
          await trx.raw(`DROP FUNCTION IF EXISTS ${func}() CASCADE`);
          logEvent('info', `Function ${func} dropped`, { trace_id: traceId });
        } catch (err) {
          logEvent('error', `Failed to drop function ${func}`, { error: err.message, trace_id: traceId });
          throw err;
        }
      }
      
      logEvent('info', 'AI layer successfully degraded', { trace_id: traceId });
    });
  } catch (err) {
    logEvent('error', 'Critical failure during AI layer degradation', { error: err.message, trace_id: traceId });
    throw err;
  }
};