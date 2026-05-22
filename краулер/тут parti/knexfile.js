// ======================================================================
// Part 1 — Core Database Layer (Neural Data Network v2.1)
// ======================================================================
// File: core/knexfile.js
// Purpose: Production-grade connection configuration with multi-layer
//          failover, Vault integration, and adaptive pooling.

const path = require('path');
const fs = require('fs');

// ----------------------------------------------------------------------
// Advanced Vault integration with caching and retry
// ----------------------------------------------------------------------
let vaultCache = null;
let vaultLastFetch = 0;
const VAULT_CACHE_TTL = 300_000; // 5 minutes

async function fetchVaultSecrets() {
  if (process.env.NODE_ENV !== 'production') return null;
  const now = Date.now();
  if (vaultCache && (now - vaultLastFetch) < VAULT_CACHE_TTL) {
    return vaultCache;
  }

  const vaultAddr = process.env.VAULT_ADDR || 'http://vault.vault.svc.cluster.local:8200';
  const vaultToken = process.env.VAULT_TOKEN;
  if (!vaultAddr || !vaultToken) {
    console.warn('[DB] Vault not configured – using env‑based credentials.');
    return null;
  }

  // Exponential backoff retry for Vault
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 3000);
      const response = await fetch(`${vaultAddr}/v1/secret/data/crawler/database`, {
        headers: { 'X-Vault-Token': vaultToken },
        signal: controller.signal
      });
      clearTimeout(timeoutId);
      if (!response.ok) throw new Error(`Vault returned ${response.status}`);
      const { data } = (await response.json()).data;
      vaultCache = data;
      vaultLastFetch = now;
      console.log('[DB] Secrets refreshed from Vault');
      return data;
    } catch (err) {
      const delay = Math.pow(2, attempt) * 200;
      console.warn(`[DB] Vault attempt ${attempt}/5 failed: ${err.message}. Retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  console.error('[DB] All Vault attempts failed. Falling back to env.');
  return null;
}

// Load environment variables (in dev/staging they come from .env)
require('dotenv').config({ path: path.join(__dirname, '..', 'infra', '.env') });

module.exports = {
  // ====================================================================
  // Development
  // ====================================================================
  development: {
    client: 'pg',
    connection: {
      host: process.env.PG_HOST || 'localhost',
      port: parseInt(process.env.PG_PORT, 10) || 5432,
      database: process.env.PG_DATABASE || 'crawler_meta',
      user: process.env.PG_USER || 'crawler',
      password: process.env.PG_PASSWORD || 'crawler_db_password',
      application_name: 'crawler-dev',
    },
    pool: { min: 2, max: 10 },
    migrations: {
      directory: path.join(__dirname, 'migrations'),
      tableName: '_migrations',
      extension: 'js',
    },
    seeds: {
      directory: path.join(__dirname, 'seeds'),
    },
    acquireConnectionTimeout: 10000,
  },

  // ====================================================================
  // Staging
  // ====================================================================
  staging: {
    client: 'pg',
    connection: {
      host: process.env.PG_HOST,
      port: parseInt(process.env.PG_PORT, 10) || 5432,
      database: process.env.PG_DATABASE || 'crawler_staging',
      user: process.env.PG_USER || 'crawler',
      password: process.env.PG_PASSWORD,
      application_name: 'crawler-staging',
    },
    pool: { min: 3, max: 20 },
    migrations: {
      directory: path.join(__dirname, 'migrations'),
      tableName: '_migrations',
      extension: 'js',
    },
    seeds: {
      directory: path.join(__dirname, 'seeds'),
    },
    acquireConnectionTimeout: 15000,
  },

  // ====================================================================
  // Production – Kubernetes with Citus & Patroni
  // ====================================================================
  production: {
    client: 'pg',
    connection: async () => {
      const secrets = await fetchVaultSecrets();
      const cfg = {
        host: secrets?.host || process.env.PG_HOST,
        port: parseInt(secrets?.port || process.env.PG_PORT, 10) || 5432,
        database: secrets?.database || process.env.PG_DATABASE || 'crawler_prod',
        user: secrets?.username || process.env.PG_USER || 'crawler',
        password: secrets?.password || process.env.PG_PASSWORD,
        application_name: `worker-${process.env.HOSTNAME || 'unknown'}`,
        keepAlive: true,
        keepAliveInitialDelayMillis: 10000,
      };
      return cfg;
    },
    pool: {
      min: 5,
      max: 80,
      acquireTimeoutMillis: 30000,
      createTimeoutMillis: 15000,
      reapIntervalMillis: 1000,
      createRetryIntervalMillis: 200,
      afterCreate: (conn, done) => {
        const role = process.env.APP_ROLE || 'crawler';
        conn.query(`SET SESSION "app.role" = '${role}'`, (err) => {
          if (err) console.error('[DB] RLS set error:', err.message);
          done(err, conn);
        });
      },
    },
    migrations: {
      directory: path.join(__dirname, 'migrations'),
      tableName: '_migrations',
      extension: 'js',
    },
    seeds: {
      directory: path.join(__dirname, 'seeds'),
    },
    acquireConnectionTimeout: 30000,
  },
};