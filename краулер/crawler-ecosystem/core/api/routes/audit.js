const express = require('express');
const router = express.Router();
const Joi = require('joi');
const { validate } = require('./_validate');
const { classifyError } = require('./_errors');
const promClient = require('prom-client');
const SentientCore = require('./_sentient');

const auditRequests = new promClient.Counter({
  name: 'crawler_audit_requests_total',
  help: 'Total audit route requests',
  labelNames: ['method', 'route', 'status'],
});

function withTimeout(promise, ms = 30_000) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('Operation timed out')), ms))
  ]);
}

const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(err => {
    const info = classifyError(err);
    auditRequests.inc({ method: req.method, route: req.path, status: info.httpStatus });
    res.status(info.httpStatus).json({ error: err.message || 'Internal error' });
  });
};

// Жизненный цикл sentient
let sentient = null;
const getSentient = (db, bus) => {
  if (!sentient || !sentient.health) {
    if (sentient) sentient.destroy();
    sentient = new SentientCore('audit', db, bus);
  }
  return sentient;
};

router.use((req, res, next) => {
  getSentient(req.app.locals.db, req.app.locals.bus).markRequest();
  next();
});

// Белый список допустимых действий и таблиц
const ALLOWED_ACTIONS = ['CREATE', 'UPDATE', 'DELETE', 'LOGIN', 'EXPORT'];
const ALLOWED_TABLES = ['emails', 'sessions', 'proxy_pool', 'audit_log', 'visited_urls'];

router.get('/', validate(Joi.object({
  action: Joi.string().valid(...ALLOWED_ACTIONS).optional(),
  tableName: Joi.string().valid(...ALLOWED_TABLES).optional(),
  changedBy: Joi.string().optional(),
  limit: Joi.number().integer().min(1).max(1000).default(100),
  offset: Joi.number().integer().min(0).default(0),
}), 'query'), asyncHandler(async (req, res) => {
  const db = req.app.locals.db;
  const { action, tableName, changedBy, limit, offset } = req.query;
  const qSig = { action, tableName, changedBy };
  const s = getSentient(db, req.app.locals.bus);

  let query = db.knex('audit_log').orderBy('created_at', 'desc');
  if (action) query = query.where('action', action);
  if (tableName) query = query.where('table_name', tableName);
  if (changedBy) query = query.where('changed_by', changedBy);

  const start = Date.now();
  const rows = await withTimeout(query.limit(limit).offset(offset).select('*'));
  s.learn(qSig, Date.now() - start, rows.length);
  res.setHeader('X-Total-Count', rows.length);
  res.json(rows);
}));

module.exports = router;