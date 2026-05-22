const express = require('express');
const router = express.Router();
const Joi = require('joi');
const { validate } = require('./_validate');
const { classifyError } = require('./_errors');
const promClient = require('prom-client');
const SentientCore = require('./_sentient');

const searchRequests = new promClient.Counter({
  name: 'crawler_search_requests_total',
  help: 'Total search requests',
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
    searchRequests.inc({ method: req.method, route: req.path, status: info.httpStatus });
    res.status(info.httpStatus).json({ error: err.message || 'Internal error' });
  });
};

let sentient = null;
const getSentient = (db, bus) => {
  if (!sentient || !sentient.health) {
    if (sentient) sentient.destroy();
    sentient = new SentientCore('search', db, bus);
  }
  return sentient;
};

router.use((req, res, next) => {
  getSentient(req.app.locals.db, req.app.locals.bus).markRequest();
  next();
});

// GET / — полнотекстовый поиск с безопасным ILIKE
router.get('/', validate(Joi.object({
  q: Joi.string().required().min(1).max(200),
  limit: Joi.number().integer().min(1).max(1000).default(50),
  offset: Joi.number().integer().min(0).default(0),
}), 'query'), asyncHandler(async (req, res) => {
  const db = req.app.locals.db;
  const { q, limit, offset } = req.query;
  const start = Date.now();
  let rows;

  try {
    const result = await withTimeout(db.knex.raw(`
      SELECT *, ts_rank(to_tsvector('english', email || ' ' || source_url), plainto_tsquery('english', ?)) as rank
      FROM emails
      WHERE to_tsvector('english', email || ' ' || source_url) @@ plainto_tsquery('english', ?)
      ORDER BY rank DESC
      LIMIT ? OFFSET ?
    `, [q, q, limit, offset]));
    rows = result.rows;
  } catch (ftsError) {
    console.warn(`[Search] Fulltext search failed, falling back to ILIKE:`, ftsError.message);
    rows = await withTimeout(
      db.knex('emails')
        .where('email', 'ILIKE', `%${q}%`)
        .orWhere('source_url', 'ILIKE', `%${q}%`)
        .orderBy('created_at', 'desc')
        .limit(limit).offset(offset)
        .select('*')
    );
  }

  const s = getSentient(db, req.app.locals.bus);
  s.learn({ q, limit }, Date.now() - start, rows.length);
  res.setHeader('X-Total-Count', rows.length);
  res.json(rows);
}));

// POST /advanced — расширенный поиск
router.post('/advanced', validate(Joi.object({
  email: Joi.string().email().optional(),
  domain: Joi.string().pattern(/^[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}$/).optional(),
  tags: Joi.array().items(Joi.string().max(50)).optional(),
  verified: Joi.boolean().optional(),
  minConfidence: Joi.number().min(0).max(1).optional(),
  limit: Joi.number().integer().min(1).max(1000).default(50),
  offset: Joi.number().integer().min(0).default(0),
}), 'body'), asyncHandler(async (req, res) => {
  const db = req.app.locals.db;
  const { email, domain, tags, verified, minConfidence, limit, offset } = req.body;
  const qSig = { email, domain, tags, verified, minConfidence };
  const s = getSentient(db, req.app.locals.bus);

  let query = db.knex('emails').orderBy('created_at', 'desc');
  if (email) query = query.where('email', email);
  if (domain) query = query.whereRaw('email ILIKE ?', [`%@${domain}`]);
  if (tags && tags.length > 0) query = query.whereRaw('tags @> ?::jsonb', [JSON.stringify(tags)]);
  if (verified !== undefined) query = query.where('verified', verified);
  if (minConfidence !== undefined) query = query.where('confidence', '>=', minConfidence);

  const optimalLimit = s.predictOptimalLimit(qSig, limit);
  query = query.limit(optimalLimit).offset(offset);

  const start = Date.now();
  const rows = await withTimeout(query.select('*'));
  s.learn(qSig, Date.now() - start, rows.length);
  res.setHeader('X-Total-Count', rows.length);
  res.json(rows);
}));

module.exports = router;