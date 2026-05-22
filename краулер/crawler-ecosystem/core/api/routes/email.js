const express = require('express');
const router = express.Router();
const Joi = require('joi');
const { validate } = require('./_validate');
const { classifyError } = require('./_errors');
const promClient = require('prom-client');
const SentientCore = require('./_sentient');

// Статический импорт ExcelJS (с fallback)
let ExcelJS;
try { ExcelJS = require('exceljs'); } catch (e) { ExcelJS = null; }

const emailRequests = new promClient.Counter({
  name: 'crawler_email_requests_total',
  help: 'Total email route requests',
  labelNames: ['method', 'route', 'status'],
});

// Таймаут для операций БД (30 секунд)
function withTimeout(promise, ms = 30_000) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('Operation timed out')), ms))
  ]);
}

const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(err => {
    const info = classifyError(err);
    emailRequests.inc({ method: req.method, route: req.path, status: info.httpStatus });
    res.status(info.httpStatus).json({ error: err.message || 'Internal error' });
  });
};

// Разумный sentient с авто‑восстановлением
let sentient = null;
const getSentient = (db, bus) => {
  if (!sentient || !sentient.health) {
    if (sentient) sentient.destroy();
    sentient = new SentientCore('email', db, bus);
    sentient.on('evolution', (info) => console.log(`[EMAIL] Evolution: gen ${info.generation}`));
  }
  return sentient;
};

router.use((req, res, next) => {
  const s = getSentient(req.app.locals.db, req.app.locals.bus);
  s.markRequest();
  next();
});

// GET / — предсказательный список
router.get('/', validate(Joi.object({
  tags: Joi.string().allow('').optional(),
  verified: Joi.boolean().optional(),
  limit: Joi.number().integer().min(1).max(1000).default(50),
  offset: Joi.number().integer().min(0).default(0),
  forcePrimary: Joi.boolean().default(false),
}), 'query'), asyncHandler(async (req, res) => {
  const db = req.app.locals.db;
  const { tags, verified, limit, offset, forcePrimary } = req.query;
  const parsedTags = tags ? tags.split(',').map(t => t.trim()).filter(Boolean) : [];
  const querySig = { tags: parsedTags, verified, limit, offset };
  const s = getSentient(db, req.app.locals.bus);

  const optimal = s.predictOptimalLimit(querySig, limit);
  const start = Date.now();
  const result = await withTimeout(db.getEmails({
    tags: parsedTags.length ? parsedTags : undefined,
    verified,
    limit: optimal,
    offset,
    forcePrimary,
  }));
  s.learn(querySig, Date.now() - start, result.length);
  res.setHeader('X-Total-Count', result.length);
  res.json(result);
}));

// GET /stats
router.get('/stats', asyncHandler(async (req, res) => {
  const db = req.app.locals.db;
  const stats = await withTimeout(db.getEmailStats());
  res.json(stats);
}));

// GET /:id — с валидацией UUID
router.get('/:id', validate(Joi.object({
  id: Joi.string().uuid().required(),
}), 'params'), asyncHandler(async (req, res) => {
  const db = req.app.locals.db;
  const row = await withTimeout(db.knex('emails').where('id', req.params.id).first());
  if (!row) {
    const err = new Error('Email not found');
    err.code = 'ERR_NOT_FOUND';
    throw err;
  }
  if (row.context && db.encryption) {
    row.context = await withTimeout(db.encryption.decrypt(row.context));
  }
  res.json(row);
}));

// DELETE /:id
router.delete('/:id', validate(Joi.object({
  id: Joi.string().uuid().required(),
}), 'params'), asyncHandler(async (req, res) => {
  const db = req.app.locals.db;
  const deleted = await withTimeout(db.knex('emails').where('id', req.params.id).del());
  if (!deleted) {
    const err = new Error('Email not found');
    err.code = 'ERR_NOT_FOUND';
    throw err;
  }
  if (db.cache) await db.cache.invalidateLists();
  res.status(204).end();
}));

// POST /export — с валидацией limit и корректным Content-Disposition
router.post('/export', validate(Joi.object({
  format: Joi.string().valid('json', 'csv', 'xlsx').default('json'),
  filters: Joi.object({
    tags: Joi.array().items(Joi.string()).optional(),
    verified: Joi.boolean().optional(),
    limit: Joi.number().integer().min(1).max(10_000).default(1000),
    offset: Joi.number().integer().min(0).max(10_000).default(0),
  }).default({}),
}), 'body'), asyncHandler(async (req, res) => {
  const db = req.app.locals.db;
  const { format, filters } = req.body;
  const emails = await withTimeout(db.getEmails(filters));
  const timestamp = Date.now();

  if (format === 'json') {
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename=emails-${timestamp}.json`);
    return res.json(emails);
  }
  if (format === 'csv') {
    const csv = '\uFEFF' + ['email,sourceUrl,confidence,verified,lastSeen']
      .concat(emails.map(e => `${e.email},${e.source_url || ''},${e.confidence},${e.verified},${e.last_seen || ''}`))
      .join('\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename=emails-${timestamp}.csv`);
    return res.send(csv);
  }
  if (format === 'xlsx' && ExcelJS) {
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Emails');
    sheet.columns = [
      { header: 'Email', key: 'email' },
      { header: 'Source URL', key: 'source_url' },
      { header: 'Confidence', key: 'confidence' },
      { header: 'Verified', key: 'verified' },
      { header: 'Last Seen', key: 'last_seen' },
    ];
    emails.forEach(e => sheet.addRow(e));
    const buffer = await workbook.xlsx.writeBuffer();
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=emails-${timestamp}.xlsx`);
    return res.send(buffer);
  }
  // Fallback to JSON
  res.json(emails);
}));

module.exports = router;