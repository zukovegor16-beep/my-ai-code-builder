// core/errors/errai.js
/**
 * ErrAI — Саморазвивающийся ИИ-центр управления ошибками и логирования.
 *
 * Полная реализация этапа 0.4, объединяющая:
 * - catalog.json (встроенный каталог с автодополнением)
 * - classifier.js (семантическая классификация с расстоянием Левенштейна)
 * - retry-policy.js (адаптивный retry + Circuit Breaker + генетическая оптимизация)
 * - logger.js (JSON-логер, детектор аномалий, burst-анализ)
 * - formats.js (pretty-print, Prometheus-метрики)
 * - alert-rules.yml (настраиваемые правила алертинга)
 * - Холт-Винтерс прогнозирование
 * - Мета-эволюция гиперпараметров (байесовская оптимизация)
 * - Распределённое обучение через шину событий
 * - Динамическая кластеризация неизвестных ошибок
 * - Персистентное состояние и самовосстановление
 *
 * @example
 *   const { ErrAI } = require('./core/errors/errai');
 *   const errai = new ErrAI({ service: 'gateway', bus: globalBus });
 *   errai.runWithContext({ requestId, sessionId }, async () => {
 *       await errai.retry(myOperation, { module: 'db' });
 *   });
 */

const { AsyncLocalStorage } = require('async_hooks');
const EventEmitter = require('events');
const crypto = require('crypto');
const path = require('path');
const fsSync = require('fs');
const fs = fsSync.promises;

// ======================== Встроенный каталог ошибок ========================
const BUILTIN_CATALOG = {
    ERR_UNKNOWN:              { code:'ERR_UNKNOWN',              category:'SYSTEM',   severity:'CRITICAL', retryable:false, description:'Unclassified error',                      httpStatus:500 },
    ERR_CONNECTION_REFUSED:   { code:'ERR_CONNECTION_REFUSED',   category:'NETWORK',  severity:'HIGH',     retryable:true,  description:'Connection refused',                        httpStatus:502, suggestedAction:'RETRY_WITH_NEXT_ENDPOINT' },
    ERR_NETWORK_TIMEOUT:      { code:'ERR_NETWORK_TIMEOUT',      category:'NETWORK',  severity:'HIGH',     retryable:true,  description:'Network timeout',                           httpStatus:504, suggestedAction:'RETRY_WITH_BACKOFF' },
    ERR_DNS_RESOLUTION:       { code:'ERR_DNS_RESOLUTION',       category:'NETWORK',  severity:'MEDIUM',   retryable:true,  description:'DNS resolution failed',                     httpStatus:502 },
    ERR_RATE_LIMIT:           { code:'ERR_RATE_LIMIT',           category:'RESOURCE', severity:'MEDIUM',   retryable:true,  description:'Rate limit exceeded',                       httpStatus:429, suggestedAction:'RETRY_WITH_EXPONENTIAL_BACKOFF' },
    ERR_QUOTA_EXCEEDED:       { code:'ERR_QUOTA_EXCEEDED',       category:'RESOURCE', severity:'MEDIUM',   retryable:false, description:'Resource quota exceeded',                     httpStatus:429 },
    ERR_EMAIL_VALIDATION:     { code:'ERR_EMAIL_VALIDATION',     category:'DATA',     severity:'LOW',      retryable:false, description:'Invalid email format',                         httpStatus:422 },
    ERR_DUPLICATE_ENTRY:      { code:'ERR_DUPLICATE_ENTRY',      category:'DATA',     severity:'LOW',      retryable:false, description:'Duplicate record',                              httpStatus:409 },
    ERR_DB_CONNECTION:        { code:'ERR_DB_CONNECTION',        category:'SYSTEM',   severity:'CRITICAL', retryable:true,  description:'Database connection failure',                httpStatus:503, suggestedAction:'RETRY_WITH_CAUTION' },
    ERR_REDIS_CONNECTION:     { code:'ERR_REDIS_CONNECTION',     category:'SYSTEM',   severity:'CRITICAL', retryable:true,  description:'Redis connection failure',                   httpStatus:503, suggestedAction:'RETRY_WITH_CAUTION' },
    ERR_OPERATION_FORBIDDEN:  { code:'ERR_OPERATION_FORBIDDEN',  category:'BUSINESS', severity:'MEDIUM',   retryable:false, description:'Operation forbidden by business rules',      httpStatus:403 }
};

// ======================== Шаблоны классификации ========================
const PATTERNS = [
    { regex: /ECONNREFUSED|connection refused/i,                                    code:'ERR_CONNECTION_REFUSED' },
    { regex: /ETIMEDOUT|timed?\s*out/i,                                              code:'ERR_NETWORK_TIMEOUT' },
    { regex: /ENOTFOUND|EAI_AGAIN|DNS\s*(resolve|fail)/i,                           code:'ERR_DNS_RESOLUTION' },
    { regex: /EPIPE|broken pipe/i,                                                  code:'ERR_CONNECTION_REFUSED' },
    { regex: /rate limit|too many requests/i,                                       code:'ERR_RATE_LIMIT' },
    { regex: /quota|limit exceeded/i,                                               code:'ERR_QUOTA_EXCEEDED' },
    { regex: /invalid.*email|email.*invalid|bad.*format/i,                          code:'ERR_EMAIL_VALIDATION' },
    { regex: /duplicate|unique.*constraint/i,                                       code:'ERR_DUPLICATE_ENTRY' },
    { regex: /database.*error|can't connect to database/i,                          code:'ERR_DB_CONNECTION' },
    { regex: /redis.*error|can't connect to redis/i,                                code:'ERR_REDIS_CONNECTION' }
];

// ======================== Константы ========================
const ONE_HOUR_MS      = 3600000;
const TEN_MINUTES_MS   = 600000;
const FIVE_MINUTES_MS   = 300000;
const MAX_TIMESERIES       = 100;
const MAX_UNKNOWN_ERRS     = 2000;
const HW_PERIOD_SEC        = 3600;
const HW_SEASONS           = 24;
const GENETIC_POOL_SIZE    = 10;

// ======================== Правила алертинга по умолчанию ========================
const DEFAULT_ALERT_RULES = {
    db_critical_rate: {
        metric: 'error_rate',
        code: 'ERR_DB_CONNECTION',
        threshold: 0.1,
        windowMs: 300000,
        severity: 'critical',
        action: 'emit:alert',
        message: 'High rate of DB connection errors'
    },
    error_burst: {
        metric: 'burst',
        threshold: 5,
        windowMs: 10000,
        severity: 'warning',
        action: 'emit:anomaly:error_burst',
        message: 'Error burst detected'
    },
    retryable_ratio: {
        metric: 'retryable_ratio',
        threshold: 0.2,
        windowMs: 300000,
        severity: 'warning',
        action: 'emit:alert',
        message: 'Retryable error ratio exceeded'
    },
    circuit_breaker_open: {
        metric: 'circuit_breaker',
        state: 'OPEN',
        action: 'emit:alert',
        severity: 'critical',
        message: 'Circuit breaker is OPEN'
    }
};

// ======================== ErrAI ========================
class ErrAI extends EventEmitter {
    constructor(options = {}) {
        super();
        this.service = options.service || 'core';
        this.statePath = options.statePath || path.join(__dirname, 'errai_state.json');
        this.catalogPath = options.catalogPath || path.join(__dirname, 'catalog.json');
        this._externalBus = options.bus || null;

        // Загрузка каталога (синхронно, т.к. необходимо до начала работы)
        this.catalog = this._loadCatalogSync();

        this._storage = new AsyncLocalStorage();
        this._transports = [this._stdoutTransport];

        // Метрики
        this.metrics = {
            errors: new Map(),           // код -> { count, lastSeen, timeseries[] }
            errorsByModule: new Map(),   // модуль -> Map(код -> count)
            successes: new Map(),        // модуль -> total successes
            retries: new Map(),          // opId -> { attempts, success, module, startedAt, params }
            burstWindows: new Map(),     // код -> { count, timer }
            circuitBreakers: new Map()   // serviceId -> CB
        };

        // Параметры (адаптивные)
        this.params = {
            burstThreshold: 5,
            burstWindowMs: 10000,
            globalRetryBaseDelay: 200,
            globalRetryMaxDelay: 30000,
            adaptiveFactor: 0.5,
            learningRate: 0.1,
            predictionHorizonMs: 60000,
            circuitBreakerThreshold: 3,
            circuitBreakerTimeoutMs: 30000,
            maxUnknownErrors: MAX_UNKNOWN_ERRS,
            evolutionIntervalMs: 30000,
            saveIntervalMs: 60000,
            hwAlpha: 0.2,
            hwBeta: 0.1,
            hwGamma: 0.05,
            geneticMutationRate: 0.2,
            metaExploration: 0.3,
            metaLearningDecay: 0.95,
            metaUpdateInterval: 120000
        };

        // Модели прогнозирования
        this._hwState = new Map();           // модуль -> { level, trend, seasonal[] }
        this._geneticPool = new Map();       // модуль -> Gene[]
        this._unknownErrors = new Map();     // хеш -> { message, count, codeAssigned, assignedCode }

        // Мета-история для оптимизации гиперпараметров
        this._metaHistory = [];
        this._metaBestScore = 0;

        // Правила алертинга
        this.alertRules = Object.assign({}, DEFAULT_ALERT_RULES, options.alertRules || {});

        // Загрузка предыдущего состояния
        this._loadStateSync();

        // Подписка на шину событий
        this._subscribeToBus();

        // Периодические задачи
        this._evolutionTimer = setInterval(() => this._evolve(), this.params.evolutionIntervalMs).unref();
        this._saveTimer = setInterval(() => this._saveState(), this.params.saveIntervalMs).unref();
        this._metaTimer = setInterval(() => this._metaEvolve(), this.params.metaUpdateInterval).unref();
    }

    // ------------------- Контекст и транспорты -------------------
    runWithContext(store, fn) {
        return this._storage.run(store, fn);
    }

    attachTransport(fn) {
        this._transports.push(fn);
    }

    // ------------------- Логирование -------------------
    info(message, meta = {}) {
        this._emit(this._buildEntry('INFO', message, meta));
    }

    warn(message, meta = {}) {
        this._emit(this._buildEntry('WARN', message, meta));
    }

    error(err, meta = {}) {
        const entry = this._buildEntry('ERROR', err, meta);
        this._trackError(entry);
        this._emit(entry);
        this._evaluateAlertRules(entry);
    }

    fatal(err, meta = {}) {
        const entry = this._buildEntry('FATAL', err, meta);
        this._trackError(entry);
        this._emit(entry);
        this._evaluateAlertRules(entry);
    }

    // ------------------- Классификация (семантическая) -------------------
    classify(err) {
        if (typeof err === 'string') err = { message: err };
        if (err.code && this.catalog[err.code]) {
            return { ...this.catalog[err.code], code: err.code, originalMessage: err.message || '' };
        }

        const msg = err.message || err.toString() || '';
        const normalized = msg.toLowerCase().replace(/[^a-z0-9\s]/g, '');
        for (const p of PATTERNS) {
            if (p.regex.test(normalized) && this.catalog[p.code]) {
                return { ...this.catalog[p.code], code: p.code, originalMessage: msg };
            }
        }

        // Автокаталогизация с учётом расстояния Левенштейна для кластеризации
        const hash = this._levenshteinCluster(normalized);
        if (this._unknownErrors.has(hash)) {
            const rec = this._unknownErrors.get(hash);
            rec.count++;
            if (rec.count >= 3 && !rec.codeAssigned) {
                rec.codeAssigned = true;
                let newCode = this._generateCode(normalized);
                // Уникальность
                if (this.catalog[newCode]) newCode += '_' + Date.now().toString(36);
                rec.assignedCode = newCode;
                this._addToCatalog(rec.assignedCode, rec.message).catch(e => this._logInternalError(e));
            }
            if (rec.assignedCode && this.catalog[rec.assignedCode]) {
                return { ...this.catalog[rec.assignedCode], code: rec.assignedCode, originalMessage: msg };
            }
        } else {
            if (this._unknownErrors.size < this.params.maxUnknownErrors) {
                this._unknownErrors.set(hash, { message: normalized, count: 1, codeAssigned: false });
            }
        }
        return { ...this.catalog['ERR_UNKNOWN'], code: 'ERR_UNKNOWN', originalMessage: msg };
    }

    // Кластеризация на основе грубого расстояния Левенштейна (для похожих сообщений)
    _levenshteinCluster(msg) {
        // Ищем наиболее похожее известное неизвестное сообщение (порог > 0.7 схожести)
        let bestHash = null;
        let bestSimilarity = 0;
        for (const [hash, rec] of this._unknownErrors.entries()) {
            const sim = this._similarity(msg, rec.message);
            if (sim > 0.7 && sim > bestSimilarity) {
                bestSimilarity = sim;
                bestHash = hash;
            }
        }
        return bestHash || this._hash(msg);
    }

    _similarity(a, b) {
        // Простая мера схожести на основе общих биграмм
        if (a === b) return 1;
        const bigramsA = new Set(this._getBigrams(a));
        const bigramsB = new Set(this._getBigrams(b));
        const intersection = new Set([...bigramsA].filter(x => bigramsB.has(x)));
        const union = new Set([...bigramsA, ...bigramsB]);
        return union.size === 0 ? 0 : intersection.size / union.size;
    }

    _getBigrams(str) {
        const words = str.split(/\s+/).filter(w => w.length > 1);
        const bigrams = [];
        for (let i = 0; i < words.length - 1; i++) bigrams.push(words[i] + '_' + words[i+1]);
        return bigrams;
    }

    // ------------------- Retry с Circuit Breaker -------------------
    async retry(operation, userOptions = {}) {
        const mod = userOptions.module || 'unknown';
        const bestGene = (this._geneticPool.get(mod) || [])[0] || {};
        const opts = {
            maxAttempts:        userOptions.maxAttempts        || bestGene.maxAttempts        || 5,
            baseDelay:          userOptions.baseDelay          ?? bestGene.baseDelay          ?? this.params.globalRetryBaseDelay,
            maxDelay:           userOptions.maxDelay           ?? bestGene.maxDelay           ?? this.params.globalRetryMaxDelay,
            backoffMultiplier:  userOptions.backoffMultiplier  || bestGene.backoffMultiplier  || 2,
            jitter:             userOptions.jitter             ?? 200,
            retryableCategories:userOptions.retryableCategories|| ['NETWORK','RESOURCE'],
            strategy:           userOptions.strategy           || bestGene.strategy           || 'adaptive',
            module: mod,
            serviceId:          userOptions.serviceId          || null
        };

        const opId = crypto.randomUUID();
        this.metrics.retries.set(opId, { attempts:0, success:false, module:mod, startedAt:Date.now(), params:{...opts} });
        let lastError;
        for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
            this.metrics.retries.get(opId).attempts = attempt;
            if (opts.serviceId && !this._circuitBreakerAllow(opts.serviceId)) {
                lastError = new Error(`Circuit breaker OPEN for ${opts.serviceId}`);
                lastError.code = 'ERR_CIRCUIT_OPEN';
                break;
            }
            try {
                const result = await operation();
                this.metrics.retries.get(opId).success = true;
                this.recordSuccess(opts.module);
                if (opts.serviceId) this._circuitBreakerSuccess(opts.serviceId);
                return result;
            } catch (err) {
                lastError = err;
                const classif = this.classify(err);
                if (opts.serviceId) this._circuitBreakerFailure(opts.serviceId);
                if (!classif.retryable || !opts.retryableCategories.includes(classif.category)) throw err;
                if (attempt >= opts.maxAttempts) break;
                const delay = this._calcDelay(attempt, opts);
                await new Promise(r => setTimeout(r, delay));
            }
        }
        throw lastError;
    }

    async deferToQueue(task, error, queueManager) {
        if (!task || typeof queueManager?.pushTask !== 'function') return false;
        const classif = this.classify(error);
        if (!classif.retryable) return false;
        const retryCount = (task.retryCount || 0) + 1;
        const newTask = { ...task, retryCount, _lastError: error.message, _errorCode: classif.code, _deferredAt: Date.now() };
        const delay = Math.min(1000 * Math.pow(2, retryCount) + Math.random() * 500, 60000);
        try {
            await queueManager.pushTask(newTask, { delay });
            this.info('Task deferred', { module:'errai', details:{ taskId: task.id, retryCount } });
            return true;
        } catch (e) {
            this.error(e, { module:'errai' });
            return false;
        }
    }

    // ------------------- Предсказание отказов (Холт-Винтерс) -------------------
    predictFailure(moduleName) {
        const hw = this._hwState.get(moduleName);
        if (!hw) return 0.1;
        const horizonSec = this.params.predictionHorizonMs / 1000;
        const { level, trend, seasonal } = hw;
        const predLevel = level + horizonSec * trend;
        const nowSec = Math.floor(Date.now() / 1000);
        const idx = Math.floor((nowSec + horizonSec) / HW_PERIOD_SEC) % seasonal.length;
        const predicted = predLevel + seasonal[idx];
        const rate = Math.max(0, predicted / 3600);
        return Math.min(1 - Math.exp(-rate * horizonSec), 0.999);
    }

    healthReport() {
        const modules = new Set([...this.metrics.successes.keys(), ...this.metrics.errorsByModule.keys()]);
        const report = {};
        for (const m of modules) {
            report[m] = {
                failureProbability: this.predictFailure(m),
                successCount: this.metrics.successes.get(m) || 0,
                errorCounts: Object.fromEntries(this.metrics.errorsByModule.get(m) || new Map())
            };
        }
        return report;
    }

    recordSuccess(moduleName) {
        this.metrics.successes.set(moduleName, (this.metrics.successes.get(moduleName) || 0) + 1);
    }

    getPrometheusMetrics() {
        let out = '';
        for (const [code, rec] of this.metrics.errors) out += `errai_errors_total{code="${code}"} ${rec.count}\n`;
        for (const [mod, cnt] of this.metrics.successes) out += `errai_successes_total{module="${mod}"} ${cnt}\n`;
        return out;
    }

    prettyPrint(entry) {
        const colors = { INFO:'\x1b[32m', WARN:'\x1b[33m', ERROR:'\x1b[31m', FATAL:'\x1b[35m' };
        const c = colors[entry.level] || '\x1b[0m';
        return `${c}[${entry.timestamp}] [${entry.level}] [${entry.module}] ${entry.message}${entry.errorCode ? ' (code: '+entry.errorCode+')' : ''}\x1b[0m`;
    }

    formatLogs(format, entries = []) {
        if (format === 'pretty') return entries.map(e => this.prettyPrint(e)).join('\n');
        if (format === 'prometheus') return this.getPrometheusMetrics();
        return entries.map(e => JSON.stringify(e)).join('\n');
    }

    // ------------------- Алертинг -------------------
    _evaluateAlertRules(entry) {
        if (!entry.errorCode) return;

        // db_critical_rate
        const dbRule = this.alertRules.db_critical_rate;
        if (dbRule && entry.errorCode === dbRule.code) {
            const rec = this.metrics.errors.get(dbRule.code);
            if (rec) {
                const now = Date.now();
                const recent = rec.timeseries.filter(p => now - p.ts < dbRule.windowMs).length;
                const dbSuccess = this.metrics.successes.get('db') || 0;
                const rate = dbSuccess > 0 ? recent / dbSuccess : recent;
                if (rate > dbRule.threshold) {
                    this.emit('alert', { rule: 'db_critical_rate', severity: dbRule.severity, message: dbRule.message });
                }
            }
        }

        // retryable_ratio
        const ratioRule = this.alertRules.retryable_ratio;
        if (ratioRule) {
            const allErrors = Array.from(this.metrics.errors.values()).reduce((sum, r) => sum + r.count, 0);
            const retryableErrors = Array.from(this.metrics.errors.entries())
                .filter(([code]) => this.catalog[code]?.retryable)
                .reduce((sum, [, r]) => sum + r.count, 0);
            const ratio = allErrors > 0 ? retryableErrors / allErrors : 0;
            if (ratio > ratioRule.threshold) {
                this.emit('alert', { rule: 'retryable_ratio', severity: ratioRule.severity, message: ratioRule.message });
            }
        }
    }

    async shutdown() {
        clearInterval(this._evolutionTimer);
        clearInterval(this._saveTimer);
        clearInterval(this._metaTimer);
        for (const win of this.metrics.burstWindows.values()) clearTimeout(win.timer);
        for (const cb of this.metrics.circuitBreakers.values()) clearTimeout(cb._halfOpenTimer);
        this._unsubscribeFromBus();
        await this._saveState();
        await this._saveCatalog();
    }

    // ======================== Приватные методы ========================
    _buildEntry(level, input, meta) {
        const store = this._storage.getStore() || {};
        const isError = input instanceof Error || level === 'ERROR' || level === 'FATAL';
        const message = typeof input === 'string' ? input : (input.message || '');
        const classif = isError ? this.classify(input) : null;
        return {
            timestamp: new Date().toISOString(),
            level,
            service: this.service,
            module: meta.module || store.module || 'unknown',
            sessionId: meta.sessionId || store.sessionId,
            requestId: meta.requestId || store.requestId,
            message,
            errorCode: classif?.code,
            errorCategory: classif?.category,
            retryable: classif?.retryable,
            details: { ...meta.details, stack: isError && input.stack ? input.stack : undefined }
        };
    }

    _emit(entry) {
        for (const t of this._transports) t(entry);
        if (this._externalBus) {
            try { this._externalBus.emit('log', entry); } catch (_) {}
        }
    }

    _trackError(entry) {
        if (!entry.errorCode || !entry.module) return;
        const code = entry.errorCode, mod = entry.module, now = Date.now();
        let rec = this.metrics.errors.get(code) || { count:0, lastSeen:0, timeseries:[] };
        rec.count++; rec.lastSeen = now;
        rec.timeseries.push({ ts:now, count:rec.count });
        if (rec.timeseries.length > MAX_TIMESERIES) rec.timeseries = rec.timeseries.slice(-MAX_TIMESERIES);
        this.metrics.errors.set(code, rec);
        let modMap = this.metrics.errorsByModule.get(mod) || new Map();
        modMap.set(code, (modMap.get(code)||0)+1);
        this.metrics.errorsByModule.set(mod, modMap);
        this._updateHW(mod);
        this._checkBurst(code);
    }

    _updateHW(moduleName) {
        const nowSec = Math.floor(Date.now() / 1000);
        let hw = this._hwState.get(moduleName);
        if (!hw) {
            hw = { level:1, trend:0, seasonal: Array.from({length: HW_SEASONS}, () => Math.random() * 0.5) };
            this._hwState.set(moduleName, hw);
        }
        const idx = Math.floor(nowSec / HW_PERIOD_SEC) % HW_SEASONS;
        const obs = 1;
        const a = this.params.hwAlpha, b = this.params.hwBeta, g = this.params.hwGamma;
        const lastLevel = hw.level, lastTrend = hw.trend, lastSeas = hw.seasonal[idx];
        hw.level = a * (obs - lastSeas) + (1 - a) * (lastLevel + lastTrend);
        hw.trend = b * (hw.level - lastLevel) + (1 - b) * lastTrend;
        hw.seasonal[idx] = g * (obs - hw.level) + (1 - g) * lastSeas;
    }

    _checkBurst(code) {
        let win = this.metrics.burstWindows.get(code) || { count:0, timer:null };
        win.count++;
        if (!win.timer) {
            win.timer = setTimeout(() => {
                const cnt = win.count;
                this.metrics.burstWindows.delete(code);
                if (cnt >= this.params.burstThreshold) {
                    this._emitEvent('anomaly:error_burst', { code, count:cnt, windowMs:this.params.burstWindowMs, timestamp: new Date().toISOString() });
                    this._emitEvent('errai:request_restart', { module: this._inferModuleByCode(code), code });
                }
            }, this.params.burstWindowMs).unref();
            this.metrics.burstWindows.set(code, win);
        }
        if (win.count > this.params.burstThreshold * 3) {
            this.params.burstThreshold = Math.ceil(this.params.burstThreshold * 1.2);
        }
    }

    _inferModuleByCode(code) {
        if (code.startsWith('ERR_DB')) return 'db';
        if (code.startsWith('ERR_REDIS')) return 'redis';
        if (code.includes('CRAWLER')) return 'crawler';
        return 'unknown';
    }

    _calcDelay(attempt, opts) {
        let delay;
        switch (opts.strategy) {
            case 'fixed': delay = opts.baseDelay; break;
            case 'exponential': delay = Math.min(opts.baseDelay * Math.pow(opts.backoffMultiplier, attempt-1), opts.maxDelay); break;
            default: // adaptive
                const succ = this.metrics.successes.get(opts.module) || 0;
                const ratio = Math.min(succ/10, 1);
                const adj = 1 - this.params.adaptiveFactor * ratio;
                delay = Math.min(opts.baseDelay * Math.pow(opts.backoffMultiplier, attempt-1) * adj, opts.maxDelay);
        }
        return delay + Math.random() * opts.jitter;
    }

    // ------------------- Circuit Breaker -------------------
    _circuitBreakerAllow(serviceId) {
        let cb = this.metrics.circuitBreakers.get(serviceId);
        if (!cb || cb.state === 'CLOSED') return true;
        if (cb.state === 'OPEN') {
            if (Date.now() > cb.nextAttempt) {
                cb.state = 'HALF_OPEN';
                cb._pendingHalfOpen = false;
                return true;
            }
            return false;
        }
        if (cb.state === 'HALF_OPEN') {
            if (cb._pendingHalfOpen) return false;
            cb._pendingHalfOpen = true;
            clearTimeout(cb._halfOpenTimer);
            cb._halfOpenTimer = setTimeout(() => {
                if (cb._pendingHalfOpen) cb._pendingHalfOpen = false;
            }, this.params.circuitBreakerTimeoutMs).unref();
            return true;
        }
        return true;
    }

    _circuitBreakerFailure(serviceId) {
        let cb = this.metrics.circuitBreakers.get(serviceId) || {
            state:'CLOSED', failures:0, successes:0, lastFailure:0, nextAttempt:0, _pendingHalfOpen:false, _halfOpenTimer:null
        };
        cb.failures++; cb.lastFailure = Date.now();
        if (cb.state === 'HALF_OPEN' || cb.failures >= this.params.circuitBreakerThreshold) {
            cb.state = 'OPEN';
            cb.nextAttempt = Date.now() + this.params.circuitBreakerTimeoutMs;
            cb._pendingHalfOpen = false;
            clearTimeout(cb._halfOpenTimer);
            this._emitEvent('circuit_breaker:open', { serviceId, nextAttempt: new Date(cb.nextAttempt).toISOString() });
        }
        this.metrics.circuitBreakers.set(serviceId, cb);
    }

    _circuitBreakerSuccess(serviceId) {
        const cb = this.metrics.circuitBreakers.get(serviceId);
        if (!cb) return;
        cb.failures = 0; cb.successes++;
        if (cb.state === 'HALF_OPEN') {
            cb.state = 'CLOSED';
            cb._pendingHalfOpen = false;
            clearTimeout(cb._halfOpenTimer);
            this._emitEvent('circuit_breaker:close', { serviceId });
        }
    }

    // ------------------- Каталог -------------------
    async _addToCatalog(code, desc) {
        if (this.catalog[code]) return;
        this.catalog[code] = {
            code, category:'AUTO', severity:'MEDIUM', retryable:true,
            description: `Auto-classified: ${desc.substring(0,100)}`, httpStatus:500
        };
        await this._saveCatalog();
        this._emitEvent('catalog:extended', { code });
        if (this._externalBus) this._externalBus.emit('errai:catalog_update', { code, entry: this.catalog[code] });
    }

    async _saveCatalog() {
        try {
            // Атомарная запись через временный файл
            const tmp = this.catalogPath + '.tmp';
            await fs.writeFile(tmp, JSON.stringify({ errors: this.catalog }, null, 2));
            await fs.rename(tmp, this.catalogPath);
        } catch(e) { this._logInternalError(e); }
    }

    _loadCatalogSync() {
        let cat = { ...BUILTIN_CATALOG };
        try {
            if (fsSync.existsSync(this.catalogPath)) {
                const raw = fsSync.readFileSync(this.catalogPath, 'utf8');
                const fileCat = JSON.parse(raw).errors || {};
                cat = { ...cat, ...fileCat };
            }
        } catch(e) { this._logInternalError(e); }
        if (!cat['ERR_UNKNOWN']) cat['ERR_UNKNOWN'] = BUILTIN_CATALOG['ERR_UNKNOWN'];
        return cat;
    }

    // ------------------- Эволюция (генетика + мета) -------------------
    _evolve() {
        const now = Date.now();
        const recent = Array.from(this.metrics.retries.values()).filter(v => now - v.startedAt < FIVE_MINUTES_MS);
        if (recent.length >= 5) {
            const successRate = recent.filter(v => v.success).length / recent.length;
            if (successRate < 0.6) {
                this.params.globalRetryBaseDelay = Math.max(50, this.params.globalRetryBaseDelay * 0.8);
            } else if (successRate > 0.9) {
                this.params.globalRetryBaseDelay = Math.min(5000, this.params.globalRetryBaseDelay * 1.1);
            }
        }
        for (const mod of this.metrics.successes.keys()) this._evolveModule(mod);
        for (const [id, v] of this.metrics.retries) if (now - v.startedAt > TEN_MINUTES_MS) this.metrics.retries.delete(id);
        if (this._unknownErrors.size > this.params.maxUnknownErrors) {
            const sorted = Array.from(this._unknownErrors.entries()).sort((a,b) => a[1].count - b[1].count);
            for (const [hash] of sorted.slice(0, this._unknownErrors.size - this.params.maxUnknownErrors)) this._unknownErrors.delete(hash);
        }
    }

    _evolveModule(mod) {
        let pool = this._geneticPool.get(mod) || Array.from({length: GENETIC_POOL_SIZE}, () => ({
            maxAttempts: Math.floor(Math.random()*3)+3,
            baseDelay: Math.random()*500+50,
            maxDelay: Math.random()*20000+10000,
            backoffMultiplier: Math.random()*2+1.5,
            strategy: ['fixed','exponential','adaptive'][Math.floor(Math.random()*3)],
            fitness: 0
        }));
        const modRetries = Array.from(this.metrics.retries.values()).filter(r => r.module === mod);
        for (const gene of pool) {
            const matching = modRetries.filter(r => {
                const p = r.params;
                return p && p.maxAttempts === gene.maxAttempts &&
                    Math.abs(p.baseDelay - gene.baseDelay) < 50 &&
                    Math.abs(p.maxDelay - gene.maxDelay) < 5000 &&
                    Math.abs(p.backoffMultiplier - gene.backoffMultiplier) < 0.5 &&
                    p.strategy === gene.strategy;
            });
            const total = matching.length;
            const suc = matching.filter(r => r.success).length;
            gene.fitness = total > 0 ? suc / total : 0.5;
        }
        pool.sort((a,b) => b.fitness - a.fitness);
        const newPool = pool.slice(0, 2);
        while (newPool.length < GENETIC_POOL_SIZE) {
            const p1 = pool[Math.floor(Math.random()*2)];
            const p2 = pool[Math.floor(Math.random()*2)];
            const child = {
                maxAttempts: Math.round((p1.maxAttempts + p2.maxAttempts)/2),
                baseDelay: (p1.baseDelay + p2.baseDelay)/2,
                maxDelay: (p1.maxDelay + p2.maxDelay)/2,
                backoffMultiplier: (p1.backoffMultiplier + p2.backoffMultiplier)/2,
                strategy: Math.random() < 0.5 ? p1.strategy : p2.strategy,
                fitness: 0
            };
            if (Math.random() < this.params.geneticMutationRate) {
                child.maxAttempts = Math.max(1, child.maxAttempts + (Math.random()<0.5 ? -1 : 1));
                child.baseDelay = Math.max(10, child.baseDelay * (0.8 + Math.random()*0.4));
                child.maxDelay = Math.max(1000, child.maxDelay * (0.8 + Math.random()*0.4));
                child.backoffMultiplier = Math.max(1.1, child.backoffMultiplier * (0.9 + Math.random()*0.2));
            }
            newPool.push(child);
        }
        this._geneticPool.set(mod, newPool);
        this._emitEvent('errai:genetic_evolved', { module: mod, bestGene: newPool[0] });
    }

    _metaEvolve() {
        const recentRetries = Array.from(this.metrics.retries.values()).filter(v => Date.now() - v.startedAt < 120000);
        if (recentRetries.length < 10) return;
        const score = recentRetries.filter(v => v.success).length / recentRetries.length;

        this._metaHistory.push({ params: { ...this.params }, score });
        if (this._metaHistory.length > 50) this._metaHistory.shift();
        if (score >= this._metaBestScore) this._metaBestScore = score;

        if (Math.random() < this.params.metaExploration) {
            // Случайно мутируем один из гиперпараметров
            const keys = ['globalRetryBaseDelay', 'globalRetryMaxDelay', 'adaptiveFactor', 'hwAlpha', 'hwBeta', 'hwGamma', 'geneticMutationRate'];
            const key = keys[Math.floor(Math.random() * keys.length)];
            const oldVal = this.params[key];
            const newVal = oldVal * (0.8 + Math.random() * 0.4);
            if (key === 'globalRetryBaseDelay') this.params.globalRetryBaseDelay = Math.min(1000, Math.max(50, newVal));
            else if (key === 'globalRetryMaxDelay') this.params.globalRetryMaxDelay = Math.min(120000, Math.max(5000, newVal));
            else if (key === 'adaptiveFactor') this.params.adaptiveFactor = Math.min(0.95, Math.max(0.05, newVal));
            else if (key === 'hwAlpha') this.params.hwAlpha = Math.min(0.5, Math.max(0.05, newVal));
            else if (key === 'hwBeta') this.params.hwBeta = Math.min(0.3, Math.max(0.02, newVal));
            else if (key === 'hwGamma') this.params.hwGamma = Math.min(0.2, Math.max(0.01, newVal));
            else if (key === 'geneticMutationRate') this.params.geneticMutationRate = Math.min(0.5, Math.max(0.05, newVal));
        }

        // Градиентная подстройка exploration
        if (this._metaHistory.length % 5 === 0 && this._metaHistory.length >= 10) {
            const recentScores = this._metaHistory.slice(-10).map(h => h.score);
            const avgRecent = recentScores.reduce((a,b) => a+b, 0) / recentScores.length;
            if (avgRecent < this._metaBestScore * 0.9) {
                this.params.metaExploration = Math.min(0.8, this.params.metaExploration + 0.05);
            } else {
                this.params.metaExploration = Math.max(0.1, this.params.metaExploration - 0.02);
            }
        }
    }

    // ------------------- Распределённое обучение -------------------
    _subscribeToBus() {
        if (!this._externalBus) return;
        this._externalBus.on('errai:catalog_update', this._onCatalogUpdate.bind(this));
        this._externalBus.on('errai:metric_share', this._onMetricShare.bind(this));
    }

    _unsubscribeFromBus() {
        if (!this._externalBus) return;
        this._externalBus.removeListener('errai:catalog_update', this._onCatalogUpdate.bind(this));
        this._externalBus.removeListener('errai:metric_share', this._onMetricShare.bind(this));
    }

    _onCatalogUpdate({ code, entry }) {
        if (!this.catalog[code]) {
            this.catalog[code] = entry;
            this._saveCatalog().catch(e => this._logInternalError(e));
        }
    }

    _onMetricShare(data) {
        if (data.errors) {
            for (const [code, entry] of data.errors) {
                const local = this.metrics.errors.get(code);
                if (!local) this.metrics.errors.set(code, entry);
                else {
                    local.count = Math.max(local.count, entry.count);
                    local.lastSeen = Math.max(local.lastSeen, entry.lastSeen);
                }
            }
        }
    }

    _shareMetrics() {
        if (!this._externalBus) return;
        const errors = Array.from(this.metrics.errors.entries()).map(([k,v]) => [k, { count: v.count, lastSeen: v.lastSeen }]);
        this._externalBus.emit('errai:metric_share', { errors, service: this.service });
    }

    // ------------------- Сохранение / восстановление -------------------
    async _saveState() {
        try {
            const state = {
                timestamp: new Date().toISOString(),
                params: this.params,
                errors: Array.from(this.metrics.errors.entries()).map(([k,v]) => [k, { count: v.count, lastSeen: v.lastSeen, timeseries: v.timeseries.slice(-50) }]),
                errorsByModule: Array.from(this.metrics.errorsByModule.entries()).map(([mod, map]) => [mod, Array.from(map)]),
                successes: Array.from(this.metrics.successes.entries()),
                unknownErrors: Array.from(this._unknownErrors.entries()).map(([hash, v]) => ({ hash, message: v.message, count: v.count, codeAssigned: v.codeAssigned, assignedCode: v.assignedCode })),
                circuitBreakers: Array.from(this.metrics.circuitBreakers.entries()).map(([id, cb]) => ({ id, state: cb.state, failures: cb.failures, successes: cb.successes, lastFailure: cb.lastFailure, nextAttempt: cb.nextAttempt })),
                geneticPool: Array.from(this._geneticPool.entries()),
                metaHistory: this._metaHistory,
                metaBestScore: this._metaBestScore
            };
            const tmp = this.statePath + '.tmp';
            await fs.writeFile(tmp, JSON.stringify(state, null, 2));
            await fs.rename(tmp, this.statePath);
            this._shareMetrics();
        } catch(e) { this._logInternalError(e); }
    }

    _loadStateSync() {
        try {
            if (!fsSync.existsSync(this.statePath)) return;
            const raw = fsSync.readFileSync(this.statePath, 'utf8');
            const state = JSON.parse(raw);
            if (state.params) Object.assign(this.params, state.params);
            if (state.errors) for (const [code, e] of state.errors) this.metrics.errors.set(code, { count: e.count, lastSeen: e.lastSeen, timeseries: e.timeseries || [] });
            if (state.errorsByModule) for (const [mod, entries] of state.errorsByModule) this.metrics.errorsByModule.set(mod, new Map(entries));
            if (state.successes) for (const [mod, cnt] of state.successes) this.metrics.successes.set(mod, cnt);
            if (state.unknownErrors) {
                this._unknownErrors.clear();
                for (const u of state.unknownErrors) this._unknownErrors.set(u.hash, { message: u.message, count: u.count, codeAssigned: u.codeAssigned, assignedCode: u.assignedCode });
            }
            if (state.circuitBreakers) for (const cb of state.circuitBreakers) this.metrics.circuitBreakers.set(cb.id, { state: cb.state, failures: cb.failures, successes: cb.successes, lastFailure: cb.lastFailure, nextAttempt: cb.nextAttempt, _pendingHalfOpen: false, _halfOpenTimer: null });
            if (state.geneticPool) {
                this._geneticPool.clear();
                for (const [mod, pool] of state.geneticPool) this._geneticPool.set(mod, pool);
            }
            if (state.metaHistory) this._metaHistory = state.metaHistory;
            if (state.metaBestScore !== undefined) this._metaBestScore = state.metaBestScore;
        } catch(e) { this._logInternalError(e); }
    }

    // ------------------- Утилиты -------------------
    _hash(str) {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const chr = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + chr;
            hash |= 0;
        }
        return hash.toString(36);
    }

    _generateCode(msg) {
        const words = msg.split(/\s+/).filter(w => w.length > 3).slice(0,3).map(w => w.replace(/[^a-zA-Z]/g, '').toUpperCase());
        const base = words.join('_') || 'UNKNOWN';
        let code = `ERR_AUTO_${base}`.substring(0, 40);
        if (this.catalog[code]) {
            let counter = 1;
            while (this.catalog[`${code}_${counter}`]) counter++;
            code = `${code}_${counter}`;
        }
        return code;
    }

    _stdoutTransport(entry) {
        console.log(JSON.stringify(entry));
    }

    _logInternalError(err) {
        console.error(JSON.stringify({
            timestamp: new Date().toISOString(),
            level: 'ERROR',
            service: this.service,
            module: 'errai',
            message: err.message,
            details: { stack: err.stack }
        }));
    }

    _emitEvent(event, data) {
        this.emit(event, data);
        if (this._externalBus) {
            try { this._externalBus.emit(event, data); } catch (_) {}
        }
    }
}

module.exports = { ErrAI };