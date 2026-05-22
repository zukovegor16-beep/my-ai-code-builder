// core/api/routes/_sentient.js
const EventEmitter = require('events');

// Константы (никакой магии)
const DEGRADATION_ANOMALY_MS = 45_000;
const DEGRADATION_CIRCUIT_MS = 90_000;
const DEGRADATION_POOL_MS = 30_000;
const PATTERN_TTL_REDIS = 3600;
const EVOLUTION_THRESHOLD = 1000;
const MAX_PATTERNS = 300;
const RPS_WINDOW_MS = 60_000;
const HIGH_LOAD_RPS = 500;
const HEALTH_CHECK_INTERVAL = 15_000;
const CONSOLIDATION_INTERVAL = 120_000;

class SentientCore extends EventEmitter {
  constructor(name, db, bus, options = {}) {
    super();
    this.name = name;
    this.db = db;
    this.bus = bus;

    // Раздельные хранилища (никакой путаницы)
    this.requestTimestamps = new Map();       // для RPS
    this.degradationFlags = new Map();        // флаги деградации
    this.domainCounters = new Map();          // счётчики доменов
    this.patterns = new Map();                // долговременные паттерны

    this.health = true;
    this.generation = 0;
    this._intervals = [];

    if (this.bus) {
      this.bus.on('anomaly:error_burst', (data) => this.onGlobalAnomaly(data));
      this.bus.on('circuit:open', (data) => this.onCircuitOpen(data));
      this.bus.on('email:found', (record) => this.onEmailFound(record));
      this.bus.on('pool:suggest', (info) => this.onPoolSuggest(info));
    }

    this._intervals.push(
      setInterval(() => this.selfCheck(), HEALTH_CHECK_INTERVAL),
      setInterval(() => this.consolidateLearning(), CONSOLIDATION_INTERVAL)
    );

    this._restoreMemory();
  }

  async _restoreMemory() {
    try {
      if (!this.db?.cache) return;
      const saved = await this.db.cache.get(`sentient:${this.name}:patterns`);
      if (saved && typeof saved === 'object') {
        for (const [key, val] of Object.entries(saved)) {
          this.patterns.set(key, { ...val, lastSeen: Date.now() });
        }
        this.generation = Math.max(1, Math.floor(this.patterns.size / EVOLUTION_THRESHOLD));
        this.emit('restored', { route: this.name, patterns: this.patterns.size });
      }
    } catch (e) {
      // Логируем ошибку Redis, но не падаем
      console.error(`[Sentient:${this.name}] Failed to restore memory:`, e.message);
    }
  }

  learn(querySignature, responseTimeMs, resultCount = 0) {
    if (responseTimeMs < 0 || resultCount < 0) return; // Базовая валидация
    const key = JSON.stringify(querySignature);
    let stats = this.patterns.get(key);
    if (!stats) {
      stats = { count: 0, totalTime: 0, minTime: Infinity, maxTime: 0, avgResultCount: 0, lastSeen: Date.now() };
      this.patterns.set(key, stats);
    }
    stats.count++;
    stats.totalTime += responseTimeMs;
    stats.minTime = Math.min(stats.minTime, responseTimeMs);
    stats.maxTime = Math.max(stats.maxTime, responseTimeMs);
    stats.avgResultCount = (stats.avgResultCount * (stats.count - 1) + resultCount) / stats.count;
    stats.lastSeen = Date.now();

    if (stats.count % EVOLUTION_THRESHOLD === 0) {
      this.generation++;
      this.emit('evolution', { route: this.name, generation: this.generation, pattern: key });
    }

    // Ограничение размера: удаляем самый старый паттерн
    if (this.patterns.size > MAX_PATTERNS) {
      let oldestKey = null;
      let oldestTime = Infinity;
      for (const [k, v] of this.patterns) {
        if (v.lastSeen < oldestTime) {
          oldestTime = v.lastSeen;
          oldestKey = k;
        }
      }
      if (oldestKey) this.patterns.delete(oldestKey);
    }
  }

  predictOptimalLimit(querySignature, requestedLimit) {
    const key = JSON.stringify(querySignature);
    const stats = this.patterns.get(key);
    if (!stats || stats.count < 5) return requestedLimit;

    const avgTime = stats.totalTime / stats.count;
    // Чем быстрее запрос и больше данных, тем выше лимит
    const optimal = Math.floor(
      stats.avgResultCount * Math.min(3, 150 / Math.max(1, avgTime))
    );
    return Math.min(1000, Math.max(10, optimal));
  }

  // Новый эффективный метод подсчёта RPS: окно фиксированной длины
  markRequest() {
    const now = Date.now();
    // Удаляем старые метки (только те, что за границей окна)
    if (this.requestTimestamps.size > 0) {
      const cutoff = now - RPS_WINDOW_MS;
      // Удаляем только первый элемент, так как Map сохраняет порядок вставки
      for (const [ts] of this.requestTimestamps) {
        if (ts < cutoff) this.requestTimestamps.delete(ts);
        else break; // Дальше все свежие
      }
    }
    this.requestTimestamps.set(now, 1);
    if (this.requestTimestamps.size > HIGH_LOAD_RPS) {
      this.emit('high_load_predicted', { route: this.name, rps: this.requestTimestamps.size });
    }
  }

  onGlobalAnomaly({ pattern }) {
    if (this.patterns.has(pattern)) {
      this.degradationFlags.set(`anomaly:${pattern}`, Date.now() + DEGRADATION_ANOMALY_MS);
    }
  }

  onCircuitOpen({ failures }) {
    if (failures > 8) {
      this.health = false;
      this.degradationFlags.set('global_circuit', Date.now() + DEGRADATION_CIRCUIT_MS);
      this.emit('degraded', { route: this.name, reason: 'circuit_open' });
    }
  }

  onPoolSuggest(info) {
    if (info.suggested < info.current) {
      this.degradationFlags.set('pool_shrink', Date.now() + DEGRADATION_POOL_MS);
    }
  }

  onEmailFound(record) {
    const domain = record.email.split('@')[1];
    const count = this.domainCounters.get(domain) || 0;
    this.domainCounters.set(domain, count + 1);
  }

  async selfCheck() {
    let dbHealthy = true;
    try {
      dbHealthy = this.db ? await this.db.healthCheck() : false;
    } catch (e) { dbHealthy = false; }

    // Проверяем флаги деградации
    const now = Date.now();
    for (const [key, expires] of this.degradationFlags) {
      if (now > expires) this.degradationFlags.delete(key);
    }
    const degraded = this.degradationFlags.size > 0;
    const previous = this.health;
    this.health = dbHealthy && !degraded;
    if (previous !== this.health) {
      this.emit(this.health ? 'recovered' : 'degraded', { route: this.name });
    }
    return this.health;
  }

  async consolidateLearning() {
    if (!this.db?.cache) return;
    try {
      const snapshot = {};
      for (const [key, val] of this.patterns) {
        snapshot[key] = {
          count: val.count,
          totalTime: val.totalTime,
          minTime: val.minTime,
          maxTime: val.maxTime,
          avgResultCount: val.avgResultCount,
        };
      }
      await this.db.cache.set(`sentient:${this.name}:patterns`, snapshot, PATTERN_TTL_REDIS);
    } catch (e) {
      console.error(`[Sentient:${this.name}] Consolidation failed:`, e.message);
    }
  }

  // Полный сброс состояния (используется при перезагрузке сервиса)
  reset() {
    this.patterns.clear();
    this.requestTimestamps.clear();
    this.degradationFlags.clear();
    this.domainCounters.clear();
    this.generation = 0;
    this.health = true;
  }

  destroy() {
    for (const id of this._intervals) clearInterval(id);
    if (this.bus) {
      this.bus.removeAllListeners('anomaly:error_burst');
      this.bus.removeAllListeners('circuit:open');
      this.bus.removeAllListeners('email:found');
      this.bus.removeAllListeners('pool:suggest');
    }
  }
}

module.exports = SentientCore;