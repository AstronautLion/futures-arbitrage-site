const STORAGE_KEY = 'fas-settings-v2';
const REFRESH_API_SESSION_KEY = 'fas-refresh-api-key-v1';
const TRADE_TRACKER_STORAGE_KEY = 'fas-trade-tracker-v1';
const RUNTIME_CONFIG_PATH = './config.json';
const DATA_FILES = {
  cross: './data/latest.json',
  funding: './data/funding.json',
  metrics: './data/metrics.json',
  adapterHealth: './data/adapter_health.json',
  glossary: './data/glossary.json',
  historyIndex: './data/history/index.json',
  historyDownsampledIndex: './data/history/downsampled-index.json',
};
const REFRESH_ENDPOINT = '/api/refresh';
const REFRESH_STATUS_ENDPOINT = '/api/refresh-status';
const REFRESH_RESULT_ENDPOINT = '/api/refresh-result';
const MARKET_STREAM_ENDPOINT = '/ws/market-stream';
const AI_CONFIDENCE_ENDPOINT = '/ai/v1/confidence';
const AI_PREDICT_ENDPOINT = '/ai/v1/predict-spread';
const AI_WHALE_ENDPOINT = '/ai/v1/whale-activity';
const AI_LIQUIDATION_ENDPOINT = '/ai/v1/liquidation-risk';
const AI_SMART_ROUTE_ENDPOINT = '/ai/v1/smart-route';
const AI_EXECUTION_ENDPOINT = '/ai/v1/execution-recommendation';
const AI_ADVICE_ENDPOINT = '/ai/v1/opportunity-advice';
const REFRESH_API_KEY_HEADER = 'X-Refresh-Key';
const ASSET_VERSION = '20260411-04';
const DATASET_CACHE_STORAGE_KEY = 'fas-dataset-cache-v1';
const DATASET_CACHE_MAX_AGE_MS = 12 * 60 * 60 * 1000;
const DATASET_CACHE_MAX_CHARS = 3_500_000;
const AUTO_REFRESH_MIN_INTERVAL_SECONDS = 15;
const AUTO_REFRESH_DEFAULT_INTERVAL_SECONDS = 120;
const AUTO_REFRESH_INTERVAL_OPTIONS = [15, 30, 60, 120, 300, 900];
const AUTO_REFRESH_FULL_INTERVAL_OPTIONS = [900, 1800, 3600];
const AUTO_REFRESH_DEFAULT_STRATEGY = 'filtered-only';
const AUTO_REFRESH_DEFAULT_FULL_INTERVAL_SECONDS = 1800;
const AI_PREDICTION_HORIZON_OPTIONS = [5, 10, 15];
const AI_DEFAULT_PREDICTION_HORIZON = 5;
const AI_PREFETCH_LIMIT = 3;
const AI_ANALYTICS_CARD_LIMIT = 10;
const AI_RETRY_COOLDOWN_MS = 60000;
const AI_REQUEST_TIMEOUT_MS = 8000;
const AI_ADVICE_REQUEST_TIMEOUT_MS = 120000;
const ENTRY_READY_MAX_WAIT_MINUTES = 5;
const EXIT_READY_MAX_ETA_MINUTES = 20;
const HOLD_ENDING_WARNING_MINUTES = 30;
const TRADE_SMART_NOTICE_LIMIT = 4;
const TABLE_PAGE_SIZE = 100;
const MOBILE_TABLE_PAGE_SIZES = {
  cross: 8,
  funding: 10,
  health: 8,
};
const VIRTUAL_ROW_OVERSCAN = 10;
const VIRTUAL_ROW_HEIGHT = {
  cross: 36,
  funding: 46,
};
const REFRESH_POLL_BASE_INTERVAL_MS = 500;
const REFRESH_POLL_MAX_INTERVAL_MS = 2500;
const REFRESH_POLL_MAX_ATTEMPTS_FILTERED = 600;
const REFRESH_POLL_MAX_ATTEMPTS_FULL = 900;
const REFRESH_POLL_MAX_TRANSIENT_FAILURES = 8;
const AUTO_REFRESH_BUSY_RETRY_MS = 3000;
const LIVE_DATA_RENDER_INTERVAL_MS = 5000;
const AI_SNAPSHOT_STALE_MS = 180_000;
const MARKET_STREAM_RECONNECT_BASE_MS = 1500;
const MARKET_STREAM_RECONNECT_MAX_MS = 15000;
const MARKET_STREAM_STARTUP_WAIT_MS = 1500;
const MARKET_STREAM_FRESHNESS_MS = 45000;
const SWR_DEDUP_INTERVAL_MS = 2000;
const SWR_FOCUS_THROTTLE_MS = 5000;
const SWR_RECONNECT_THROTTLE_MS = 3000;
const DEFAULT_RUNTIME_CONFIG = {
  refreshApiBaseUrl: '',
  aiApiBaseUrl: '',
};

function detectLiveRefreshApi() {
  const { protocol } = window.location;
  return protocol !== 'file:';
}

function normalizeRefreshApiBaseUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    return '';
  }
  // Strip trailing slashes
  let normalized = raw.replace(/\/+$/, '');

  // If user provided only a port like ":8000" or "8000", assume localhost
  if (/^:\d+$/.test(normalized) || /^\d+$/.test(normalized)) {
    const port = normalized.replace(/^:+/, '');
    return `http://127.0.0.1:${port}`;
  }

  // If starts with // (protocol-relative), use the current page protocol.
  if (normalized.startsWith('//')) {
    const proto = typeof window !== 'undefined' ? window.location.protocol : 'https:';
    return `${proto}${normalized}`;
  }

  // If contains host:port but missing protocol (e.g. "localhost:8000"), add http://
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(normalized) && /:\d+$/.test(normalized)) {
    return `http://${normalized}`;
  }

  return normalized;
}

function normalizeAiPredictionHorizon(value) {
  const numericValue = Number(value);
  return AI_PREDICTION_HORIZON_OPTIONS.includes(numericValue) ? numericValue : AI_DEFAULT_PREDICTION_HORIZON;
}

function normalizeRefreshApiKey(value) {
  return String(value || '').trim();
}

function resolveRefreshApiBaseUrl(settings) {
  const configured = normalizeRefreshApiBaseUrl(settings?.refreshApiBaseUrl);
  if (configured) {
    return configured;
  }
  const runtimeConfigured = normalizeRefreshApiBaseUrl(state?.runtimeConfig?.refreshApiBaseUrl);
  if (runtimeConfigured) {
    return runtimeConfigured;
  }
  const detected = normalizeRefreshApiBaseUrl(state?.detectedRefreshApiBaseUrl);
  if (detected) {
    return detected;
  }
  if (detectLiveRefreshApi()) {
    return '';
  }
  return null;
}

function getRefreshApiBaseUrl() {
  return resolveRefreshApiBaseUrl(state?.settings);
}

function resolveAiApiBaseUrl(settings) {
  if (settings?.useExternalAiApi) {
    const configured = normalizeRefreshApiBaseUrl(settings?.aiApiBaseUrl);
    if (configured) {
      return configured;
    }
    const runtimeConfigured = normalizeRefreshApiBaseUrl(state?.runtimeConfig?.aiApiBaseUrl);
    if (runtimeConfigured) {
      return runtimeConfigured;
    }
    return null;
  }
  const refreshConfigured = resolveRefreshApiBaseUrl(settings);
  if (refreshConfigured !== null) {
    return refreshConfigured;
  }
  return null;
}

function getAiApiBaseUrl() {
  return resolveAiApiBaseUrl(state?.settings);
}

function buildWebSocketUrl(baseUrl, path = MARKET_STREAM_ENDPOINT) {
  try {
    const url = new URL(baseUrl || window.location.origin, window.location.origin);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    url.pathname = path;
    url.search = '';
    url.hash = '';
    return url.toString();
  } catch {
    return null;
  }
}

function getMarketStreamWebSocketUrl() {
  const explicitAi = state?.settings?.useExternalAiApi
    ? normalizeRefreshApiBaseUrl(state?.settings?.aiApiBaseUrl)
    : '';
  if (explicitAi) {
    return buildWebSocketUrl(explicitAi);
  }
  const runtimeAi = normalizeRefreshApiBaseUrl(state?.runtimeConfig?.aiApiBaseUrl);
  if (runtimeAi) {
    return buildWebSocketUrl(runtimeAi);
  }
  // Evaluate each candidate only once and short-circuit on the first truthy result.
  const settingsRefresh = normalizeRefreshApiBaseUrl(state?.settings?.refreshApiBaseUrl);
  const runtimeRefresh = settingsRefresh || normalizeRefreshApiBaseUrl(state?.runtimeConfig?.refreshApiBaseUrl);
  const configuredRefresh = runtimeRefresh || normalizeRefreshApiBaseUrl(state?.detectedRefreshApiBaseUrl);
  if (configuredRefresh) {
    return buildWebSocketUrl(configuredRefresh);
  }
  if (typeof window !== 'undefined' && window.location.protocol !== 'file:' && detectLiveRefreshApi()) {
    const origin = window.location.origin;
    if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1' || window.location.hostname === '0.0.0.0') {
      return `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.hostname}:8010${MARKET_STREAM_ENDPOINT}`;
    }
    return buildWebSocketUrl(origin);
  }
  return null;
}

function clearMarketStreamReconnectTimer() {
  if (state.marketStreamReconnectTimer) {
    window.clearTimeout(state.marketStreamReconnectTimer);
    state.marketStreamReconnectTimer = null;
  }
}

function scheduleMarketStreamReconnect() {
  clearMarketStreamReconnectTimer();
  if (document.visibilityState === 'hidden') {
    return;
  }
  const delayMs = Math.min(
    MARKET_STREAM_RECONNECT_MAX_MS,
    MARKET_STREAM_RECONNECT_BASE_MS * (2 ** Math.min(state.marketStreamConnectAttempt, 4)),
  );
  state.marketStreamReconnectTimer = window.setTimeout(() => {
    state.marketStreamReconnectTimer = null;
    connectMarketStream();
  }, delayMs);
}

async function waitForMarketStreamSnapshot(timeoutMs = MARKET_STREAM_STARTUP_WAIT_MS, sinceMs = Date.now()) {
  const deadline = Date.now() + Math.max(200, timeoutMs);
  while (Date.now() < deadline) {
    const lastMessageMs = state.marketStreamLastMessageAt ? Date.parse(state.marketStreamLastMessageAt) : 0;
    if (Number.isFinite(lastMessageMs) && lastMessageMs >= sinceMs) {
      return true;
    }
    await delay(100);
  }
  return false;
}

async function handleMarketStreamMessage(event) {
  let payload = null;
  try {
    payload = JSON.parse(String(event?.data || 'null'));
  } catch {
    return;
  }
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return;
  }
  if (payload.type === 'pong') {
    return;
  }
  if (!['snapshot', 'update'].includes(String(payload.type || ''))) {
    return;
  }
  let snapshot = payload.data && typeof payload.data === 'object' && !Array.isArray(payload.data) ? payload.data : null;
  if (!snapshot) {
    return;
  }
  const snapshotCross = Array.isArray(snapshot.cross) ? snapshot.cross : [];
  if (!snapshotCross.length && Array.isArray(state.cross) && state.cross.length > 0) {
    snapshot = {
      ...snapshot,
      cross: state.cross,
      funding: Array.isArray(snapshot.funding) && snapshot.funding.length ? snapshot.funding : state.funding,
      metrics: snapshot.metrics && typeof snapshot.metrics === 'object' && !Array.isArray(snapshot.metrics) ? snapshot.metrics : state.metrics,
      adapterHealth: snapshot.adapterHealth && typeof snapshot.adapterHealth === 'object' && !Array.isArray(snapshot.adapterHealth) ? snapshot.adapterHealth : state.adapterHealth,
      snapshot_source: snapshot.snapshot_source === 'empty' ? 'live-cache' : snapshot.snapshot_source,
    };
  }
  state.marketStreamLastMessageAt = payload.timestamp || new Date().toISOString();
  state.marketStreamConnected = true;
  state.marketStreamMode = 'live';
  renderDataSourceStatus();
  await applySnapshotState(snapshot, state.refreshState, {
    origin: snapshot.snapshot_source === 'history-fallback' ? 'historical-fallback' : 'websocket',
  });
}

function connectMarketStream() {
  const wsUrl = getMarketStreamWebSocketUrl();
  state.marketStreamAvailable = Boolean(wsUrl && typeof window.WebSocket === 'function');
  renderDataSourceStatus();
  if (!state.marketStreamAvailable) {
    return;
  }
  const currentSocket = state.marketStreamSocket;
  if (currentSocket && (currentSocket.readyState === WebSocket.OPEN || currentSocket.readyState === WebSocket.CONNECTING)) {
    return;
  }
  clearMarketStreamReconnectTimer();
  state.marketStreamMode = 'connecting';
  renderDataSourceStatus();
  try {
    const socket = new WebSocket(wsUrl);
    state.marketStreamSocket = socket;
    socket.addEventListener('open', () => {
      state.marketStreamConnected = true;
      state.marketStreamConnectAttempt = 0;
      state.marketStreamMode = 'live';
      renderDataSourceStatus();
    });
    socket.addEventListener('message', (event) => {
      void handleMarketStreamMessage(event);
    });
    socket.addEventListener('close', () => {
      if (state.marketStreamSocket !== socket) {
        return;
      }
      state.marketStreamSocket = null;
      state.marketStreamConnected = false;
      state.marketStreamMode = 'fallback';
      state.marketStreamConnectAttempt += 1;
      renderDataSourceStatus();
      scheduleMarketStreamReconnect();
    });
    socket.addEventListener('error', () => {
      state.marketStreamConnected = false;
      state.marketStreamMode = 'fallback';
      renderDataSourceStatus();
    });
  } catch {
    state.marketStreamConnected = false;
    state.marketStreamMode = 'fallback';
    renderDataSourceStatus();
  }
}

// ── SSE live-stream (instant cached data for multi-user) ─────────────────────
const SSE_STREAM_ENDPOINT = '/api/stream';
const SSE_LIVE_API_ENDPOINT = '/api/live';

function connectSseStream() {
  if (typeof window.EventSource !== 'function') {
    return;
  }
  if (state.sseEventSource) {
    try { state.sseEventSource.close(); } catch {}
    state.sseEventSource = null;
  }

  const url = buildDataRequestUrl(SSE_STREAM_ENDPOINT, false);
  if (!url) {
    return;
  }

  const es = new EventSource(url);
  state.sseEventSource = es;
  state.sseConnected = false;

  es.addEventListener('open', () => {
    state.sseConnected = true;
    state.sseReconnectAttempt = 0;
  });

  es.addEventListener('message', (event) => {
    try {
      const payload = JSON.parse(event.data);
      if (payload && payload.type === 'snapshot') {
        // Initial snapshot — apply immediately
        void applySseSnapshot(payload);
      } else if (payload && payload.type === 'market-update') {
        // Incremental update — fetch fresh data from /api/live
        void fetchLiveApiUpdate(payload.version);
      }
    } catch {
      // Ignore parse errors
    }
  });

  es.addEventListener('error', () => {
    state.sseConnected = false;
    state.sseEventSource = null;
    es.close();
    scheduleSseReconnect();
  });
}

function scheduleSseReconnect() {
  if (state.sseReconnectTimer) {
    return;
  }
  const attempt = state.sseReconnectAttempt || 0;
  const delay = Math.min(
    MARKET_STREAM_RECONNECT_BASE_MS * (2 ** Math.min(attempt, 4)),
    MARKET_STREAM_RECONNECT_MAX_MS,
  );
  state.sseReconnectTimer = window.setTimeout(() => {
    state.sseReconnectTimer = null;
    state.sseReconnectAttempt = attempt + 1;
    connectSseStream();
  }, delay);
}

async function applySseSnapshot(payload) {
  if (Array.isArray(payload.latest) && payload.latest.length) {
    applyDatasetValue('cross', payload.latest);
    updateDatasetStatus('cross', { loaded: true, loading: false, refreshing: false, error: '' });
    state.dataOrigin = 'sse-stream';
    renderAll();
  }
  if (Array.isArray(payload.funding) && payload.funding.length) {
    applyDatasetValue('funding', payload.funding);
    updateDatasetStatus('funding', { loaded: true, loading: false, refreshing: false, error: '' });
    renderCurrentTab();
  }
  setLastUpdated();
  scheduleDatasetCachePersist();
}

async function fetchLiveApiUpdate(serverVersion) {
  const currentVersion = state.liveApiVersion || 0;
  if (serverVersion <= currentVersion) {
    return;
  }

  try {
    const url = buildDataRequestUrl(`${SSE_LIVE_API_ENDPOINT}?version=${currentVersion}`, false);
    if (!url) return;

    const response = await fetch(url, { cache: 'no-store' });
    if (response.status === 304) {
      // Already have latest version
      return;
    }
    if (!response.ok) {
      return;
    }

    const data = await response.json();
    if (data.version && data.version > currentVersion) {
      state.liveApiVersion = data.version;
      if (Array.isArray(data.latest)) {
        applyDatasetValue('cross', data.latest);
        updateDatasetStatus('cross', { loaded: true, loading: false, refreshing: true, error: '' });
        state.dataOrigin = 'sse-stream';
        renderAll();
      }
      if (Array.isArray(data.funding)) {
        applyDatasetValue('funding', data.funding);
        updateDatasetStatus('funding', { loaded: true, loading: false, refreshing: true, error: '' });
        renderCurrentTab();
      }
      if (data.metrics && typeof data.metrics === 'object') {
        applyDatasetValue('metrics', data.metrics);
      }
      if (data.adapterHealth && typeof data.adapterHealth === 'object') {
        applyDatasetValue('adapterHealth', data.adapterHealth);
      }
      if (data.timestamp) {
        state.lastRefreshAt = data.timestamp;
      }
      setLastUpdated();
      scheduleDatasetCachePersist();
    }
  } catch {
    // SSE stream will handle reconnection
  }
}

async function trySseLiveApiStartup() {
  try {
    const url = buildDataRequestUrl(SSE_LIVE_API_ENDPOINT, false);
    if (!url) return false;

    const response = await fetch(url, { cache: 'no-store' });
    if (!response.ok) {
      return false;
    }

    const data = await response.json();
    if (data.version) {
      state.liveApiVersion = data.version;
    }
    let loaded = false;
    if (Array.isArray(data.latest) && data.latest.length) {
      applyDatasetValue('cross', data.latest);
      updateDatasetStatus('cross', { loaded: true, loading: false, refreshing: false, error: '' });
      state.dataOrigin = 'sse-stream';
      loaded = true;
    }
    if (Array.isArray(data.funding) && data.funding.length) {
      applyDatasetValue('funding', data.funding);
      updateDatasetStatus('funding', { loaded: true, loading: false, refreshing: false, error: '' });
    }
    if (data.metrics && typeof data.metrics === 'object') {
      applyDatasetValue('metrics', data.metrics);
      updateDatasetStatus('metrics', { loaded: true, loading: false, refreshing: false, error: '' });
    }
    if (data.adapterHealth && typeof data.adapterHealth === 'object') {
      applyDatasetValue('adapterHealth', data.adapterHealth);
    }
    if (data.timestamp) {
      state.lastRefreshAt = data.timestamp;
    }
    if (loaded) {
      renderAll();
      setLastUpdated();
      scheduleDatasetCachePersist();
    }
    return loaded;
  } catch {
    return false;
  }
}

function hasAiApi() {
  return getAiApiBaseUrl() !== null;
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function visibilityMatchesPreset(visibility, presetColumns) {
  if (!isPlainObject(visibility)) {
    return false;
  }
  const enabledColumns = Object.entries(visibility)
    .filter(([, enabled]) => enabled !== false)
    .map(([key]) => key)
    .sort();
  const expectedColumns = [...presetColumns].sort();
  return enabledColumns.length === expectedColumns.length && enabledColumns.every((key, index) => key === expectedColumns[index]);
}

function buildAiApiUrl(path) {
  const baseUrl = getAiApiBaseUrl();
  if (baseUrl === null) {
    return null;
  }
  return `${baseUrl}${path}`;
}

function hasRefreshApi() {
  return getRefreshApiBaseUrl() !== null;
}

function getRefreshEndpoint() {
  const baseUrl = getRefreshApiBaseUrl();
  if (baseUrl === null) {
    return null;
  }
  return `${baseUrl}${REFRESH_ENDPOINT}`;
}

function getRefreshStatusEndpoint() {
  const baseUrl = getRefreshApiBaseUrl();
  if (baseUrl === null) {
    return null;
  }
  return `${baseUrl}${REFRESH_STATUS_ENDPOINT}`;
}

function getRefreshResultEndpoint() {
  const baseUrl = getRefreshApiBaseUrl();
  if (baseUrl === null) {
    return null;
  }
  return `${baseUrl}${REFRESH_RESULT_ENDPOINT}`;
}

function buildDataUrl(path) {
  const baseUrl = getRefreshApiBaseUrl();
  if (!baseUrl) {
    return path;
  }
  const normalizedPath = String(path || '').replace(/^\.\//, '/');
  return `${baseUrl}${normalizedPath}`;
}

function buildDataRequestUrl(path, force = false) {
  const url = buildDataUrl(path);
  if (!force) {
    return url;
  }
  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}t=${Date.now()}`;
}

function getRefreshApiKey() {
  return normalizeRefreshApiKey(state?.settings?.refreshApiKey);
}

function isSameOriginRefreshApi() {
  const baseUrl = getRefreshApiBaseUrl();
  if (baseUrl === null) {
    return false;
  }
  if (!baseUrl) {
    return true;
  }
  try {
    return new URL(baseUrl, window.location.origin).origin === window.location.origin;
  } catch {
    return false;
  }
}

function canUseImplicitRefreshAuth() {
  return Boolean(hasRefreshApi() && isSameOriginRefreshApi());
}

function buildRefreshApiHeaders(headers = {}) {
  const nextHeaders = { ...headers };
  const refreshApiKey = getRefreshApiKey();
  if (refreshApiKey) {
    nextHeaders[REFRESH_API_KEY_HEADER] = refreshApiKey;
  }
  return nextHeaders;
}

async function fetchRefreshApiHealth(baseUrl) {
  const normalizedBaseUrl = normalizeRefreshApiBaseUrl(baseUrl);
  const healthUrl = `${normalizedBaseUrl}/api/health?t=${Date.now()}`;
  try {
    const response = await fetch(healthUrl, {
      headers: buildRefreshApiHeaders({
        Accept: 'application/json',
      }),
      cache: 'no-store',
    });
    if (!response.ok) {
      return null;
    }
    const payload = await response.json().catch(() => null);
    return payload && typeof payload === 'object' && !Array.isArray(payload) ? payload : null;
  } catch {
    return null;
  }
}

async function fetchRefreshApiStatus(force = false) {
  const refreshStatusEndpoint = getRefreshStatusEndpoint();
  if (!refreshStatusEndpoint) {
    return null;
  }
  const separator = refreshStatusEndpoint.includes('?') ? '&' : '?';
  const url = force ? `${refreshStatusEndpoint}${separator}t=${Date.now()}` : refreshStatusEndpoint;
  try {
    const response = await fetch(url, {
      cache: 'no-store',
      headers: buildRefreshApiHeaders({
        Accept: 'application/json',
      }),
    });
    if (!response.ok) {
      return null;
    }
    const payload = await response.json().catch(() => null);
    return payload && typeof payload === 'object' && !Array.isArray(payload) ? payload : null;
  } catch {
    return null;
  }
}

function buildRefreshStateFromHealth(health, jobId = null) {
  if (!health || typeof health !== 'object' || Array.isArray(health)) {
    return null;
  }
  const status = String(health.refresh_status || '').trim();
  if (!status) {
    return null;
  }
  const pollIntervalMs = Number(health.refresh_poll_interval_ms);
  return {
    ok: status !== 'failed',
    job_id: health.refresh_job_id || jobId || null,
    status,
    scope: String(health.refresh_scope || 'filtered'),
    message: String(health.refresh_message || ''),
    started_at: health.refresh_started_at || null,
    finished_at: health.refresh_finished_at || null,
    result_available: Boolean(health.refresh_result_available),
    result_expires_at: health.refresh_result_expires_at || null,
    poll_interval_ms: Number.isFinite(pollIntervalMs) && pollIntervalMs > 0 ? Math.round(pollIntervalMs) : null,
    steps: [],
  };
}

async function syncRefreshApiProbe() {
  state.refreshApiHealth = null;
  state.refreshApiProbeError = '';

  const configuredBaseUrl = normalizeRefreshApiBaseUrl(state?.settings?.refreshApiBaseUrl);
  if (configuredBaseUrl) {
    state.detectedRefreshApiBaseUrl = '';
    const health = await fetchRefreshApiHealth(configuredBaseUrl);
    state.refreshApiHealth = health;
    if (!health) {
      state.refreshApiProbeError = 'unreachable';
    }
    return;
  }

  const runtimeConfiguredBaseUrl = normalizeRefreshApiBaseUrl(state?.runtimeConfig?.refreshApiBaseUrl);
  if (runtimeConfiguredBaseUrl) {
    state.detectedRefreshApiBaseUrl = '';
    const health = await fetchRefreshApiHealth(runtimeConfiguredBaseUrl);
    state.refreshApiHealth = health;
    if (!health) {
      state.refreshApiProbeError = 'unreachable';
    }
    return;
  }

  if (window.location.protocol === 'file:') {
    state.detectedRefreshApiBaseUrl = '';
    return;
  }

  if (detectLiveRefreshApi()) {
    state.detectedRefreshApiBaseUrl = '';
    state.refreshApiHealth = await fetchRefreshApiHealth('');
    return;
  }

  const candidateBaseUrl = normalizeRefreshApiBaseUrl(window.location.origin);
  if (!candidateBaseUrl) {
    state.detectedRefreshApiBaseUrl = '';
    return;
  }

  const health = await fetchRefreshApiHealth(candidateBaseUrl);
  if (health) {
    state.detectedRefreshApiBaseUrl = candidateBaseUrl;
    state.refreshApiHealth = health;
    return;
  }

  state.detectedRefreshApiBaseUrl = '';
}

function detectDefaultLanguage() {
  return navigator.language && navigator.language.toLowerCase().startsWith('ru') ? 'ru' : 'en';
}

function safeLocalStorageGet(key) {
  try {
    return localStorage.getItem(key);
  } catch (error) {
    console.warn('Failed to read localStorage key', key, error);
    return null;
  }
}

function safeLocalStorageSet(key, value) {
  try {
    localStorage.setItem(key, value);
    return true;
  } catch (error) {
    console.warn('Failed to write localStorage key', key, error);
    return false;
  }
}

function safeSessionStorageGet(key) {
  try {
    return sessionStorage.getItem(key);
  } catch (error) {
    console.warn('Failed to read sessionStorage key', key, error);
    return null;
  }
}

function safeSessionStorageSet(key, value) {
  try {
    sessionStorage.setItem(key, value);
    return true;
  } catch (error) {
    console.warn('Failed to write sessionStorage key', key, error);
    return false;
  }
}

function safeSessionStorageRemove(key) {
  try {
    sessionStorage.removeItem(key);
  } catch (error) {
    console.warn('Failed to remove sessionStorage key', key, error);
  }
}

function loadPersistedLanguage() {
  try {
    const raw = safeLocalStorageGet(STORAGE_KEY);
    if (!raw) {
      return detectDefaultLanguage();
    }
    const parsed = JSON.parse(raw);
    return parsed?.language === 'ru' ? 'ru' : 'en';
  } catch {
    return detectDefaultLanguage();
  }
}

let currentLanguage = loadPersistedLanguage();

const TRANSLATIONS = {
  en: {
    'meta.description': 'Monitor cross-exchange and funding-rate futures arbitrage opportunities with live backend refresh support.',
    'title.app': 'Futures Arbitrage Scanner',
    'hero.eyebrow': 'Realtime arbitrage workspace',
    'hero.copy': 'Cross-exchange spreads and funding dislocations for USDT perpetuals.',
    'hero.lastUpdate': 'Last update',
    'hero.waiting': 'Waiting for data',
    'button.refreshFiltered': 'Refresh filtered',
    'button.refreshFull': 'Refresh full',
    'button.autoRefreshManual': 'Manual mode',
    'button.autoRefreshAuto': 'Auto refresh on',
    'button.autoRefreshEnable': 'Enable automatic refresh',
    'button.autoRefreshDisable': 'Disable automatic refresh',
    'button.reloadSnapshot': 'Reload snapshot',
    'button.language': 'EN',
    'button.languageAria': 'Switch language',
    'button.toggleTheme': 'Toggle theme',
    'button.openSettings': 'Open settings',
    'button.closeSettings': 'Close settings',
    'button.openHeatmap': 'Market heatmap',
    'button.openGlossary': 'Arbitrage handbook',
    'button.backToCross': 'Back to spread table',
    'button.apply': 'Apply',
    'button.reset': 'Reset',
    'stat.cross': 'Cross opportunities',
    'stat.funding': 'Funding dislocations',
    'stat.exchanges': 'Tracked exchanges',
    'stat.health': 'Healthy exchanges',
    'stat.quarantined': 'Quarantined',
    'tabs.cross': 'Cross-exchange',
    'tabs.funding': 'Funding rate',
    'tabs.ai': 'AI Analytics',
    'tabs.glossary': 'Glossary',
    'tabs.heatmap': 'Heatmap',
    'tabs.health': 'Health',
    'guide.read.title': 'What a workable trade looks like',
    'guide.read.body': 'Arbitrage starts with workable edge, not with a pretty raw spread. A real setup still shows positive net spread after fees, latency reserve, funding drag and expected slippage. If the math works only before costs, it is not an opportunity.',
    'guide.watch.title': 'How a beginner should filter',
    'guide.watch.body': 'Use a strict order: net spread and net after execution first, then liquidity and worst-leg width, then quote freshness and synchronization, then funding, and only after that AI timing. If one of the early blocks is weak, keep the setup on watch.',
    'guide.use.title': 'How to extract more profit',
    'guide.use.body': 'Most trades do not pay best when you wait stubbornly for a perfect zero spread. Profit is usually larger when the entry zone, size, first take-profit zone and structural exit are planned before the trade is opened.',
    'panel.cross.title': 'Cross-exchange monitor',
    'panel.cross.desc': 'Realtime spread table with user-side fee, leverage and funding recalculation.',
    'panel.cross.note1': 'Hint: in a workable setup spread and net spread should move back toward zero after entry. The closer the value gets to 0 on exit, the closer the market is to normalization.',
    'panel.cross.note2': 'Click a row to open the setup card: score, lifecycle, execution notes, risk profile and spread history.',
    'panel.cross.filter': 'Pair filter',
    'panel.cross.placeholder': 'BTCUSDT, ETH...',
    'panel.cross.empty': 'No cross-exchange records match the current filters.',
    'panel.funding.title': 'Funding rate spreads',
    'panel.funding.desc': 'Funding arbitrage routes ranked by expected net carry after opening costs and latency reserve.',
    'panel.funding.empty': 'No funding-rate records match the current filters.',
    'panel.ai.title': 'AI Analytics',
    'panel.ai.desc': 'Dedicated AI review for setups that already survived the basic spread check: when to enter, when to wait, how much edge is left and where execution risk starts to break the trade.',
    'panel.ai.note': 'Use this tab after the spread list and pair card, not before. If the base structure is weak, AI should not rescue the trade; if the base structure is strong, AI helps with timing and risk sizing.',
    'panel.ai.empty': 'AI analytics are waiting for cross-exchange opportunities and AI snapshots.',
    'panel.glossary.title': 'Handbook',
    'panel.glossary.desc': 'Beginner-first arbitrage handbook: what each term means, how to read it on the dashboard and how to act before entry, during the trade and on exit.',
    'panel.glossary.search': 'Search term',
    'panel.glossary.searchPlaceholder': 'spread, funding, entry, exit...',
    'panel.glossary.category': 'Category',
    'panel.glossary.empty': 'No glossary terms match the current filter.',
    'panel.heatmap.title': 'Market heatmap',
    'panel.heatmap.desc': 'Cross-market heatmap ranked by anomaly, reversion probability and multi-window confirmation.',
    'panel.heatmap.note': 'Tiles are colored by anomaly intensity: the more saturated the tile, the stronger the deviation from its own history.',
    'panel.heatmap.empty': 'No cross-exchange records are available for the heatmap.',
    'panel.health.title': 'Health & reliability',
    'panel.health.desc': 'Live adapter reliability, coverage quality and API degradation indicators for every tracked exchange.',
    'panel.health.empty': 'No exchange health data is available yet.',
    'settings.eyebrow': 'Personal workspace',
    'settings.title': 'Scanner settings',
    'settings.minSpread': 'Minimum spread %',
    'settings.leverage': 'Leverage',
    'settings.deposit': 'Deposit $',
    'settings.holdingHours': 'Holding hours',
    'settings.sparklineDays': 'Sparkline days',
    'settings.refreshApi.title': 'Backend connection',
    'settings.refreshApi.desc': 'Connect a deployed local-dashboard backend to load data and run refresh the same way as localhost.',
    'settings.refreshApiUrl': 'Backend base URL',
    'settings.refreshApiUrlPlaceholder': 'https://refresh.example.com',
    'settings.refreshApiKey': 'Refresh API key',
    'settings.refreshApiKeyPlaceholder': 'Only needed for a refresh backend on another origin',
    'settings.refreshApiTest': 'Test backend',
    'settings.refreshApiStatusSnapshot': 'Snapshot mode only. Connect a backend to test live refresh and diagnostics.',
    'settings.refreshApiStatusChecking': 'Checking backend...',
    'settings.refreshApiStatusUnreachable': 'Backend did not respond. Check URL, deployment and CORS settings.',
    'settings.refreshApiStatusUrl': 'Backend URL',
    'settings.refreshApiStatusAccess': 'Access',
    'settings.refreshApiStatusState': 'Refresh state',
    'settings.refreshApiStatusVersion': 'Server version',
    'settings.refreshApiStatusStarted': 'Started',
    'settings.refreshApiStatusCleanup': 'Startup cleanup',
    'settings.refreshApiStatusCleanupSummary': 'history {history} · removed {removed} · stale data {staleData} · reports {reports} · tmp files {tmpFiles}',
    'settings.refreshApiStatusAutoCleanup': 'Automatic history cleanup',
    'settings.refreshApiStatusAutoCleanupLastRun': 'last run {value}',
    'settings.refreshApiStatusAutoCleanupRemoved': 'removed {value} snapshots',
    'settings.refreshApiStatusAutoCleanupSkipped': 'skipped {value} cycles during refresh',
    'settings.refreshApiStatusProtected': 'Protected endpoint ready',
    'settings.refreshApiStatusKeyMissing': 'API key required',
    'settings.refreshApiStatusOpen': 'Open endpoint',
    'settings.refreshApiStatusUnknown': 'Unknown',
    'settings.ai.title': 'Built-in AI and open endpoints',
    'settings.ai.desc': 'The dashboard uses free built-in AI heuristics on the current backend by default. Set a base URL only when you run a compatible self-hosted API on another origin.',
    'settings.aiFilters.title': 'AI filters',
    'settings.aiFilters.desc': 'These controls shape the AI shortlist without exposing operator connectivity settings in the main workflow.',
    'settings.operator.title': 'Operator connectivity',
    'settings.operator.desc': 'Backend refresh address and optional external AI endpoint are rarely needed in daily manual work, so they are hidden here.',
    'settings.aiExternalToggle': 'Use external AI API',
    'settings.aiApiUrl': 'Optional AI API base URL',
    'settings.aiApiUrlPlaceholder': 'Leave empty to use built-in /ai endpoints',
    'settings.aiPredictionHorizon': 'Forecast horizon',
    'settings.minAiScore': 'Minimum AI score in AI tab',
    'settings.maxAiEntryWaitMinutes': 'Max entry wait, min',
    'settings.maxAiHoldingHours': 'Max holding, h',
    'settings.aiThresholdOptimize': 'Optimize threshold',
    'button.applyAiThreshold': 'Apply AI threshold',
    'settings.autoRefresh.title': 'Refresh mode',
    'settings.autoRefresh.desc': 'Choose whether the scanner updates only on demand or refreshes automatically while this tab is open.',
    'settings.autoRefreshMode': 'Update mode',
    'settings.autoRefreshInterval': 'Auto refresh every',
    'settings.autoRefreshModeManual': 'Manual only',
    'settings.autoRefreshModeAuto': 'Automatic',
    'settings.autoRefreshIntervalSeconds': '{value} sec',
    'settings.autoRefreshIntervalMinutes': '{value} min',
    'settings.autoRefreshStrategy': 'Automatic strategy',
    'settings.autoRefreshStrategyFilteredOnly': 'Filtered only',
    'settings.autoRefreshStrategyFilteredPlusFull': 'Filtered + rare full',
    'settings.autoRefreshFullInterval': 'Rare full refresh every',
    'settings.fees.title': 'Taker fee by exchange, %',
    'settings.fees.desc': 'These values affect net spread and opportunity scoring only in your browser.',
    'settings.exchanges.title': 'Visible exchanges',
    'settings.exchanges.desc': 'Use the checkboxes to hide venues from all tables and selectors.',
    'settings.columns.title': 'Cross table columns',
    'settings.columns.desc': 'Choose which columns stay visible in Cross-exchange monitor.',
    'settings.columnsAdvanced.title': 'Advanced table settings',
    'settings.columnsAdvanced.desc': 'The spread table stays compact by default. Expand this section only when you need extra columns or another preset.',
    'modal.close': 'Close modal',
    'footer.deploy': 'Project repository',
    'footer.disclaimer': 'Educational analytics only. Validate execution, liquidity, fees and jurisdictional constraints independently.',
    'common.na': 'n/a',
    'common.noDataYet': 'No data yet',
    'common.noDetails': 'No details',
    'common.allColumns': 'All columns',
    'common.compact': 'Compact',
    'common.profitFocus': 'Profit focus',
    'common.carryFocus': 'Carry focus',
    'common.liquidity': 'Liquidity',
    'common.score': 'Score',
    'common.routeStatus': 'Route status',
    'common.exchange': 'Exchange',
    'common.cycle': 'Cycle',
    'common.startAmount': 'Start amount',
    'common.endAmount': 'End amount',
    'common.profitPercent': 'Profit %',
    'common.effectiveProfit': 'Effective profit %',
    'common.grossProfit': 'Gross %',
    'common.feeDrag': 'Fee drag',
    'common.slippageDrag': 'Slippage drag',
    'common.quoteGap': 'Cross quote gap',
    'common.totalVolume': 'Total volume',
    'common.crossPair': 'Cross pair',
    'common.minLegVolume': 'Min leg volume',
    'common.method': 'Method',
    'common.updated': 'Updated',
    'common.lastSuccess': 'Last success',
    'common.coverage': 'Coverage %',
    'common.status': 'Status',
    'common.apiAlert': 'API alert',
    'common.excludedWhy': 'Excluded / why',
    'common.never': 'never',
    'refresh.scope': 'Scope',
    'refresh.status': 'Status',
    'refresh.started': 'Started',
    'refresh.finished': 'Finished',
    'refresh.message': 'Message',
    'refresh.lastSuccess': 'Last successful refresh: {value}',
    'refresh.selectExchange': 'Select at least one exchange before refreshing.',
    'refresh.running': 'Refreshing...',
    'refresh.filtered': 'Refreshing filtered data...',
    'refresh.full': 'Refreshing full dataset...',
    'refresh.snapshotReloaded': 'Published snapshot reloaded.',
    'refresh.snapshotReloadFailed': 'Failed to reload published snapshot.',
    'refresh.queuedFiltered': 'Filtered refresh queued.',
    'refresh.queuedFull': 'Full refresh queued.',
    'refresh.apiUnavailable': 'Live refresh API is not available here. Reloaded current snapshot.',
    'refresh.apiUnavailableDetail': 'Local refresh API is unavailable. Snapshot reloaded only.',
    'refresh.apiReadyDetail': 'This page is using the configured backend for data and refresh, the same flow as local dashboard mode.',
    'refresh.apiUnreachable': 'Refresh API is configured but did not respond. Check the API URL and CORS settings.',
    'refresh.sourceSnapshot': 'Source: published snapshot',
    'refresh.sourceLocal': 'Source: local backend',
    'refresh.sourceSameOrigin': 'Source: same-origin backend',
    'refresh.sourceConfigured': 'Source: configured backend',
    'refresh.sourceEphemeralFiltered': 'Source: ephemeral filtered snapshot',
    'refresh.accessOpen': 'Refresh: open endpoint',
    'refresh.accessReady': 'Refresh: protected endpoint ready',
    'refresh.accessMissing': 'Refresh: API key required',
    'refresh.accessSnapshot': 'Refresh: snapshot only',
    'refresh.accessUnreachable': 'Refresh: backend unreachable',
    'refresh.ageNow': 'Snapshot age: <1m',
    'refresh.ageMinutes': 'Snapshot age: {value}m',
    'refresh.ageHours': 'Snapshot age: {value}h',
    'refresh.runLink': 'Run link',
    'refresh.inProgress': 'Refresh in progress...',
    'refresh.dataInProgress': 'Refreshing data...',
    'refresh.failed': 'Refresh failed.',
    'refresh.statusFailed': 'Refresh status request failed.',
    'refresh.timeout': 'Refresh polling timed out.',
    'refresh.success': 'Data refreshed successfully.',
    'refresh.noRows': 'No {scope} rows to export.',
    'autoRefresh.badgeManual': 'Refresh mode: manual',
    'autoRefresh.badgeAuto': 'Refresh mode: auto every {value}s',
    'autoRefresh.badgePaused': 'Refresh mode: auto paused',
    'autoRefresh.badgeFilteredOnly': 'Strategy: filtered only',
    'autoRefresh.badgeFilteredPlusFull': 'Strategy: filtered + full every {value}',
    'autoRefresh.running': 'Automatic refresh is running...',
    'quality.visibleExchanges': 'Visible exchanges {visible}/{total}',
    'quality.crossHits': 'Cross hits {count}',
    'quality.hotAnomalies': 'Hot anomalies {count}',
    'quality.quarantined': 'Quarantined {count}',
    'quality.systemAlerts': 'System alerts {count}',
    'quality.publicationProfile': 'Publish profile {mode}',
    'quality.quoteAge': 'Quote p95 {value}s',
    'quality.refreshStatus': 'Refresh status {status}',
    'quality.refreshScope': 'Refresh scope {scope}',
    'quality.snapshotOriginEphemeral': 'Snapshot: ephemeral filtered view',
    'settings.refreshApiStatusResultCache': 'Ephemeral refresh cache',
    'settings.refreshApiStatusResultCacheStats': 'hits {hits} · misses {misses} · stores {stores} · active {active}',
    'settings.refreshApiStatusAlerts': 'alerts {active} · critical {critical} · warning {warning}',
    'settings.refreshApiStatusAdaptivePublication': 'net >= {spread}% · min volume >= {volume} · max bid/ask <= {bidAsk}%',
    'settings.refreshApiStatusQuoteAge': 'avg {avg}s · p95 {p95}s · stale {stale}%',
    'heatmap.marketVolatility': 'Market volatility',
    'heatmap.backdrop': 'Backdrop',
    'heatmap.strongest': 'Displayed strongest anomaly',
    'heatmap.reversion': 'Displayed reversion',
    'heatmap.aligned': 'Aligned anomalies',
    'heatmap.cleanConvergence': 'Clean convergence',
    'heatmap.anomaly': 'Anomaly {value}',
    'heatmap.marketAvg': 'Market avg {value}%',
    'heatmap.market': 'Market {value}/{total}',
    'heatmap.tiles': '{count} tiles',
    'heatmap.displayedTiles': 'Displayed tiles',
    'heatmap.net': 'Net',
    'heatmap.z': 'Z',
    'heatmap.pct': 'Pct',
    'heatmap.rev': 'Rev',
    'hero.volatility': 'Volatility regime',
    'hero.backdrop': 'Risk backdrop',
    'hero.fundingDrag': 'Funding drag',
    'hero.eventStress': 'Event stress',
    'hero.systemAlerts': 'System alerts',
    'hero.publicationProfile': 'Publish profile',
    'hero.whale': 'Whale activity',
    'hero.whaleWaiting': 'Waiting for AI flow data',
    'hero.whaleHot': '{symbol} · {value}/100',
    'hero.whaleEvents': '{count} large-flow events',
    'hero.elevated': '{value}/{total} elevated',
    'hero.published': '{published}/{total} published · {regime} {share}%',
    'hero.flagged': '{value}% flagged',
    'hero.alertsActive': '{count} active',
    'hero.alertsClear': 'no active alerts',
    'hero.alertTopSeverity': 'highest severity {severity}',
    'hero.publicationThresholds': 'net >= {spread}% · vol >= {volume} · bid/ask <= {bidAsk}%',
    'hero.smartNotices': 'Smart trade notices',
    'hero.pairsAdverse': '{count} pairs adverse',
    'hero.proxies': '{count} news-style proxies',
    'health.normal': 'normal',
    'health.degraded': 'degraded',
    'health.quarantined': 'quarantined',
    'health.active': 'Active in current universe',
    'health.until': 'Quarantined until {value}',
    'health.summaryHealthy': 'Healthy exchanges',
    'health.summaryDegraded': 'Degraded exchanges',
    'health.summaryCoverage': 'Average coverage',
    'health.summaryAlerts': 'System alerts',
    'health.summaryQuoteAge': 'Quote age p95',
    'health.summaryUpdated': 'Last health update',
    'publication.open': 'open',
    'publication.cautious': 'cautious',
    'publication.blocked': 'blocked',
    'alertSeverity.info': 'info',
    'alertSeverity.warning': 'warning',
    'alertSeverity.critical': 'critical',
    'detail.executionNotes': 'Execution notes',
    'detail.riskNotes': 'Risk notes',
    'detail.nextStep': 'What to do next',
    'detail.riskFilters': 'Сработавшие ограничения риска',
    'detail.rankingScore': 'Ranking score',
    'detail.lifecycle': 'Lifecycle',
    'detail.spreadRegime': 'Spread regime',
    'detail.historicalZ': 'Historical Z-score',
    'detail.currentPercentile': 'Current percentile',
    'detail.anomalyScore': 'Anomaly score',
    'detail.reversionProbability': 'Reversion probability',
    'detail.spreadQuality': 'Spread quality',
    'detail.fundingQuality': 'Funding quality',
    'detail.liquidityQuality': 'Liquidity quality',
    'detail.stability': 'Stability of spread',
    'detail.executionDifficulty': 'Execution difficulty',
    'detail.alignment': 'Short/long alignment',
    'detail.meanReversionEta': 'Mean reversion ETA',
    'detail.tradeAlert': 'Trade alert',
    'detail.positionState': 'Position state',
    'detail.holdingTimer': 'Holding timer',
    'detail.entryZone': 'Suggested entry zone',
    'detail.exitZone': 'Suggested exit zone',
    'detail.notEnoughSignal': 'not enough signal',
    'detail.execution.high': 'high',
    'detail.execution.medium': 'medium',
    'detail.execution.low': 'low',
    'risk.volatility': 'Volatility',
    'risk.liquidity': 'Liquidity',
    'risk.funding': 'Funding',
    'risk.newsStyle': 'News-style',
    'risk.noteWideSpread': 'Bid/ask spread is wide on at least one leg. Executed PnL can be materially lower than table estimates.',
    'risk.noteLowLiquidity': 'Liquidity is limited on one of the legs. Size carefully to avoid slippage spikes.',
    'risk.noteWidening': 'The spread is still widening. Early entry can be painful without staged sizing.',
    'risk.noteNoisy': 'Historical regime is noisy. Signals can flip quickly even when the spread looks statistically extreme.',
    'risk.noteLongHold': 'Convergence model suggests a long hold. Funding drift and venue risk become more important.',
    'risk.noteClean': 'No elevated structural risk flags beyond normal execution risk are visible in the latest snapshot.',
    'exchange.noTelemetry': '{exchange}: no health telemetry available.',
    'exchange.base': '{exchange}: status {status}, coverage {coverage}%',
    'exchange.baseError': '{exchange}: status {status}, coverage {coverage}%. {error}',
    'execution.enterFirst': 'Enter the {exchange} leg first to reduce immediate slippage risk.',
    'execution.safer': '{exchange} currently has the tighter bid/ask and is safer for aggressive execution.',
    'execution.fundingAdverse': 'Funding carry is currently adverse. Holding too long can reduce realized edge.',
    'execution.fundingSupportive': 'Funding carry supports the trade while the spread converges.',
    'execution.fundingNeutral': 'Funding is roughly neutral. Main edge comes from spread convergence.',
    'regime.clean-convergence': 'clean convergence',
    'regime.stable-carry': 'stable carry',
    'regime.noisy-range': 'noisy range',
    'regime.widening-trend': 'widening trend',
    'regime.volatile-expansion': 'volatile expansion',
    'regime.compressed': 'compressed',
    'regime.forming': 'forming',
    'status.new': 'new',
    'status.widening': 'widening',
    'status.stable': 'stable',
    'status.converging': 'converging',
    'status.expired': 'expired',
    'status.ok': 'ok',
    'status.failed': 'failed',
    'status.idle': 'idle',
    'status.running': 'running',
    'status.completed': 'completed',
    'status.quarantined': 'quarantined',
    'status.unknown': 'unknown',
    'volatility.contained': 'contained volatility',
    'volatility.mixed': 'mixed backdrop',
    'volatility.stressed': 'stressed volatility',
    'backdrop.calm': 'calm',
    'backdrop.watch': 'watch list',
    'backdrop.stressed': 'stressed',
    'cross.header.exchangeA': 'Exch A',
    'cross.header.pairA': 'Pair',
    'cross.header.exchangeB': 'Exch B',
    'cross.header.direction': 'Dir',
    'cross.header.score': 'Score',
    'cross.header.aiScore': 'AI Score',
    'cross.header.aiForecast': 'Forecast',
    'cross.header.entryWaitMinutes': 'Entry in',
    'cross.header.holdingHoursRecommended': 'Hold',
    'cross.header.exitTarget': 'Exit target',
    'cross.header.tradeAlert': 'Trade alert',
    'cross.header.positionState': 'Position',
    'cross.header.lifecycle': 'State',
    'cross.header.regime': 'Regime',
    'cross.header.zScore': 'Откл.',
    'cross.header.percentile': 'Ист.',
    'cross.header.reversionProbability': 'Rev %',
    'cross.header.spread': 'Спред %',
    'cross.header.netSpread': 'После комиссий %',
    'cross.header.expectedFundingCarryPercent': 'Carry %',
    'cross.header.openingFeePercent': 'Fees %',
    'cross.header.latencyReservePercent': 'Latency %',
    'cross.header.annualizedNetEdge': 'Ann. edge %',
    'cross.header.roi': 'ROI %',
    'cross.header.profit': 'Прибыль $',
    'cross.header.volumeA': 'Vol A',
    'cross.header.volumeB': 'Vol B',
    'cross.header.fundingA': 'Fund A %',
    'cross.header.fundingB': 'Fund B %',
    'cross.header.spreadA': 'B/A A %',
    'cross.header.spreadB': 'B/A B %',
    'cross.label.exchangeA': 'Exchange A',
    'cross.label.pairA': 'Pair',
    'cross.label.exchangeB': 'Exchange B',
    'cross.label.direction': 'Direction',
    'cross.label.score': 'Ranking score',
    'cross.label.aiScore': 'AI confidence score',
    'cross.label.aiForecast': 'AI spread forecast',
    'cross.label.entryWaitMinutes': 'Entry wait time',
    'cross.label.holdingHoursRecommended': 'Recommended holding time',
    'cross.label.exitTarget': 'Exit target',
    'cross.label.tradeAlert': 'Smart trade alert',
    'cross.label.positionState': 'Tracked position',
    'cross.label.lifecycle': 'Lifecycle',
    'cross.label.regime': 'Spread regime',
    'cross.label.zScore': 'Z-score',
    'cross.label.percentile': 'Percentile',
    'cross.label.reversionProbability': 'Reversion probability',
    'cross.label.spread': 'Spread %',
    'cross.label.netSpread': 'Net spread %',
    'cross.label.expectedFundingCarryPercent': 'Expected funding carry %',
    'cross.label.openingFeePercent': 'Opening fee %',
    'cross.label.latencyReservePercent': 'Latency reserve %',
    'cross.label.annualizedNetEdge': 'Annualized net edge %',
    'cross.label.roi': 'ROI xL %',
    'cross.label.profit': 'Profit $',
    'cross.label.volumeA': 'Vol A',
    'cross.label.volumeB': 'Vol B',
    'cross.label.fundingA': 'Funding A %',
    'cross.label.fundingB': 'Funding B %',
    'cross.label.spreadA': 'Bid/Ask A %',
    'cross.label.spreadB': 'Bid/Ask B %',
    'cross.mobile.netSpread': 'Net %',
    'cross.mobile.roi': 'ROI',
    'cross.mobile.reversion': 'Reversion',
    'cross.mobile.profit': 'PnL $',
    'cross.mobile.tradeAlert': 'Trade alert',
    'cross.mobile.positionState': 'Position',
    'cross.mobile.fundingA': 'Funding A',
    'cross.mobile.fundingB': 'Funding B',
    'cross.mobile.fundingDelta': 'Funding delta',
    'cross.hint.exchangeA': 'Exchange for the first leg of the position. Read it together with volume, bid/ask width and adapter health: a strong spread on a weak venue is usually worse than a smaller spread on a stable venue.',
    'cross.hint.pairA': 'Contract used for the spread comparison. Use it to confirm you are looking at the same underlying and the same type of perpetual before treating the spread as executable.',
    'cross.hint.exchangeB': 'Exchange for the second leg of the position. Compare its liquidity and health with the first venue before assuming the hedge can be opened and closed cleanly.',
    'cross.hint.direction': 'Suggested long/short direction for a mean-reversion trade. Use it as a starting scenario, then validate funding, execution quality and whether both legs can be entered at the same time.',
    'cross.hint.score': 'Composite quality score built from spread edge, funding, liquidity, stability and execution difficulty. Use it to rank candidates quickly, but still confirm net spread, venue health and slippage risk manually.',
    'cross.hint.aiScore': 'AI confidence score based on current spread, microstructure, funding, liquidity and recent spread persistence. Use it as a fast tradability filter before deeper review.',
    'cross.hint.aiForecast': 'Short-horizon forecast for spread direction and expected move. Read it together with probability and AI score, not as a standalone trading trigger.',
    'cross.hint.entryWaitMinutes': 'Recommended delay before opening the position. Higher values usually mean the model wants either cleaner execution, better spread stabilization or stronger reversion confirmation.',
    'cross.hint.holdingHoursRecommended': 'Recommended holding horizon from the execution model. Use it to estimate carry risk, capital tie-up and whether the idea still fits your working timeframe.',
    'cross.hint.exitTarget': 'Target normalization level or exit mode suggested by the AI layer. Read it together with ETA and current net spread so you do not wait for a perfect exit after the edge is already spent.',
    'cross.hint.tradeAlert': 'Actionable trade state for this row: whether the setup is ready to enter, already in position, approaching the end of hold or should be exited now.',
    'cross.hint.positionState': 'Locally tracked open position for this row. Shows the exact pair and venue route you marked as entered inside the scanner.',
    'cross.hint.lifecycle': 'Current stage of the setup: fresh, widening, stable, converging or already fading. New and stable states are useful for monitoring, while converging setups are closer to the expected exit behavior.',
    'cross.hint.regime': 'Behavior pattern of the spread itself. Clean convergence and stable carry are usually easier to manage, while noisy or expanding regimes need more caution and stricter confirmation.',
    'cross.hint.zScore': 'Statistical distance of the current absolute net spread from its own history. Higher absolute values mean the move is more unusual; use it together with percentile so you do not rely on one metric alone.',
    'cross.hint.percentile': 'How rare the current absolute net spread is in its historical distribution. Values near 100 mean an extreme event; use them to separate ordinary noise from truly stretched situations.',
    'cross.hint.reversionProbability': 'Model estimate of how likely the spread is to narrow rather than keep drifting. Treat it as a confidence meter, not a guarantee, and confirm it with lifecycle, regime and exchange health.',
    'cross.hint.spread': 'Raw price gap before fees and funding. Good for spotting dislocation, but not good enough for execution decisions because it ignores the carrying and trading costs.',
    'cross.hint.netSpread': 'Spread after taker fees and funding carry. This is the more practical entry metric: in a working mean-reversion setup it should have room to move back toward 0 on exit.',
    'cross.hint.expectedFundingCarryPercent': 'Expected carry contribution from funding over the selected or default holding horizon. Positive values support the trade while you wait for convergence; negative values mean holding drag is real.',
    'cross.hint.openingFeePercent': 'Estimated combined taker-fee burden for both futures legs. It shows how much of the raw spread disappears before slippage.',
    'cross.hint.latencyReservePercent': 'Extra reserve kept for hedge lag and quote movement between venues. This is a safety buffer, not a direct exchange fee.',
    'cross.hint.annualizedNetEdge': 'Net edge scaled to a yearly rate using the active holding horizon. Useful for comparing routes with different expected holding times.',
    'cross.hint.roi': 'Scenario return after multiplying net spread by the selected leverage. Use it only for rough sizing and comparison, because real execution can still be worse than the displayed estimate.',
    'cross.hint.profit': 'Estimated dollar PnL for the selected deposit and leverage. Read it as a planning aid, not a promise, because slippage, funding changes and partial fills can materially change the outcome.',
    'cross.hint.volumeA': 'Approximate 24h quote volume on the first exchange. Higher volume usually means easier entry and exit; low volume makes even a beautiful spread harder to monetize.',
    'cross.hint.volumeB': 'Approximate 24h quote volume on the second exchange. Compare both legs together: the thinner venue often becomes the real bottleneck for trade size.',
    'cross.hint.fundingA': 'Current funding on the first exchange. Use it to understand whether holding this leg helps or hurts the carry while you wait for the spread to normalize.',
    'cross.hint.fundingB': 'Current funding on the second exchange. The pair of funding values matters more than either side alone, because the net carry can improve or destroy the apparent opportunity.',
    'cross.hint.spreadA': 'Bid/ask width on the first exchange. Lower values usually mean cleaner fills and less slippage, which matters when the displayed edge is not very large.',
    'cross.hint.spreadB': 'Bid/ask width on the second exchange. Watch it with Volume B: a wide book on the hedge leg can erase the edge even when the headline spread looks attractive.',
    'funding.label.expectedFundingCarryPercent': 'Gross carry %',
    'funding.label.openingFeePercent': 'Fees %',
    'funding.label.latencyReservePercent': 'Latency %',
    'funding.label.expectedNetCarryPercent': 'Net carry %',
    'funding.label.annualizedFundingEdge': 'Ann. carry %',
    'trade.noticeEntryReady': 'Entry is ready now',
    'trade.noticeWatch': 'Watch the setup',
    'trade.noticeHolding': 'Position is open',
    'trade.noticeHoldEnding': 'Holding window is ending',
    'trade.noticeExitReady': 'Exit the position',
    'trade.noticeIdle': 'No active trade alert',
    'trade.positionActive': '{pair} · {exchangeA}/{exchangeB}',
    'trade.positionMissing': 'No tracked position',
    'trade.timerEntry': 'Entry in {value}',
    'trade.timerHold': '{value} left in hold',
    'trade.timerPlan': 'Plan hold {value}',
    'trade.timerExpired': 'Hold time is exhausted',
    'trade.timerAwaiting': 'Waiting for fresh timing',
    'trade.watchAction': 'Track route',
    'trade.unwatchAction': 'Stop tracking',
    'trade.enterAction': 'Mark entry',
    'trade.exitAction': 'Close trade',
    'trade.routeLabel': 'Tracked route',
    'trade.enteredAt': 'Entered at {value}',
    'trade.watchStatusOn': 'This route is in your smart watchlist.',
    'trade.watchStatusOff': 'This route is not tracked yet. Add it here to receive smart trade notices.',
    'trade.smartSummary': 'Tracks whether you can enter, whether the hold is expiring, and when the route should already be exited.',
    'trade.noticeEmpty': 'No actionable trade notices yet.',
    'funding.hint.exchangeA': 'First venue in the carry-rate comparison. Use it together with the matching rate value and volume to judge whether the carry difference is actually tradable.',
    'funding.hint.exchangeB': 'Second venue in the carry-rate comparison. The trade idea is only as good as the weaker execution leg, so compare both venues rather than chasing the larger number alone.',
    'funding.hint.pair': 'Contract whose funding rates are being compared. Confirm the underlying matches across venues before interpreting the difference as a real carry dislocation.',
    'funding.hint.fundingA': 'Current funding on the first exchange. Positive and negative values matter differently depending on which side you intend to hold, so always interpret it in the context of the suggested action.',
    'funding.hint.fundingB': 'Current funding on the second exchange. Read it against Funding A, because the edge comes from the net carry between the two legs rather than from one isolated print.',
    'funding.hint.difference_abs': 'Absolute gap between the two funding rates. This is the fastest way to spot magnitude, but you still need to check liquidity and venue quality before acting on it.',
    'funding.hint.difference_rel': 'Relative funding gap in percent terms. Useful for comparing opportunities across contracts, especially when raw funding numbers have different scales.',
    'funding.hint.volumeA': '24h volume on the first venue. Use it as a practical filter: thin contracts often make carry arbitrage look better on screen than it will be in execution.',
    'funding.hint.volumeB': '24h volume on the second venue. Compare both sides together, because sizing is constrained by the weaker leg of the pair.',
    'funding.hint.expectedFundingCarryPercent': 'Funding carry expected over the active holding horizon before opening fees and latency reserve. This is the gross carry idea, not the fully tradable result.',
    'funding.hint.openingFeePercent': 'Estimated combined taker-fee burden to open both futures legs. It is the first deterministic cost that the carry must absorb.',
    'funding.hint.latencyReservePercent': 'Extra reserve for hedge lag and quote drift while both futures legs are being opened. Treat it as execution buffer rather than exchange fee.',
    'funding.hint.expectedNetCarryPercent': 'Expected carry after opening fees and latency reserve over the working holding horizon. This is the more practical ranking metric for funding arbitrage.',
    'funding.hint.annualizedFundingEdge': 'Expected net carry translated into yearly rate using the active or default holding horizon. Useful for comparing routes with different capital lock-up.',
    'funding.hint.recommended_action': 'Suggested direction under the current funding relationship. Treat it as an operational hint, then verify borrow, margin rules, fees and whether the carry is likely to persist long enough.',
    'health.hint.exchange': 'Tracked exchange name. Use this column as the anchor for reading all operational diagnostics tied to that venue.',
    'health.hint.status': 'Current condition of the adapter and upstream API. A degraded or quarantined status is a warning that otherwise attractive numbers from this venue deserve lower trust.',
    'health.hint.lastSuccess': 'Most recent confirmed successful response from the venue. If it is stale, treat the market data with caution even when the table still shows values.',
    'health.hint.coverage': 'Share of successfully loaded markets relative to discovered pairs. Lower coverage often means the venue is only partially observable, which weakens signal quality.',
    'health.hint.apiAlert': 'Shows whether degradation, repeated errors or quarantine are active. Use it as a hard risk flag before trusting spreads that depend on this exchange.',
    'health.hint.excluded': 'Reason the venue was excluded, degraded or skipped. This is the first place to check when a venue disappears from opportunities or behaves inconsistently.',
  },
  ru: {
    'meta.description': 'Мониторинг арбитражных возможностей во фьючерсах: межбиржевые спреды и расхождения ставок финансирования.',
    'title.app': 'Сканер фьючерсного арбитража',
    'hero.eyebrow': 'Рабочее пространство арбитража в реальном времени',
    'hero.copy': 'Межбиржевые спреды и расхождения ставок финансирования для бессрочных USDT-контрактов.',
    'hero.lastUpdate': 'Последнее обновление',
    'hero.waiting': 'Ожидание данных',
    'button.refreshFiltered': 'Обновить фильтр',
    'button.refreshFull': 'Обновить всё',
    'button.autoRefreshManual': 'Ручной режим',
    'button.autoRefreshAuto': 'Автообновление включено',
    'button.autoRefreshEnable': 'Включить автообновление',
    'button.autoRefreshDisable': 'Выключить автообновление',
    'button.reloadSnapshot': 'Перезагрузить снимок',
    'button.language': 'RU',
    'button.languageAria': 'Переключить язык',
    'button.toggleTheme': 'Переключить тему',
    'button.openSettings': 'Открыть настройки',
    'button.closeSettings': 'Закрыть настройки',
    'button.openHeatmap': 'Теплокарта рынка',
    'button.openGlossary': 'Справочник арбитража',
    'button.backToCross': 'К таблице спредов',
    'button.apply': 'Применить',
    'button.reset': 'Сбросить',
    'stat.cross': 'Межбиржевые спреды',
    'stat.funding': 'Расхождения ставок',
    'stat.exchanges': 'Отслеживаемые биржи',
    'stat.health': 'Здоровые биржи',
    'stat.quarantined': 'В карантине',
    'tabs.cross': 'Межбиржевые спреды',
    'tabs.funding': 'Ставки финансирования',
    'tabs.ai': 'ИИ-аналитика',
    'tabs.glossary': 'Справочник',
    'tabs.heatmap': 'Теплокарта',
    'tabs.health': 'Надёжность',
    'guide.read.title': 'Что такое рабочая сделка',
    'guide.read.body': 'Арбитраж начинается не с красивого большого числа в таблице, а с рабочего edge. У хорошей сделки чистый спред остаётся положительным после комиссий, latency reserve, funding и ожидаемого проскальзывания. Если эта математика держится только на сыром спреде, перед вами не возможность, а ловушка.',
    'guide.watch.title': 'Как отбирать новичку',
    'guide.watch.body': 'Идите по жёсткому порядку: сначала чистый спред и net after execution, потом ликвидность и bid/ask на худшей ноге, потом свежесть и синхронность котировок, затем funding и только после этого AI-подсказки. Если хотя бы один из первых блоков слабый, не пытайтесь спасать сетап верой в идеальный вход.',
    'guide.use.title': 'Как забирать максимум',
    'guide.use.body': 'Максимум выгоды редко даёт ожидание идеального нуля. Обычно больше зарабатывает тот, кто заранее знает зону входа, размер позиции, где забрать первую часть прибыли и при каком ухудшении структуры выйти без спора с рынком. Сначала план сделки, потом вход; не наоборот.',
    'panel.cross.title': 'Межбиржевые спреды',
    'panel.cross.desc': 'Таблица межбиржевых спредов в реальном времени с пересчётом комиссий, плеча и ставок финансирования прямо в браузере.',
    'panel.cross.note1': 'Подсказка: в рабочем сценарии спред после входа должен постепенно сходиться к нулю. Чем ближе значение к нулю на выходе, тем ближе рынок к нормальному состоянию.',
    'panel.cross.note2': 'Нажатие по строке открывает подробную карточку связки: итоговую оценку, стадию сигнала, заметки по исполнению, риски и историю спреда.',
    'panel.cross.filter': 'Фильтр пары',
    'panel.cross.placeholder': 'BTCUSDT, ETH...',
    'panel.cross.empty': 'Под текущие фильтры нет подходящих межбиржевых связок.',
    'panel.funding.title': 'Ставки финансирования',
    'panel.funding.desc': 'Маршруты funding arbitrage, отсортированные по ожидаемому чистому carry после комиссий на вход и резерва на задержку.',
    'panel.funding.empty': 'Под текущие фильтры нет подходящих расхождений по ставкам финансирования.',
    'panel.ai.title': 'ИИ-аналитика',
    'panel.ai.desc': 'Отдельная вкладка для разбора уже отобранных связок с помощью ИИ: когда входить, когда ждать, сколько ещё сохраняется запас преимущества, какой ожидаемый результат и в какой момент риск исполнения начинает ломать идею.',
    'panel.ai.note': 'Сначала отберите кандидатов в таблице спредов, потом проверьте карточку пары, и только затем переходите в ИИ-аналитику. Если базовая структура слабая, ИИ не должен становиться причиной входа; если структура сильная, ИИ помогает выбрать тайминг и размер риска.',
    'panel.ai.empty': 'ИИ-аналитика ждёт подходящие межбиржевые связки и расчёты по метрикам ИИ.',
    'panel.glossary.title': 'Справочник',
    'panel.glossary.desc': 'Пошаговый справочник по арбитражу для новичка: что означает каждый термин, как его читать на дашборде и как действовать до входа, в позиции и на выходе, чтобы не отдавать edge рынку.',
    'panel.glossary.search': 'Поиск термина',
    'panel.glossary.searchPlaceholder': 'спред, funding, вход, выход...',
    'panel.glossary.category': 'Категория',
    'panel.glossary.empty': 'Термины по текущему фильтру не найдены.',
    'panel.heatmap.title': 'Теплокарта рынка',
    'panel.heatmap.desc': 'Теплокарта межбиржевого рынка, ранжированная по аномалии, вероятности возврата и подтверждению на нескольких окнах.',
    'panel.heatmap.note': 'Карточки окрашены по силе аномалии: чем насыщеннее плитка, тем сильнее отклонение связки от собственной истории.',
    'panel.heatmap.empty': 'Для теплокарты нет доступных межбиржевых связок.',
    'panel.health.title': 'Надёжность и состояние',
    'panel.health.desc': 'Живая надёжность адаптеров, качество покрытия и признаки деградации API по каждой отслеживаемой бирже.',
    'panel.health.empty': 'Данные о состоянии бирж пока недоступны.',
    'settings.eyebrow': 'Персональное рабочее пространство',
    'settings.title': 'Настройки сканера',
    'settings.minSpread': 'Минимальный спред %',
    'settings.leverage': 'Плечо',
    'settings.deposit': 'Депозит $',
    'settings.holdingHours': 'Часы удержания',
    'settings.sparklineDays': 'Дней для мини-графиков',
    'settings.refreshApi.title': 'Подключение к серверу данных',
    'settings.refreshApi.desc': 'Подключите развернутый сервер панели, чтобы страница загружала данные и запускала обновление так же, как в локальном режиме.',
    'settings.refreshApiUrl': 'Базовый адрес сервера',
    'settings.refreshApiUrlPlaceholder': 'https://refresh.example.com',
    'settings.refreshApiKey': 'Ключ доступа к API обновления',
    'settings.refreshApiKeyPlaceholder': 'Нужен только для backend обновления на другом домене',
    'settings.refreshApiTest': 'Проверить сервер',
    'settings.refreshApiStatusSnapshot': 'Сейчас доступен только режим снимка. Подключите сервер, чтобы проверить живое обновление и диагностику.',
    'settings.refreshApiStatusChecking': 'Проверка сервера...',
    'settings.refreshApiStatusUnreachable': 'Сервер не ответил. Проверьте адрес, развёртывание и настройки CORS.',
    'settings.refreshApiStatusUrl': 'Адрес сервера',
    'settings.refreshApiStatusAccess': 'Доступ',
    'settings.refreshApiStatusState': 'Состояние обновления',
    'settings.refreshApiStatusVersion': 'Версия сервера',
    'settings.refreshApiStatusStarted': 'Время старта',
    'settings.refreshApiStatusCleanup': 'Очистка при старте',
    'settings.refreshApiStatusCleanupSummary': 'история {history} · удалено {removed} · stale data {staleData} · отчёты {reports} · tmp-файлы {tmpFiles}',
    'settings.refreshApiStatusAutoCleanup': 'Автоочистка истории',
    'settings.refreshApiStatusAutoCleanupLastRun': 'последний запуск {value}',
    'settings.refreshApiStatusAutoCleanupRemoved': 'удалено {value} снимков',
    'settings.refreshApiStatusAutoCleanupSkipped': 'пропущено {value} циклов во время обновления',
    'settings.refreshApiStatusProtected': 'Защищённая точка входа готова',
    'settings.refreshApiStatusKeyMissing': 'Нужен ключ API',
    'settings.refreshApiStatusOpen': 'Точка входа открыта',
    'settings.refreshApiStatusUnknown': 'Неизвестно',
    'settings.ai.title': 'Встроенный ИИ и открытые точки API',
    'settings.ai.desc': 'По умолчанию дашборд использует бесплатные встроенные ИИ-эвристики на текущем сервере. Указывайте базовый адрес только если у вас развёрнут совместимый внешний API ИИ на другом домене.',
    'settings.aiFilters.title': 'Фильтры ИИ',
    'settings.aiFilters.desc': 'Эти настройки формируют shortlist во вкладке ИИ и не засоряют основной сценарий операторскими подключениями.',
    'settings.operator.title': 'Операторские подключения',
    'settings.operator.desc': 'Адрес backend refresh и внешний AI API нужны редко, поэтому спрятаны в отдельный раскрывающийся блок.',
    'settings.aiExternalToggle': 'Использовать внешний API ИИ',
    'settings.aiApiUrl': 'Необязательный базовый адрес API ИИ',
    'settings.aiApiUrlPlaceholder': 'Оставьте пустым для встроенных маршрутов /ai',
    'settings.aiPredictionHorizon': 'Горизонт прогноза',
    'settings.minAiScore': 'Минимальная ИИ-оценка во вкладке аналитики',
    'settings.maxAiEntryWaitMinutes': 'Максимум до входа, мин',
    'settings.maxAiHoldingHours': 'Максимум удержания, ч',
    'settings.aiThresholdOptimize': 'Оптимизировать порог',
    'button.applyAiThreshold': 'Применить порог ИИ',
    'settings.autoRefresh.title': 'Режим обновления',
    'settings.autoRefresh.desc': 'Выберите, должен ли сканер обновляться только вручную или автоматически, пока эта вкладка открыта.',
    'settings.autoRefreshMode': 'Режим обновления',
    'settings.autoRefreshInterval': 'Автообновление каждые',
    'settings.autoRefreshModeManual': 'Только вручную',
    'settings.autoRefreshModeAuto': 'Автоматически',
    'settings.autoRefreshIntervalSeconds': '{value} сек',
    'settings.autoRefreshIntervalMinutes': '{value} мин',
    'settings.autoRefreshStrategy': 'Стратегия автообновления',
    'settings.autoRefreshStrategyFilteredOnly': 'Только по текущим фильтрам',
    'settings.autoRefreshStrategyFilteredPlusFull': 'По фильтрам и с редким полным обновлением',
    'settings.autoRefreshFullInterval': 'Полное обновление каждые',
    'settings.fees.title': 'Комиссия за рыночное исполнение по биржам, %',
    'settings.fees.desc': 'Эти значения влияют на спред после комиссий и итоговую оценку только в вашем браузере.',
    'settings.exchanges.title': 'Видимые биржи',
    'settings.exchanges.desc': 'Используйте флажки, чтобы скрывать площадки из всех таблиц и селекторов.',
    'settings.columns.title': 'Колонки таблицы межбиржевых спредов',
    'settings.columns.desc': 'Выберите, какие колонки должны оставаться видимыми в таблице межбиржевых спредов.',
    'settings.columnsAdvanced.title': 'Расширенная настройка таблицы',
    'settings.columnsAdvanced.desc': 'По умолчанию таблица остаётся компактной. Разворачивайте этот блок только если нужны дополнительные колонки или другой пресет.',
    'modal.close': 'Закрыть модальное окно',
    'footer.deploy': 'Репозиторий проекта',
    'footer.disclaimer': 'Только образовательная аналитика. Самостоятельно проверяйте исполнение, ликвидность, комиссии и юридические ограничения.',
    'common.na': 'н/д',
    'common.noDataYet': 'Данных пока нет',
    'common.noDetails': 'Без деталей',
    'common.allColumns': 'Все колонки',
    'common.compact': 'Компактно',
    'common.profitFocus': 'Фокус на прибыли',
    'common.carryFocus': 'Упор на доход от удержания',
    'common.liquidity': 'Ликвидность',
    'common.score': 'Итоговая оценка',
    'common.routeStatus': 'Статус маршрута',
    'common.exchange': 'Биржа',
    'common.cycle': 'Цикл',
    'common.startAmount': 'Стартовая сумма',
    'common.endAmount': 'Итоговая сумма',
    'common.profitPercent': 'После комиссий %',
    'common.effectiveProfit': 'После комиссий и проскальзывания %',
    'common.grossProfit': 'До комиссий %',
    'common.feeDrag': 'Потери на комиссиях %',
    'common.slippageDrag': 'Грубая оценка проскальзывания %',
    'common.quoteGap': 'Отклонение кросс-цены %',
    'common.totalVolume': 'Суммарная ликвидность',
    'common.crossPair': 'Кросс-контракт',
    'common.minLegVolume': 'Ликвидность самой слабой ноги',
    'common.method': 'Способ замыкания',
    'common.updated': 'Обновлено',
    'common.lastSuccess': 'Последний успех',
    'common.coverage': 'Покрытие %',
    'common.status': 'Статус',
    'common.apiAlert': 'Предупреждение API',
    'common.excludedWhy': 'Исключено / почему',
    'common.never': 'никогда',
    'refresh.scope': 'Объём',
    'refresh.status': 'Статус',
    'refresh.started': 'Старт',
    'refresh.finished': 'Завершено',
    'refresh.message': 'Сообщение',
    'refresh.lastSuccess': 'Последнее успешное обновление: {value}',
    'refresh.selectExchange': 'Перед обновлением выберите хотя бы одну биржу.',
    'refresh.running': 'Обновление...',
    'refresh.filtered': 'Обновляется отфильтрованный набор...',
    'refresh.full': 'Обновляется полный набор...',
    'refresh.snapshotReloaded': 'Опубликованный снимок перезагружен.',
    'refresh.snapshotReloadFailed': 'Не удалось перезагрузить опубликованный снимок.',
    'refresh.queuedFiltered': 'Обновление по текущим фильтрам поставлено в очередь.',
    'refresh.queuedFull': 'Полное обновление поставлено в очередь.',
    'refresh.apiUnavailable': 'API живого обновления здесь недоступен. Текущий снимок просто перезагружен.',
    'refresh.apiUnavailableDetail': 'Локальный API обновления недоступен. Выполнена только перезагрузка снимка.',
    'refresh.apiReadyDetail': 'Страница использует настроенный сервер и для данных, и для обновления, то есть тот же поток, что и в локальной версии.',
    'refresh.apiUnreachable': 'API обновления настроен, но не ответил. Проверьте адрес API и настройки CORS.',
    'refresh.sourceSnapshot': 'Источник: опубликованный снимок',
    'refresh.sourceLocal': 'Источник: локальный сервер',
    'refresh.sourceSameOrigin': 'Источник: сервер на том же домене',
    'refresh.sourceConfigured': 'Источник: настроенный сервер',
    'refresh.sourceEphemeralFiltered': 'Источник: временный снимок по текущим фильтрам',
    'refresh.accessOpen': 'Обновление: точка входа открыта',
    'refresh.accessReady': 'Обновление: защищённая точка входа готова',
    'refresh.accessMissing': 'Обновление: нужен ключ API',
    'refresh.accessSnapshot': 'Обновление: доступен только снимок',
    'refresh.accessUnreachable': 'Обновление: сервер недоступен',
    'refresh.ageNow': 'Возраст снимка: <1м',
    'refresh.ageMinutes': 'Возраст снимка: {value}м',
    'refresh.ageHours': 'Возраст снимка: {value}ч',
    'refresh.runLink': 'Ссылка на запуск',
    'refresh.inProgress': 'Обновление в процессе...',
    'refresh.dataInProgress': 'Идёт обновление данных...',
    'refresh.failed': 'Обновление завершилось ошибкой.',
    'refresh.statusFailed': 'Не удалось получить статус обновления.',
    'refresh.timeout': 'Ожидание обновления превысило лимит.',
    'refresh.success': 'Данные успешно обновлены.',
    'refresh.noRows': 'Нет строк {scope} для экспорта.',
    'autoRefresh.badgeManual': 'Режим обновления: вручную',
    'autoRefresh.badgeAuto': 'Режим обновления: авто каждые {value}с',
    'autoRefresh.badgePaused': 'Режим обновления: авто на паузе',
    'autoRefresh.badgeFilteredOnly': 'Стратегия: только по текущим фильтрам',
    'autoRefresh.badgeFilteredPlusFull': 'Стратегия: по фильтрам и полное обновление каждые {value}',
    'autoRefresh.running': 'Идёт автоматическое обновление...',
    'quality.visibleExchanges': 'Видимые биржи {visible}/{total}',
    'quality.crossHits': 'Межбиржевые сигналы {count}',
    'quality.hotAnomalies': 'Горячие аномалии {count}',
    'quality.quarantined': 'В карантине {count}',
    'quality.systemAlerts': 'Системные алерты {count}',
    'quality.publicationProfile': 'Профиль публикации {mode}',
    'quality.quoteAge': 'Котировки p95 {value}с',
    'quality.refreshStatus': 'Статус обновления {status}',
    'quality.refreshScope': 'Объём обновления {scope}',
    'quality.snapshotOriginEphemeral': 'Снимок: временная выборка по текущим фильтрам',
    'settings.refreshApiStatusResultCache': 'Кэш временных результатов обновления',
    'settings.refreshApiStatusResultCacheStats': 'попадания {hits} · промахи {misses} · сохранения {stores} · активные {active}',
    'settings.refreshApiStatusAlerts': 'алерты {active} · критичных {critical} · предупреждений {warning}',
    'settings.refreshApiStatusAdaptivePublication': 'net >= {spread}% · мин. объём >= {volume} · max bid/ask <= {bidAsk}%',
    'settings.refreshApiStatusQuoteAge': 'среднее {avg}с · p95 {p95}с · stale {stale}%',
    'heatmap.marketVolatility': 'Рыночная волатильность',
    'heatmap.backdrop': 'Фон риска',
    'heatmap.strongest': 'Сильнейшая видимая аномалия',
    'heatmap.reversion': 'Видимая вероятность возврата',
    'heatmap.aligned': 'Согласованные аномалии',
    'heatmap.cleanConvergence': 'Чистая сходимость',
    'heatmap.anomaly': 'Аномалия {value}',
    'heatmap.marketAvg': 'Среднее по рынку {value}%',
    'heatmap.market': 'Рынок {value}/{total}',
    'heatmap.tiles': 'Плиток {count}',
    'heatmap.displayedTiles': 'Показанные плитки',
    'heatmap.net': 'Чист.',
    'heatmap.z': 'Z',
    'heatmap.pct': 'Перц.',
    'heatmap.rev': 'Возв.',
    'hero.volatility': 'Режим волатильности',
    'hero.backdrop': 'Риск-фон',
    'hero.fundingDrag': 'Давление ставок финансирования',
    'hero.eventStress': 'Нагрузка от событий',
    'hero.systemAlerts': 'Системные алерты',
    'hero.publicationProfile': 'Профиль публикации',
    'hero.whale': 'Крупные потоки',
    'hero.whaleWaiting': 'Ожидание AI-данных по потокам',
    'hero.whaleHot': '{symbol} · {value}/100',
    'hero.whaleEvents': '{count} крупных переводов',
    'hero.elevated': '{value}/{total} повышены',
    'hero.published': '{published}/{total} опубликовано · {regime} {share}%',
    'hero.flagged': '{value}% отмечено как риск',
    'hero.alertsActive': '{count} активны',
    'hero.alertsClear': 'активных алертов нет',
    'hero.alertTopSeverity': 'максимальная серьёзность {severity}',
    'hero.publicationThresholds': 'net >= {spread}% · объём >= {volume} · bid/ask <= {bidAsk}%',
    'hero.smartNotices': 'Умные уведомления по сделкам',
    'hero.pairsAdverse': '{count} связок неблагоприятны',
    'hero.proxies': '{count} сигналов новостного типа',
    'health.normal': 'норма',
    'health.degraded': 'деградация',
    'health.quarantined': 'карантин',
    'health.active': 'Активно в текущей вселенной',
    'health.until': 'Карантин до {value}',
    'health.summaryHealthy': 'Здоровые биржи',
    'health.summaryDegraded': 'Деградировавшие биржи',
    'health.summaryCoverage': 'Среднее покрытие',
    'health.summaryAlerts': 'Системные алерты',
    'health.summaryQuoteAge': 'Возраст котировок p95',
    'health.summaryUpdated': 'Последнее обновление состояния бирж',
    'publication.open': 'открытый',
    'publication.cautious': 'осторожный',
    'publication.blocked': 'заблокированный',
    'alertSeverity.info': 'инфо',
    'alertSeverity.warning': 'предупреждение',
    'alertSeverity.critical': 'критично',
    'detail.executionNotes': 'Заметки по исполнению',
    'detail.riskNotes': 'Риск-заметки',
    'detail.nextStep': 'Что делать дальше',
    'detail.riskFilters': 'Сработавшие ограничения риска',
    'detail.rankingScore': 'Итоговая оценка',
    'detail.lifecycle': 'Стадия сигнала',
    'detail.spreadRegime': 'Режим спреда',
    'detail.historicalZ': 'Историческое отклонение от нормы',
    'detail.currentPercentile': 'Текущее положение в истории',
    'detail.anomalyScore': 'Оценка аномалии',
    'detail.reversionProbability': 'Вероятность возврата',
    'detail.spreadQuality': 'Качество спреда',
    'detail.fundingQuality': 'Качество ставки финансирования',
    'detail.liquidityQuality': 'Качество ликвидности',
    'detail.stability': 'Стабильность спреда',
    'detail.executionDifficulty': 'Сложность исполнения',
    'detail.alignment': 'Согласованность покупки и продажи',
    'detail.meanReversionEta': 'Оценка времени возврата к норме',
    'detail.tradeAlert': 'Статус сделки',
    'detail.positionState': 'Состояние позиции',
    'detail.holdingTimer': 'Таймер удержания',
    'detail.entryZone': 'Рекомендуемая зона входа',
    'detail.exitZone': 'Рекомендуемая зона выхода',
    'detail.notEnoughSignal': 'недостаточно сигнала',
    'detail.execution.high': 'высокая',
    'detail.execution.medium': 'средняя',
    'detail.execution.low': 'низкая',
    'risk.volatility': 'Волатильность',
    'risk.liquidity': 'Ликвидность',
    'risk.funding': 'Ставки финансирования',
    'risk.newsStyle': 'Новостной риск',
    'risk.noteWideSpread': 'Спред между лучшей покупкой и продажей на одной из ног слишком широк. Реальный результат может оказаться заметно хуже табличной оценки.',
    'risk.noteLowLiquidity': 'На одной из ног ограниченная ликвидность. Размер позиции нужно дозировать аккуратно, чтобы не получить всплеск проскальзывания.',
    'risk.noteWidening': 'Спред всё ещё расширяется. Ранний вход без поэтапного набора может быть болезненным.',
    'risk.noteNoisy': 'Исторический режим шумный. Сигналы могут быстро переворачиваться даже при статистически экстремальном спреде.',
    'risk.noteLongHold': 'Модель сходимости предполагает длинное удержание. В этом случае влияние ставок финансирования и риск площадки становятся важнее.',
    'risk.noteClean': 'Помимо обычного риска исполнения в последнем снимке не видно повышенных структурных рисков.',
    'exchange.noTelemetry': '{exchange}: данные о состоянии биржи недоступны.',
    'exchange.base': '{exchange}: статус {status}, покрытие {coverage}%',
    'exchange.baseError': '{exchange}: статус {status}, покрытие {coverage}%. {error}',
    'execution.enterFirst': 'Сначала открывайте ногу на {exchange}, чтобы снизить риск мгновенного проскальзывания.',
    'execution.safer': 'Сейчас на {exchange} уже спред между лучшей покупкой и продажей, поэтому для агрессивного исполнения эта площадка безопаснее.',
    'execution.fundingAdverse': 'Доход от удержания позиции сейчас неблагоприятен. Слишком долгое удержание может съесть ожидаемое преимущество.',
    'execution.fundingSupportive': 'Доход от удержания позиции поддерживает идею, пока спред сходится.',
    'execution.fundingNeutral': 'Ставки финансирования примерно нейтральны. Основное преимущество здесь идёт от сужения спреда.',
    'regime.clean-convergence': 'чистая сходимость',
    'regime.stable-carry': 'стабильный доход от удержания',
    'regime.noisy-range': 'шумный диапазон',
    'regime.widening-trend': 'расширяющийся тренд',
    'regime.volatile-expansion': 'волатильное расширение',
    'regime.compressed': 'сжатый',
    'regime.forming': 'формируется',
    'status.new': 'новая',
    'status.widening': 'расширяется',
    'status.stable': 'стабильная',
    'status.converging': 'сходится',
    'status.expired': 'сжата',
    'status.ok': 'ok',
    'status.failed': 'ошибка',
    'status.idle': 'ожидание',
    'status.running': 'выполняется',
    'status.completed': 'завершён',
    'status.quarantined': 'карантин',
    'status.unknown': 'неизвестно',
    'volatility.contained': 'сдержанная волатильность',
    'volatility.mixed': 'смешанный фон',
    'volatility.stressed': 'стрессовая волатильность',
    'backdrop.calm': 'спокойный',
    'backdrop.watch': 'зона наблюдения',
    'backdrop.stressed': 'стресс',
    'cross.header.exchangeA': 'Биржа A',
    'cross.header.pairA': 'Пара',
    'cross.header.exchangeB': 'Биржа B',
    'cross.header.direction': 'Напр.',
    'cross.header.score': 'Оценка',
    'cross.header.aiScore': 'ИИ',
    'cross.header.aiForecast': 'Прогноз',
    'cross.header.entryWaitMinutes': 'Вход через',
    'cross.header.holdingHoursRecommended': 'Удержание',
    'cross.header.exitTarget': 'Цель выхода',
    'cross.header.tradeAlert': 'Статус сделки',
    'cross.header.positionState': 'Позиция',
    'cross.header.lifecycle': 'Сост.',
    'cross.header.regime': 'Режим',
    'cross.header.zScore': 'Z',
    'cross.header.percentile': 'Перц.',
    'cross.header.reversionProbability': 'Возврат %',
    'cross.header.spread': 'Спред %',
    'cross.header.netSpread': 'Чист. %',
    'cross.header.expectedFundingCarryPercent': 'Carry %',
    'cross.header.openingFeePercent': 'Комиссии %',
    'cross.header.latencyReservePercent': 'Задержка %',
    'cross.header.annualizedNetEdge': 'Год. edge %',
    'cross.header.roi': 'ROI %',
    'cross.header.profit': 'Прибыль $',
    'cross.header.volumeA': 'Объём A',
    'cross.header.volumeB': 'Объём B',
    'cross.header.fundingA': 'Финансир. A %',
    'cross.header.fundingB': 'Финансир. B %',
    'cross.header.spreadA': 'Покупка/продажа A %',
    'cross.header.spreadB': 'Покупка/продажа B %',
    'cross.label.exchangeA': 'Биржа A',
    'cross.label.pairA': 'Пара',
    'cross.label.exchangeB': 'Биржа B',
    'cross.label.direction': 'Направление',
    'cross.label.score': 'Итоговая оценка',
    'cross.label.aiScore': 'Оценка уверенности ИИ',
    'cross.label.aiForecast': 'Прогноз спреда ИИ',
    'cross.label.entryWaitMinutes': 'Рекомендованное ожидание до входа',
    'cross.label.holdingHoursRecommended': 'Рекомендуемая длительность удержания',
    'cross.label.exitTarget': 'Цель выхода',
    'cross.label.tradeAlert': 'Умное уведомление по сделке',
    'cross.label.positionState': 'Отмеченная позиция',
    'cross.label.lifecycle': 'Стадия сигнала',
    'cross.label.regime': 'Режим спреда',
    'cross.label.zScore': 'Отклонение от нормы',
    'cross.label.percentile': 'Положение в истории',
    'cross.label.reversionProbability': 'Вероятность возврата',
    'cross.label.spread': 'Исходный спред %',
    'cross.label.netSpread': 'Спред после комиссий %',
    'cross.label.expectedFundingCarryPercent': 'Ожидаемый carry по funding %',
    'cross.label.openingFeePercent': 'Комиссия на открытие %',
    'cross.label.latencyReservePercent': 'Резерв на задержку %',
    'cross.label.annualizedNetEdge': 'Годовой чистый edge %',
    'cross.label.roi': 'ROI xL %',
    'cross.label.profit': 'Прибыль $',
    'cross.label.volumeA': 'Объём A',
    'cross.label.volumeB': 'Объём B',
    'cross.label.fundingA': 'Ставка финансирования A %',
    'cross.label.fundingB': 'Ставка финансирования B %',
    'cross.label.spreadA': 'Покупка/продажа A %',
    'cross.label.spreadB': 'Покупка/продажа B %',
    'cross.mobile.netSpread': 'Чист. %',
    'cross.mobile.roi': 'ROI',
    'cross.mobile.reversion': 'Возврат',
    'cross.mobile.profit': 'PnL $',
    'cross.mobile.tradeAlert': 'Статус сделки',
    'cross.mobile.positionState': 'Позиция',
    'cross.mobile.fundingA': 'Funding A',
    'cross.mobile.fundingB': 'Funding B',
    'cross.mobile.fundingDelta': 'Дельта funding',
    'cross.hint.exchangeA': 'Биржа первой ноги позиции. Оценивайте её вместе с объёмом, спредом между лучшей покупкой и продажей и общим состоянием биржи: сильный спред на слабой площадке часто хуже, чем более скромный спред на надёжной бирже.',
    'cross.hint.pairA': 'Контракт, по которому сравнивается спред. Используйте это поле, чтобы убедиться, что вы сравниваете один и тот же базовый актив и сопоставимый бессрочный контракт, а не похожие, но разные инструменты.',
    'cross.hint.exchangeB': 'Биржа второй ноги позиции. Сравнивайте её ликвидность и состояние API с первой площадкой, потому что реальная исполнимость связки ограничивается более слабой стороной.',
    'cross.hint.direction': 'Подсказка, где предполагается покупка и где продажа в сценарии на сужение спреда. Используйте её как стартовую идею, а перед входом проверьте ставки финансирования, ликвидность и возможность открыть обе ноги синхронно.',
    'cross.hint.score': 'Сводная оценка качества по спреду, ставкам финансирования, ликвидности, стабильности, сложности исполнения и ожидаемой скорости возврата к норме. Удобна для ранжирования, но окончательное решение лучше принимать по спреду после комиссий, состоянию бирж и качеству исполнения.',
    'cross.hint.aiScore': 'Оценка уверенности ИИ учитывает текущий спред, микроструктуру, ставки финансирования, ликвидность и недавнюю устойчивость спреда. Используйте её как быстрый фильтр пригодности сделки перед ручной проверкой.',
    'cross.hint.aiForecast': 'Краткосрочный прогноз направления спреда и ожидаемого движения. Его лучше читать вместе с вероятностью и оценкой ИИ, а не как самостоятельный торговый сигнал.',
    'cross.hint.entryWaitMinutes': 'Рекомендуемое время ожидания перед входом. Чем выше значение, тем сильнее модель советует дождаться стабилизации структуры, лучшей ликвидности или подтверждения возврата к норме.',
    'cross.hint.holdingHoursRecommended': 'Рекомендуемый горизонт удержания по модели исполнения. Нужен для оценки carry-риска, заморозки капитала и совместимости идеи с вашим рабочим таймфреймом.',
    'cross.hint.exitTarget': 'Целевой уровень нормализации или тип выхода, который предлагает AI-слой. Читайте его вместе с ETA и текущим чистым спредом, чтобы не пересиживать идею в ожидании идеального выхода.',
    'cross.hint.tradeAlert': 'Рабочий статус по сделке для этой строки: можно ли уже входить, открыта ли позиция, заканчивается ли удержание и пора ли уже выходить.',
    'cross.hint.positionState': 'Локально отмеченная открытая позиция по этой строке. Показывает точную пару и маршрут по биржам, который вы пометили как открытый внутри сканнера.',
    'cross.hint.lifecycle': 'Фаза жизни сигнала: он только появился, расширяется, стабилизировался, уже сходится или теряет актуальность. Используйте показатель, чтобы понимать, наблюдаете ли вы ранний импульс или уже позднюю стадию идеи.',
    'cross.hint.regime': 'Характер поведения спреда во времени. Чистая сходимость и стабильный доход от удержания обычно читаются лучше, а шумный диапазон и расширение требуют более осторожного входа и меньшего доверия к цифрам.',
    'cross.hint.zScore': 'Насколько текущий спред после комиссий отклонился от своей исторической нормы в статистических отклонениях. Чем выше модуль, тем необычнее движение; использовать лучше вместе с положением в истории, а не отдельно.',
    'cross.hint.percentile': 'Насколько редким является текущий спред после комиссий на фоне собственной истории. Значения ближе к 100 означают экстремум; это помогает отделять обычный шум от действительно редкой деформации рынка.',
    'cross.hint.reversionProbability': 'Оценка вероятности того, что спред начнёт сужаться, а не продолжит уходить дальше. Смотрите на этот показатель как на уровень уверенности модели и подтверждайте его стадией сигнала, режимом спреда и состоянием бирж.',
    'cross.hint.spread': 'Сырой ценовой разрыв до комиссий и ставок финансирования. Помогает заметить саму деформацию, но для сделки почти всегда важнее спред после комиссий, потому что именно он ближе к реальному результату.',
    'cross.hint.netSpread': 'Спред после учёта комиссии за рыночное исполнение и влияния ставок финансирования. Это один из главных практических показателей для входа: в рабочем сценарии возврата к норме у него должно оставаться пространство для движения обратно к нулю на выходе.',
    'cross.hint.expectedFundingCarryPercent': 'Ожидаемый вклад funding на выбранном или дефолтном горизонте удержания. Положительное значение поддерживает идею во времени, отрицательное означает, что удержание само по себе съедает часть преимущества.',
    'cross.hint.openingFeePercent': 'Оценка суммарной комиссии за вход по обеим фьючерсным ногам. Показывает, какая часть сырого спреда исчезает ещё до проскальзывания.',
    'cross.hint.latencyReservePercent': 'Дополнительный резерв под задержку исполнения и сдвиг котировок между площадками. Это защитный буфер против leg-risk и устаревших цен.',
    'cross.hint.annualizedNetEdge': 'Чистое преимущество, приведённое к годовому темпу с учётом активного горизонта удержания. Удобно для сравнения маршрутов с разным временем жизни.',
    'cross.hint.roi': 'Сценарная доходность после умножения спреда после комиссий на выбранное плечо. Удобна для сравнения вариантов и грубой оценки размера сделки, но не гарантирует такой же фактический результат после проскальзывания и изменений ставок финансирования.',
    'cross.hint.profit': 'Оценочная прибыль в долларах для выбранного депозита и плеча. Используйте её как ориентир по масштабу идеи, а не как обещание прибыли: в реальности итог меняют проскальзывание, ставки финансирования и неполное исполнение.',
    'cross.hint.volumeA': 'Примерный 24h quote-volume на первой бирже. Чем выше значение, тем проще входить и выходить без лишнего давления на цену; маленький объём делает даже красивый спред трудно реализуемым.',
    'cross.hint.volumeB': 'Примерный 24h quote-volume на второй бирже. Смотрите обе стороны вместе, потому что размер позиции в итоге ограничивает более тонкая площадка.',
    'cross.hint.fundingA': 'Текущее значение ставки финансирования на первой бирже. Оно показывает, помогает ли удержание этой ноги общей экономике сделки или, наоборот, съедает часть преимущества, пока вы ждёте сужения.',
    'cross.hint.fundingB': 'Текущее значение ставки финансирования на второй бирже. Его полезно читать только в паре со ставкой на первой бирже, потому что итоговый доход от удержания определяется разницей двух ног, а не одной цифрой.',
    'cross.hint.spreadA': 'Ширина спреда между лучшей покупкой и продажей на первой бирже. Чем она меньше, тем чище ожидаемое исполнение; широкий стакан особенно опасен, когда преимущество по спреду после комиссий и так невелико.',
    'cross.hint.spreadB': 'Ширина спреда между лучшей покупкой и продажей на второй бирже. Смотрите её вместе с объёмом второй ноги: если хеджирующая нога дорогая в исполнении, она может обнулить красивую картинку в таблице.',
    'funding.label.expectedFundingCarryPercent': 'Carry до издержек %',
    'funding.label.openingFeePercent': 'Комиссии %',
    'funding.label.latencyReservePercent': 'Резерв задержки %',
    'funding.label.expectedNetCarryPercent': 'Чистый carry %',
    'funding.label.annualizedFundingEdge': 'Годовой carry %',
    'trade.noticeEntryReady': 'Можно входить сейчас',
    'trade.noticeWatch': 'Следить за входом',
    'trade.noticeHolding': 'Позиция открыта',
    'trade.noticeHoldEnding': 'Удержание заканчивается',
    'trade.noticeExitReady': 'Пора выходить',
    'trade.noticeIdle': 'Активного сигнала по сделке нет',
    'trade.positionActive': '{pair} · {exchangeA}/{exchangeB}',
    'trade.positionMissing': 'Позиция не отмечена',
    'trade.timerEntry': 'До входа {value}',
    'trade.timerHold': 'До конца удержания {value}',
    'trade.timerPlan': 'План удержания {value}',
    'trade.timerExpired': 'Время удержания уже вышло',
    'trade.timerAwaiting': 'Ожидаем уточнение тайминга',
    'trade.watchAction': 'Отслеживать маршрут',
    'trade.unwatchAction': 'Не отслеживать',
    'trade.enterAction': 'Отметить вход',
    'trade.exitAction': 'Закрыть сделку',
    'trade.routeLabel': 'Маршрут позиции',
    'trade.enteredAt': 'Вход отмечен в {value}',
    'trade.watchStatusOn': 'Маршрут добавлен в умное отслеживание.',
    'trade.watchStatusOff': 'Маршрут пока не отслеживается. Добавьте его здесь, чтобы получать умные уведомления по сделке.',
    'trade.smartSummary': 'Показывает, можно ли входить, заканчивается ли удержание и не пора ли уже закрывать маршрут.',
    'trade.noticeEmpty': 'Пока нет активных уведомлений по сделкам.',
    'funding.hint.exchangeA': 'Первая биржа в сравнении ставок финансирования. Читайте её вместе со ставкой и объёмом, чтобы понимать, можно ли вообще реализовать наблюдаемую разницу.',
    'funding.hint.exchangeB': 'Вторая биржа в сравнении ставок финансирования. Не гонитесь только за большей разницей ставок: качество слабой ноги почти всегда определяет реальную пригодность идеи.',
    'funding.hint.pair': 'Контракт, по которому сопоставляются ставки финансирования. Сначала убедитесь, что базовый актив и тип инструмента совпадают на обеих площадках, и только потом интерпретируйте расхождение как возможность на доходе от удержания.',
    'funding.hint.fundingA': 'Ставка финансирования на первой бирже. Смысл показателя зависит от стороны удержания позиции, поэтому интерпретируйте его вместе с рекомендованным действием, а не изолированно.',
    'funding.hint.fundingB': 'Ставка финансирования на второй бирже. Важна не сама по себе, а в паре со ставкой на первой бирже, потому что торговая идея живёт на чистой разнице дохода от удержания между двумя ногами.',
    'funding.hint.difference_abs': 'Абсолютное расхождение между ставками финансирования. Это быстрый способ увидеть масштаб возможности, но перед входом всё равно нужно проверить ликвидность, комиссии и надёжность бирж.',
    'funding.hint.difference_rel': 'Относительная разница в процентах. Удобна для сравнения разных контрактов между собой, когда абсолютные значения ставок находятся в разных диапазонах.',
    'funding.hint.volumeA': '24h объём на первой бирже. Используйте как практический фильтр: на тонком рынке арбитраж по ставкам на экране выглядит лучше, чем в реальном исполнении.',
    'funding.hint.volumeB': '24h объём на второй бирже. Для размера позиции ориентируйтесь на более слабую сторону, а не на среднее по двум площадкам.',
    'funding.hint.expectedFundingCarryPercent': 'Ожидаемый carry по funding на активном горизонте удержания до вычета комиссий на вход и резерва на задержку. Это валовая идея дохода от удержания, а не конечный исполнимый результат.',
    'funding.hint.openingFeePercent': 'Оценка суммарной комиссии за открытие обеих фьючерсных ног. Это первая детерминированная издержка, которую должен перекрыть carry.',
    'funding.hint.latencyReservePercent': 'Дополнительный резерв на lag между ногами и сдвиг котировок в момент открытия. Это защитный буфер исполнения, а не биржевая комиссия.',
    'funding.hint.expectedNetCarryPercent': 'Ожидаемый чистый carry после комиссий на вход и резерва на задержку на рабочем горизонте удержания. Именно этот показатель ближе всего к практическому ранжированию funding arbitrage.',
    'funding.hint.annualizedFundingEdge': 'Ожидаемый чистый carry, приведённый к годовому темпу на активном или дефолтном горизонте удержания. Удобен для сравнения маршрутов с разной длительностью блокировки капитала.',
    'funding.hint.recommended_action': 'Предпочтительное направление при текущем соотношении ставок финансирования. Это операционная подсказка, после которой ещё стоит проверить правила по марже, комиссии и насколько устойчив сам доход от удержания.',
    'health.hint.exchange': 'Название отслеживаемой биржи. От этой колонки удобно отталкиваться при чтении всех остальных операционных сигналов по площадке.',
    'health.hint.status': 'Текущее состояние подключения к бирже и её API. Если площадка деградирует или находится в карантине, любым сигналам с её участием стоит доверять заметно меньше.',
    'health.hint.lastSuccess': 'Последний подтверждённый успешный ответ от биржи. Если он устарел, текущие цифры могут быть технически не свежими, даже если таблица всё ещё заполнена.',
    'health.hint.coverage': 'Доля успешно загруженных рынков от числа найденных пар. Низкое покрытие означает, что наблюдаемость площадки неполная, а значит и качество сигналов хуже.',
    'health.hint.apiAlert': 'Показывает, есть ли деградация API, серия ошибок или карантин. Используйте как жёсткий риск-флаг перед тем, как доверять спредам с этой биржей.',
    'health.hint.excluded': 'Причина исключения, пропуска или деградации площадки. Это первое место, куда стоит смотреть, если биржа исчезла из идей или ведёт себя странно.',
  },
};

const CROSS_COLUMN_SCHEMA = [
  { key: 'exchangeA' },
  { key: 'pairA' },
  { key: 'exchangeB' },
  { key: 'direction' },
  { key: 'score' },
  { key: 'aiScore' },
  { key: 'aiForecast' },
  { key: 'entryWaitMinutes' },
  { key: 'holdingHoursRecommended' },
  { key: 'exitTarget' },
  { key: 'tradeAlert' },
  { key: 'positionState' },
  { key: 'lifecycle' },
  { key: 'regime' },
  { key: 'zScore' },
  { key: 'percentile' },
  { key: 'reversionProbability' },
  { key: 'spread' },
  { key: 'netSpread' },
  { key: 'expectedFundingCarryPercent' },
  { key: 'openingFeePercent' },
  { key: 'latencyReservePercent' },
  { key: 'annualizedNetEdge' },
  { key: 'roi' },
  { key: 'profit' },
  { key: 'volumeA' },
  { key: 'volumeB' },
  { key: 'fundingA' },
  { key: 'fundingB' },
  { key: 'spreadA' },
  { key: 'spreadB' },
];

const STATIC_TRANSLATION_BINDINGS = [
  { selector: 'meta[name="description"]', attr: 'content', key: 'meta.description' },
  { selector: 'title', key: 'title.app' },
  { selector: '.hero .eyebrow', key: 'hero.eyebrow' },
  { selector: '.hero-copy', key: 'hero.copy' },
  { selector: '.hero-metric span', key: 'hero.lastUpdate' },
  { selector: '#refresh-button', key: 'button.refreshFiltered' },
  { selector: '#refresh-full-button', key: 'button.refreshFull' },
  { selector: '#language-toggle', key: 'button.language' },
  { selector: '#language-toggle', attr: 'aria-label', key: 'button.languageAria' },
  { selector: '#theme-toggle', attr: 'aria-label', key: 'button.toggleTheme' },
  { selector: '#settings-toggle', attr: 'aria-label', key: 'button.openSettings' },
  { selector: '#open-heatmap-button', key: 'button.openHeatmap' },
  { selector: '#open-glossary-button', key: 'button.openGlossary' },
  { selector: '#heatmap-back-button', key: 'button.backToCross' },
  { selector: '#glossary-back-button', key: 'button.backToCross' },
  { selector: '.stat-card:nth-child(1) span', key: 'stat.cross' },
  { selector: '.stat-card:nth-child(2) span', key: 'stat.funding' },
  { selector: '.stat-card:nth-child(3) span', key: 'stat.exchanges' },
  { selector: '.stat-card:nth-child(4) span', key: 'stat.health' },
  { selector: '.stat-card:nth-child(5) span', key: 'stat.quarantined' },
  { selector: '.tabs .tab[data-tab="cross"]', key: 'tabs.cross' },
  { selector: '.tabs .tab[data-tab="funding"]', key: 'tabs.funding' },
  { selector: '.tabs .tab[data-tab="ai"]', key: 'tabs.ai' },
  { selector: '.tabs .tab[data-tab="glossary"]', key: 'tabs.glossary' },
  { selector: '.tabs .tab[data-tab="heatmap"]', key: 'tabs.heatmap' },
  { selector: '.tabs .tab[data-tab="health"]', key: 'tabs.health' },
  { selector: '#guide-read-title', key: 'guide.read.title' },
  { selector: '#guide-read-body', key: 'guide.read.body' },
  { selector: '#guide-watch-title', key: 'guide.watch.title' },
  { selector: '#guide-watch-body', key: 'guide.watch.body' },
  { selector: '#guide-use-title', key: 'guide.use.title' },
  { selector: '#guide-use-body', key: 'guide.use.body' },
  { selector: '[data-panel="cross"] .panel-head h2', key: 'panel.cross.title' },
  { selector: '#cross-panel-desc', key: 'panel.cross.desc' },
  { selector: '#cross-panel-note-1', key: 'panel.cross.note1' },
  { selector: '#cross-panel-note-2', key: 'panel.cross.note2' },
  { selector: '[data-panel="cross"] .search-box span', key: 'panel.cross.filter' },
  { selector: '#cross-search', attr: 'placeholder', key: 'panel.cross.placeholder' },
  { selector: '#cross-empty', key: 'panel.cross.empty' },
  { selector: '[data-panel="funding"] .panel-head h2', key: 'panel.funding.title' },
  { selector: '#funding-panel-desc', key: 'panel.funding.desc' },
  { selector: '#funding-empty', key: 'panel.funding.empty' },
  { selector: '[data-panel="ai"] .panel-head h2', key: 'panel.ai.title' },
  { selector: '#ai-panel-desc', key: 'panel.ai.desc' },
  { selector: '#ai-panel-note', key: 'panel.ai.note' },
  { selector: '#ai-empty', key: 'panel.ai.empty' },
  { selector: '[data-panel="glossary"] .panel-head h2', key: 'panel.glossary.title' },
  { selector: '#glossary-panel-desc', key: 'panel.glossary.desc' },
  { selector: 'label:has(#glossary-search) > span', key: 'panel.glossary.search' },
  { selector: '#glossary-search', attr: 'placeholder', key: 'panel.glossary.searchPlaceholder' },
  { selector: 'label:has(#glossary-category) > span', key: 'panel.glossary.category' },
  { selector: '#glossary-empty', key: 'panel.glossary.empty' },
  { selector: '[data-panel="heatmap"] .panel-head h2', key: 'panel.heatmap.title' },
  { selector: '#heatmap-panel-desc', key: 'panel.heatmap.desc' },
  { selector: '#heatmap-panel-note', key: 'panel.heatmap.note' },
  { selector: '#heatmap-empty', key: 'panel.heatmap.empty' },
  { selector: '[data-panel="health"] .panel-head h2', key: 'panel.health.title' },
  { selector: '#health-panel-desc', key: 'panel.health.desc' },
  { selector: '#health-empty', key: 'panel.health.empty' },
  { selector: '#settings-drawer .eyebrow', key: 'settings.eyebrow' },
  { selector: '#settings-drawer h2', key: 'settings.title' },
  { selector: '#settings-close', attr: 'aria-label', key: 'button.closeSettings' },
  { selector: 'label:has([name="minSpread"]) > span', key: 'settings.minSpread' },
  { selector: 'label:has([name="leverage"]) > span', key: 'settings.leverage' },
  { selector: 'label:has([name="deposit"]) > span', key: 'settings.deposit' },
  { selector: 'label:has([name="holdingHours"]) > span', key: 'settings.holdingHours' },
  { selector: 'label:has([name="sparklineDays"]) > span', key: 'settings.sparklineDays' },
  { selector: '#refresh-api-card h3', key: 'settings.refreshApi.title' },
  { selector: '#refresh-api-card p', key: 'settings.refreshApi.desc' },
  { selector: 'label:has([name="refreshApiBaseUrl"]) > span', key: 'settings.refreshApiUrl' },
  { selector: 'input[name="refreshApiBaseUrl"]', attr: 'placeholder', key: 'settings.refreshApiUrlPlaceholder' },
  { selector: 'label:has([name="refreshApiKey"]) > span', key: 'settings.refreshApiKey' },
  { selector: 'input[name="refreshApiKey"]', attr: 'placeholder', key: 'settings.refreshApiKeyPlaceholder' },
  { selector: '#refresh-api-test', key: 'settings.refreshApiTest' },
  { selector: '#ai-api-card h3', key: 'settings.ai.title' },
  { selector: '#ai-api-card p', key: 'settings.ai.desc' },
  { selector: '#ai-filters-card h3', key: 'settings.aiFilters.title' },
  { selector: '#ai-filters-card p', key: 'settings.aiFilters.desc' },
  { selector: '#operator-settings-card > summary > span', key: 'settings.operator.title' },
  { selector: '#operator-settings-card > p', key: 'settings.operator.desc' },
  { selector: 'label:has([name="useExternalAiApi"]) > span', key: 'settings.aiExternalToggle' },
  { selector: 'label:has([name="aiApiBaseUrl"]) > span', key: 'settings.aiApiUrl' },
  { selector: 'input[name="aiApiBaseUrl"]', attr: 'placeholder', key: 'settings.aiApiUrlPlaceholder' },
  { selector: 'label:has([name="aiPredictionHorizon"]) > span', key: 'settings.aiPredictionHorizon' },
  { selector: 'label:has([name="minAiScore"]) > span', key: 'settings.minAiScore' },
  { selector: 'label:has([name="maxAiEntryWaitMinutes"]) > span', key: 'settings.maxAiEntryWaitMinutes' },
  { selector: 'label:has([name="maxAiHoldingHours"]) > span', key: 'settings.maxAiHoldingHours' },
  { selector: '#settings-ai-threshold', key: 'settings.aiThresholdOptimize' },
  { selector: '#ai-apply-threshold', key: 'button.applyAiThreshold' },
  { selector: '#auto-refresh-card h3', key: 'settings.autoRefresh.title' },
  { selector: '#auto-refresh-card p', key: 'settings.autoRefresh.desc' },
  { selector: 'label:has([name="autoRefreshMode"]) > span', key: 'settings.autoRefreshMode' },
  { selector: 'label:has([name="autoRefreshIntervalSeconds"]) > span', key: 'settings.autoRefreshInterval' },
  { selector: 'label:has([name="autoRefreshStrategy"]) > span', key: 'settings.autoRefreshStrategy' },
  { selector: 'label:has([name="autoRefreshFullIntervalSeconds"]) > span', key: 'settings.autoRefreshFullInterval' },
  { selector: '#fees-card h3', key: 'settings.fees.title' },
  { selector: '#fees-card p', key: 'settings.fees.desc' },
  { selector: '#exchanges-card h3', key: 'settings.exchanges.title' },
  { selector: '#exchanges-card p', key: 'settings.exchanges.desc' },
  { selector: '#cross-columns-disclosure > summary > span', key: 'settings.columnsAdvanced.title' },
  { selector: '#cross-columns-disclosure > p', key: 'settings.columnsAdvanced.desc' },
  { selector: '#cross-columns-card h3', key: 'settings.columns.title' },
  { selector: '#cross-columns-card p', key: 'settings.columns.desc' },
  { selector: '.settings-actions .action-button', key: 'button.apply' },
  { selector: '#settings-reset', key: 'button.reset' },
  { selector: '#modal-close', attr: 'aria-label', key: 'modal.close' },
  { selector: '.site-footer a', key: 'footer.deploy' },
  { selector: '.site-footer p', key: 'footer.disclaimer' },
];

function getCurrentLanguage() {
  return currentLanguage === 'ru' ? 'ru' : 'en';
}

function formatMessage(template, params = {}) {
  return String(template).replace(/\{(\w+)\}/g, (_, key) => String(params[key] ?? ''));
}

function t(key, params = {}) {
  const language = getCurrentLanguage();
  const pack = TRANSLATIONS[language] || TRANSLATIONS.en;
  const fallback = TRANSLATIONS.en[key] || key;
  return formatMessage(pack[key] ?? fallback, params);
}

function pickLocalized(en, ru) {
  return getCurrentLanguage() === 'ru' ? ru : en;
}

let _lastAppliedLang = '';
function applyStaticTranslations() {
  const lang = getCurrentLanguage();
  if (lang === _lastAppliedLang) {
    return;
  }
  _lastAppliedLang = lang;
  document.documentElement.lang = lang;
  STATIC_TRANSLATION_BINDINGS.forEach(({ selector, attr, key }) => {
    const node = document.querySelector(selector);
    if (!node) {
      return;
    }
    if (attr) {
      if (attr === 'value' && 'value' in node) {
        node.value = t(key);
      } else {
        node.setAttribute(attr, t(key));
      }
      return;
    }
    node.textContent = t(key);
  });
  populateAutoRefreshSelectOptions();
  syncRefreshControls();
  syncAiInlineControls();
}

function syncAiInlineControls() {
  const horizonLabel = document.querySelector('#ai-horizon-label');
  if (horizonLabel) {
    horizonLabel.textContent = t('settings.aiPredictionHorizon');
  }
  const minScoreLabel = document.querySelector('#ai-min-score-label');
  if (minScoreLabel) {
    minScoreLabel.textContent = t('settings.minAiScore');
  }
  if (dom.aiHorizonSelect) {
    dom.aiHorizonSelect.innerHTML = AI_PREDICTION_HORIZON_OPTIONS
      .map((minutes) => `<option value="${minutes}">${escapeHtml(`${minutes} ${pickLocalized('min', 'мин')}`)}</option>`)
      .join('');
    dom.aiHorizonSelect.value = String(normalizeAiPredictionHorizon(state.settings.aiPredictionHorizon));
  }
  if (dom.aiMinScoreInput) {
    dom.aiMinScoreInput.value = String(Math.max(0, Math.min(100, Number(state.settings.minAiScore || 0))));
  }
}

function clampAutoRefreshIntervalSeconds(value) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return AUTO_REFRESH_DEFAULT_INTERVAL_SECONDS;
  }
  return Math.max(AUTO_REFRESH_MIN_INTERVAL_SECONDS, Math.round(numericValue));
}

function formatAutoRefreshIntervalLabel(seconds) {
  const normalizedSeconds = clampAutoRefreshIntervalSeconds(seconds);
  if (normalizedSeconds < 60) {
    return t('settings.autoRefreshIntervalSeconds', { value: normalizedSeconds });
  }
  return t('settings.autoRefreshIntervalMinutes', { value: normalizedSeconds / 60 });
}

function populateAutoRefreshSelectOptions() {
  const modeSelect = dom.settingsForm?.elements?.autoRefreshMode;
  const intervalSelect = dom.settingsForm?.elements?.autoRefreshIntervalSeconds;
  if (modeSelect) {
    modeSelect.innerHTML = `
      <option value="manual">${escapeHtml(t('settings.autoRefreshModeManual'))}</option>
      <option value="auto">${escapeHtml(t('settings.autoRefreshModeAuto'))}</option>
    `;
  }
  if (intervalSelect) {
    intervalSelect.innerHTML = AUTO_REFRESH_INTERVAL_OPTIONS
      .map((seconds) => `<option value="${seconds}">${escapeHtml(formatAutoRefreshIntervalLabel(seconds))}</option>`)
      .join('');
  }
}

function isAutoRefreshEnabled() {
  return state.settings.autoRefreshMode === 'auto';
}

function getAutoRefreshIntervalSeconds() {
  return clampAutoRefreshIntervalSeconds(state.settings.autoRefreshIntervalSeconds);
}

function getAutoRefreshStrategy() {
  return 'filtered-only';
}

function getAutoRefreshBusyRetryDelayMs() {
  return Math.min(Math.max(AUTO_REFRESH_BUSY_RETRY_MS, Math.round(getAutoRefreshIntervalSeconds() * 250)), 30000);
}

function clearAutoRefreshTimer() {
  if (state.autoRefreshTimer) {
    window.clearTimeout(state.autoRefreshTimer);
    state.autoRefreshTimer = null;
  }
  state.autoRefreshNextAt = null;
}

function updateAutoRefreshButton() {
  if (!dom.autoRefreshToggle) {
    return;
  }
  const autoEnabled = isAutoRefreshEnabled();
  dom.autoRefreshToggle.textContent = autoEnabled ? t('button.autoRefreshAuto') : t('button.autoRefreshManual');
  dom.autoRefreshToggle.setAttribute('aria-label', autoEnabled ? t('button.autoRefreshDisable') : t('button.autoRefreshEnable'));
  dom.autoRefreshToggle.title = autoEnabled
    ? `${t('button.autoRefreshDisable')} · ${formatAutoRefreshIntervalLabel(getAutoRefreshIntervalSeconds())}`
    : t('button.autoRefreshEnable');
  dom.autoRefreshToggle.classList.toggle('is-active', autoEnabled);
}

function getAutoRefreshBadge() {
  if (!isAutoRefreshEnabled()) {
    return { tone: 'warning', text: t('autoRefresh.badgeManual') };
  }
  if (document.visibilityState === 'hidden') {
    return { tone: 'warning', text: t('autoRefresh.badgePaused') };
  }
  return {
    tone: 'positive',
    text: t('autoRefresh.badgeAuto', { value: getAutoRefreshIntervalSeconds() }),
  };
}

function getAutoRefreshStrategyBadge() {
  if (!isAutoRefreshEnabled()) {
    return null;
  }
  return { tone: 'positive', text: t('autoRefresh.badgeFilteredOnly') };
}

function scheduleAutoRefresh(delayOverrideMs = null) {
  clearAutoRefreshTimer();
  if (!isAutoRefreshEnabled() || document.visibilityState === 'hidden') {
    renderDataSourceStatus();
    return;
  }
  const requestedDelayMs = Number(delayOverrideMs);
  const delayMs = Number.isFinite(requestedDelayMs) && requestedDelayMs > 0
    ? Math.max(1000, Math.round(requestedDelayMs))
    : getAutoRefreshIntervalSeconds() * 1000;
  state.autoRefreshNextAt = Date.now() + delayMs;
  state.autoRefreshTimer = window.setTimeout(async () => {
    clearAutoRefreshTimer();
    if (!isAutoRefreshEnabled() || document.visibilityState === 'hidden') {
      scheduleAutoRefresh();
      return;
    }
    if (hasFreshMarketStreamData()) {
      scheduleAutoRefresh();
      return;
    }
    // SWR: skip refresh API call if all datasets have fresh cached data
    const swrAllFresh = Object.keys(DATA_FILES).every((k) => swrIsFresh(k, getAutoRefreshIntervalSeconds() * 1000));
    if (swrAllFresh && hasDatasetValue('cross')) {
      scheduleAutoRefresh();
      return;
    }
    if (state.refreshInFlight || state.refreshState?.status === 'running') {
      scheduleAutoRefresh(getAutoRefreshBusyRetryDelayMs());
      return;
    }
    try {
      await handleRefreshButton('filtered', { trigger: 'auto' });
    } finally {
      scheduleAutoRefresh();
    }
  }, delayMs);
  renderDataSourceStatus();
}

function handleAutoRefreshVisibilityChange() {
  if (document.visibilityState !== 'hidden') {
    connectMarketStream();
    // SWR: revalidate on focus (throttled inside swrHandleVisibilityChange)
    swrHandleVisibilityChange();
    if (state.activeTab === 'cross' || state.activeTab === 'ai' || state.activeDetailId) {
      refreshLiveDataForActiveTab();
      if (state.activeDetailId) {
        rerenderActiveDetail();
      }
    }
    if (state.activeTab === 'funding' || state.activeTab === 'health' || state.activeTab === 'glossary') {
      void ensureDatasetsForTab(state.activeTab, true);
    }
  }
  if (!isAutoRefreshEnabled()) {
    renderDataSourceStatus();
    return;
  }
  scheduleAutoRefresh();
}

function toggleAutoRefreshMode() {
  state.settings.autoRefreshMode = isAutoRefreshEnabled() ? 'manual' : 'auto';
  persistSettings();
  populateSettingsForm(getAllExchanges());
  renderAll();
  scheduleAutoRefresh();
}

function syncRefreshControls() {
  if (!dom.refreshButton) {
    return;
  }

  updateAutoRefreshButton();

  if (hasRefreshApi()) {
    dom.refreshButton.textContent = t('button.refreshFiltered');
    if (dom.refreshFullButton) {
      dom.refreshFullButton.textContent = t('button.refreshFull');
      dom.refreshFullButton.disabled = false;
    }
    if (state.refreshApiProbeError === 'unreachable') {
      dom.refreshButton.title = t('refresh.apiUnreachable');
    } else if (state.refreshApiHealth?.refresh_auth_required && !getRefreshApiKey() && !canUseImplicitRefreshAuth()) {
      dom.refreshButton.title = t('refresh.accessMissing');
    } else if (getRefreshApiBaseUrl()) {
      dom.refreshButton.title = t('refresh.apiReadyDetail');
    } else {
      dom.refreshButton.removeAttribute('title');
    }
    return;
  }

  dom.refreshButton.textContent = t('button.reloadSnapshot');
  dom.refreshButton.title = t('refresh.apiUnavailableDetail');
  if (dom.refreshFullButton) {
    dom.refreshFullButton.textContent = t('button.reloadSnapshot');
    dom.refreshFullButton.disabled = false;
  }
}

function renderBackendDiagnostics(options = {}) {
  if (!dom.refreshApiStatus) {
    return;
  }

  const tone = options.tone || (!hasRefreshApi() ? 'is-warning' : state.refreshApiProbeError === 'unreachable' ? 'is-negative' : 'is-positive');
  const lines = [];

  if (options.message) {
    lines.push(`<div>${escapeHtml(options.message)}</div>`);
  } else if (!hasRefreshApi()) {
    lines.push(`<div>${escapeHtml(t('settings.refreshApiStatusSnapshot'))}</div>`);
  } else if (state.refreshApiProbeError === 'unreachable' || !state.refreshApiHealth) {
    lines.push(`<div>${escapeHtml(t('settings.refreshApiStatusUnreachable'))}</div>`);
  } else {
    const url = getRefreshApiBaseUrl() || window.location.origin;
    const access = state.refreshApiHealth.refresh_auth_required
      ? (getRefreshApiKey() || canUseImplicitRefreshAuth())
        ? t('settings.refreshApiStatusProtected')
        : t('settings.refreshApiStatusKeyMissing')
      : t('settings.refreshApiStatusOpen');
    const refreshState = state.refreshApiStatus?.status ? t(`status.${state.refreshApiStatus.status}`) : t('settings.refreshApiStatusUnknown');
    const cleanup = state.refreshApiHealth.startup_cleanup;
    const autoCleanup = state.refreshApiHealth.auto_history_cleanup;
    const resultCache = state.refreshApiHealth.refresh_result_cache;
    const activeAlerts = state.refreshApiHealth.active_alerts;
    const adaptivePublication = state.refreshApiHealth.adaptive_publication;
    const quoteAge = state.refreshApiHealth.quote_age;
    const cleanupText = cleanup && typeof cleanup === 'object'
      ? t('settings.refreshApiStatusCleanupSummary', {
        history: Number(cleanup.history_snapshots || 0),
        removed: Number(cleanup.removed_history_snapshots || 0),
        staleData: Number(cleanup.cleared_stale_primary_files || 0),
        reports: Number(cleanup.removed_reports || 0),
        tmpFiles: Number(cleanup.removed_tmp_files || 0),
      })
      : t('settings.refreshApiStatusUnknown');
    const autoCleanupText = autoCleanup && typeof autoCleanup === 'object'
      ? [
        t('settings.refreshApiStatusAutoCleanupLastRun', {
          value: autoCleanup.last_run_at ? formatDateTime(autoCleanup.last_run_at) : t('common.never'),
        }),
        t('settings.refreshApiStatusAutoCleanupRemoved', {
          value: Number(autoCleanup.last_summary?.removed_history_snapshots || 0),
        }),
        t('settings.refreshApiStatusAutoCleanupSkipped', {
          value: Number(autoCleanup.skipped_while_refresh_running || 0),
        }),
      ].join(' · ')
      : t('settings.refreshApiStatusUnknown');

    lines.push(`<div><strong>${escapeHtml(t('settings.refreshApiStatusUrl'))}</strong>: ${escapeHtml(url)}</div>`);
    lines.push(`<div><strong>${escapeHtml(t('settings.refreshApiStatusAccess'))}</strong>: ${escapeHtml(access)}</div>`);
    lines.push(`<div><strong>${escapeHtml(t('settings.refreshApiStatusState'))}</strong>: ${escapeHtml(refreshState)}</div>`);
    if (state.refreshApiHealth.server_version) {
      lines.push(`<div><strong>${escapeHtml(t('settings.refreshApiStatusVersion'))}</strong>: ${escapeHtml(String(state.refreshApiHealth.server_version))}</div>`);
    }
    if (state.refreshApiHealth.started_at) {
      lines.push(`<div><strong>${escapeHtml(t('settings.refreshApiStatusStarted'))}</strong>: ${escapeHtml(formatDateTime(state.refreshApiHealth.started_at))}</div>`);
    }
    lines.push(`<div><strong>${escapeHtml(t('settings.refreshApiStatusCleanup'))}</strong>: ${escapeHtml(cleanupText)}</div>`);
    if (autoCleanup && typeof autoCleanup === 'object') {
      lines.push(`<div><strong>${escapeHtml(t('settings.refreshApiStatusAutoCleanup'))}</strong>: ${escapeHtml(autoCleanupText)}</div>`);
    }
    if (resultCache && typeof resultCache === 'object') {
      lines.push(`<div><strong>${escapeHtml(t('settings.refreshApiStatusResultCache'))}</strong>: ${escapeHtml(t('settings.refreshApiStatusResultCacheStats', {
        hits: Number(resultCache.hits || 0),
        misses: Number(resultCache.misses || 0),
        stores: Number(resultCache.stores || 0),
        active: Number(resultCache.active_results || 0),
      }))}</div>`);
    }
    if (activeAlerts && typeof activeAlerts === 'object') {
      lines.push(`<div><strong>${escapeHtml(t('hero.systemAlerts'))}</strong>: ${escapeHtml(t('settings.refreshApiStatusAlerts', {
        active: Number(activeAlerts.active_count || 0),
        critical: Number(activeAlerts.critical_count || 0),
        warning: Number(activeAlerts.warning_count || 0),
      }))}</div>`);
    }
    if (adaptivePublication && typeof adaptivePublication === 'object' && adaptivePublication.mode) {
      lines.push(`<div><strong>${escapeHtml(t('hero.publicationProfile'))}</strong>: ${escapeHtml(`${translatePublicationMode(adaptivePublication.mode)} · ${t('settings.refreshApiStatusAdaptivePublication', {
        spread: formatNumber(Number(adaptivePublication.min_net_spread || 0), 2),
        volume: formatCompact(Number(adaptivePublication.min_volume || 0)),
        bidAsk: formatNumber(Number(adaptivePublication.max_bid_ask || 0), 2),
      })}`)}</div>`);
    }
    if (quoteAge && typeof quoteAge === 'object' && quoteAge.available) {
      lines.push(`<div><strong>${escapeHtml(t('health.summaryQuoteAge'))}</strong>: ${escapeHtml(t('settings.refreshApiStatusQuoteAge', {
        avg: formatNumber(Number(quoteAge.avg_seconds || 0), 0),
        p95: formatNumber(Number(quoteAge.p95_seconds || 0), 0),
        stale: formatNumber(Number(quoteAge.stale_share || 0) * 100, 0),
      }))}</div>`);
    }
  }

  dom.refreshApiStatus.className = `backend-diagnostics ${tone}`;
  dom.refreshApiStatus.innerHTML = lines.join('');
}

async function handleRefreshApiTest() {
  if (!dom.refreshApiTest) {
    return;
  }

  const initialLabel = dom.refreshApiTest.textContent;
  dom.refreshApiTest.disabled = true;
  renderBackendDiagnostics({ message: t('settings.refreshApiStatusChecking'), tone: 'is-warning' });

  try {
    await syncRefreshApiProbe();
    state.refreshApiAvailable = hasRefreshApi();
    state.refreshApiStatus = hasRefreshApi() ? await fetchRefreshApiStatus(true) : null;
    renderBackendDiagnostics();
    renderDataSourceStatus();
    syncRefreshControls();
  } finally {
    dom.refreshApiTest.disabled = false;
    dom.refreshApiTest.textContent = initialLabel || t('settings.refreshApiTest');
  }
}

function getCrossColumnDefs() {
  return CROSS_COLUMN_SCHEMA.map((column) => ({
    key: column.key,
    header: t(`cross.header.${column.key}`),
    label: t(`cross.label.${column.key}`),
    hint: t(`cross.hint.${column.key}`),
  }));
}

function getTableHeaderHints() {
  return {
    funding: {
      exchangeA: t('funding.hint.exchangeA'),
      exchangeB: t('funding.hint.exchangeB'),
      pair: t('funding.hint.pair'),
      fundingA: t('funding.hint.fundingA'),
      fundingB: t('funding.hint.fundingB'),
      difference_abs: t('funding.hint.difference_abs'),
      difference_rel: t('funding.hint.difference_rel'),
      volumeA: t('funding.hint.volumeA'),
      volumeB: t('funding.hint.volumeB'),
      expectedFundingCarryPercent: t('funding.hint.expectedFundingCarryPercent'),
      openingFeePercent: t('funding.hint.openingFeePercent'),
      latencyReservePercent: t('funding.hint.latencyReservePercent'),
      expectedNetCarryPercent: t('funding.hint.expectedNetCarryPercent'),
      annualizedFundingEdge: t('funding.hint.annualizedFundingEdge'),
      recommended_action: t('funding.hint.recommended_action'),
    },
    health: {
      exchange: t('health.hint.exchange'),
      status: t('health.hint.status'),
      lastSuccess: t('health.hint.lastSuccess'),
      coverage: t('health.hint.coverage'),
      apiAlert: t('health.hint.apiAlert'),
      excluded: t('health.hint.excluded'),
    },
  };
}

function translateLifecycle(lifecycle) {
  return t(`status.${lifecycle || 'unknown'}`);
}

function translateRegime(regime) {
  return t(`regime.${regime || 'forming'}`);
}

function translateHealthStatus(status) {
  return t(`status.${status || 'unknown'}`);
}

function translateVolatilityRegime(regime) {
  return t(`volatility.${regime || 'mixed'}`);
}

function translateRiskBackdrop(backdrop) {
  return t(`backdrop.${backdrop || 'watch'}`);
}

function translatePublicationMode(mode) {
  return t(`publication.${mode || 'open'}`);
}

function translateAlertSeverity(severity) {
  return t(`alertSeverity.${severity || 'info'}`);
}

function alertSummaryTone(summary) {
  const severity = String(summary?.highest_severity || 'info');
  if (severity === 'critical') {
    return 'negative';
  }
  if (severity === 'warning') {
    return 'warning';
  }
  return 'positive';
}

function getSystemAlertSummary() {
  const runtimeSummary = state.refreshApiHealth?.active_alerts;
  if (state.dataOrigin === 'ephemeral-filtered' && runtimeSummary && typeof runtimeSummary === 'object' && !Array.isArray(runtimeSummary)) {
    return runtimeSummary;
  }
  const metricsSummary = state.metrics?.alert_summary;
  if (metricsSummary && typeof metricsSummary === 'object' && !Array.isArray(metricsSummary)) {
    return metricsSummary;
  }
  if (runtimeSummary && typeof runtimeSummary === 'object' && !Array.isArray(runtimeSummary)) {
    return runtimeSummary;
  }
  return {};
}

function getSystemAlertsList() {
  const runtimeAlerts = state.refreshApiHealth?.alerts;
  if (state.dataOrigin === 'ephemeral-filtered' && Array.isArray(runtimeAlerts)) {
    return runtimeAlerts;
  }
  if (Array.isArray(state.metrics?.alerts)) {
    return state.metrics.alerts;
  }
  return Array.isArray(runtimeAlerts) ? runtimeAlerts : [];
}

function formatSystemAlertLine(alert) {
  const severity = translateAlertSeverity(alert?.severity || 'info');
  const title = String(alert?.title || alert?.message || '').trim();
  const detail = String(alert?.detail || alert?.reason || '').trim();
  const body = title && detail ? `${title}: ${detail}` : title || detail || pickLocalized('backend alert', 'backend alert');
  return `${severity}: ${body}`;
}

function buildTooltipAriaLabel(label, hint) {
  return hint ? `${label}. ${hint}` : label;
}

function renderTooltipButtonContent(label, hint) {
  return `<span class="tooltip-trigger__text">${escapeHtml(label)}</span>`;
}

function renderMetricLabel(label, hint) {
  if (!hint) {
    return `<span class="detail-metric-label">${escapeHtml(label)}</span>`;
  }
  return `
    <span
      class="detail-metric-label tooltip-trigger"
      tabindex="0"
      data-tooltip="${escapeHtml(hint)}"
      aria-label="${escapeHtml(buildTooltipAriaLabel(label, hint))}"
    >
      <span class="detail-metric-label__text">${escapeHtml(label)}</span>
    </span>
  `;
}

function getDetailMetricItems(item) {
  const tradeAlertModel = getTradeAlertModel(item);
  return [
    {
      label: t('detail.rankingScore'),
      hint: pickLocalized(
        'Composite score that blends edge, carry, liquidity, stability and execution quality. Use it to prioritize which setup deserves attention first, not as a substitute for checking net spread and venue health.',
        'Сводный балл, который объединяет запас ценового преимущества, экономику удержания позиции, ликвидность, стабильность и качество исполнения. Используйте его, чтобы быстро понять, какую идею проверять первой, но не вместо анализа спреда после комиссий и состояния бирж.',
      ),
      value: `${item.score}/100`,
    },
    {
      label: pickLocalized('Signal class', 'Класс сигнала'),
      hint: pickLocalized(
        'Short label that helps separate stronger setups from weaker ones inside the same table.',
        'Короткая метка, которая помогает отделять более сильные сигналы от более слабых внутри одной таблицы.',
      ),
      value: item.reliabilityClass,
    },
    { label: t('detail.lifecycle'), hint: t('cross.hint.lifecycle'), value: translateLifecycle(item.lifecycle) },
    { label: t('detail.spreadRegime'), hint: t('cross.hint.regime'), value: item.regimeLabel },
    { label: t('detail.historicalZ'), hint: t('cross.hint.zScore'), value: formatNumber(item.zScore, 2) },
    { label: t('detail.currentPercentile'), hint: t('cross.hint.percentile'), value: `${formatNumber(item.percentile, 0)}%` },
    {
      label: t('detail.anomalyScore'),
      hint: pickLocalized(
        'Composite anomaly reading built from statistical stretch and multi-window confirmation. Use it to spot setups that are unusual in more than one way, not just temporarily noisy.',
        'Композитная оценка аномалии, которая учитывает и статистическую экстремальность, и подтверждение на нескольких горизонтах. Помогает находить не просто шум, а действительно необычные деформации рынка.',
      ),
      value: `${item.anomalyScore}/100`,
    },
    { label: t('detail.reversionProbability'), hint: t('cross.hint.reversionProbability'), value: `${formatNumber(item.reversionProbability * 100, 0)}%` },
    {
      label: t('detail.spreadQuality'),
      hint: pickLocalized(
        'Quality of the raw and net price edge itself. Higher values usually mean the dislocation is large enough to survive friction and still matter after costs.',
        'Качество самого ценового преимущества. Чем выше показатель, тем больше шанс, что расхождение достаточно велико и сохранит смысл даже после комиссий и прочих издержек.',
      ),
      value: `${Math.round(item.spreadQuality)}/100`,
    },
    {
      label: t('detail.fundingQuality'),
      hint: pickLocalized(
        'How supportive the funding profile is for holding the setup. High values mean carry is helping or at least not seriously damaging the idea while you wait for convergence.',
        'Насколько профиль ставок финансирования поддерживает удержание позиции. Высокие значения означают, что удержание помогает сделке или хотя бы не разрушает её экономику, пока вы ждёте сужения.',
      ),
      value: `${Math.round(item.fundingQuality)}/100`,
    },
    {
      label: pickLocalized('Funding carry over hold', 'Funding carry на удержании'),
      hint: pickLocalized(
        'Expected funding contribution over the active holding horizon. Positive values mean time works in your favor while you wait for convergence.',
        'Ожидаемый вклад funding на активном горизонте удержания. Положительное значение означает, что время помогает сделке, пока вы ждёте сужения.',
      ),
      value: `${formatNumber(item.expectedFundingCarryPercent, 4)}%`,
    },
    {
      label: pickLocalized('Opening fee burden', 'Комиссия на открытие'),
      hint: pickLocalized(
        'Combined entry fee estimate for both futures legs under the current browser-side fee assumptions.',
        'Совокупная оценка комиссии за вход по обеим фьючерсным ногам при текущих браузерных допущениях по fees.',
      ),
      value: `${formatNumber(item.openingFeePercent, 4)}%`,
    },
    {
      label: pickLocalized('Latency reserve', 'Резерв на задержку'),
      hint: pickLocalized(
        'Safety reserve for quote drift and hedge lag between venues. This buffer keeps the displayed edge conservative.',
        'Защитный резерв под сдвиг котировок и лаг хеджа между площадками. Этот буфер делает показанное преимущество более консервативным.',
      ),
      value: `${formatNumber(item.latencyReservePercent, 4)}%`,
    },
    {
      label: pickLocalized('Annualized net edge', 'Годовой чистый edge'),
      hint: pickLocalized(
        'Net edge scaled by the active holding horizon. Useful when comparing routes that converge at different speeds.',
        'Чистое преимущество, пересчитанное через активный горизонт удержания. Полезно, когда вы сравниваете маршруты с разной скоростью схождения.',
      ),
      value: `${formatNumber(item.annualizedNetEdge, 1)}%`,
    },
    {
      label: t('detail.liquidityQuality'),
      hint: pickLocalized(
        'Estimate of how tradable the setup is based on volumes and market quality. Use it to filter out ideas that look attractive mathematically but are too thin operationally.',
        'Оценка торговой пригодности идеи по объёму и качеству рынка. Нужна, чтобы отсекать связки, которые красивы математически, но слишком тонкие для нормального исполнения.',
      ),
      value: `${Math.round(item.liquidityQuality)}/100`,
    },
    {
      label: t('detail.stability'),
      hint: pickLocalized(
        'How orderly the spread has behaved historically. Stable behavior usually makes timing, sizing and risk control easier than a setup that constantly flips character.',
        'Насколько упорядоченно спред вёл себя исторически. Стабильное поведение обычно упрощает тайминг, размер позиции и контроль риска по сравнению с режимом, который постоянно меняет характер.',
      ),
      value: `${Math.round(item.stabilityQuality)}/100`,
    },
    {
      label: t('detail.executionDifficulty'),
      hint: pickLocalized(
        'Operational difficulty of opening and closing both legs cleanly. Use it as a friction warning: a beautiful signal with hard execution can still be a poor trade.',
        'Операционная сложность входа и выхода по обеим ногам. Смотрите на показатель как на предупреждение о трении: красивая идея с тяжёлым исполнением всё равно может быть плохой сделкой.',
      ),
      value: `${Math.round(item.executionDifficulty)}/100`,
    },
    {
      label: t('detail.alignment'),
      hint: pickLocalized(
        'Agreement between shorter and longer lookback windows. Higher alignment means the anomaly is being confirmed across horizons instead of appearing only in one noisy slice.',
        'Насколько короткие и длинные окна подтверждают друг друга. Чем выше согласованность, тем меньше шанс, что вы видите локальный шум только на одном таймфрейме.',
      ),
      value: `${formatNumber(item.timeframeAlignment, 0)}/100`,
    },
    {
      label: t('detail.meanReversionEta'),
      hint: pickLocalized(
        'Estimated time the setup may need to move back toward normal. Use it for holding-period planning, not as a precise timer, because market structure can change after entry.',
        'Оценка времени, которое может понадобиться для возврата к норме. Полезна для планирования горизонта удержания, но не как точный таймер: структура рынка после входа может измениться.',
      ),
      value: item.meanReversionHours ? `${formatNumber(item.meanReversionHours, 1)} h` : t('detail.notEnoughSignal'),
    },
    {
      label: t('detail.tradeAlert'),
      hint: t('cross.hint.tradeAlert'),
      value: tradeAlertModel.title,
    },
    {
      label: t('detail.positionState'),
      hint: t('cross.hint.positionState'),
      value: tradeAlertModel.positionLabel,
    },
    {
      label: t('detail.holdingTimer'),
      hint: pickLocalized(
        'Shows either the remaining hold time for a tracked position or the current entry/hold timing plan when no position is marked as open.',
        'Показывает либо оставшееся время удержания для отмеченной позиции, либо текущий план по входу и удержанию, если позиция ещё не открыта.',
      ),
      value: tradeAlertModel.timerLabel,
    },
    {
      label: t('detail.entryZone'),
      hint: pickLocalized(
        'Suggested area where the setup still offers acceptable asymmetry. Use it as a discipline tool so you do not chase a move that has already spent most of its edge.',
        'Рекомендуемая зона, где у идеи ещё остаётся приемлемая асимметрия. Используйте её как дисциплинарный ориентир, чтобы не догонять движение, в котором запас преимущества уже почти израсходован.',
      ),
      value: item.execution.entryZone,
    },
    {
      label: t('detail.exitZone'),
      hint: pickLocalized(
        'Suggested normalization zone for taking profits or reducing exposure. Read it together with net spread and lifecycle to avoid waiting for the perfect exit after the signal has already normalized.',
        'Рекомендуемая зона нормализации для фиксации прибыли или снижения позиции. Смотрите её вместе с чистым спредом и стадией сигнала, чтобы не пересиживать сделку в ожидании идеального выхода после того, как сигнал уже отработал.',
      ),
      value: item.execution.exitZone,
    },
  ];
}

let CROSS_COLUMN_DEFS = getCrossColumnDefs();
let TABLE_HEADER_HINTS = getTableHeaderHints();

function refreshLocalizationCaches() {
  CROSS_COLUMN_DEFS = getCrossColumnDefs();
  TABLE_HEADER_HINTS = getTableHeaderHints();
}

const CROSS_COLUMN_PRESETS = {
  all: CROSS_COLUMN_DEFS.map((column) => column.key),
  compact: ['exchangeA', 'pairA', 'exchangeB', 'direction', 'score', 'tradeAlert', 'lifecycle', 'netSpread', 'profit'],
  profit: ['exchangeA', 'pairA', 'exchangeB', 'direction', 'score', 'aiScore', 'netSpread', 'profit', 'roi', 'entryWaitMinutes', 'tradeAlert'],
  carry: ['exchangeA', 'pairA', 'exchangeB', 'direction', 'score', 'lifecycle', 'fundingA', 'fundingB', 'expectedFundingCarryPercent', 'openingFeePercent', 'latencyReservePercent', 'netSpread', 'annualizedNetEdge'],
  liquidity: ['exchangeA', 'pairA', 'exchangeB', 'direction', 'score', 'regime', 'volumeA', 'volumeB', 'spreadA', 'spreadB', 'netSpread'],
};

const LEGACY_COMPACT_CROSS_COLUMNS = ['exchangeA', 'pairA', 'exchangeB', 'direction', 'score', 'regime', 'netSpread', 'roi'];
const CROSS_COLUMNS_SCHEMA_VERSION = 3;

const DEFAULT_CROSS_COLUMN_VISIBILITY = Object.fromEntries(
  CROSS_COLUMN_DEFS.map((column) => [column.key, CROSS_COLUMN_PRESETS.compact.includes(column.key)]),
);

const DEFAULT_FEE = 0.04;
const DEFAULT_LANGUAGE = detectDefaultLanguage();
const DEFAULT_SETTINGS = {
  theme: 'dark',
  language: DEFAULT_LANGUAGE,
  minSpread: 0.1,
  leverage: 5,
  deposit: 1000,
  holdingHours: 0,
  sparklineDays: 7,
  search: '',
  autoRefreshMode: 'manual',
  autoRefreshIntervalSeconds: AUTO_REFRESH_DEFAULT_INTERVAL_SECONDS,
  autoRefreshStrategy: AUTO_REFRESH_DEFAULT_STRATEGY,
  autoRefreshFullIntervalSeconds: AUTO_REFRESH_DEFAULT_FULL_INTERVAL_SECONDS,
  refreshApiBaseUrl: '',
  refreshApiKey: '',
  useExternalAiApi: false,
  aiApiBaseUrl: '',
  aiPredictionHorizon: AI_DEFAULT_PREDICTION_HORIZON,
  minAiScore: 0,
  crossColumnsSchemaVersion: CROSS_COLUMNS_SCHEMA_VERSION,
  maxAiEntryWaitMinutes: 0,
  maxAiHoldingHours: 0,
  fees: {},
  visibleExchanges: {},
  visibleCrossColumns: { ...DEFAULT_CROSS_COLUMN_VISIBILITY },
};

const state = {
  cross: [],
  funding: [],
  metrics: null,
  adapterHealth: null,
  glossary: [],
  tradeTracker: loadTradeTrackerState(),
  datasetStatus: {
    cross: { loaded: false, loading: false, refreshing: false, error: '' },
    funding: { loaded: false, loading: false, refreshing: false, error: '' },
    metrics: { loaded: false, loading: false, refreshing: false, error: '' },
    adapterHealth: { loaded: false, loading: false, refreshing: false, error: '' },
    glossary: { loaded: false, loading: false, refreshing: false, error: '' },
  },
  datasetPromises: {},
  historyIndex: [],
  historyDownsampledIndex: [],
  historyIndexLoaded: false,
  historyCache: new Map(),
  crossHistoryAnalytics: new Map(),
  crossHistoryPendingIds: new Set(),
  activeTab: 'cross',
  settings: loadSettings(),
  runtimeConfig: { ...DEFAULT_RUNTIME_CONFIG },
  sorts: {
    cross: { key: 'score', direction: 'desc' },
    funding: { key: 'expectedNetCarryPercent', direction: 'desc' },
    heatmap: { key: 'anomalyScore', direction: 'desc' },
    health: { key: 'statusRank', direction: 'desc' },
  },
  tablePages: {
    cross: 1,
    funding: 1,
    health: 1,
  },
  sparklineLimit: 120,
  searchRenderTimer: null,
  refreshInFlight: false,
  refreshUiTimer: null,
  refreshUiStartedAt: 0,
  refreshApiAvailable: false,
  detectedRefreshApiBaseUrl: '',
  refreshApiHealth: null,
  refreshApiProbeError: '',
  lastRefreshAt: null,
  refreshJobId: null,
  refreshState: null,
  refreshApiStatus: null,
  dataOrigin: 'persistent',
  activeDetailId: null,
  aiById: new Map(),
  aiPendingIds: new Set(),
  aiAnalyticsItemCache: new Map(),
  aiAnalyticsBundleCache: { signature: '', value: null },
  aiFocusedOpportunityId: null,
  glossarySearch: '',
  glossaryCategory: 'all',
  crossOpportunityItemCache: new Map(),
  crossOpportunityRevision: 0,
  filteredCrossCache: { signature: '', value: [] },
  sortedCrossCache: { signature: '', value: [] },
  aiDeferredRenderTimer: null,
  aiDeferredRenderPending: false,
  tradeAlertTicker: null,
  detailRenderToken: 0,
  detailOverviewFrameId: 0,
  detailNotesFrameId: 0,
  virtualization: {
    cross: { scrollTop: 0, frameId: 0, boundShell: null },
    funding: { scrollTop: 0, frameId: 0, boundShell: null },
  },
  autoRefreshTimer: null,
  autoRefreshNextAt: null,
  lastAutoFullRefreshAt: null,
  datasetCacheWriteTimer: null,
  liveDataRenderTimer: null,
  marketStreamSocket: null,
  marketStreamConnected: false,
  marketStreamAvailable: false,
  marketStreamMode: 'fallback',
  marketStreamConnectAttempt: 0,
  marketStreamReconnectTimer: null,
  marketStreamLastMessageAt: null,
  sseEventSource: null,
  sseConnected: false,
  sseReconnectAttempt: 0,
  sseReconnectTimer: null,
  liveApiVersion: 0,
  swrCache: {},
  swrInFlight: {},
  swrLastFocusRevalidateAt: 0,
  swrLastReconnectRevalidateAt: 0,
};

state.refreshApiAvailable = hasRefreshApi();

currentLanguage = state.settings.language === 'ru' ? 'ru' : DEFAULT_LANGUAGE;

const dom = {};
let activeTooltipTarget = null;

document.addEventListener('DOMContentLoaded', async () => {
  cacheDom();
  refreshLocalizationCaches();
  applyStaticTranslations();
  bindEvents();
  syncTheme();
  swrInit();
  const restoredWarmCache = restorePersistedDatasetCache();
  updateDatasetStatus('cross', {
    loaded: restoredWarmCache && hasDatasetValue('cross'),
    loading: !restoredWarmCache,
    refreshing: false,
    error: '',
  });
  updateDatasetStatus('metrics', {
    loaded: restoredWarmCache && hasDatasetValue('metrics'),
    loading: !restoredWarmCache,
    refreshing: false,
    error: '',
  });
  renderAll();
  await loadRuntimeConfig();
  await syncRefreshApiProbe();
  state.refreshApiAvailable = hasRefreshApi();
  connectMarketStream();
  connectSseStream();
  const receivedLiveSnapshot = await waitForMarketStreamSnapshot(MARKET_STREAM_STARTUP_WAIT_MS, Date.now());
  if (!receivedLiveSnapshot) {
    // Try SSE live API first (instant, no disk I/O)
    const sseLoaded = await trySseLiveApiStartup();
    if (!sseLoaded) {
      await loadAllData();
    }
  }
  void ensureDatasetsForTab(state.activeTab);
  if (shouldStartRefreshOnStartup()) {
    dom.lastUpdated.textContent = t('refresh.running');
    void handleRefreshButton('filtered', { trigger: 'startup' });
  } else {
    scheduleAutoRefresh();
  }
  scheduleLiveDataRender();
  setupPwa();
});

async function loadRuntimeConfig() {
  const requestUrl = `${RUNTIME_CONFIG_PATH}?t=${Date.now()}`;
  try {
    const response = await fetch(requestUrl, { cache: 'no-store' });
    if (!response.ok) {
      state.runtimeConfig = { ...DEFAULT_RUNTIME_CONFIG };
      return;
    }
    const payload = await response.json().catch(() => null);
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      state.runtimeConfig = { ...DEFAULT_RUNTIME_CONFIG };
      return;
    }
    state.runtimeConfig = {
      ...DEFAULT_RUNTIME_CONFIG,
      refreshApiBaseUrl: normalizeRefreshApiBaseUrl(payload.refreshApiBaseUrl),
      aiApiBaseUrl: normalizeRefreshApiBaseUrl(payload.aiApiBaseUrl),
    };
  } catch {
    state.runtimeConfig = { ...DEFAULT_RUNTIME_CONFIG };
  }
}

function cacheDom() {
  dom.lastUpdated = document.querySelector('#last-updated');
  dom.refreshButton = document.querySelector('#refresh-button');
  dom.refreshFullButton = document.querySelector('#refresh-full-button');
  dom.autoRefreshToggle = document.querySelector('#auto-refresh-toggle');
  dom.languageToggle = document.querySelector('#language-toggle');
  dom.refreshStatus = document.querySelector('#refresh-status');
  dom.refreshMeta = document.querySelector('#refresh-meta');
  dom.dataSourceStatus = document.querySelector('#data-source-status');
  dom.refreshDetail = document.querySelector('#refresh-detail');
  dom.qualityBadges = document.querySelector('#quality-badges');
  dom.heroRiskSummary = document.querySelector('#hero-risk-summary');
  dom.tradeSmartNotices = document.querySelector('#trade-smart-notices');
  dom.themeToggle = document.querySelector('#theme-toggle');
  dom.settingsToggle = document.querySelector('#settings-toggle');
  dom.settingsClose = document.querySelector('#settings-close');
  dom.settingsDrawer = document.querySelector('#settings-drawer');
  dom.settingsForm = document.querySelector('#settings-form');
  dom.settingsReset = document.querySelector('#settings-reset');
  dom.refreshApiTest = document.querySelector('#refresh-api-test');
  dom.refreshApiStatus = document.querySelector('#refresh-api-status');
  dom.tabs = [...document.querySelectorAll('.tab')];
  dom.panels = [...document.querySelectorAll('.tab-panel')];
  dom.crossSearch = document.querySelector('#cross-search');
  dom.crossTable = document.querySelector('#cross-table');
  dom.crossMobileList = document.querySelector('#cross-mobile-list');
  dom.fundingTable = document.querySelector('#funding-table');
  dom.fundingMobileList = document.querySelector('#funding-mobile-list');
  dom.aiSummary = document.querySelector('#ai-summary');
  dom.aiAnalyticsList = document.querySelector('#ai-analytics-list');
  dom.aiEmpty = document.querySelector('#ai-empty');
  dom.glossarySearch = document.querySelector('#glossary-search');
  dom.glossaryCategory = document.querySelector('#glossary-category');
  dom.glossaryList = document.querySelector('#glossary-list');
  dom.glossaryEmpty = document.querySelector('#glossary-empty');
  dom.aiPlaybook = document.querySelector('#ai-playbook');
  dom.aiFocusBanner = document.querySelector('#ai-focus-banner');
  dom.aiApplyThreshold = document.querySelector('#ai-apply-threshold');
  dom.aiThresholdNote = document.querySelector('#ai-threshold-note');
  dom.aiHorizonSelect = document.querySelector('#ai-horizon-select');
  dom.aiMinScoreInput = document.querySelector('#ai-min-score-input');
  dom.settingsAiThreshold = document.querySelector('#settings-ai-threshold');
  dom.settingsAiThresholdNote = document.querySelector('#settings-ai-threshold-note');
  dom.heatmapSummary = document.querySelector('#heatmap-summary');
  dom.heatmapGrid = document.querySelector('#heatmap-grid');
  dom.crossEmpty = document.querySelector('#cross-empty');
  dom.fundingEmpty = document.querySelector('#funding-empty');
  dom.heatmapEmpty = document.querySelector('#heatmap-empty');
  dom.statCross = document.querySelector('#stat-cross');
  dom.statFunding = document.querySelector('#stat-funding');
  dom.statExchanges = document.querySelector('#stat-exchanges');
  dom.statHealth = document.querySelector('#stat-health');
  dom.statQuarantined = document.querySelector('#stat-quarantined');
  dom.healthTable = document.querySelector('#health-table');
  dom.healthMobileList = document.querySelector('#health-mobile-list');
  dom.healthSummary = document.querySelector('#health-summary');
  dom.healthDiagnostics = document.querySelector('#health-diagnostics');
  dom.healthEmpty = document.querySelector('#health-empty');
  dom.feeGrid = document.querySelector('#fee-grid');
  dom.exchangeCheckboxes = document.querySelector('#exchange-checkboxes');
  dom.crossColumnCheckboxes = document.querySelector('#cross-column-checkboxes');
  dom.crossColumnPresets = document.querySelector('#cross-column-presets');
  dom.exportButtons = [...document.querySelectorAll('[data-export-scope]')];
  dom.modal = document.querySelector('#modal');
  dom.modalTitle = document.querySelector('#modal-title');
  dom.modalSubtitle = document.querySelector('#modal-subtitle');
  dom.detailOverview = document.querySelector('#detail-overview');
  dom.detailNotes = document.querySelector('#detail-notes');
  dom.modalClose = document.querySelector('#modal-close');
  dom.floatingTooltip = document.createElement('div');
  dom.floatingTooltip.className = 'floating-tooltip hidden';
  dom.floatingTooltip.setAttribute('role', 'tooltip');
  document.body.appendChild(dom.floatingTooltip);
}

function bindEvents() {
  dom.refreshButton.addEventListener('click', () => handleRefreshButton('filtered'));
  dom.refreshFullButton?.addEventListener('click', () => handleRefreshButton('full'));
  dom.autoRefreshToggle?.addEventListener('click', () => toggleAutoRefreshMode());
  dom.settingsForm?.elements?.useExternalAiApi?.addEventListener('change', () => {
    syncAiApiSettingsVisibility();
  });
  dom.settingsForm?.elements?.autoRefreshMode?.addEventListener('change', (event) => {
    if (!dom.settingsForm?.elements?.autoRefreshIntervalSeconds) {
      return;
    }
    const autoEnabled = event.target.value === 'auto';
    dom.settingsForm.elements.autoRefreshIntervalSeconds.disabled = !autoEnabled;
  });
  dom.languageToggle?.addEventListener('click', () => {
    state.settings.language = getCurrentLanguage() === 'en' ? 'ru' : 'en';
    currentLanguage = state.settings.language;
    invalidateCrossOpportunityCaches();
    persistSettings();
    refreshLocalizationCaches();
    applyStaticTranslations();
    populateSettingsForm(getAllExchanges());
    renderAll();
    rerenderActiveDetail();
  });
  dom.themeToggle.addEventListener('click', () => {
    state.settings.theme = state.settings.theme === 'dark' ? 'light' : 'dark';
    persistSettings();
    syncTheme();
  });
  dom.settingsToggle.addEventListener('click', openSettings);
  dom.settingsClose.addEventListener('click', closeSettings);
  dom.settingsReset.addEventListener('click', resetSettings);
  dom.settingsForm.addEventListener('submit', handleSettingsSubmit);
  dom.refreshApiTest?.addEventListener('click', handleRefreshApiTest);
  dom.aiApplyThreshold?.addEventListener('click', applyAiSuggestedThreshold);
  dom.settingsAiThreshold?.addEventListener('click', applyAiSuggestedThreshold);
  dom.aiHorizonSelect?.addEventListener('change', () => {
    state.settings.aiPredictionHorizon = normalizeAiPredictionHorizon(dom.aiHorizonSelect.value);
    persistSettings();
    invalidateAiAnalyticsCaches();
    invalidateCrossOpportunityCaches();
    renderAll();
  });
  dom.aiMinScoreInput?.addEventListener('input', () => {
    state.settings.minAiScore = Math.max(0, Math.min(100, Number(dom.aiMinScoreInput.value) || 0));
    persistSettings();
    invalidateAiAnalyticsCaches();
    renderAll();
  });
  dom.glossarySearch?.addEventListener('input', () => {
    state.glossarySearch = String(dom.glossarySearch.value || '').trim();
    renderCurrentTab();
  });
  dom.glossaryCategory?.addEventListener('change', () => {
    state.glossaryCategory = String(dom.glossaryCategory.value || 'all');
    renderCurrentTab();
  });
  dom.crossSearch.addEventListener('input', (event) => {
    state.settings.search = event.target.value.trim();
    persistSettings();
    scheduleRender();
  });
  dom.tabs.forEach((button) => {
    button.addEventListener('click', () => switchTab(button.dataset.tab));
  });
  document.querySelectorAll('[data-open-tab]').forEach((button) => {
    button.addEventListener('click', () => switchTab(button.dataset.openTab));
  });
  dom.exportButtons.forEach((button) => {
    button.addEventListener('click', () => handleExport(button.dataset.exportScope, button.dataset.exportFormat));
  });
  dom.modalClose.addEventListener('click', closeModal);
  dom.modal.addEventListener('click', (event) => {
    if (event.target === dom.modal) {
      closeModal();
    }
  });
  document.addEventListener('pointerdown', (event) => {
    if (!dom.settingsDrawer.classList.contains('is-open')) {
      return;
    }
    if (!event.target || dom.settingsDrawer.contains(event.target)) {
      return;
    }
    if (dom.settingsToggle.contains(event.target)) {
      return;
    }
    closeSettings();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') {
      return;
    }
    if (!dom.modal.classList.contains('hidden')) {
      closeModal();
      return;
    }
    if (dom.settingsDrawer.classList.contains('is-open')) {
      closeSettings();
    }
  });
  document.addEventListener('mouseover', handleTooltipMouseOver);
  document.addEventListener('mouseout', handleTooltipMouseOut);
  document.addEventListener('focusin', handleTooltipFocusIn);
  document.addEventListener('focusout', handleTooltipFocusOut);
  document.addEventListener('visibilitychange', handleAutoRefreshVisibilityChange);
  window.addEventListener('scroll', handleTooltipViewportChange, true);
  window.addEventListener('resize', handleTooltipViewportChange);
  dom.crossTable?.addEventListener('click', (event) => {
    const row = event.target.closest('[data-detail-id]');
    if (!row) return;
    const item = getFilteredCross().find((entry) => entry.id === row.dataset.detailId);
    if (item) openOpportunityDetail(item);
  });
  dom.crossTable?.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    const row = event.target.closest('[data-detail-id]');
    if (!row || row.tagName === 'BUTTON') return;
    event.preventDefault();
    const item = getFilteredCross().find((entry) => entry.id === row.dataset.detailId);
    if (item) openOpportunityDetail(item);
  });
  dom.crossMobileList?.addEventListener('click', (event) => {
    const card = event.target.closest('[data-detail-id]');
    if (!card) return;
    const item = getFilteredCross().find((entry) => entry.id === card.dataset.detailId);
    if (item) openOpportunityDetail(item);
  });

  if (!state.tradeAlertTicker) {
    state.tradeAlertTicker = window.setInterval(() => {
      if (state.activeTab !== 'cross' || document.hidden) {
        return;
      }
      refreshVisibleTradeAlertUi();
    }, 1000);
  }
}

function refreshVisibleTradeAlertUi() {
  const tableRows = dom.crossTable?.querySelectorAll('tr[data-detail-id]') || [];
  const mobileCards = dom.crossMobileList?.querySelectorAll('[data-detail-id]') || [];
  if (!tableRows.length && !mobileCards.length) {
    return;
  }

  const rowsById = new Map(getFilteredCross().map((item) => [item.id, item]));

  tableRows.forEach((row) => {
    const detailId = row.dataset.detailId;
    const item = rowsById.get(detailId);
    if (!item) {
      return;
    }
    const model = getTradeAlertModel(item);

    const tradeAlertCell = row.querySelector('[data-column-key="tradeAlert"]');
    if (tradeAlertCell) {
      tradeAlertCell.innerHTML = `<span class="chip risk-chip risk-${escapeHtml(model.tone)}">${escapeHtml(model.title)}</span><br><small>${escapeHtml(model.timerLabel)}</small>`;
    }

    const positionCell = row.querySelector('[data-column-key="positionState"]');
    if (positionCell) {
      positionCell.textContent = model.positionLabel;
    }
  });

  mobileCards.forEach((card) => {
    const detailId = card.dataset.detailId;
    const item = rowsById.get(detailId);
    if (!item) {
      return;
    }
    const model = getTradeAlertModel(item);
    const chip = card.querySelector('[data-cross-mobile-trade-title]');
    if (chip) {
      chip.className = `chip risk-chip risk-${model.tone}`;
      chip.textContent = model.title;
    }
    const timer = card.querySelector('[data-cross-mobile-trade-timer]');
    if (timer) {
      timer.textContent = model.timerLabel;
    }
    const position = card.querySelector('[data-cross-mobile-position]');
    if (position) {
      position.textContent = `${t('cross.mobile.positionState')}: ${model.positionLabel}`;
    }
  });
}

function getTooltipTarget(node) {
  if (!(node instanceof Element)) {
    return null;
  }
  const explicitTarget = node.closest('[data-tooltip]');
  if (explicitTarget) {
    return explicitTarget;
  }
  const overflowTarget = node.closest('[data-overflow-tooltip="true"]');
  if (!(overflowTarget instanceof HTMLElement)) {
    return null;
  }
  if (overflowTarget.scrollWidth <= overflowTarget.clientWidth + 1) {
    return null;
  }
  const tooltipText = overflowTarget.textContent?.trim();
  if (!tooltipText) {
    return null;
  }
  overflowTarget.dataset.tooltip = tooltipText;
  overflowTarget.setAttribute('aria-label', tooltipText);
  return overflowTarget;
}

function showFloatingTooltip(target) {
  const text = target?.dataset?.tooltip;
  if (!target || !text || !dom.floatingTooltip) {
    return;
  }
  activeTooltipTarget = target;
  dom.floatingTooltip.textContent = text;
  dom.floatingTooltip.classList.remove('hidden');
  positionFloatingTooltip(target);
}

function hideFloatingTooltip() {
  activeTooltipTarget = null;
  if (!dom.floatingTooltip) {
    return;
  }
  dom.floatingTooltip.classList.add('hidden');
  dom.floatingTooltip.classList.remove('is-above');
  dom.floatingTooltip.textContent = '';
  dom.floatingTooltip.style.removeProperty('--tooltip-left');
  dom.floatingTooltip.style.removeProperty('--tooltip-top');
  dom.floatingTooltip.style.removeProperty('--tooltip-arrow-left');
}

function positionFloatingTooltip(target) {
  if (!target || !dom.floatingTooltip || dom.floatingTooltip.classList.contains('hidden')) {
    return;
  }
  const rect = target.getBoundingClientRect();
  const tooltipRect = dom.floatingTooltip.getBoundingClientRect();
  const margin = 12;
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;
  let left = rect.left + rect.width / 2 - tooltipRect.width / 2;
  left = Math.min(Math.max(margin, left), viewportWidth - tooltipRect.width - margin);

  let top = rect.bottom + 14;
  let placeAbove = false;
  if (top + tooltipRect.height > viewportHeight - margin) {
    top = rect.top - tooltipRect.height - 14;
    placeAbove = true;
  }
  top = Math.max(margin, top);

  const targetCenter = rect.left + rect.width / 2;
  const arrowLeft = Math.min(Math.max(24, targetCenter - left), tooltipRect.width - 24);

  dom.floatingTooltip.style.setProperty('--tooltip-left', `${Math.round(left)}px`);
  dom.floatingTooltip.style.setProperty('--tooltip-top', `${Math.round(top)}px`);
  dom.floatingTooltip.style.setProperty('--tooltip-arrow-left', `${Math.round(arrowLeft)}px`);
  dom.floatingTooltip.classList.toggle('is-above', placeAbove);
}

function handleTooltipMouseOver(event) {
  const target = getTooltipTarget(event.target);
  if (!target || target === activeTooltipTarget) {
    return;
  }
  showFloatingTooltip(target);
}

function handleTooltipMouseOut(event) {
  const target = getTooltipTarget(event.target);
  if (!target || target !== activeTooltipTarget) {
    return;
  }
  const nextTarget = getTooltipTarget(event.relatedTarget);
  if (nextTarget === target) {
    return;
  }
  hideFloatingTooltip();
}

function handleTooltipFocusIn(event) {
  const target = getTooltipTarget(event.target);
  if (!target) {
    return;
  }
  showFloatingTooltip(target);
}

function handleTooltipFocusOut(event) {
  const target = getTooltipTarget(event.target);
  if (!target || target !== activeTooltipTarget) {
    return;
  }
  const nextTarget = getTooltipTarget(event.relatedTarget);
  if (nextTarget === target) {
    return;
  }
  hideFloatingTooltip();
}

function handleTooltipViewportChange() {
  if (!activeTooltipTarget) {
    return;
  }
  if (!document.contains(activeTooltipTarget)) {
    hideFloatingTooltip();
    return;
  }
  positionFloatingTooltip(activeTooltipTarget);
}

function syncOverlayState() {
  document.body.classList.toggle('is-drawer-open', dom.settingsDrawer?.classList.contains('is-open'));
  document.body.classList.toggle('is-modal-open', !dom.modal?.classList.contains('hidden'));
}

function loadSettings() {
  try {
    const raw = safeLocalStorageGet(STORAGE_KEY);
    const sessionRefreshApiKey = normalizeRefreshApiKey(safeSessionStorageGet(REFRESH_API_SESSION_KEY));
    if (!raw) {
      return {
        ...DEFAULT_SETTINGS,
        refreshApiKey: sessionRefreshApiKey,
      };
    }
    const parsed = JSON.parse(raw);
    const migrateCrossColumns = !Object.prototype.hasOwnProperty.call(parsed || {}, 'crossColumnsSchemaVersion')
      || Number(parsed.crossColumnsSchemaVersion) < CROSS_COLUMNS_SCHEMA_VERSION;
    const parsedVisibleCrossColumns = isPlainObject(parsed.visibleCrossColumns) && Object.keys(parsed.visibleCrossColumns).length
      ? { ...DEFAULT_SETTINGS.visibleCrossColumns, ...parsed.visibleCrossColumns }
      : { ...DEFAULT_SETTINGS.visibleCrossColumns };
    const shouldResetVisibleCrossColumns = migrateCrossColumns
      && visibilityMatchesPreset(parsedVisibleCrossColumns, LEGACY_COMPACT_CROSS_COLUMNS);
    return {
      ...DEFAULT_SETTINGS,
      ...parsed,
      refreshApiKey: sessionRefreshApiKey || normalizeRefreshApiKey(parsed.refreshApiKey),
      language: parsed.language === 'ru' ? 'ru' : DEFAULT_SETTINGS.language,
      autoRefreshMode: parsed.autoRefreshMode === 'auto' ? 'auto' : 'manual',
      autoRefreshIntervalSeconds: clampAutoRefreshIntervalSeconds(parsed.autoRefreshIntervalSeconds),
      autoRefreshStrategy: AUTO_REFRESH_DEFAULT_STRATEGY,
      autoRefreshFullIntervalSeconds: AUTO_REFRESH_DEFAULT_FULL_INTERVAL_SECONDS,
      useExternalAiApi: typeof parsed.useExternalAiApi === 'boolean'
        ? parsed.useExternalAiApi
        : normalizeRefreshApiBaseUrl(parsed.aiApiBaseUrl) !== '',
      aiApiBaseUrl: normalizeRefreshApiBaseUrl(parsed.aiApiBaseUrl),
      aiPredictionHorizon: normalizeAiPredictionHorizon(parsed.aiPredictionHorizon),
      minAiScore: Number.isFinite(Number(parsed.minAiScore)) ? Math.max(0, Math.min(100, Number(parsed.minAiScore))) : 0,
      crossColumnsSchemaVersion: CROSS_COLUMNS_SCHEMA_VERSION,
      maxAiEntryWaitMinutes: Number.isFinite(Number(parsed.maxAiEntryWaitMinutes)) ? Math.max(0, Math.min(240, Number(parsed.maxAiEntryWaitMinutes))) : 0,
      maxAiHoldingHours: Number.isFinite(Number(parsed.maxAiHoldingHours)) ? Math.max(0, Math.min(168, Number(parsed.maxAiHoldingHours))) : 0,
      visibleCrossColumns: shouldResetVisibleCrossColumns
        ? { ...DEFAULT_SETTINGS.visibleCrossColumns }
        : parsedVisibleCrossColumns,
    };
  } catch {
    return { ...DEFAULT_SETTINGS };
  }
}

function persistSettings() {
  const persistedSettings = {
    ...state.settings,
    refreshApiKey: '',
  };
  safeLocalStorageSet(STORAGE_KEY, JSON.stringify(persistedSettings));
  if (state.settings.refreshApiKey) {
    safeSessionStorageSet(REFRESH_API_SESSION_KEY, state.settings.refreshApiKey);
  } else {
    safeSessionStorageRemove(REFRESH_API_SESSION_KEY);
  }
}

function sanitizeTrackedPosition(id, position) {
  if (!position || typeof position !== 'object' || Array.isArray(position)) {
    return null;
  }
  const enteredAtDate = position.enteredAt ? new Date(position.enteredAt) : null;
  return {
    id,
    pair: String(position.pair || position.pairA || ''),
    exchangeA: String(position.exchangeA || ''),
    exchangeB: String(position.exchangeB || ''),
    direction: String(position.direction || ''),
    enteredAt: enteredAtDate && !Number.isNaN(enteredAtDate.getTime()) ? enteredAtDate.toISOString() : new Date().toISOString(),
    holdingHours: Number.isFinite(Number(position.holdingHours)) ? Math.max(0, Number(position.holdingHours)) : null,
    exitTarget: String(position.exitTarget || ''),
  };
}

function loadTradeTrackerState() {
  try {
    const raw = safeLocalStorageGet(TRADE_TRACKER_STORAGE_KEY);
    if (!raw) {
      return { watched: {}, positions: {} };
    }
    const parsed = JSON.parse(raw);
    const rawWatched = parsed && typeof parsed.watched === 'object' && !Array.isArray(parsed.watched)
      ? parsed.watched
      : {};
    const rawPositions = parsed && typeof parsed.positions === 'object' && !Array.isArray(parsed.positions)
      ? parsed.positions
      : {};
    const watched = {};
    Object.entries(rawWatched).forEach(([id, enabled]) => {
      if (enabled) {
        watched[id] = true;
      }
    });
    const positions = {};
    Object.entries(rawPositions).forEach(([id, position]) => {
      const sanitized = sanitizeTrackedPosition(id, position);
      if (sanitized) {
        positions[id] = sanitized;
      }
    });
    return { watched, positions };
  } catch {
    return { watched: {}, positions: {} };
  }
}

function persistTradeTrackerState() {
  safeLocalStorageSet(TRADE_TRACKER_STORAGE_KEY, JSON.stringify(state.tradeTracker));
}

function getTrackedPosition(itemOrId) {
  const id = typeof itemOrId === 'string' ? itemOrId : itemOrId?.id;
  if (!id) {
    return null;
  }
  return state.tradeTracker.positions[id] || null;
}

function isTrackedTrade(itemOrId) {
  const id = typeof itemOrId === 'string' ? itemOrId : itemOrId?.id;
  if (!id) {
    return false;
  }
  return Boolean(state.tradeTracker.watched[id]);
}

function getTradeRouteLabel(itemOrPosition) {
  return t('trade.positionActive', {
    pair: itemOrPosition?.pairA || itemOrPosition?.pair || '',
    exchangeA: itemOrPosition?.exchangeA || '',
    exchangeB: itemOrPosition?.exchangeB || '',
  });
}

function buildTrackedPosition(item) {
  return {
    id: item.id,
    pair: String(item.pairA || ''),
    exchangeA: String(item.exchangeA || ''),
    exchangeB: String(item.exchangeB || ''),
    direction: String(item.direction || ''),
    enteredAt: new Date().toISOString(),
    holdingHours: Number.isFinite(Number(item.holdingHoursRecommended))
      ? Math.max(0, Number(item.holdingHoursRecommended))
      : Number.isFinite(Number(item.meanReversionHours))
        ? Math.max(0, Number(item.meanReversionHours))
        : Number.isFinite(Number(state.settings.holdingHours)) && Number(state.settings.holdingHours) > 0
          ? Number(state.settings.holdingHours)
          : null,
    exitTarget: String(item.exitTarget || ''),
  };
}

function watchTrackedTrade(item) {
  state.tradeTracker.watched[item.id] = true;
  persistTradeTrackerState();
  setRefreshStatus(`${t('trade.watchAction')}: ${getTradeRouteLabel(item)}`);
  renderAll();
  rerenderActiveDetail();
}

function unwatchTrackedTrade(itemOrId) {
  const id = typeof itemOrId === 'string' ? itemOrId : itemOrId?.id;
  if (!id || !state.tradeTracker.watched[id]) {
    return;
  }
  const item = typeof itemOrId === 'string'
    ? getSortedCrossRows().find((row) => row.id === id) || getTrackedPosition(id)
    : itemOrId;
  delete state.tradeTracker.watched[id];
  persistTradeTrackerState();
  if (item) {
    setRefreshStatus(`${t('trade.unwatchAction')}: ${getTradeRouteLabel(item)}`);
  }
  renderAll();
  rerenderActiveDetail();
}

function markTrackedTradeEntry(item) {
  state.tradeTracker.watched[item.id] = true;
  state.tradeTracker.positions[item.id] = buildTrackedPosition(item);
  persistTradeTrackerState();
  setRefreshStatus(`${t('trade.noticeHolding')}: ${getTradeRouteLabel(item)}`);
  renderAll();
  rerenderActiveDetail();
}

function closeTrackedTrade(itemOrId) {
  const position = getTrackedPosition(itemOrId);
  const id = typeof itemOrId === 'string' ? itemOrId : itemOrId?.id;
  if (!id || !position) {
    return;
  }
  delete state.tradeTracker.positions[id];
  persistTradeTrackerState();
  setRefreshStatus(`${t('trade.exitAction')}: ${getTradeRouteLabel(position)}`);
  renderAll();
  rerenderActiveDetail();
}

function formatDurationCompactFromMs(value) {
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds)) {
    return pickLocalized('n/a', 'н/д');
  }
  const totalMinutes = Math.max(0, Math.round(milliseconds / 60000));
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const minutes = totalMinutes % 60;
  const parts = [];
  if (days > 0) {
    parts.push(`${days}${pickLocalized('d', 'д')}`);
  }
  if (hours > 0) {
    parts.push(`${hours}${pickLocalized('h', 'ч')}`);
  }
  if (minutes > 0 || !parts.length) {
    parts.push(`${minutes}${pickLocalized('m', 'м')}`);
  }
  return parts.slice(0, 2).join(' ');
}

function formatHoldingHoursLabel(hours) {
  const numericHours = Number(hours);
  if (!Number.isFinite(numericHours) || numericHours <= 0) {
    return pickLocalized('n/a', 'н/д');
  }
  return `${formatNumber(numericHours, numericHours >= 10 ? 0 : 1)} ${pickLocalized('h', 'ч')}`;
}

function getTrackedHoldRemainingMs(position) {
  if (!position) {
    return null;
  }
  const enteredAt = new Date(position.enteredAt);
  if (Number.isNaN(enteredAt.getTime()) || !Number.isFinite(Number(position.holdingHours))) {
    return null;
  }
  return enteredAt.getTime() + Number(position.holdingHours) * 60 * 60 * 1000 - Date.now();
}

function getTradeSignalState(item) {
  return String(item.signalState || item.signalLifecycle?.state || 'idle');
}

function isTradeEntryReady(item) {
  const signalState = getTradeSignalState(item);
  const readyFromSignal = signalState === 'ready' || item.signalLifecycle?.practicalSignal === true;
  const readyFromPlan = item.tradePlan?.action === 'ready' || item.aiAdviceVerdict === 'enter-soon';
  const waitMinutes = getLiveEntryWaitMinutes(item);
  const waitIsShort = waitMinutes == null || waitMinutes <= ENTRY_READY_MAX_WAIT_MINUTES;
  const netEdge = Number(item.netAfterExecution ?? item.netSpread ?? 0);
  return waitIsShort && netEdge > 0 && (readyFromSignal || readyFromPlan);
}

function shouldExitTrackedTrade(item, position, remainingMs = getTrackedHoldRemainingMs(position)) {
  const signalState = getTradeSignalState(item);
  const exitEtaMinutes = Number(item.exitEtaMinutes);
  const exitTargetSpreadPct = Number(item.exitTargetSpreadPct);
  const currentAbsNetSpread = Math.abs(Number(item.netSpread || 0));
  const targetReached = Number.isFinite(exitTargetSpreadPct)
    ? currentAbsNetSpread <= Math.max(0.02, exitTargetSpreadPct + 0.02)
    : false;
  return signalState === 'cooldown'
    || item.tradePlan?.action === 'blocked'
    || (Number.isFinite(exitEtaMinutes) && exitEtaMinutes <= EXIT_READY_MAX_ETA_MINUTES)
    || targetReached
    || (remainingMs != null && remainingMs <= 0);
}

function getTradeAlertModel(item) {
  const position = getTrackedPosition(item);
  const signalState = getTradeSignalState(item);
  const liveEntryWaitMinutes = getLiveEntryWaitMinutes(item);
  const positionLabel = position
    ? t('trade.positionActive', {
      pair: position.pair,
      exchangeA: position.exchangeA,
      exchangeB: position.exchangeB,
    })
    : t('trade.positionMissing');
  const remainingMs = position ? getTrackedHoldRemainingMs(position) : null;
  const plannedHoldLabel = item.holdingHoursRecommended != null
    ? formatHoldingHoursLabel(item.holdingHoursRecommended)
    : item.meanReversionHours != null
      ? formatHoldingHoursLabel(item.meanReversionHours)
      : pickLocalized('n/a', 'н/д');

  if (position) {
    if (shouldExitTrackedTrade(item, position, remainingMs)) {
      return {
        status: 'exit-ready',
        tone: 'negative',
        title: t('trade.noticeExitReady'),
        timerLabel: remainingMs != null && remainingMs > 0
          ? t('trade.timerHold', { value: formatDurationCompactFromMs(remainingMs) })
          : t('trade.timerExpired'),
        positionLabel,
        position,
      };
    }
    if (remainingMs != null && remainingMs > 0 && remainingMs <= HOLD_ENDING_WARNING_MINUTES * 60 * 1000) {
      return {
        status: 'hold-ending',
        tone: 'warning',
        title: t('trade.noticeHoldEnding'),
        timerLabel: t('trade.timerHold', { value: formatDurationCompactFromMs(remainingMs) }),
        positionLabel,
        position,
      };
    }
    return {
      status: 'holding',
      tone: 'positive',
      title: t('trade.noticeHolding'),
      timerLabel: remainingMs != null
        ? t('trade.timerHold', { value: formatDurationCompactFromMs(remainingMs) })
        : t('trade.timerAwaiting'),
      positionLabel,
      position,
    };
  }

  if (isTradeEntryReady(item)) {
    return {
      status: 'entry-ready',
      tone: 'positive',
      title: t('trade.noticeEntryReady'),
      timerLabel: liveEntryWaitMinutes != null
        ? t('trade.timerEntry', { value: formatEtaMinutes(liveEntryWaitMinutes) })
        : t('trade.timerPlan', { value: plannedHoldLabel }),
      positionLabel: t('trade.positionActive', {
        pair: item.pairA,
        exchangeA: item.exchangeA,
        exchangeB: item.exchangeB,
      }),
      position: null,
    };
  }

  if (signalState === 'watch' || (liveEntryWaitMinutes != null && Number(liveEntryWaitMinutes) > ENTRY_READY_MAX_WAIT_MINUTES)) {
    return {
      status: 'watch',
      tone: 'warning',
      title: t('trade.noticeWatch'),
      timerLabel: liveEntryWaitMinutes != null
        ? t('trade.timerEntry', { value: formatEtaMinutes(liveEntryWaitMinutes) })
        : t('trade.timerPlan', { value: plannedHoldLabel }),
      positionLabel: t('trade.positionActive', {
        pair: item.pairA,
        exchangeA: item.exchangeA,
        exchangeB: item.exchangeB,
      }),
      position: null,
    };
  }

  return {
    status: 'idle',
    tone: 'warning',
    title: signalState === 'cooldown' ? translateLifecycle(signalState) : t('trade.noticeIdle'),
    timerLabel: plannedHoldLabel === pickLocalized('n/a', 'н/д')
      ? t('trade.timerAwaiting')
      : t('trade.timerPlan', { value: plannedHoldLabel }),
    positionLabel,
    position: null,
  };
}

function buildStoredPositionNotice(position) {
  const remainingMs = getTrackedHoldRemainingMs(position);
  const tone = remainingMs != null && remainingMs <= 0
    ? 'negative'
    : remainingMs != null && remainingMs <= HOLD_ENDING_WARNING_MINUTES * 60 * 1000
      ? 'warning'
      : 'positive';
  return {
    id: position.id,
    itemId: null,
    tone,
    title: tone === 'negative'
      ? t('trade.noticeExitReady')
      : tone === 'warning'
        ? t('trade.noticeHoldEnding')
        : t('trade.noticeHolding'),
    positionLabel: t('trade.positionActive', {
      pair: position.pair,
      exchangeA: position.exchangeA,
      exchangeB: position.exchangeB,
    }),
    timerLabel: remainingMs != null
      ? (remainingMs <= 0 ? t('trade.timerExpired') : t('trade.timerHold', { value: formatDurationCompactFromMs(remainingMs) }))
      : t('trade.timerAwaiting'),
  };
}

function buildTradeSmartNotices(rows = null) {
  const sourceRows = Array.isArray(rows) ? rows : getSortedCrossRows();
  const rowsById = new Map(sourceRows.map((row) => [row.id, row]));
  const notices = [];
  const seen = new Set();

  Object.values(state.tradeTracker.positions)
    .sort((left, right) => new Date(right.enteredAt).getTime() - new Date(left.enteredAt).getTime())
    .forEach((position) => {
      const item = rowsById.get(position.id);
      const model = item ? getTradeAlertModel(item) : buildStoredPositionNotice(position);
      notices.push({
        id: position.id,
        itemId: item?.id || null,
        tone: model.tone,
        title: model.title,
        positionLabel: model.positionLabel,
        timerLabel: model.timerLabel,
      });
      seen.add(position.id);
    });

  sourceRows.forEach((item) => {
    if (notices.length >= TRADE_SMART_NOTICE_LIMIT || seen.has(item.id) || !isTrackedTrade(item)) {
      return;
    }
    const model = getTradeAlertModel(item);
    if (!['entry-ready', 'watch'].includes(model.status)) {
      return;
    }
    notices.push({
      id: item.id,
      itemId: item.id,
      tone: model.tone,
      title: model.title,
      positionLabel: model.positionLabel,
      timerLabel: model.timerLabel,
    });
    seen.add(item.id);
  });

  return notices.slice(0, TRADE_SMART_NOTICE_LIMIT);
}

function renderTradeSmartNotices() {
  if (!dom.tradeSmartNotices) {
    return;
  }
  const rows = getSortedCrossRows();
  const rowsById = new Map(rows.map((row) => [row.id, row]));
  const notices = buildTradeSmartNotices(rows);
  if (!notices.length) {
    dom.tradeSmartNotices.classList.add('hidden');
    dom.tradeSmartNotices.innerHTML = '';
    return;
  }
  dom.tradeSmartNotices.classList.remove('hidden');
  dom.tradeSmartNotices.innerHTML = notices
    .map((notice) => `
      <article class="trade-smart-card trade-smart-card-${escapeHtml(notice.tone)}">
        <span class="trade-smart-card__eyebrow">${escapeHtml(t('hero.smartNotices'))}</span>
        <strong>${escapeHtml(notice.title)}</strong>
        <p>${escapeHtml(notice.positionLabel)}</p>
        <small>${escapeHtml(notice.timerLabel)}</small>
        ${notice.itemId ? `<button class="ghost-button trade-smart-card__action" type="button" data-trade-notice-open="${escapeHtml(notice.itemId)}">${escapeHtml(pickLocalized('Open pair', 'Открыть связку'))}</button>` : ''}
      </article>
    `)
    .join('');
  dom.tradeSmartNotices.querySelectorAll('[data-trade-notice-open]').forEach((button) => {
    button.addEventListener('click', () => {
      const item = rowsById.get(button.dataset.tradeNoticeOpen);
      if (item) {
        openOpportunityDetail(item);
      }
    });
  });
}

function shouldStartRefreshOnStartup() {
  if (!hasRefreshApi() || state.refreshState?.status === 'running') {
    return false;
  }
  if (hasFreshMarketStreamData()) {
    return false;
  }
  if (!state.refreshApiHealth?.refresh_auth_required) {
    return true;
  }
  return Boolean(getRefreshApiKey() || canUseImplicitRefreshAuth());
}

function hasFreshMarketStreamData(maxAgeMs = MARKET_STREAM_FRESHNESS_MS) {
  if (!state.marketStreamConnected || !state.marketStreamLastMessageAt) {
    return false;
  }
  const lastMessageMs = Date.parse(state.marketStreamLastMessageAt);
  if (!Number.isFinite(lastMessageMs)) {
    return false;
  }
  return Date.now() - lastMessageMs <= Math.max(1000, Number(maxAgeMs) || MARKET_STREAM_FRESHNESS_MS);
}

function syncAiApiSettingsVisibility() {
  const form = dom.settingsForm;
  const urlGroup = document.querySelector('#ai-api-url-group');
  if (!form?.elements?.useExternalAiApi || !form?.elements?.aiApiBaseUrl || !urlGroup) {
    return;
  }
  const externalEnabled = Boolean(form.elements.useExternalAiApi.checked);
  urlGroup.classList.toggle('hidden', !externalEnabled);
  form.elements.aiApiBaseUrl.disabled = !externalEnabled;
}

function defaultCrossColumnVisibility() {
  return { ...DEFAULT_CROSS_COLUMN_VISIBILITY };
}

function setRefreshStatus(message, isError = false) {
  if (!dom.refreshStatus) {
    return;
  }
  dom.refreshStatus.textContent = message;
  dom.refreshStatus.classList.toggle('value-negative', isError);
  dom.refreshStatus.classList.toggle('value-positive', !isError && Boolean(message));
}

function setRefreshMeta(message) {
  if (!dom.refreshMeta) {
    return;
  }
  dom.refreshMeta.textContent = message;
}

function formatElapsedTimer(seconds) {
  const safeSeconds = Math.max(0, Number(seconds) || 0);
  const minutes = Math.floor(safeSeconds / 60);
  const restSeconds = safeSeconds % 60;
  return `${String(minutes).padStart(2, '0')}:${String(restSeconds).padStart(2, '0')}`;
}

function stopRefreshUiTimer() {
  if (state.refreshUiTimer) {
    window.clearInterval(state.refreshUiTimer);
    state.refreshUiTimer = null;
  }
  state.refreshUiStartedAt = 0;
}

function startRefreshUiTimer(mode = 'filtered') {
  stopRefreshUiTimer();
  state.refreshUiStartedAt = Date.now();
  const refreshScopeLabel = mode === 'full'
    ? pickLocalized('full refresh', 'полное обновление')
    : pickLocalized('filtered refresh', 'обновление по фильтрам');
  const updateMeta = () => {
    if (!state.refreshInFlight || !state.refreshUiStartedAt) {
      return;
    }
    const elapsedSeconds = Math.floor((Date.now() - state.refreshUiStartedAt) / 1000);
    setRefreshMeta(
      pickLocalized(
        `Refreshing (${refreshScopeLabel}): ${formatElapsedTimer(elapsedSeconds)}`,
        `Идёт обновление (${refreshScopeLabel}): ${formatElapsedTimer(elapsedSeconds)}`,
      ),
    );
  };
  updateMeta();
  state.refreshUiTimer = window.setInterval(updateMeta, 1000);
}

function getRefreshSourceMode() {
  if (!hasRefreshApi()) {
    return 'snapshot';
  }
  if (normalizeRefreshApiBaseUrl(state.settings.refreshApiBaseUrl)) {
    return 'configured';
  }
  if (normalizeRefreshApiBaseUrl(state.runtimeConfig.refreshApiBaseUrl)) {
    return 'configured';
  }
  if (normalizeRefreshApiBaseUrl(state.detectedRefreshApiBaseUrl)) {
    return 'same-origin';
  }
  return 'local';
}

function buildDataAgeBadge() {
  const rawTimestamp = state.metrics?.generated_at || state.refreshState?.finished_at;
  if (!rawTimestamp) {
    return null;
  }
  const timestamp = new Date(rawTimestamp).getTime();
  if (!Number.isFinite(timestamp)) {
    return null;
  }
  const ageMinutes = Math.max(0, Math.round((Date.now() - timestamp) / 60000));
  if (ageMinutes < 1) {
    return { tone: 'positive', text: t('refresh.ageNow') };
  }
  if (ageMinutes < 60) {
    return {
      tone: ageMinutes <= 15 ? 'positive' : 'warning',
      text: t('refresh.ageMinutes', { value: ageMinutes }),
    };
  }
  const ageHours = formatNumber(ageMinutes / 60, ageMinutes < 180 ? 1 : 0);
  return {
    tone: ageMinutes <= 180 ? 'warning' : 'negative',
    text: t('refresh.ageHours', { value: ageHours }),
  };
}

function getSnapshotOriginBadge() {
  if (state.dataOrigin === 'warm-cache') {
    return {
      tone: 'positive',
      text: pickLocalized('Instant cache', 'Мгновенный кэш'),
    };
  }
  if (state.dataOrigin === 'historical-fallback') {
    return {
      tone: 'warning',
      text: pickLocalized('Historical fallback', 'Исторический fallback'),
    };
  }
  if (state.dataOrigin !== 'ephemeral-filtered') {
    return null;
  }
  return {
    tone: 'warning',
    text: t('refresh.sourceEphemeralFiltered'),
  };
}

function getMarketTransportBadge() {
  if (!state.marketStreamAvailable) {
    // Show SWR status when no WebSocket
    const hasSwrCache = Object.keys(state.swrCache).length > 0;
    if (hasSwrCache) {
      return {
        tone: 'positive',
        text: pickLocalized('SWR cache active', 'SWR-кеш активен'),
      };
    }
    return null;
  }
  if (state.marketStreamConnected) {
    return {
      tone: 'positive',
      text: pickLocalized('Live WebSocket', 'Live WebSocket'),
    };
  }
  if (state.marketStreamMode === 'connecting') {
    return {
      tone: 'warning',
      text: pickLocalized('WebSocket reconnecting', 'WebSocket переподключается'),
    };
  }
  return {
    tone: 'warning',
    text: pickLocalized('HTTP fallback', 'HTTP-фолбэк'),
  };
}

function renderDataSourceStatus() {
  if (!dom.dataSourceStatus) {
    return;
  }

  const sourceMode = getRefreshSourceMode();
  const sourceBadge = {
    tone: sourceMode === 'snapshot' ? 'warning' : 'positive',
    text: sourceMode === 'configured'
      ? t('refresh.sourceConfigured')
      : sourceMode === 'same-origin'
        ? t('refresh.sourceSameOrigin')
        : sourceMode === 'local'
          ? t('refresh.sourceLocal')
          : t('refresh.sourceSnapshot'),
  };

  const accessBadge = !hasRefreshApi()
    ? {
        tone: 'warning',
        text: t('refresh.accessSnapshot'),
      }
    : state.refreshApiProbeError === 'unreachable'
      ? {
          tone: 'negative',
          text: t('refresh.accessUnreachable'),
        }
      : state.refreshApiHealth?.refresh_auth_required
        ? (() => {
            const hasAuth = getRefreshApiKey() || canUseImplicitRefreshAuth();
            return {
              tone: hasAuth ? 'positive' : 'warning',
              text: hasAuth ? t('refresh.accessReady') : t('refresh.accessMissing'),
            };
          })()
        : {
            tone: 'positive',
            text: t('refresh.accessOpen'),
          };

  const badges = [sourceBadge, getMarketTransportBadge(), getSnapshotOriginBadge(), accessBadge, getAutoRefreshBadge(), getAutoRefreshStrategyBadge(), buildDataAgeBadge()].filter(Boolean);
  dom.dataSourceStatus.innerHTML = badges
    .map((badge) => `<span class="quality-badge ${escapeHtml(badge.tone)}">${escapeHtml(badge.text)}</span>`)
    .join('');
}

function syncRefreshFeedback() {
  if (state.refreshInFlight && state.refreshUiStartedAt) {
    return;
  }
  if (hasRefreshApi()) {
    if (!state.refreshInFlight) {
      setRefreshMeta(buildRefreshSuccessMeta(state.lastRefreshAt));
    }
    return;
  }

  if (state.refreshState?.status === 'running') {
    setRefreshStatus(state.refreshState.message || t('refresh.dataInProgress'));
  } else if (state.refreshState?.status === 'failed') {
    setRefreshStatus(state.refreshState.message || t('refresh.failed'), true);
  } else {
    setRefreshStatus('');
  }
  setRefreshMeta(buildRefreshSuccessMeta(state.refreshState?.finished_at || state.lastRefreshAt));
}

function buildRefreshStagePlan(scope) {
  if (scope === 'full') {
    return [
      { key: 'queued', label: pickLocalized('Queued', 'В очереди') },
      { key: 'markets', label: pickLocalized('Markets', 'Маркеты') },
      { key: 'cross', label: pickLocalized('Cross+enrich', 'Спред+обогащение') },
      { key: 'ai', label: pickLocalized('AI signals', 'ИИ-сигналы') },
      { key: 'validate', label: pickLocalized('Validate', 'Проверка') },
    ];
  }
  return [
    { key: 'queued', label: pickLocalized('Queued', 'В очереди') },
    { key: 'markets', label: pickLocalized('Markets', 'Маркеты') },
    { key: 'cross', label: pickLocalized('Cross+enrich', 'Спред+обогащение') },
    { key: 'ai', label: pickLocalized('AI signals', 'ИИ-сигналы') },
    { key: 'done', label: pickLocalized('Finalize', 'Финализация') },
  ];
}

function detectRefreshStageKey(detail, elapsedSec = 0) {
  const message = String(detail?.message || '').toLowerCase();
  const stepsText = Array.isArray(detail?.steps)
    ? detail.steps.map((step) => `${step?.stdout || ''}\n${step?.stderr || ''}`).join('\n').toLowerCase()
    : '';
  const sourceText = `${message}\n${stepsText}`;

  if (sourceText.includes('validate_outputs') || sourceText.includes('validate')) {
    return 'validate';
  }
  if (sourceText.includes('ai-signals') || sourceText.includes('opportunity-advice') || sourceText.includes('predict-spread')) {
    return 'ai';
  }
  if (sourceText.includes('cross+enrich') || sourceText.includes('cross-exchange')) {
    return 'cross';
  }
  if (sourceText.includes('markets')) {
    return 'markets';
  }

  // Fallback for running state when backend message is coarse-grained.
  if (detail?.status === 'running') {
    if (elapsedSec > 34) {
      return detail?.scope === 'full' ? 'validate' : 'ai';
    }
    if (elapsedSec > 22) {
      return 'cross';
    }
    if (elapsedSec > 6) {
      return 'markets';
    }
  }
  return 'queued';
}

function renderRefreshStageTimeline(detail) {
  if (!detail || !detail.status) {
    return '';
  }
  const scope = String(detail.scope || 'filtered');
  const stages = buildRefreshStagePlan(scope);
  if (!stages.length) {
    return '';
  }
  const startedAtMs = detail.started_at ? Date.parse(detail.started_at) : NaN;
  const elapsedSec = Number.isFinite(startedAtMs) ? Math.max(0, Math.floor((Date.now() - startedAtMs) / 1000)) : 0;
  const currentKey = detail.status === 'completed' ? stages[stages.length - 1].key : detectRefreshStageKey(detail, elapsedSec);
  const currentIndex = Math.max(0, stages.findIndex((stage) => stage.key === currentKey));
  return `
    <div class="refresh-stage-timeline" role="status" aria-live="polite">
      ${stages
        .map((stage, index) => {
          const statusClass = detail.status === 'completed'
            ? 'is-done'
            : index < currentIndex
              ? 'is-done'
              : index === currentIndex
                ? 'is-current'
                : 'is-pending';
          return `<span class="refresh-stage-pill ${statusClass}">${escapeHtml(stage.label)}</span>`;
        })
        .join('')}
    </div>
  `;
}

function renderRefreshDetail(payload) {
  if (!dom.refreshDetail) {
    return;
  }
  state.refreshState = payload && typeof payload === 'object' ? { ...payload } : null;
  const detail = state.refreshState;
  if (!detail || (!detail.started_at && !detail.finished_at && !detail.message && !detail.steps?.length)) {
    dom.refreshDetail.classList.add('hidden');
    dom.refreshDetail.innerHTML = '';
    return;
  }

  const steps = Array.isArray(detail.steps) ? detail.steps : [];
  const stepMarkup = steps.length
    ? steps
        .map((step) => {
          const status = t(`status.${Number(step.returncode) === 0 ? 'ok' : 'failed'}`);
          const detailText = String(step.stderr || step.stdout || '').trim().split(/\r?\n/).filter(Boolean).slice(-1)[0] || t('common.noDetails');
          return `<div><strong>${escapeHtml(step.script || 'step')}</strong>: ${escapeHtml(status)} · ${escapeHtml(detailText)}</div>`;
        })
        .join('')
    : '';
  const runLinkMarkup = detail.run_url && isSafeUrl(detail.run_url)
    ? `<div><strong>${escapeHtml(t('refresh.runLink'))}</strong>: <a href="${escapeHtml(detail.run_url)}" target="_blank" rel="noreferrer">${escapeHtml(detail.run_url)}</a></div>`
    : '';

  dom.refreshDetail.innerHTML = `
    ${renderRefreshStageTimeline(detail)}
    <div><strong>${escapeHtml(t('refresh.scope'))}</strong>: ${escapeHtml(String(detail.scope || 'filtered'))}</div>
    <div><strong>${escapeHtml(t('refresh.status'))}</strong>: ${escapeHtml(t(`status.${String(detail.status || 'idle')}`))}</div>
    ${detail.started_at ? `<div><strong>${escapeHtml(t('refresh.started'))}</strong>: ${escapeHtml(formatDateTime(detail.started_at))}</div>` : ''}
    ${detail.finished_at ? `<div><strong>${escapeHtml(t('refresh.finished'))}</strong>: ${escapeHtml(formatDateTime(detail.finished_at))}</div>` : ''}
    ${detail.message ? `<div><strong>${escapeHtml(t('refresh.message'))}</strong>: ${escapeHtml(detail.message)}</div>` : ''}
    ${runLinkMarkup}
    ${stepMarkup}
  `;
  dom.refreshDetail.classList.remove('hidden');
}

function buildRefreshSuccessMeta(refreshAt) {
  if (!refreshAt) {
    return '';
  }
  return t('refresh.lastSuccess', { value: formatDateTime(refreshAt) });
}

function buildRefreshErrorMessage(payload, fallbackMessage, responseStatus = null) {
  if (payload && typeof payload.message === 'string' && payload.message.trim()) {
    return payload.message.trim();
  }
  if (responseStatus === 401 || responseStatus === 403) {
    return t('refresh.accessMissing');
  }
  if (responseStatus === 429) {
    return pickLocalized('Refresh is rate-limited. Try again in a moment.', 'Refresh временно ограничен по частоте. Попробуйте ещё раз через минуту.');
  }
  if ([502, 503, 504].includes(responseStatus)) {
    return pickLocalized('Backend is temporarily unavailable. Try again shortly.', 'Backend временно недоступен. Попробуйте ещё раз чуть позже.');
  }
  return fallbackMessage;
}

function buildRefreshFilters() {
  const configuredVisibility = isPlainObject(state.settings.visibleExchanges) ? state.settings.visibleExchanges : {};
  const knownExchanges = getAllExchanges();
  const exchangesToEvaluate = knownExchanges.length ? knownExchanges : Object.keys(configuredVisibility);
  const visibleExchanges = exchangesToEvaluate.filter((exchange) => configuredVisibility[exchange] !== false);

  return {
    visibleExchanges,
    search: state.settings.search || '',
    minSpread: Number(state.settings.minSpread || 0),
    holdingHours: Number(state.settings.holdingHours || 0),
    fees: state.settings.fees || {},
  };
}

async function handleRefreshButton(mode = 'filtered', options = {}) {
  mode = mode === 'full' ? 'full' : 'filtered';
  if (state.refreshInFlight) {
    return;
  }

  if (!hasRefreshApi()) {
    await handleSnapshotReload();
    if (options.trigger !== 'auto') {
      scheduleAutoRefresh();
    }
    return;
  }

  state.refreshInFlight = true;
  startRefreshUiTimer(mode);
  dom.refreshButton.disabled = true;
  if (dom.refreshFullButton) {
    dom.refreshFullButton.disabled = true;
  }
  const button = dom.refreshButton;
  const initialPrimaryLabel = dom.refreshButton.textContent;
  const initialFullLabel = dom.refreshFullButton ? dom.refreshFullButton.textContent : '';
  button.textContent = t('refresh.running');
  setRefreshStatus(options.trigger === 'auto' ? t('autoRefresh.running') : t(mode === 'full' ? 'refresh.full' : 'refresh.filtered'));
  renderRefreshDetail({ status: 'running', scope: mode, message: t(mode === 'full' ? 'refresh.queuedFull' : 'refresh.queuedFiltered') });

  try {
    const refreshEndpoint = getRefreshEndpoint();
    if (!refreshEndpoint) {
      throw new Error(t('refresh.apiUnavailableDetail'));
    }
    const filters = buildRefreshFilters();
    const response = await fetch(refreshEndpoint, {
      method: 'POST',
      headers: buildRefreshApiHeaders({
        'Content-Type': 'application/json',
      }),
      body: JSON.stringify({ source: 'ui', mode, filters }),
    });

    if (response.status === 404 || response.status === 405) {
      await loadAllData(true);
      setRefreshStatus(t('refresh.apiUnavailable'));
      renderRefreshDetail({ status: 'completed', scope: mode, message: t('refresh.apiUnavailableDetail') });
      if (!state.lastRefreshAt && state.metrics?.generated_at) {
        state.lastRefreshAt = state.metrics.generated_at;
      }
      setRefreshMeta(buildRefreshSuccessMeta(state.lastRefreshAt));
      return;
    }

    const payload = await response.json().catch(() => ({}));
    if (!response.ok && response.status !== 202) {
      const message = buildRefreshErrorMessage(payload, t('refresh.failed'), response.status);
      throw new Error(message);
    }

    state.refreshJobId = payload.job_id || null;
    renderRefreshDetail(payload);
    if (payload.status === 'running') {
      setRefreshStatus(payload.message || t('refresh.inProgress'));
      const result = await waitForRefreshCompletion(state.refreshJobId, mode);
      if (result.status !== 'completed') {
        throw new Error(buildRefreshErrorMessage(result, t('refresh.failed')));
      }
      await applyRefreshSuccess(result);
      return;
    }

    if (payload.status === 'completed') {
      await applyRefreshSuccess(payload);
      return;
    }

    if (payload.status === 'failed') {
      throw new Error(buildRefreshErrorMessage(payload, t('refresh.failed'), response.status));
    }
  } catch (error) {
    if (hasRefreshApi() && error instanceof TypeError) {
      error = new Error(pickLocalized('Backend request failed. Check URL, CORS and whether the service is awake.', 'Запрос к backend не прошёл. Проверьте URL, CORS и не спит ли сервис.'));
    }
    await loadAllData(true);
    setRefreshStatus(error.message || t('refresh.failed'), true);
    renderRefreshDetail({
      status: 'failed',
      scope: mode,
      message: error.message || t('refresh.failed'),
      finished_at: new Date().toISOString(),
    });
    if (!state.lastRefreshAt && state.metrics?.generated_at) {
      state.lastRefreshAt = state.metrics.generated_at;
    }
    setRefreshMeta(buildRefreshSuccessMeta(state.lastRefreshAt));
  } finally {
    stopRefreshUiTimer();
    state.refreshInFlight = false;
    dom.refreshButton.disabled = false;
    if (dom.refreshFullButton) {
      dom.refreshFullButton.disabled = false;
      dom.refreshFullButton.textContent = initialFullLabel;
    }
    dom.refreshButton.textContent = initialPrimaryLabel;
    if (options.trigger !== 'auto') {
      scheduleAutoRefresh();
    }
  }
}

async function handleSnapshotReload() {
  state.refreshInFlight = true;
  startRefreshUiTimer('filtered');
  dom.refreshButton.disabled = true;
  const initialLabel = dom.refreshButton.textContent;
  dom.refreshButton.textContent = t('refresh.running');

  try {
    await loadAllData(true);
    if (!state.lastRefreshAt && state.metrics?.generated_at) {
      state.lastRefreshAt = state.metrics.generated_at;
    }
    setRefreshStatus(t('refresh.snapshotReloaded'));
    setRefreshMeta(buildRefreshSuccessMeta(state.lastRefreshAt));
    renderRefreshDetail(null);
  } catch (error) {
    setRefreshStatus(error.message || t('refresh.snapshotReloadFailed'), true);
  } finally {
    stopRefreshUiTimer();
    dom.refreshButton.disabled = false;
    dom.refreshButton.textContent = initialLabel;
    syncRefreshControls();
    state.refreshInFlight = false;
  }
}

async function waitForRefreshCompletion(jobId, scope = 'filtered') {
  const refreshStatusEndpoint = getRefreshStatusEndpoint();
  if (!refreshStatusEndpoint) {
    throw new Error(t('refresh.apiUnavailableDetail'));
  }
  const maxAttempts = scope === 'full' ? REFRESH_POLL_MAX_ATTEMPTS_FULL : REFRESH_POLL_MAX_ATTEMPTS_FILTERED;
  let transientFailures = 0;
  let pollDelayMs = REFRESH_POLL_BASE_INTERVAL_MS;
  const refreshStartTime = Date.now();
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    await delay(pollDelayMs);
    let payload = null;
    let pollError = null;

    try {
      const response = await fetch(`${refreshStatusEndpoint}?job=${encodeURIComponent(jobId || '')}&t=${Date.now()}`, {
        cache: 'no-store',
        headers: buildRefreshApiHeaders(),
      });
      const responsePayload = await response.json().catch(() => ({}));
      if (!response.ok) {
        if ([502, 503, 504].includes(response.status)) {
          pollError = new Error(buildRefreshErrorMessage(responsePayload, t('refresh.statusFailed'), response.status));
        } else {
          throw new Error(buildRefreshErrorMessage(responsePayload, t('refresh.statusFailed'), response.status));
        }
      } else {
        payload = responsePayload;
      }
    } catch (error) {
      pollError = error instanceof Error ? error : new Error(t('refresh.statusFailed'));
    }

    if (!payload) {
      const health = await fetchRefreshApiHealth(getRefreshApiBaseUrl());
      state.refreshApiHealth = health;
      payload = buildRefreshStateFromHealth(health, jobId);
      if (!payload) {
        transientFailures += 1;
        if (transientFailures >= REFRESH_POLL_MAX_TRANSIENT_FAILURES) {
          throw pollError || new Error(t('refresh.statusFailed'));
        }
        pollDelayMs = Math.min(REFRESH_POLL_MAX_INTERVAL_MS, REFRESH_POLL_BASE_INTERVAL_MS + transientFailures * 500);
        setRefreshStatus(
          pickLocalized(
            'Refresh status polling hit a transient backend error. Retrying...',
            'Polling статуса обновления временно сбоит на backend. Повторяю...'
          )
        );
        continue;
      }
    }

    transientFailures = 0;
    renderRefreshDetail(payload);
    if (payload.status === 'running') {
      const elapsedSec = Math.round((Date.now() - refreshStartTime) / 1000);
      const baseMsg = payload.message || t('refresh.dataInProgress');
      const progressHint = attempt > maxAttempts * 0.7 ? pickLocalized(' (это может занять несколько минут)', ' (это может занять несколько минут)') : '';
      setRefreshStatus(elapsedSec > 2 ? `${baseMsg} (${elapsedSec}s)${progressHint}` : baseMsg);
      pollDelayMs = resolveRefreshPollDelayMs(payload, attempt);
      continue;
    }
    return payload;
  }
  const elapsedSec = Math.round((Date.now() - refreshStartTime) / 1000);
  throw new Error(pickLocalized(
    `Refresh timed out after ${elapsedSec}s. The backend may still be running.`,
    `Обновление превысило лимит ожидания (${elapsedSec}с). Backend всё ещё может работать.`
  ));
}

async function fetchRefreshResult(jobId) {
  const refreshResultEndpoint = getRefreshResultEndpoint();
  if (!refreshResultEndpoint || !jobId) {
    throw new Error(t('refresh.apiUnavailableDetail'));
  }
  const response = await fetch(`${refreshResultEndpoint}?job=${encodeURIComponent(jobId)}&t=${Date.now()}`, {
    cache: 'no-store',
    headers: buildRefreshApiHeaders(),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(buildRefreshErrorMessage(payload, t('refresh.failed'), response.status));
  }
  return payload;
}

async function applySnapshotState(snapshot, refreshState = null, options = {}) {
  state.cross = Array.isArray(snapshot?.cross) ? snapshot.cross : [];
  state.crossOverlapIndex = buildCrossOverlapIndex(state.cross);
  invalidateCrossOpportunityCaches();
  state.funding = Array.isArray(snapshot?.funding) ? snapshot.funding : [];
  state.metrics = snapshot?.metrics && typeof snapshot.metrics === 'object' && !Array.isArray(snapshot.metrics) ? snapshot.metrics : null;
  state.adapterHealth = snapshot?.adapterHealth && typeof snapshot.adapterHealth === 'object' && !Array.isArray(snapshot.adapterHealth) ? snapshot.adapterHealth : null;
  invalidateAiAnalyticsCaches();
  // Seed SWR cache from WebSocket snapshot so subsequent loads serve fresh data
  if (Array.isArray(snapshot?.cross) && snapshot.cross.length) swrSetCached('cross', snapshot.cross);
  if (Array.isArray(snapshot?.funding) && snapshot.funding.length) swrSetCached('funding', snapshot.funding);
  if (state.metrics) swrSetCached('metrics', state.metrics);
  if (state.adapterHealth) swrSetCached('adapterHealth', state.adapterHealth);
  updateDatasetStatus('cross', { loaded: true, loading: false, error: '' });
  updateDatasetStatus('metrics', { loaded: Boolean(state.metrics), loading: false, error: '' });
  if (snapshot?.funding) {
    updateDatasetStatus('funding', { loaded: true, loading: false, error: '' });
  }
  if (snapshot?.adapterHealth) {
    updateDatasetStatus('adapterHealth', { loaded: true, loading: false, error: '' });
  }
  state.refreshState = refreshState && typeof refreshState === 'object' && !Array.isArray(refreshState) ? refreshState : state.refreshState;
  state.refreshApiStatus = state.refreshState;
  const requestedOrigin = typeof options?.origin === 'string' && options.origin ? options.origin : null;
  state.dataOrigin = requestedOrigin || (snapshot?.scope === 'filtered' ? 'ephemeral-filtered' : 'persistent');
  if (hasRefreshApi()) {
    state.refreshApiHealth = await fetchRefreshApiHealth(getRefreshApiBaseUrl());
  }
  if (state.refreshState?.finished_at) {
    state.lastRefreshAt = state.refreshState.finished_at;
  } else if (state.metrics?.generated_at) {
    state.lastRefreshAt = state.metrics.generated_at;
  }
  invalidateCrossHistoryAnalytics({ resetIndices: true });
  hydrateDynamicSettings();
  scheduleDatasetCachePersist();
  setLastUpdated();
  syncRefreshFeedback();
  renderBackendDiagnostics();
  renderAll();
}

async function applyRefreshSuccess(payload) {
  let appliedViaWebSocket = false;
  if (state.marketStreamAvailable) {
    appliedViaWebSocket = await waitForMarketStreamSnapshot(2200, Date.now());
  }
  if (!appliedViaWebSocket) {
    if (payload?.job_id && payload?.status === 'completed' && payload?.result_available !== false) {
      try {
        const snapshot = await fetchRefreshResult(payload.job_id);
        await applySnapshotState(snapshot, payload);
      } catch {
        await loadAllData(true);
      }
    } else {
      await loadAllData(true);
    }
  }
  state.lastRefreshAt = payload.finished_at || state.metrics?.generated_at || new Date().toISOString();
  setRefreshStatus(payload.message || t('refresh.success'));
  setRefreshMeta(buildRefreshSuccessMeta(state.lastRefreshAt));
  renderRefreshDetail(payload);
}

function delay(milliseconds) {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

function resolveRefreshPollDelayMs(payload, attempt = 0) {
  const suggestedMs = Number(payload?.poll_interval_ms);
  if (Number.isFinite(suggestedMs) && suggestedMs > 0) {
    return Math.max(400, Math.min(REFRESH_POLL_MAX_INTERVAL_MS, Math.round(suggestedMs)));
  }
  const scope = payload?.scope === 'full' ? 'full' : 'filtered';
  const baselineMs = scope === 'full' ? 1500 : REFRESH_POLL_BASE_INTERVAL_MS;
  const maxAttempts = scope === 'full' ? REFRESH_POLL_MAX_ATTEMPTS_FULL : REFRESH_POLL_MAX_ATTEMPTS_FILTERED;
  const progressRatio = attempt / maxAttempts;
  if (progressRatio < 0.5) {
    return Math.min(REFRESH_POLL_MAX_INTERVAL_MS, baselineMs + Math.min(attempt, 15) * 100);
  }
  if (progressRatio < 0.8) {
    return Math.min(REFRESH_POLL_MAX_INTERVAL_MS, baselineMs + 1500 + Math.min(attempt - Math.floor(maxAttempts * 0.5), 20) * 50);
  }
  return REFRESH_POLL_MAX_INTERVAL_MS;
}

function syncTheme() {
  document.body.classList.toggle('theme-dark', state.settings.theme !== 'light');
  document.body.classList.toggle('theme-light', state.settings.theme === 'light');
}

function getDatasetStatus(key) {
  return state.datasetStatus[key] || {
    loaded: false,
    loading: false,
    refreshing: false,
    error: '',
  };
}

function updateDatasetStatus(key, patch) {
  state.datasetStatus[key] = {
    ...getDatasetStatus(key),
    ...patch,
  };
}

function getTabDatasetKeys(tab) {
  if (tab === 'funding') {
    return ['funding'];
  }
  if (tab === 'glossary') {
    return ['glossary'];
  }
  if (tab === 'health') {
    return ['adapterHealth'];
  }
  return [];
}

function getTabLoadingMessage(tab) {
  if (tab === 'cross') {
    return pickLocalized('Loading cross-exchange opportunities...', 'Загрузка межбиржевых связок...');
  }
  if (tab === 'funding') {
    return pickLocalized('Loading cost-adjusted funding carry opportunities...', 'Загрузка cost-adjusted funding carry-возможностей...');
  }
  if (tab === 'ai') {
    return pickLocalized('Preparing AI analytics...', 'Подготовка ИИ-аналитики...');
  }
  if (tab === 'glossary') {
    return pickLocalized('Loading glossary...', 'Загрузка справочника...');
  }
  if (tab === 'heatmap') {
    return pickLocalized('Preparing heatmap...', 'Подготовка теплокарты...');
  }
  if (tab === 'health') {
    return pickLocalized('Loading exchange health...', 'Загрузка состояния бирж...');
  }
  return pickLocalized('Loading...', 'Загрузка...');
}

function getTabDatasetError(tab) {
  const errors = getTabDatasetKeys(tab)
    .map((key) => getDatasetStatus(key).error)
    .filter(Boolean);
  return errors[0] || '';
}

function isTabDatasetLoading(tab) {
  return getTabDatasetKeys(tab).some((key) => {
    const status = getDatasetStatus(key);
    return status.loading || (!status.loaded && !status.error);
  });
}

function resetTabContent(table, mobileList, summaryNode, gridNode) {
  if (table?.querySelector('tbody')) {
    table.querySelector('tbody').innerHTML = '';
  }
  if (mobileList) {
    mobileList.innerHTML = '';
  }
  if (summaryNode) {
    summaryNode.innerHTML = '';
  }
  if (gridNode) {
    gridNode.innerHTML = '';
  }
}

function showPanelState(node, message) {
  if (!node) {
    return;
  }
  node.textContent = message;
  node.classList.remove('hidden');
}

async function ensureDatasetsForTab(tab, force = false) {
  const datasetKeys = getTabDatasetKeys(tab);
  if (!datasetKeys.length) {
    return;
  }
  await Promise.all(datasetKeys.map((key) => loadDataset(key, force)));
}

function normalizeDatasetPayload(key, payload) {
  if (key === 'metrics' || key === 'adapterHealth') {
    return payload && typeof payload === 'object' && !Array.isArray(payload) ? payload : null;
  }
  return Array.isArray(payload) ? payload : [];
}

function applyDatasetValue(key, payload) {
  const normalizedPayload = normalizeDatasetPayload(key, payload);
  if (key === 'cross') {
    state.cross = normalizedPayload;
    state.crossOverlapIndex = buildCrossOverlapIndex(state.cross);
    invalidateCrossOpportunityCaches();
    const ids = new Set(state.cross.map((item) => item.id).filter(Boolean));
    const aiById = new Map([...state.aiById.entries()].filter(([id]) => ids.has(id)));
    state.cross.forEach((item) => {
      if (!item?.id) {
        return;
      }
      const embeddedSnapshot = normalizeEmbeddedAiSnapshot(item.ai);
      if (!embeddedSnapshot) {
        return;
      }
      aiById.set(item.id, mergeAiSnapshots(embeddedSnapshot, aiById.get(item.id)));
    });
    state.aiById = aiById;
    state.aiPendingIds = new Set([...state.aiPendingIds].filter((id) => ids.has(id)));
    state.aiAnalyticsItemCache = new Map([...state.aiAnalyticsItemCache.entries()].filter(([id]) => ids.has(id)));
    state.aiAnalyticsBundleCache = { signature: '', value: null };
    state.filteredCrossCache = { signature: '', value: [] };
    state.sortedCrossCache = { signature: '', value: [] };
    invalidateCrossHistoryAnalytics({ resetIndices: true });
    hydrateDynamicSettings();
    return;
  }
  if (key === 'funding') {
    state.funding = normalizedPayload;
    return;
  }
  if (key === 'glossary') {
    state.glossary = normalizedPayload;
    return;
  }
  if (key === 'metrics') {
    state.metrics = normalizedPayload;
    return;
  }
  if (key === 'adapterHealth') {
    state.adapterHealth = normalizedPayload;
  }
}

function hasDatasetValue(key) {
  if (key === 'metrics' || key === 'adapterHealth') {
    return Boolean(state[key] && typeof state[key] === 'object' && !Array.isArray(state[key]));
  }
  return Array.isArray(state[key]) && state[key].length > 0;
}

function buildDatasetCachePayload() {
  return {
    version: ASSET_VERSION,
    cachedAt: new Date().toISOString(),
    cross: Array.isArray(state.cross) ? state.cross : [],
    funding: Array.isArray(state.funding) ? state.funding : [],
    metrics: state.metrics && typeof state.metrics === 'object' && !Array.isArray(state.metrics) ? state.metrics : null,
    adapterHealth: state.adapterHealth && typeof state.adapterHealth === 'object' && !Array.isArray(state.adapterHealth) ? state.adapterHealth : null,
  };
}

function persistDatasetCacheSnapshot() {
  try {
    let payload = buildDatasetCachePayload();
    let serialized = JSON.stringify(payload);
    if (serialized.length > DATASET_CACHE_MAX_CHARS) {
      payload = {
        ...payload,
        cross: payload.cross.slice(0, Math.min(payload.cross.length, 250)),
        funding: payload.funding.slice(0, Math.min(payload.funding.length, 120)),
        truncated: true,
      };
      serialized = JSON.stringify(payload);
    }
    safeLocalStorageSet(DATASET_CACHE_STORAGE_KEY, serialized);
  } catch (error) {
    console.warn('Failed to persist dataset cache', error);
  }
}

function scheduleDatasetCachePersist() {
  if (state.datasetCacheWriteTimer) {
    window.clearTimeout(state.datasetCacheWriteTimer);
  }
  state.datasetCacheWriteTimer = window.setTimeout(() => {
    state.datasetCacheWriteTimer = null;
    persistDatasetCacheSnapshot();
  }, 80);
}

function restorePersistedDatasetCache() {
  const raw = safeLocalStorageGet(DATASET_CACHE_STORAGE_KEY);
  if (!raw) {
    return false;
  }
  try {
    const payload = JSON.parse(raw);
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      return false;
    }
    const cachedAtMs = Date.parse(String(payload.cachedAt || ''));
    if (Number.isFinite(cachedAtMs) && Date.now() - cachedAtMs > DATASET_CACHE_MAX_AGE_MS) {
      return false;
    }

    let restoredAny = false;
    ['cross', 'funding', 'metrics', 'adapterHealth'].forEach((key) => {
      const value = payload[key];
      if (value == null) {
        return;
      }
      if ((key === 'metrics' || key === 'adapterHealth')) {
        if (!value || typeof value !== 'object' || Array.isArray(value)) {
          return;
        }
      } else if (!Array.isArray(value)) {
        return;
      }
      applyDatasetValue(key, value);
      swrSetCached(key, value);
      updateDatasetStatus(key, {
        loaded: hasDatasetValue(key),
        loading: false,
        refreshing: false,
        error: '',
      });
      if (key === 'cross' || key === 'funding') {
        restoredAny = restoredAny || hasDatasetValue(key);
      }
    });

    if (!restoredAny) {
      return false;
    }

    state.dataOrigin = 'warm-cache';
    if (state.metrics?.generated_at) {
      state.lastRefreshAt = state.metrics.generated_at;
    } else if (Number.isFinite(cachedAtMs)) {
      state.lastRefreshAt = new Date(cachedAtMs).toISOString();
    }
    setLastUpdated();
    return true;
  } catch (error) {
    console.warn('Failed to restore dataset cache', error);
    return false;
  }
}

// ---------------------------------------------------------------------------
// SWR (Stale-While-Revalidate) helpers
// ---------------------------------------------------------------------------

function swrGetCached(key) {
  const entry = state.swrCache[key];
  return entry ? entry.data : undefined;
}

function swrSetCached(key, data) {
  state.swrCache[key] = {
    data,
    updatedAt: Date.now(),
  };
}

function swrIsFresh(key, maxAgeMs) {
  const entry = state.swrCache[key];
  if (!entry) return false;
  return Date.now() - entry.updatedAt < maxAgeMs;
}

function swrDedupFetch(key, fetcher) {
  const inFlight = state.swrInFlight[key];
  if (inFlight) {
    const elapsed = Date.now() - inFlight.startedAt;
    if (elapsed < SWR_DEDUP_INTERVAL_MS) {
      return inFlight.promise;
    }
  }
  const promise = fetcher().finally(() => {
    delete state.swrInFlight[key];
  });
  state.swrInFlight[key] = { promise, startedAt: Date.now() };
  return promise;
}

function swrRevalidateAll() {
  const keys = Object.keys(DATA_FILES);
  keys.forEach((key) => {
    void loadDataset(key, true);
  });
}

function swrHandleVisibilityChange() {
  if (document.visibilityState !== 'visible') return;
  const now = Date.now();
  if (now - state.swrLastFocusRevalidateAt < SWR_FOCUS_THROTTLE_MS) return;
  state.swrLastFocusRevalidateAt = now;
  if (hasFreshMarketStreamData()) return;
  swrRevalidateAll();
}

function swrHandleOnline() {
  const now = Date.now();
  if (now - state.swrLastReconnectRevalidateAt < SWR_RECONNECT_THROTTLE_MS) return;
  state.swrLastReconnectRevalidateAt = now;
  swrRevalidateAll();
}

function swrInit() {
  document.addEventListener('visibilitychange', swrHandleVisibilityChange);
  window.addEventListener('online', swrHandleOnline);
}

async function loadLatestCrossFallbackSnapshot(force = false) {
  const [historyIndex, historyDownsampledIndex] = await Promise.all([
    fetchJson(buildDataRequestUrl(DATA_FILES.historyIndex, force)),
    fetchJson(buildDataRequestUrl(DATA_FILES.historyDownsampledIndex, force)),
  ]);
  const mergedEntries = [...(Array.isArray(historyIndex) ? historyIndex : []), ...(Array.isArray(historyDownsampledIndex) ? historyDownsampledIndex : [])]
    .filter((entry) => entry && typeof entry === 'object' && entry.file)
    .sort((left, right) => parseSnapshotTimestamp(left.timestamp) - parseSnapshotTimestamp(right.timestamp));
  const uniqueEntries = [];
  const seenFiles = new Set();
  for (let index = mergedEntries.length - 1; index >= 0; index -= 1) {
    const entry = mergedEntries[index];
    const fileName = String(entry.file || '').trim();
    if (!fileName || seenFiles.has(fileName)) {
      continue;
    }
    seenFiles.add(fileName);
    uniqueEntries.push(entry);
    if (uniqueEntries.length >= 6) {
      break;
    }
  }

  for (const entry of uniqueEntries) {
    const snapshot = await fetchJson(buildDataRequestUrl(`./data/history/${entry.file}`, force));
    if (Array.isArray(snapshot) && snapshot.length) {
      return snapshot;
    }
  }
  return [];
}

async function loadDataset(key, force = false) {
  const status = getDatasetStatus(key);
  const filePath = DATA_FILES[key];
  if (!filePath) {
    return null;
  }

  // SWR: return cached data immediately on non-forced calls, revalidate in background
  if (!force && status.loaded && hasDatasetValue(key)) {
    const cached = swrGetCached(key);
    if (cached !== undefined) {
      // Stale-while-revalidate: serve cached, kick off background refresh
      if (!state.datasetPromises[key]) {
        state.datasetPromises[key] = (async () => {
          try {
            const payload = await swrDedupFetch(key, () => fetchJson(buildDataRequestUrl(filePath, true), { allowHttpFallback: false }));
            if (payload != null) {
              applyDatasetValue(key, payload);
              swrSetCached(key, payload);
              updateDatasetStatus(key, { loaded: true, loading: false, refreshing: false, error: '' });
              scheduleDatasetCachePersist();
              setLastUpdated();
              if (key === 'metrics' || key === 'cross') {
                renderAll();
              } else {
                renderCurrentTab();
              }
            }
          } catch {
            // Keep showing stale data on background revalidation error
          } finally {
            delete state.datasetPromises[key];
          }
        })();
      }
      return cached;
    }
    return key === 'metrics' || key === 'adapterHealth' ? state[key] : state[key] || [];
  }

  // Deduplicate concurrent requests for the same key
  if (state.datasetPromises[key]) {
    return state.datasetPromises[key];
  }

  const hasWarmValue = hasDatasetValue(key);
  updateDatasetStatus(key, {
    loaded: status.loaded || hasWarmValue,
    loading: !hasWarmValue,
    refreshing: hasWarmValue,
    error: '',
  });
  if (key === 'cross' || key === 'metrics') {
    renderAll();
  } else if (state.activeTab === 'health' || state.activeTab === 'funding' || state.activeTab === 'glossary') {
    renderCurrentTab();
  }

  state.datasetPromises[key] = (async () => {
    try {
      let payload = await swrDedupFetch(key, () => fetchJson(buildDataRequestUrl(filePath, force), { allowHttpFallback: false }));
      if (key === 'cross' && Array.isArray(payload) && !payload.length) {
        const hasCurrentCrossState = Array.isArray(state.cross) && state.cross.length > 0;
        if (hasCurrentCrossState) {
          payload = state.cross;
          if (state.marketStreamConnected) {
            state.dataOrigin = 'websocket';
          }
        } else if (!state.marketStreamAvailable) {
          const fallbackPayload = await loadLatestCrossFallbackSnapshot(force);
          if (fallbackPayload.length) {
            payload = fallbackPayload;
            state.dataOrigin = 'historical-fallback';
          }
        }
      }
      applyDatasetValue(key, payload);
      swrSetCached(key, payload);
      updateDatasetStatus(key, { loaded: true, loading: false, refreshing: false, error: '' });
      scheduleDatasetCachePersist();
      setLastUpdated();
      if (key === 'metrics' || key === 'cross') {
        renderAll();
      } else {
        renderCurrentTab();
      }
      return payload;
    } catch (error) {
      const message = error instanceof Error && error.message ? error.message : pickLocalized('Failed to load data.', 'Не удалось загрузить данные.');
      updateDatasetStatus(key, {
        loaded: hasDatasetValue(key),
        loading: false,
        refreshing: false,
        error: hasDatasetValue(key) ? '' : message,
      });
      renderCurrentTab();
      return null;
    } finally {
      delete state.datasetPromises[key];
    }
  })();

  return state.datasetPromises[key];
}

async function loadAllData(force = false) {
  const [cross, metrics, refreshState, refreshHealth] = await Promise.all([
    loadDataset('cross', force),
    loadDataset('metrics', force),
    hasRefreshApi() ? fetchRefreshApiStatus(force) : Promise.resolve(null),
    hasRefreshApi() ? fetchRefreshApiHealth(getRefreshApiBaseUrl()) : Promise.resolve(null),
  ]);

  invalidateCrossHistoryAnalytics({ resetIndices: true });
  state.refreshState = refreshState && typeof refreshState === 'object' && !Array.isArray(refreshState) ? refreshState : null;
  state.refreshApiStatus = state.refreshState;
  state.refreshApiHealth = refreshHealth && typeof refreshHealth === 'object' && !Array.isArray(refreshHealth) ? refreshHealth : state.refreshApiHealth;
  if ((cross !== null || metrics !== null) && state.dataOrigin !== 'historical-fallback') {
    state.dataOrigin = 'persistent';
  }
  if (state.refreshState?.finished_at) {
    state.lastRefreshAt = state.refreshState.finished_at;
  } else if (!state.lastRefreshAt && state.metrics?.generated_at) {
    state.lastRefreshAt = state.metrics.generated_at;
  }
  setLastUpdated();
  syncRefreshFeedback();
  renderBackendDiagnostics();
  renderAll();

  const keysToRefresh = ['funding', 'adapterHealth'];
  keysToRefresh.forEach((key) => {
    void loadDataset(key, true);
  });
}

async function fetchJson(path, options = {}) {
  const { allowHttpFallback = true, defaultValue = [] } = options;
  const response = await fetch(path, { cache: 'no-store' });
  if (!response.ok) {
    if (allowHttpFallback) {
      return defaultValue;
    }
    throw new Error(`${response.status} ${response.statusText || pickLocalized('Request failed', 'Запрос завершился ошибкой')}`.trim());
  }
  return response.json();
}

async function fetchAiJson(path, options = {}) {
  const url = buildAiApiUrl(path);
  if (!url) {
    return null;
  }
  const { timeoutMs = AI_REQUEST_TIMEOUT_MS, ...fetchOptions } = options;
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      cache: 'no-store',
      signal: controller.signal,
      headers: {
        Accept: 'application/json',
        ...(fetchOptions.body ? { 'Content-Type': 'application/json' } : {}),
        ...(fetchOptions.headers || {}),
      },
      ...fetchOptions,
    });
    if (!response.ok) {
      return null;
    }
    const payload = await response.json().catch(() => null);
    return payload && typeof payload === 'object' && !Array.isArray(payload) ? payload : null;
  } catch {
    return null;
  } finally {
    window.clearTimeout(timeoutId);
  }
}

function getAiSnapshot(itemId) {
  return state.aiById.get(itemId) || null;
}

function numberOrNull(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function normalizeEmbeddedAiPrediction(prediction, fallbackHorizon = null) {
  if (!prediction || typeof prediction !== 'object' || Array.isArray(prediction)) {
    return null;
  }
  const horizonMinutes = numberOrNull(prediction.horizon_minutes ?? fallbackHorizon);
  return {
    horizon_minutes: horizonMinutes,
    direction: prediction.direction || null,
    probability: numberOrNull(prediction.probability),
    predicted_change: numberOrNull(prediction.predicted_change),
    confidence_interval: Array.isArray(prediction.confidence_interval)
      ? prediction.confidence_interval.map((value) => Number(value)).filter((value) => Number.isFinite(value))
      : null,
  };
}

function normalizeEmbeddedAiRiskLevel(riskScore) {
  const numericRisk = numberOrNull(riskScore);
  if (numericRisk == null) {
    return null;
  }
  if (numericRisk >= 0.68) {
    return 'high';
  }
  if (numericRisk >= 0.38) {
    return 'medium';
  }
  return 'low';
}

function normalizeEmbeddedAiRecommendation(rawAi) {
  const confidenceScore = numberOrNull(rawAi?.confidence_score);
  const waitMinutes = numberOrNull(rawAi?.entry?.wait_minutes);
  const riskScore = numberOrNull(rawAi?.risk_score);
  if (confidenceScore != null && confidenceScore >= 72 && (waitMinutes == null || waitMinutes <= 5) && (riskScore == null || riskScore <= 0.32)) {
    return pickLocalized('Enter now', 'Можно входить сейчас');
  }
  if (confidenceScore != null && confidenceScore >= 58 && (waitMinutes == null || waitMinutes <= 25)) {
    return pickLocalized('Scale in carefully', 'Входить постепенно');
  }
  return pickLocalized('Wait for cleaner setup', 'Подождать более чистый вход');
}

function normalizeEmbeddedAiSnapshot(rawAi) {
  if (!rawAi || typeof rawAi !== 'object' || Array.isArray(rawAi)) {
    return null;
  }
  const predictions = Object.fromEntries(
    Object.entries(rawAi.predictions || {})
      .map(([horizon, prediction]) => [Number(horizon), normalizeEmbeddedAiPrediction(prediction, Number(horizon))])
      .filter(([, prediction]) => prediction),
  );
  const selectedHorizon = normalizeAiPredictionHorizon(state?.settings?.aiPredictionHorizon);
  const fallbackPrediction = normalizeEmbeddedAiPrediction(rawAi.prediction, selectedHorizon);
  const selectedPrediction = predictions[selectedHorizon] || fallbackPrediction || predictions[AI_DEFAULT_PREDICTION_HORIZON] || null;
  const confidenceScore = numberOrNull(rawAi.confidence_score);
  const successProbability = numberOrNull(rawAi.success_probability);
  const riskScore = numberOrNull(rawAi.risk_score);
  const advice = normalizeAdviceResponse(rawAi.advice);
  return {
    confidence: {
      score: confidenceScore,
      factors: Array.isArray(rawAi.confidence_factors) ? rawAi.confidence_factors : [],
      recommendation: normalizeEmbeddedAiRecommendation(rawAi),
      success_probability: successProbability,
      risk_score: riskScore,
      dynamic_cost_bps: numberOrNull(rawAi.dynamic_cost_bps),
      adaptive_weights: rawAi.adaptive_weights && typeof rawAi.adaptive_weights === 'object' ? rawAi.adaptive_weights : {},
    },
    prediction: selectedPrediction,
    predictions,
    whale: {
      index: numberOrNull(rawAi?.indicators?.whale_activity_score),
      recent_whales: [],
    },
    liquidation: {
      risk_level: normalizeEmbeddedAiRiskLevel(riskScore),
      risk_score: riskScore,
      danger_levels: [],
    },
    advice,
    execution: {
      entry: rawAi.entry && typeof rawAi.entry === 'object' ? { ...rawAi.entry } : {},
      exit: rawAi.exit && typeof rawAi.exit === 'object' ? { ...rawAi.exit } : {},
      holding: rawAi.holding && typeof rawAi.holding === 'object' ? { ...rawAi.holding } : {},
      indicators: rawAi.indicators && typeof rawAi.indicators === 'object' ? { ...rawAi.indicators } : {},
      confidence_score: confidenceScore,
      success_probability: successProbability,
      risk_score: riskScore,
      dynamic_cost_bps: numberOrNull(rawAi.dynamic_cost_bps),
      dynamic_liquidity_threshold_usd: numberOrNull(rawAi.dynamic_liquidity_threshold_usd),
      adaptive_weights: rawAi.adaptive_weights && typeof rawAi.adaptive_weights === 'object' ? rawAi.adaptive_weights : {},
      model_version: rawAi.model_version || null,
      source: rawAi.source || 'snapshot',
    },
    requestedHorizon: selectedHorizon,
    complete: Boolean(advice),
    updatedAt: rawAi.updated_at || null,
    source: rawAi.source || 'snapshot',
  };
}

function normalizeExecutionResponse(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }
  return {
    entry: payload.entry && typeof payload.entry === 'object' ? { ...payload.entry } : {},
    exit: payload.exit && typeof payload.exit === 'object' ? { ...payload.exit } : {},
    holding: payload.holding && typeof payload.holding === 'object' ? { ...payload.holding } : {},
    indicators: payload.indicators && typeof payload.indicators === 'object' ? { ...payload.indicators } : {},
    confidence_score: numberOrNull(payload.confidence_score),
    success_probability: numberOrNull(payload.success_probability),
    risk_score: numberOrNull(payload.risk_score),
    dynamic_cost_bps: numberOrNull(payload.dynamic_cost_bps),
    dynamic_liquidity_threshold_usd: numberOrNull(payload.dynamic_liquidity_threshold_usd),
    adaptive_weights: payload.adaptive_weights && typeof payload.adaptive_weights === 'object' ? payload.adaptive_weights : {},
    model_version: payload.model_version || null,
    source: payload.source || null,
  };
}

function normalizeAdviceResponse(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }
  const normalizeList = (value) => Array.isArray(value)
    ? value.map((entry) => String(entry || '').trim()).filter(Boolean)
    : [];
  const normalizeModal = (value, fallback) => {
    if ((!value || typeof value !== 'object' || Array.isArray(value)) && !fallback) {
      return null;
    }
    const modalPayload = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    const fallbackPayload = fallback && typeof fallback === 'object' ? fallback : {};
    const highlights = Array.isArray(modalPayload.highlights)
      ? modalPayload.highlights
        .map((entry) => {
          if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
            return null;
          }
          const label = String(entry.label || '').trim();
          const metricValue = String(entry.value || '').trim();
          const tone = ['positive', 'neutral', 'warning', 'negative'].includes(String(entry.tone || '').trim())
            ? String(entry.tone || '').trim()
            : 'neutral';
          return label && metricValue ? { label, value: metricValue, tone } : null;
        })
        .filter(Boolean)
      : Array.isArray(fallbackPayload.highlights)
        ? fallbackPayload.highlights
        : [];
    const sections = Array.isArray(modalPayload.sections)
      ? modalPayload.sections
        .map((entry) => {
          if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
            return null;
          }
          const id = String(entry.id || '').trim() || `section-${Math.random().toString(16).slice(2, 8)}`;
          const title = String(entry.title || '').trim();
          const items = normalizeList(entry.items);
          return title && items.length ? { id, title, items } : null;
        })
        .filter(Boolean)
      : Array.isArray(fallbackPayload.sections)
        ? fallbackPayload.sections
        : [];
    const modal = {
      headline: modalPayload.headline ? String(modalPayload.headline) : String(fallbackPayload.headline || ''),
      summary: modalPayload.summary ? String(modalPayload.summary) : String(fallbackPayload.summary || ''),
      verdict: modalPayload.verdict ? String(modalPayload.verdict) : String(fallbackPayload.verdict || ''),
      highlights,
      sections,
      model_version: modalPayload.model_version || fallbackPayload.model_version || null,
      source: modalPayload.source || fallbackPayload.source || null,
      provider: modalPayload.provider || fallbackPayload.provider || null,
    };
    return modal.headline || modal.summary || modal.verdict || modal.highlights.length || modal.sections.length ? modal : null;
  };
  const normalized = {
    summary: payload.summary ? String(payload.summary) : '',
    verdict: payload.verdict ? String(payload.verdict) : '',
    entry_focus: normalizeList(payload.entry_focus),
    holding_focus: normalizeList(payload.holding_focus),
    exit_focus: normalizeList(payload.exit_focus),
    risk_watch: normalizeList(payload.risk_watch),
    next_actions: normalizeList(payload.next_actions),
    model_version: payload.model_version || null,
    source: payload.source || null,
    provider: payload.provider || null,
  };
  const synthesizedModalSections = [
    {
      id: 'entry',
      title: 'Entry and hold plan',
      items: [...normalized.entry_focus, ...normalized.holding_focus],
    },
    {
      id: 'exit',
      title: 'Exit target',
      items: normalized.exit_focus,
    },
    {
      id: 'risk',
      title: 'Risk watch',
      items: normalized.risk_watch,
    },
    {
      id: 'actions',
      title: 'Next actions',
      items: normalized.next_actions,
    },
  ].filter((section) => section.items.length);
  normalized.modal = normalizeModal(payload.modal, {
    summary: normalized.summary,
    verdict: normalized.verdict,
    sections: synthesizedModalSections,
    source: normalized.source,
    provider: normalized.provider,
    model_version: normalized.model_version,
  });
  return normalized;
}

function mergeAiSnapshots(baseSnapshot, overrideSnapshot) {
  if (!baseSnapshot && !overrideSnapshot) {
    return null;
  }
  const base = baseSnapshot || {};
  const override = overrideSnapshot || {};
  return {
    ...base,
    ...override,
    confidence: {
      ...(base.confidence || {}),
      ...(override.confidence || {}),
    },
    prediction: override.prediction || base.prediction || null,
    predictions: {
      ...(base.predictions || {}),
      ...(override.predictions || {}),
    },
    whale: {
      ...(base.whale || {}),
      ...(override.whale || {}),
    },
    liquidation: {
      ...(base.liquidation || {}),
      ...(override.liquidation || {}),
    },
    smartRoute: {
      ...(base.smartRoute || {}),
      ...(override.smartRoute || {}),
    },
    advice: {
      ...(base.advice || {}),
      ...(override.advice || {}),
      entry_focus: Array.isArray((override.advice || {}).entry_focus)
        ? (override.advice || {}).entry_focus
        : Array.isArray((base.advice || {}).entry_focus)
          ? (base.advice || {}).entry_focus
          : [],
      holding_focus: Array.isArray((override.advice || {}).holding_focus)
        ? (override.advice || {}).holding_focus
        : Array.isArray((base.advice || {}).holding_focus)
          ? (base.advice || {}).holding_focus
          : [],
      exit_focus: Array.isArray((override.advice || {}).exit_focus)
        ? (override.advice || {}).exit_focus
        : Array.isArray((base.advice || {}).exit_focus)
          ? (base.advice || {}).exit_focus
          : [],
      risk_watch: Array.isArray((override.advice || {}).risk_watch)
        ? (override.advice || {}).risk_watch
        : Array.isArray((base.advice || {}).risk_watch)
          ? (base.advice || {}).risk_watch
          : [],
      next_actions: Array.isArray((override.advice || {}).next_actions)
        ? (override.advice || {}).next_actions
        : Array.isArray((base.advice || {}).next_actions)
          ? (base.advice || {}).next_actions
          : [],
      modal: (override.advice || {}).modal || (base.advice || {}).modal || null,
    },
    execution: {
      ...(base.execution || {}),
      ...(override.execution || {}),
      entry: {
        ...((base.execution || {}).entry || {}),
        ...((override.execution || {}).entry || {}),
      },
      exit: {
        ...((base.execution || {}).exit || {}),
        ...((override.execution || {}).exit || {}),
      },
      holding: {
        ...((base.execution || {}).holding || {}),
        ...((override.execution || {}).holding || {}),
      },
      indicators: {
        ...((base.execution || {}).indicators || {}),
        ...((override.execution || {}).indicators || {}),
      },
    },
  };
}

function invalidateCrossOpportunityCaches(itemIds = null) {
  state.crossOpportunityRevision += 1;
  state.filteredCrossCache = { signature: '', value: [] };
  state.sortedCrossCache = { signature: '', value: [] };
  if (!itemIds) {
    state.crossOpportunityItemCache = new Map();
    return;
  }
  const ids = [...new Set(itemIds.filter(Boolean))];
  if (!ids.length) {
    return;
  }
  ids.forEach((id) => state.crossOpportunityItemCache.delete(id));
}

function invalidateAiAnalyticsCaches(itemIds = null) {
  state.aiAnalyticsBundleCache = { signature: '', value: null };
  if (!itemIds) {
    state.aiAnalyticsItemCache = new Map();
    return;
  }
  const ids = [...new Set(itemIds.filter(Boolean))];
  if (!ids.length) {
    return;
  }
  ids.forEach((id) => state.aiAnalyticsItemCache.delete(id));
}

function invalidateCrossHistoryAnalytics(options = {}) {
  state.crossHistoryAnalytics = new Map();
  state.crossHistoryPendingIds = new Set();
  invalidateAiAnalyticsCaches();
  invalidateCrossOpportunityCaches();
  if (options.resetIndices) {
    state.historyIndex = [];
    state.historyDownsampledIndex = [];
    state.historyIndexLoaded = false;
  }
}

async function ensureHistoryIndexLoaded(force = false) {
  if (!force && state.historyIndexLoaded) {
    return;
  }
  const [historyIndex, historyDownsampledIndex] = await Promise.all([
    fetchJson(buildDataRequestUrl(DATA_FILES.historyIndex, force)),
    fetchJson(buildDataRequestUrl(DATA_FILES.historyDownsampledIndex, force)),
  ]);
  state.historyIndex = Array.isArray(historyIndex) ? historyIndex : [];
  state.historyDownsampledIndex = Array.isArray(historyDownsampledIndex) && historyDownsampledIndex.length
    ? historyDownsampledIndex
    : state.historyIndex;
  state.historyIndexLoaded = true;
}

async function ensureCrossHistoryAnalytics(ids = []) {
  const requestedIds = [...new Set(ids.filter(Boolean))]
    .filter((id) => !state.crossHistoryAnalytics.has(id) && !state.crossHistoryPendingIds.has(id));
  if (!requestedIds.length) {
    return;
  }

  requestedIds.forEach((id) => state.crossHistoryPendingIds.add(id));

  try {
    await ensureHistoryIndexLoaded();
    if (!state.historyDownsampledIndex.length) {
      return;
    }

    const cutoff = Date.now() - Math.max(1, Number(state.settings.sparklineDays || 7)) * 24 * 60 * 60 * 1000;
    const files = state.historyDownsampledIndex
      .filter((entry) => parseSnapshotTimestamp(entry.timestamp) >= cutoff)
      .slice(-Math.min(state.sparklineLimit, 36));
    const seriesById = new Map(
      requestedIds.map((id) => [
        id,
        {
          timestamps: [],
          spread: [],
          net: [],
          fundingDelta: [],
          minVolume: [],
        },
      ]),
    );

    for (const entry of files) {
      const snapshot = await loadHistorySnapshot(entry.file);
      const timestamp = parseSnapshotTimestamp(entry.timestamp);
      snapshot.forEach((row) => {
        if (!seriesById.has(row.id)) {
          return;
        }
        const bucket = seriesById.get(row.id);
        bucket.timestamps.push(timestamp);
        bucket.spread.push(Number(row.spread || 0));
        bucket.net.push(Number(row.net_spread ?? row.netSpread ?? row.spread ?? 0));
        bucket.fundingDelta.push(Number((row.fundingB || 0) - (row.fundingA || 0)));
        bucket.minVolume.push(Math.min(Number(row.volumeA || 0), Number(row.volumeB || 0)));
      });
    }

    seriesById.forEach((series, id) => {
      state.crossHistoryAnalytics.set(id, summarizeCrossHistory(series));
    });
    invalidateAiAnalyticsCaches(requestedIds);
    invalidateCrossOpportunityCaches(requestedIds);
  } finally {
    requestedIds.forEach((id) => state.crossHistoryPendingIds.delete(id));
    if (state.activeDetailId && requestedIds.includes(state.activeDetailId)) {
      rerenderActiveDetail();
    }
    if (['cross', 'ai', 'heatmap'].includes(state.activeTab)) {
      if (state.activeTab === 'ai') {
        scheduleAiAnalyticsDeferredRender();
      } else {
        renderCurrentTab();
      }
    }
  }
}

function buildAiForecastText(item) {
  if (!item.aiForecastDirection || item.aiForecastProbability == null) {
    return pickLocalized('Pending', 'Ожидание');
  }
  const direction = item.aiForecastDirection === 'UP' ? '↑' : item.aiForecastDirection === 'DOWN' ? '↓' : '→';
  return `${direction} ${formatNumber(item.aiForecastProbability * 100, 0)}%`;
}

function buildAiForecastSummary(snapshot) {
  const prediction = snapshot?.prediction;
  if (!prediction) {
    return pickLocalized('No forecast yet', 'Прогноз ещё не загружен');
  }
  const directionLabel = prediction.direction === 'UP'
    ? pickLocalized('spread up', 'спред вверх')
    : prediction.direction === 'DOWN'
      ? pickLocalized('spread down', 'спред вниз')
      : pickLocalized('stable spread', 'стабильный спред');
  return `${directionLabel} · ${formatNumber((prediction.probability || 0) * 100, 0)}% · ${formatNumber(prediction.predicted_change || 0, 3)}%`;
}

async function fetchOpportunityAiBundle(item, options = {}) {
  const onPartial = typeof options.onPartial === 'function' ? options.onPartial : null;
  const emitPartialSnapshot = (partialSnapshot) => {
    if (!onPartial || !partialSnapshot || typeof partialSnapshot !== 'object') {
      return;
    }
    onPartial({
      ...partialSnapshot,
      updatedAt: new Date().toISOString(),
    });
  };
  const horizon = normalizeAiPredictionHorizon(state.settings.aiPredictionHorizon);
  const horizons = [...new Set([...AI_PREDICTION_HORIZON_OPTIONS, horizon])];
  const embeddedSnapshot = normalizeEmbeddedAiSnapshot(item.ai);
  const smartRoutePayload = {
    opportunity: {
      symbol: item.pairA,
      exchangeA: item.exchangeA,
      exchangeB: item.exchangeB,
      direction: item.direction,
    },
    user: {
      deposit: Number(state.settings.deposit || DEFAULT_SETTINGS.deposit),
      leverage: Number(state.settings.leverage || DEFAULT_SETTINGS.leverage),
      max_slippage_percent: 0.5,
    },
  };

  const confidencePromise = fetchAiJson(AI_CONFIDENCE_ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ exchangeA: item.exchangeA, exchangeB: item.exchangeB, symbol: item.pairA }),
    }).then((payload) => {
      if (payload) {
        emitPartialSnapshot({ confidence: payload });
      }
      return payload;
    });
  const predictionsPromise = Promise.all(
    horizons.map(async (predictionHorizon) => [
      predictionHorizon,
      await fetchAiJson(AI_PREDICT_ENDPOINT, {
        method: 'POST',
        body: JSON.stringify({ exchangeA: item.exchangeA, exchangeB: item.exchangeB, symbol: item.pairA, horizon_minutes: predictionHorizon }),
      }),
    ]),
  ).then((entries) => {
    const predictions = Object.fromEntries(entries.filter(([, payload]) => payload));
    const selectedPrediction = predictions[horizon] || predictions[AI_DEFAULT_PREDICTION_HORIZON] || null;
    if (selectedPrediction || Object.keys(predictions).length) {
      emitPartialSnapshot({
        prediction: selectedPrediction,
        predictions,
      });
    }
    return predictions;
  });
  const whalePromise = fetchAiJson(`${AI_WHALE_ENDPOINT}?symbol=${encodeURIComponent(item.pairA)}`).then((payload) => {
    if (payload) {
      emitPartialSnapshot({ whale: payload });
    }
    return payload;
  });
  const liquidationPromise = fetchAiJson(`${AI_LIQUIDATION_ENDPOINT}?symbol=${encodeURIComponent(item.pairA)}`).then((payload) => {
    if (payload) {
      emitPartialSnapshot({ liquidation: payload });
    }
    return payload;
  });
  const smartRoutePromise = fetchAiJson(AI_SMART_ROUTE_ENDPOINT, {
      method: 'POST',
      body: JSON.stringify(smartRoutePayload),
    }).then((payload) => {
      if (payload) {
        emitPartialSnapshot({ smartRoute: payload });
      }
      return payload;
    });
  const executionPromise = fetchAiJson(AI_EXECUTION_ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ exchangeA: item.exchangeA, exchangeB: item.exchangeB, symbol: item.pairA }),
    }).then((payload) => {
      const execution = normalizeExecutionResponse(payload);
      if (execution) {
        emitPartialSnapshot({ execution });
      }
      return payload;
    });
  const advicePromise = fetchAiJson(AI_ADVICE_ENDPOINT, {
      method: 'POST',
      timeoutMs: AI_ADVICE_REQUEST_TIMEOUT_MS,
      body: JSON.stringify({ exchangeA: item.exchangeA, exchangeB: item.exchangeB, symbol: item.pairA }),
    }).then((payload) => {
      const advice = normalizeAdviceResponse(payload);
      if (advice) {
        emitPartialSnapshot({ advice });
      }
      return payload;
    });

  const [confidence, predictions, whale, liquidation, smartRoute, executionPayload, advicePayload] = await Promise.all([
    confidencePromise,
    predictionsPromise,
    whalePromise,
    liquidationPromise,
    smartRoutePromise,
    executionPromise,
    advicePromise,
  ]);

  const fetchedSnapshot = {
    confidence,
    prediction: predictions[horizon] || embeddedSnapshot?.prediction || null,
    predictions,
    whale,
    liquidation,
    smartRoute,
    execution: normalizeExecutionResponse(executionPayload),
    advice: normalizeAdviceResponse(advicePayload),
    requestedHorizon: horizon,
    complete: Boolean(normalizeAdviceResponse(advicePayload) || embeddedSnapshot?.advice),
    updatedAt: new Date().toISOString(),
  };

  return mergeAiSnapshots(embeddedSnapshot, fetchedSnapshot);
}

async function ensureOpportunityAiData(item) {
  if (!hasAiApi() || !item?.id) {
    return;
  }
  if (state.aiPendingIds.has(item.id)) {
    return;
  }
  const snapshot = getAiSnapshot(item.id);
  const horizon = normalizeAiPredictionHorizon(state.settings.aiPredictionHorizon);
  if (snapshot?.nextRetryAt && snapshot.nextRetryAt > Date.now()) {
    return;
  }
  if (snapshot?.complete && snapshot?.requestedHorizon === horizon) {
    return;
  }

  state.aiPendingIds.add(item.id);
  state.aiById.set(item.id, { ...(snapshot || {}), loading: true });
  try {
    const aiBundle = await fetchOpportunityAiBundle(item, {
      onPartial: (partialSnapshot) => {
        const currentSnapshot = getAiSnapshot(item.id) || snapshot || {};
        const mergedSnapshot = mergeAiSnapshots(currentSnapshot, {
          ...partialSnapshot,
          requestedHorizon: horizon,
        });
        state.aiById.set(item.id, {
          ...mergedSnapshot,
          loading: true,
          nextRetryAt: null,
          error: '',
        });
        invalidateAiAnalyticsCaches([item.id]);
        invalidateCrossOpportunityCaches([item.id]);
        if (state.activeDetailId === item.id) {
          rerenderActiveDetail();
        }
        if (state.activeTab === 'ai') {
          scheduleAiAnalyticsDeferredRender();
        }
      },
    });
    state.aiById.set(item.id, { ...aiBundle, loading: false, nextRetryAt: aiBundle.complete ? null : Date.now() + AI_RETRY_COOLDOWN_MS, error: '' });
  } catch {
    state.aiById.set(item.id, {
      ...(snapshot || {}),
      loading: false,
      complete: false,
      requestedHorizon: horizon,
      nextRetryAt: Date.now() + AI_RETRY_COOLDOWN_MS,
      error: pickLocalized('AI request failed', 'Ошибка AI-запроса'),
    });
  } finally {
    state.aiPendingIds.delete(item.id);
    invalidateAiAnalyticsCaches([item.id]);
    invalidateCrossOpportunityCaches([item.id]);
    if (state.activeDetailId === item.id) {
      rerenderActiveDetail();
    }
    if (state.activeTab === 'ai') {
      scheduleAiAnalyticsDeferredRender();
    } else {
      scheduleCrossRender();
    }
  }
}

function isAiSnapshotStale(snapshot, item) {
  if (!snapshot || !snapshot.complete) {
    return false;
  }
  const requestedHorizon = normalizeAiPredictionHorizon(state.settings.aiPredictionHorizon);
  if (snapshot.requestedHorizon !== requestedHorizon) {
    return true;
  }
  const updatedAtMs = Number.isFinite(Date.parse(snapshot.updatedAt)) ? Date.parse(snapshot.updatedAt) : null;
  // Do not invalidate AI timing guidance on every quote tick.
  // Otherwise entry timers can keep jumping instead of converging to zero.
  if (updatedAtMs != null && Date.now() - updatedAtMs > AI_SNAPSHOT_STALE_MS) {
    return true;
  }
  return false;
}

function hydrateVisibleCrossAi(rows) {
  if (!hasAiApi()) {
    return;
  }
  rows.slice(0, AI_PREFETCH_LIMIT).forEach((item) => {
    const snapshot = getAiSnapshot(item.id);
    const horizon = normalizeAiPredictionHorizon(state.settings.aiPredictionHorizon);
    if (snapshot?.loading) {
      return;
    }
    if (snapshot?.nextRetryAt && snapshot.nextRetryAt > Date.now()) {
      return;
    }
    if (snapshot?.complete && snapshot?.requestedHorizon === horizon && !isAiSnapshotStale(snapshot, item)) {
      return;
    }
    void ensureOpportunityAiData(item);
  });
}

function clearLiveDataRenderTimer() {
  if (state.liveDataRenderTimer) {
    window.clearInterval(state.liveDataRenderTimer);
    state.liveDataRenderTimer = null;
  }
}

function refreshCrossLiveTimingCells() {
  if (!dom.crossTable) {
    return;
  }
  const rows = Array.from(dom.crossTable.querySelectorAll('tbody tr.opportunity-row[data-detail-id]'));
  if (!rows.length) {
    return;
  }
  const itemMap = new Map(state.cross.map((item) => [String(item.id), item]));

  rows.forEach((row) => {
    const item = itemMap.get(String(row.dataset.detailId));
    if (!item) {
      return;
    }

    const liveEntryWaitMinutes = getLiveEntryWaitMinutes(item);
    const entryCell = row.querySelector('td[data-column-key="entryWaitMinutes"]');
    if (entryCell) {
      const entryLabel = liveEntryWaitMinutes == null
        ? pickLocalized('n/a', 'н/д')
        : formatEtaMinutes(liveEntryWaitMinutes);
      const entryTooltip = entryCell.querySelector('.cell-overflow-tooltip');
      if (entryTooltip) {
        entryTooltip.textContent = entryLabel;
      }
      entryCell.className = liveEntryWaitMinutes != null && liveEntryWaitMinutes <= 5
        ? 'value-positive'
        : liveEntryWaitMinutes != null && liveEntryWaitMinutes <= 20
          ? 'value-warning'
          : '';
    }

    const tradeAlertCell = row.querySelector('td[data-column-key="tradeAlert"]');
    const positionCell = row.querySelector('td[data-column-key="positionState"]');
    if (tradeAlertCell || positionCell) {
      const tradeAlertModel = getTradeAlertModel(item);
      if (tradeAlertCell) {
        const content = `<span class="chip risk-chip risk-${escapeHtml(tradeAlertModel.tone)}">${escapeHtml(tradeAlertModel.title)}</span><br><small>${escapeHtml(tradeAlertModel.timerLabel)}</small>`;
        const tradeTooltip = tradeAlertCell.querySelector('.cell-overflow-tooltip');
        if (tradeTooltip) {
          tradeTooltip.innerHTML = content;
        }
      }
      if (positionCell) {
        const positionTooltip = positionCell.querySelector('.cell-overflow-tooltip');
        if (positionTooltip) {
          positionTooltip.textContent = tradeAlertModel.positionLabel;
        }
      }
    }
  });
}

function refreshLiveDataForActiveTab() {
  if (state.activeTab === 'cross') {
    refreshCrossLiveTimingCells();
    return;
  }
  if (state.activeTab === 'ai') {
    refreshAiLiveTiming();
    return;
  }
  if (state.activeDetailId) {
    rerenderActiveDetail();
  }
}

function refreshAiLiveTiming() {
  renderAiAnalyticsPage();
}

function runLiveDataTick() {
  if (document.visibilityState === 'hidden') {
    return;
  }
  renderTradeSmartNotices();
  if (state.activeTab === 'cross' || state.activeTab === 'ai' || state.activeDetailId) {
    refreshLiveDataForActiveTab();
  }
}

function scheduleLiveDataRender() {
  if (state.liveDataRenderTimer) {
    return;
  }
  state.liveDataRenderTimer = window.setInterval(() => {
    runLiveDataTick();
  }, LIVE_DATA_RENDER_INTERVAL_MS);
  runLiveDataTick();
}

function hydrateDynamicSettings() {
  refreshLocalizationCaches();
  const exchanges = getAllExchanges();
  exchanges.forEach((exchange) => {
    if (!(exchange in state.settings.fees)) {
      state.settings.fees[exchange] = DEFAULT_FEE;
    }
    if (!(exchange in state.settings.visibleExchanges)) {
      state.settings.visibleExchanges[exchange] = true;
    }
  });
  if (!state.settings.visibleCrossColumns || !Object.keys(state.settings.visibleCrossColumns).length) {
    state.settings.visibleCrossColumns = defaultCrossColumnVisibility();
  }
  CROSS_COLUMN_DEFS.forEach((column) => {
    if (!(column.key in state.settings.visibleCrossColumns)) {
      state.settings.visibleCrossColumns[column.key] = DEFAULT_CROSS_COLUMN_VISIBILITY[column.key] !== false;
    }
  });
  persistSettings();
  populateSettingsForm(exchanges);
}

function populateSettingsForm(exchanges) {
  const form = dom.settingsForm;
  populateAutoRefreshSelectOptions();
  form.elements.aiPredictionHorizon.innerHTML = AI_PREDICTION_HORIZON_OPTIONS
    .map((minutes) => `<option value="${minutes}">${escapeHtml(`${minutes} ${pickLocalized('min', 'мин')}`)}</option>`)
    .join('');
  form.elements.minSpread.value = state.settings.minSpread;
  form.elements.leverage.value = state.settings.leverage;
  form.elements.deposit.value = state.settings.deposit;
  form.elements.holdingHours.value = state.settings.holdingHours;
  form.elements.sparklineDays.value = state.settings.sparklineDays;
  if (form.elements.useExternalAiApi) {
    form.elements.useExternalAiApi.checked = Boolean(state.settings.useExternalAiApi);
  }
  if (form.elements.aiApiBaseUrl) {
    form.elements.aiApiBaseUrl.value = state.settings.aiApiBaseUrl || '';
  }
  form.elements.aiPredictionHorizon.value = String(normalizeAiPredictionHorizon(state.settings.aiPredictionHorizon));
  form.elements.minAiScore.value = Number(state.settings.minAiScore || 0);
  form.elements.maxAiEntryWaitMinutes.value = Number(state.settings.maxAiEntryWaitMinutes || 0);
  form.elements.maxAiHoldingHours.value = Number(state.settings.maxAiHoldingHours || 0);
  form.elements.autoRefreshMode.value = state.settings.autoRefreshMode;
  form.elements.autoRefreshIntervalSeconds.value = String(getAutoRefreshIntervalSeconds());
  form.elements.autoRefreshIntervalSeconds.disabled = !isAutoRefreshEnabled();
  if (form.elements.refreshApiBaseUrl) {
    form.elements.refreshApiBaseUrl.value = state.settings.refreshApiBaseUrl || '';
  }
  if (form.elements.refreshApiKey) {
    form.elements.refreshApiKey.value = state.settings.refreshApiKey || '';
    const refreshApiKeyLabel = form.elements.refreshApiKey.closest('label');
    if (refreshApiKeyLabel) {
      refreshApiKeyLabel.classList.toggle('hidden', canUseImplicitRefreshAuth());
    }
  }
  dom.crossSearch.value = state.settings.search || '';

  dom.feeGrid.innerHTML = exchanges
    .map(
      (exchange) => `
        <label>
          <span>${escapeHtml(exchange)}</span>
          <input type="number" min="0" step="0.001" data-fee-input="${escapeHtml(exchange)}" value="${state.settings.fees[exchange] ?? DEFAULT_FEE}" />
        </label>
      `,
    )
    .join('');

  dom.exchangeCheckboxes.innerHTML = exchanges
    .map(
      (exchange) => `
        <label class="checkbox-item">
          <input type="checkbox" data-exchange-checkbox="${escapeHtml(exchange)}" ${state.settings.visibleExchanges[exchange] ? 'checked' : ''} />
          <span>${escapeHtml(exchange)}</span>
        </label>
      `,
    )
    .join('');

  dom.crossColumnCheckboxes.innerHTML = CROSS_COLUMN_DEFS
    .map(
      (column) => `
        <label class="checkbox-item">
          <input type="checkbox" data-cross-column-checkbox="${escapeHtml(column.key)}" ${state.settings.visibleCrossColumns[column.key] !== false ? 'checked' : ''} />
          <span>${escapeHtml(column.label)}</span>
        </label>
      `,
    )
    .join('');

  populateCrossColumnPresets();
  syncAiApiSettingsVisibility();
  renderAiThresholdNotes();
}

let _crossPresetsFp = '';
function populateCrossColumnPresets() {
  if (!dom.crossColumnPresets) {
    return;
  }
  const activeKeys = new Set(getVisibleCrossColumns().map((column) => column.key));
  const fp = getCurrentLanguage() + ':' + [...activeKeys].sort().join(',');
  if (fp === _crossPresetsFp) {
    return;
  }
  _crossPresetsFp = fp;
  const activePreset = Object.entries(CROSS_COLUMN_PRESETS).find(([, columns]) => columns.length === activeKeys.size && columns.every((key) => activeKeys.has(key)))?.[0];
  dom.crossColumnPresets.innerHTML = [
    ['all', t('common.allColumns')],
    ['compact', t('common.compact')],
    ['profit', t('common.profitFocus')],
    ['carry', t('common.carryFocus')],
    ['liquidity', t('common.liquidity')],
  ]
    .map(
      ([key, label]) => `
        <button class="ghost-button preset-button ${activePreset === key ? 'is-active' : ''}" type="button" data-cross-preset="${escapeHtml(key)}">${escapeHtml(label)}</button>
      `,
    )
    .join('');

  dom.crossColumnPresets.querySelectorAll('[data-cross-preset]').forEach((button) => {
    button.addEventListener('click', () => applyCrossColumnPreset(button.dataset.crossPreset));
  });
}

function applyCrossColumnPreset(presetKey) {
  const columns = CROSS_COLUMN_PRESETS[presetKey] || CROSS_COLUMN_PRESETS.all;
  state.settings.visibleCrossColumns = Object.fromEntries(CROSS_COLUMN_DEFS.map((column) => [column.key, columns.includes(column.key)]));
  persistSettings();
  populateSettingsForm(getAllExchanges());
  renderAll();
}

function getAllExchanges() {
  const set = new Set();
  (state.metrics?.exchange_summary || []).forEach((item) => {
    if (item?.exchange) {
      set.add(item.exchange);
    }
  });
  [...state.cross, ...state.funding].forEach((item) => {
    if (item.exchangeA) set.add(item.exchangeA);
    if (item.exchangeB) set.add(item.exchangeB);
  });
  return [...set].sort((a, b) => a.localeCompare(b));
}

function setLastUpdated() {
  const latestRecord = state.cross[0] || state.funding[0];
  const historyLast = state.historyIndex[state.historyIndex.length - 1];
  const summary = state.metrics?.summary || {};
  const raw = state.lastRefreshAt || state.metrics?.generated_at || latestRecord?.updated_at || historyLast?.timestamp;
  dom.lastUpdated.textContent = raw ? formatDateTime(raw) : t('common.noDataYet');
  dom.statCross.textContent = String(Number(summary.cross_opportunities || state.cross.length || 0));
  dom.statFunding.textContent = String(getDatasetStatus('funding').loaded ? state.funding.length : Number(summary.funding_opportunities || state.funding.length || 0));
  dom.statExchanges.textContent = String(summary.tracked_exchanges || getAllExchanges().length);
  dom.statHealth.textContent = String(summary.healthy_exchanges || 0);
  dom.statQuarantined.textContent = String(summary.quarantined_exchanges || 0);
}

function buildCell(label, content, className = '', extraAttributes = '') {
  const classes = className ? ` class="${className}"` : '';
  return `<td data-label="${escapeHtml(label)}"${classes} ${extraAttributes}><span class="cell-overflow-tooltip" data-overflow-tooltip="true">${content}</span></td>`;
}

function aiScoreClass(score) {
  if (score == null) {
    return '';
  }
  if (score >= 80) {
    return 'ai-score-high';
  }
  if (score >= 60) {
    return 'ai-score-medium';
  }
  return 'ai-score-low';
}

function aiForecastClass(direction) {
  if (direction === 'UP') {
    return 'ai-forecast-up';
  }
  if (direction === 'DOWN') {
    return 'ai-forecast-down';
  }
  return 'ai-forecast-stable';
}

function switchTab(tab) {
  state.activeTab = tab;
  dom.tabs.forEach((button) => button.classList.toggle('is-active', button.dataset.tab === state.activeTab));
  dom.panels.forEach((panel) => panel.classList.toggle('is-active', panel.dataset.panel === state.activeTab));
  if (state.activeTab === 'ai') {
    scheduleAiAnalyticsDeferredRender();
  }
  void ensureDatasetsForTab(state.activeTab, true);
  renderCurrentTab();
}

function openSettings() {
  dom.settingsDrawer.classList.add('is-open');
  dom.settingsDrawer.setAttribute('aria-hidden', 'false');
  dom.settingsDrawer.inert = false;
  syncOverlayState();
}

function closeSettings() {
  if (dom.settingsDrawer.contains(document.activeElement)) {
    dom.settingsToggle?.focus();
  }
  dom.settingsDrawer.classList.remove('is-open');
  dom.settingsDrawer.setAttribute('aria-hidden', 'true');
  dom.settingsDrawer.inert = true;
  syncOverlayState();
}

async function resetSettings() {
  state.settings = {
    ...DEFAULT_SETTINGS,
    language: getCurrentLanguage(),
    fees: Object.fromEntries(getAllExchanges().map((exchange) => [exchange, DEFAULT_FEE])),
    visibleExchanges: Object.fromEntries(getAllExchanges().map((exchange) => [exchange, true])),
    visibleCrossColumns: defaultCrossColumnVisibility(),
  };
  currentLanguage = state.settings.language;
  await syncRefreshApiProbe();
  state.refreshApiAvailable = hasRefreshApi();
  state.refreshApiStatus = hasRefreshApi() ? await fetchRefreshApiStatus(true) : null;
  persistSettings();
  scheduleAutoRefresh();
  syncTheme();
  populateSettingsForm(getAllExchanges());
  invalidateAiAnalyticsCaches();
  invalidateCrossOpportunityCaches();
  invalidateCrossHistoryAnalytics({ resetIndices: true });
  renderAll();
}

async function handleSettingsSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  try {
    state.settings.minSpread = Number(form.elements.minSpread?.value) || DEFAULT_SETTINGS.minSpread;
    state.settings.leverage = Number(form.elements.leverage?.value) || DEFAULT_SETTINGS.leverage;
    const rawDepositValue = String(form.elements.deposit?.value || '').trim();
    const depositValue = Number(rawDepositValue);
    state.settings.deposit = rawDepositValue !== '' && Number.isFinite(depositValue) ? depositValue : DEFAULT_SETTINGS.deposit;
    state.settings.holdingHours = Number(form.elements.holdingHours?.value) || 0;
    state.settings.sparklineDays = Number(form.elements.sparklineDays?.value) || DEFAULT_SETTINGS.sparklineDays;
    state.settings.useExternalAiApi = Boolean(form.elements.useExternalAiApi?.checked);
    state.settings.aiApiBaseUrl = normalizeRefreshApiBaseUrl(form.elements.aiApiBaseUrl?.value || '');
    state.settings.aiPredictionHorizon = normalizeAiPredictionHorizon(form.elements.aiPredictionHorizon?.value);
    state.settings.minAiScore = Math.max(0, Math.min(100, Number(form.elements.minAiScore?.value) || 0));
    state.settings.maxAiEntryWaitMinutes = Math.max(0, Math.min(240, Number(form.elements.maxAiEntryWaitMinutes?.value) || 0));
    state.settings.maxAiHoldingHours = Math.max(0, Math.min(168, Number(form.elements.maxAiHoldingHours?.value) || 0));
    state.settings.autoRefreshMode = form.elements.autoRefreshMode?.value === 'auto' ? 'auto' : 'manual';
    state.settings.autoRefreshIntervalSeconds = clampAutoRefreshIntervalSeconds(form.elements.autoRefreshIntervalSeconds?.value);
    state.settings.autoRefreshStrategy = AUTO_REFRESH_DEFAULT_STRATEGY;
    state.settings.autoRefreshFullIntervalSeconds = AUTO_REFRESH_DEFAULT_FULL_INTERVAL_SECONDS;
    if (form.elements.refreshApiBaseUrl) {
      state.settings.refreshApiBaseUrl = normalizeRefreshApiBaseUrl(form.elements.refreshApiBaseUrl.value);
    }
    if (form.elements.refreshApiKey) {
      state.settings.refreshApiKey = normalizeRefreshApiKey(form.elements.refreshApiKey.value);
    }

    [...dom.feeGrid.querySelectorAll('[data-fee-input]')].forEach((input) => {
      state.settings.fees[input.dataset.feeInput] = Number(input.value) || DEFAULT_FEE;
    });
    [...dom.exchangeCheckboxes.querySelectorAll('[data-exchange-checkbox]')].forEach((input) => {
      state.settings.visibleExchanges[input.dataset.exchangeCheckbox] = input.checked;
    });

    const selectedColumns = [...dom.crossColumnCheckboxes.querySelectorAll('[data-cross-column-checkbox]')]
      .filter((input) => input.checked)
      .map((input) => input.dataset.crossColumnCheckbox);
    state.settings.visibleCrossColumns = Object.fromEntries(
      CROSS_COLUMN_DEFS.map((column) => [column.key, selectedColumns.includes(column.key)]),
    );
    if (!selectedColumns.length) {
      state.settings.visibleCrossColumns = defaultCrossColumnVisibility();
    }

    currentLanguage = state.settings.language;
    persistSettings();
    scheduleAutoRefresh();
    invalidateAiAnalyticsCaches();
    invalidateCrossOpportunityCaches();
    invalidateCrossHistoryAnalytics({ resetIndices: true });
    closeSettings();

    // Let the close animation/paint happen first, then do expensive rerender work.
    window.requestAnimationFrame(() => {
      renderAll();
    });

    // Probe backend connectivity entirely in background; never block apply UX.
    void (async () => {
      try {
        await syncRefreshApiProbe();
        state.refreshApiAvailable = hasRefreshApi();
        state.refreshApiStatus = hasRefreshApi() ? await fetchRefreshApiStatus(true) : null;
      } catch {
        // Keep previous diagnostics state if probe fails.
      } finally {
        renderBackendDiagnostics();
        renderDataSourceStatus();
        syncRefreshControls();
      }
    })();
  } catch {
    setRefreshStatus(pickLocalized('Failed to apply settings.', 'Не удалось применить настройки.'), true);
  }
}

function renderAll() {
  refreshLocalizationCaches();
  applyStaticTranslations();
  syncRefreshControls();
  syncRefreshFeedback();
  renderBackendDiagnostics();
  renderDataSourceStatus();
  renderQualityBadges();
  renderAiThresholdNotes();
  renderHeroRiskSummary();
  renderTradeSmartNotices();
  renderRefreshDetail(hasRefreshApi() || ['running', 'failed'].includes(state.refreshState?.status) ? state.refreshState : null);
  populateCrossColumnPresets();
  scheduleCurrentTabRender();
}

function rerenderActiveDetail() {
  if (!state.activeDetailId) {
    return;
  }
  const item = getFilteredCross().find((entry) => entry.id === state.activeDetailId);
  if (item) {
    openOpportunityDetail(item);
  }
}

let _tabRenderFrameId = 0;

function scheduleCurrentTabRender() {
  if (_tabRenderFrameId) {
    window.cancelAnimationFrame(_tabRenderFrameId);
  }
  _tabRenderFrameId = window.requestAnimationFrame(() => {
    _tabRenderFrameId = 0;
    renderCurrentTab();
  });
}

function scheduleRender() {
  if (state.searchRenderTimer) {
    window.clearTimeout(state.searchRenderTimer);
  }
  state.searchRenderTimer = window.setTimeout(() => {
    state.searchRenderTimer = null;
    renderAll();
  }, 120);
}

function renderCurrentTab() {
  if (state.activeTab === 'cross') {
    renderCrossTable();
    return;
  }
  if (state.activeTab === 'funding') {
    renderFundingTable();
    return;
  }
  if (state.activeTab === 'ai') {
    renderAiAnalyticsPage();
    return;
  }
  if (state.activeTab === 'glossary') {
    renderGlossaryPage();
    return;
  }
  if (state.activeTab === 'heatmap') {
    renderHeatmapPage();
    return;
  }
  if (state.activeTab === 'health') {
    renderHealthPage();
  }
}

function isExchangeVisible(exchange) {
  return state.settings.visibleExchanges[exchange] !== false;
}

function computeFundingCarry(item) {
  const configuredHoldingHours = Number(state.settings.holdingHours || 0);
  const defaultHoldingHours = Number(item.expectedHoldingHours || 0);
  const holdingHours = configuredHoldingHours > 0 ? configuredHoldingHours : defaultHoldingHours;
  const embeddedCarry = numberOrNull(item.expectedFundingCarryPercent);
  if (!holdingHours) {
    return embeddedCarry || 0;
  }
  if (embeddedCarry != null && defaultHoldingHours > 0 && Math.abs(holdingHours - defaultHoldingHours) < 0.0001) {
    return embeddedCarry;
  }
  const intervalA = Math.max(0.1, Number(item.fundingIntervalHoursA || 8));
  const intervalB = Math.max(0.1, Number(item.fundingIntervalHoursB || 8));
  const fundingA = Number(item.fundingA || 0);
  const fundingB = Number(item.fundingB || 0);
  const direction = item.direction || '';
  if (direction.includes(`LONG ${item.exchangeA}`)) {
    return fundingB * (holdingHours / intervalB) - fundingA * (holdingHours / intervalA);
  }
  if (direction.includes(`LONG ${item.exchangeB}`)) {
    return fundingA * (holdingHours / intervalA) - fundingB * (holdingHours / intervalB);
  }
  return (fundingA - fundingB) * (holdingHours / Math.max(intervalA, intervalB, 8));
}

function computeCrossDerived(item) {
  const feeA = Number(state.settings.fees[item.exchangeA] ?? DEFAULT_FEE);
  const feeB = Number(state.settings.fees[item.exchangeB] ?? DEFAULT_FEE);
  const expectedHoldingHours = Number(state.settings.holdingHours || 0) > 0
    ? Number(state.settings.holdingHours || 0)
    : Number(item.expectedHoldingHours || 0);
  const fundingCarry = computeFundingCarry(item);
  // Entry + exit (symmetric takers): must match Python calculations.py formula.
  const openingFeePercent = feeA + feeB;
  const closingFeePercent = feeA + feeB;
  const latencyReservePercent = Math.max(0, Number(item.latencyReservePercent || 0));
  const netSpread = Number(item.spread || 0) - openingFeePercent - closingFeePercent - latencyReservePercent + fundingCarry;
  const roi = netSpread * Number(state.settings.leverage || 1);
  const profit = (Number(state.settings.deposit || 0) * roi) / 100;
  const annualizedNetEdge = expectedHoldingHours > 0
    ? netSpread * (24 / Math.max(expectedHoldingHours, 0.25)) * 365
    : Number(item.annualizedNetEdge || 0);
  return {
    netSpread,
    roi,
    profit,
    fundingCarry,
    openingFeePercent,
    closingFeePercent,
    latencyReservePercent,
    expectedHoldingHours,
    annualizedNetEdge,
  };
}

function summarizeCrossHistory(series) {
  const sampleCount = series.net.length;
  const absNet = series.net.map((value) => Math.abs(value));
  const latestAbs = absNet.at(-1) ?? 0;
  const meanAbs = average(absNet);
  const baselineAbs = absNet.length > 1 ? average(absNet.slice(0, -1)) : latestAbs;
  const recentWindow = absNet.slice(-4);
  const previousWindow = absNet.slice(-8, -4);
  const recentAvg = average(recentWindow) || latestAbs;
  const previousAvg = average(previousWindow.length ? previousWindow : absNet.slice(0, -1)) || recentAvg;
  const totalHours = series.timestamps.length > 1
    ? Math.max(1 / 6, (series.timestamps.at(-1) - series.timestamps[0]) / (60 * 60 * 1000))
    : 0;
  const slopePerHour = totalHours > 0 ? (latestAbs - absNet[0]) / totalHours : 0;
  const stdAbs = standardDeviation(absNet);
  const zScore = computeZScore(absNet, latestAbs);
  const percentile = percentileRank(absNet, latestAbs);
  const shortZScore = computeZScore(absNet.slice(-6), latestAbs);
  const longZScore = computeZScore(absNet.slice(-18), latestAbs);
  const timeframeAlignment = computeTimeframeAlignment(shortZScore, longZScore);
  const stabilityScore = clamp(100 - (stdAbs / Math.max(0.2, baselineAbs || 0.2)) * 35, 8, 100);
  let lifecycle = 'new';
  if (sampleCount >= 4) {
    if (latestAbs <= 0.15) {
      lifecycle = 'expired';
    } else if (recentAvg - previousAvg > Math.max(0.12, previousAvg * 0.18)) {
      lifecycle = 'widening';
    } else if (previousAvg - recentAvg > Math.max(0.12, recentAvg * 0.18)) {
      lifecycle = 'converging';
    } else {
      lifecycle = 'stable';
    }
  }

  const meanReversionHours = lifecycle === 'converging' && slopePerHour < 0 && latestAbs > 0
    ? clamp(latestAbs / Math.abs(slopePerHour), 0.5, 168)
    : null;
  const regime = inferSpreadRegime({ sampleCount, latestAbs, recentAvg, previousAvg, stdAbs, meanAbs, slopePerHour, lifecycle });
  const regimeScore = scoreSpreadRegime(regime);
  const anomalyScore = scoreAnomaly(zScore, percentile, timeframeAlignment);
  const reversionProbability = computeReversionProbability({ zScore, percentile, stabilityScore, regime, meanReversionHours, latestAbs, timeframeAlignment });

  return {
    ...series,
    sampleCount,
    latestAbs,
    meanAbs,
    baselineAbs,
    stdAbs,
    zScore,
    percentile,
    shortZScore,
    longZScore,
    timeframeAlignment,
    slopePerHour,
    stabilityScore,
    lifecycle,
    regime,
    regimeScore,
    anomalyScore,
    reversionProbability,
    meanReversionHours,
  };
}

function buildCrossOpportunity(item) {
  const derived = computeCrossDerived(item);
  const history = state.crossHistoryAnalytics.get(item.id) || {};
  const ai = mergeAiSnapshots(normalizeEmbeddedAiSnapshot(item.ai), getAiSnapshot(item.id)) || {};
  const aiExecution = ai.execution || {};
  const aiAdvice = ai.advice || {};
  const aiEntry = aiExecution.entry || {};
  const aiExit = aiExecution.exit || {};
  const aiHolding = aiExecution.holding || {};
  const aiIndicators = aiExecution.indicators || {};
  const fallbackFundingCarryPercent = numberOrNull(aiIndicators.expected_funding_carry_percent ?? item.expectedFundingCarryPercent ?? derived.fundingCarry);
  const fallbackNetCarryPercent = numberOrNull(
    aiIndicators.expected_net_carry_percent
      ?? ((numberOrNull(item.expectedFundingCarryPercent ?? derived.fundingCarry) ?? 0)
        - (numberOrNull(item.openingFeePercent ?? derived.openingFeePercent) ?? 0)
        - (numberOrNull(item.latencyReservePercent ?? derived.latencyReservePercent) ?? 0)),
  );
  const lifecycle = item.lifecycle || history.lifecycle || inferLifecycleFromCurrent(derived.netSpread);
  const regime = item.regime || history.regime || inferSpreadRegime({
    sampleCount: history.sampleCount || 0,
    latestAbs: history.latestAbs || Math.abs(derived.netSpread),
    recentAvg: history.latestAbs || Math.abs(derived.netSpread),
    previousAvg: history.baselineAbs || Math.abs(derived.netSpread),
    stdAbs: history.stdAbs || 0,
    meanAbs: history.meanAbs || Math.abs(derived.netSpread),
    slopePerHour: history.slopePerHour || 0,
    lifecycle,
  });
  const minVolume = Math.min(Number(item.volumeA || 0), Number(item.volumeB || 0));
  const maxBidAsk = Math.max(Number(item.spreadA || 0), Number(item.spreadB || 0));
  const spreadQuality = scoreSpreadQuality(derived.netSpread);
  const fundingQuality = scoreFundingQuality(derived.fundingCarry, item.fundingA, item.fundingB);
  const liquidityQuality = scoreLiquidityQuality(minVolume);
  const stabilityQuality = Number.isFinite(history.stabilityScore) ? history.stabilityScore : 48;
  const executionDifficulty = scoreExecutionDifficulty(maxBidAsk, minVolume, item);
  const zScore = pickMetric(item.zScore, history.zScore, 0);
  const percentile = pickMetric(item.percentile, history.percentile, 0);
  const shortZScore = pickMetric(item.shortZScore, history.shortZScore, zScore);
  const longZScore = pickMetric(item.longZScore, history.longZScore, zScore);
  const timeframeAlignment = pickMetric(item.timeframeAlignment, history.timeframeAlignment, computeTimeframeAlignment(shortZScore, longZScore));
  const meanReversionHours = pickMetric(item.meanReversionHours, history.meanReversionHours, null);
  const meanReversionScore = scoreMeanReversion(meanReversionHours, lifecycle);
  const anomalyScore = pickMetric(item.anomalyScore, history.anomalyScore, scoreAnomaly(zScore, percentile, timeframeAlignment));
  const regimeScore = pickMetric(item.regimeScore, history.regimeScore, scoreSpreadRegime(regime));
  const reversionProbability = pickMetric(
    item.reversionProbability,
    history.reversionProbability,
    computeReversionProbability({
      zScore,
      percentile,
      stabilityScore: stabilityQuality,
      regime,
      meanReversionHours,
      latestAbs: history.latestAbs || Math.abs(derived.netSpread),
      timeframeAlignment,
    }),
  );
  const baseScore = Math.round(
    spreadQuality * 0.18
      + fundingQuality * 0.1
      + liquidityQuality * 0.14
      + stabilityQuality * 0.12
      + (100 - executionDifficulty) * 0.12
      + meanReversionScore * 0.12
      + anomalyScore * 0.14
      + reversionProbability * 100 * 0.05
      + regimeScore * 0.03,
  );
  const healthQuality = computeHealthQuality(item);
  const dataFreshnessScore = computeDataFreshnessScore(item.updated_at);
  const marketPermission = getMarketPermissionProfile();
  const halfLifeHours = computeHalfLifeHours(history, meanReversionHours);
  const executionReality = computeExecutionReality(item, derived, executionDifficulty, marketPermission, healthQuality);
  const overlapStats = computeOverlapStats(item);
  const execution = buildExecutionInsights(item, derived, history, executionDifficulty);
  const baseRiskFlags = normalizeRiskFlags(item.riskFlags) || buildOpportunityRiskFlags(item, derived, {
    zScore,
    percentile,
    anomalyScore,
    reversionProbability,
    meanReversionHours,
    lifecycle,
    regime,
    timeframeAlignment,
  });
  const parityModes = item.parityModes || item.indicatorAnalytics?.parityModes || {};
  const rawWatchFallback = Boolean(
    parityModes.balanced?.rawWatchFallback || parityModes.strict?.rawWatchFallback,
  );

  const marketPermissionFlag = {
    status: marketPermission.mode === 'open' ? 'positive' : marketPermission.mode === 'cautious' ? 'warning' : 'negative',
    label: marketPermission.label,
    reason: marketPermission.reason,
  };
  const overlapRiskFlag = {
    status: overlapStats.status,
    label: overlapStats.label,
    reason: overlapStats.reason,
  };
  const riskFlags = {
    ...baseRiskFlags,
    marketPermission: marketPermissionFlag,
    overlap: overlapRiskFlag,
  };
  const resolvedAiAdvice = aiAdvice && typeof aiAdvice === 'object' ? aiAdvice : {};
  const signalLifecycle = item.signalLifecycle && typeof item.signalLifecycle === 'object' ? item.signalLifecycle : {};
  const signalState = String(item.signalState || signalLifecycle.state || 'idle');
  const entryStatus = String(item.entryStatus || signalLifecycle.entryStatus || 'skip');

  let reliabilityScore = Math.round(clamp(
    baseScore * 0.24
      + reversionProbability * 100 * 0.18
      + liquidityQuality * 0.11
      + healthQuality * 0.12
      + stabilityQuality * 0.1
      + fundingQuality * 0.08
      + executionReality.score * 0.11
      + dataFreshnessScore * 0.06
      + timeframeAlignment * 0.04
      + marketPermission.score * 0.04
      - overlapStats.overlapPenalty,
    5,
    99,
  ));
  if (marketPermission.mode === 'blocked') {
    reliabilityScore = Math.min(reliabilityScore, 64);
  }
  if (executionReality.netAfterExecution <= 0) {
    reliabilityScore = Math.max(5, reliabilityScore - 8);
  }

  const reliabilityClass = classifyReliability(reliabilityScore);
  const reliabilityReasons = buildReliabilityReasons({
    marketPermission,
    executionReality,
    overlapStats,
    healthQuality,
    dataFreshnessScore,
    reversionProbability,
    netAfterExecution: executionReality.netAfterExecution,
  });

  const finalItem = {
    ...item,
    ...derived,
    score: reliabilityScore,
    baseScore,
    reliabilityScore,
    reliabilityClass,
    reliabilityReasons,
    healthQuality,
    dataFreshnessScore,
    spreadQuality,
    fundingQuality,
    liquidityQuality,
    stabilityQuality,
    executionDifficulty,
    executionRealityScore: executionReality.score,
    expectedSlippagePercent: executionReality.expectedSlippagePercent,
    deterministicCostPercent: executionReality.deterministicCostPercent,
    netAfterExecution: executionReality.netAfterExecution,
    effectiveProfit: executionReality.effectiveProfit,
    safeSizeUsd: executionReality.safeSizeUsd,
    positionPressure: executionReality.positionPressure,
    requiresScaling: executionReality.requiresScaling,
    zScore,
    percentile,
    shortZScore,
    longZScore,
    timeframeAlignment,
    anomalyScore,
    reversionProbability,
    meanReversionHours,
    halfLifeHours,
    meanReversionScore,
    lifecycle,
    regime,
    regimeLabel: formatRegimeLabel(regime),
    history,
    execution,
    marketPermission,
    overlapCount: overlapStats.overlapCount,
    overlapPenalty: overlapStats.overlapPenalty,
    overlapStatus: overlapStats.status,
    overlapLabel: overlapStats.label,
    aiScore: ai.confidence?.score ?? null,
    aiFactors: Array.isArray(ai.confidence?.factors) ? ai.confidence.factors : [],
    aiRecommendation: ai.confidence?.recommendation || '',
    aiForecastDirection: ai.prediction?.direction || null,
    aiForecastProbability: Number.isFinite(Number(ai.prediction?.probability)) ? Number(ai.prediction.probability) : null,
    aiPredictedChange: Number.isFinite(Number(ai.prediction?.predicted_change)) ? Number(ai.prediction.predicted_change) : null,
    aiForecastInterval: Array.isArray(ai.prediction?.confidence_interval) ? ai.prediction.confidence_interval : null,
    aiForecastHorizon: Number(ai.prediction?.horizon_minutes || state.settings.aiPredictionHorizon),
    whaleIndex: Number.isFinite(Number(ai.whale?.index)) ? Number(ai.whale.index) : null,
    whaleRecent: Array.isArray(ai.whale?.recent_whales) ? ai.whale.recent_whales : [],
    liquidationRisk: ai.liquidation?.risk_level || null,
    liquidationDangerLevels: Array.isArray(ai.liquidation?.danger_levels) ? ai.liquidation.danger_levels : [],
    smartRoutes: Array.isArray(ai.smartRoute?.routes) ? ai.smartRoute.routes : [],
    aiAdvice: resolvedAiAdvice,
    aiAdviceModal: resolvedAiAdvice.modal || null,
    aiAdviceSummary: resolvedAiAdvice.summary || '',
    aiAdviceVerdict: resolvedAiAdvice.verdict || '',
    aiAdviceEntryFocus: Array.isArray(resolvedAiAdvice.entry_focus) ? resolvedAiAdvice.entry_focus : [],
    aiAdviceHoldingFocus: Array.isArray(resolvedAiAdvice.holding_focus) ? resolvedAiAdvice.holding_focus : [],
    aiAdviceExitFocus: Array.isArray(resolvedAiAdvice.exit_focus) ? resolvedAiAdvice.exit_focus : [],
    aiAdviceRiskWatch: Array.isArray(resolvedAiAdvice.risk_watch) ? resolvedAiAdvice.risk_watch : [],
    aiAdviceNextActions: Array.isArray(resolvedAiAdvice.next_actions) ? resolvedAiAdvice.next_actions : [],
    aiSnapshot: ai,
    signalLifecycle,
    signalState,
    entryStatus,
    entryWaitMinutes: numberOrNull(aiEntry.wait_minutes ?? item.entryWaitMinutes),
    entryConfidence: numberOrNull(aiEntry.confidence),
    expectedEntrySpreadPct: numberOrNull(aiEntry.expected_spread_pct),
    exitEtaMinutes: numberOrNull(aiExit.eta_minutes ?? item.exitEtaMinutes),
    exitTargetSpreadPct: numberOrNull(aiExit.target_spread_pct ?? item.exitTargetSpreadPct),
    exitType: aiExit.exit_type || null,
    holdingHoursRecommended: numberOrNull(aiHolding.recommended_hours ?? item.holdingHoursRecommended),
    expectedFundingCarryBps: numberOrNull(aiHolding.expected_funding_carry_bps ?? (fallbackFundingCarryPercent == null ? null : fallbackFundingCarryPercent * 100)),
    expectedFundingCostBps: numberOrNull(aiHolding.expected_funding_cost_bps),
    expectedNetReturnBps: numberOrNull(aiHolding.expected_net_return_bps),
    exitTarget: aiExit.exit_type && Number.isFinite(Number(aiExit.target_spread_pct))
      ? `${aiExit.exit_type} · ${formatNumber(aiExit.target_spread_pct, 3)}%`
      : Number.isFinite(Number(aiExit.target_spread_pct))
        ? `${formatNumber(aiExit.target_spread_pct, 3)}%`
        : aiExit.exit_type || pickLocalized('Pending', 'Ожидание'),
    aiRiskScore: numberOrNull(aiExecution.risk_score ?? ai.confidence?.risk_score),
    aiSuccessProbability: numberOrNull(aiExecution.success_probability ?? ai.confidence?.success_probability),
    aiDynamicCostBps: numberOrNull(aiExecution.dynamic_cost_bps ?? ai.confidence?.dynamic_cost_bps),
    aiDeterministicCostPercent: numberOrNull(aiIndicators.deterministic_cost_percent ?? executionReality.deterministicCostPercent),
    aiExpectedNetCarryPercent: fallbackNetCarryPercent,
    aiAnnualizedEdgePercent: numberOrNull(aiIndicators.annualized_edge_percent ?? item.annualizedNetEdge ?? item.annualizedFundingEdge),
    aiWhaleActivityScore: numberOrNull(aiIndicators.whale_activity_score),
    aiLoading: ai.loading === true,
    aiError: ai.error || '',
    rawWatchFallback,
    profitabilityScore: scoreOpportunityProfitability({
      netAfterExecution: executionReality.netAfterExecution,
      safeSizeUsd: executionReality.safeSizeUsd,
      executionRealityScore: executionReality.score,
      healthQuality,
      marketPermission,
      overlapPenalty: overlapStats.overlapPenalty,
    }),
    riskFlags,
  };
  return finalItem;
}

function getCrossOpportunityRows() {
  return state.cross.map((item) => {
    const cached = state.crossOpportunityItemCache.get(item.id);
    if (cached) {
      return cached.value;
    }
    const value = buildCrossOpportunity(item);
    state.crossOpportunityItemCache.set(item.id, { value });
    return value;
  });
}

function buildFilteredCrossSignature() {
  const query = (state.settings.search || '').toUpperCase();
  const minSpread = Number(state.settings.minSpread || 0);
  const minAiScore = Number(state.settings.minAiScore || 0);
  const maxAiEntryWaitMinutes = Number(state.settings.maxAiEntryWaitMinutes || 0);
  const maxAiHoldingHours = Number(state.settings.maxAiHoldingHours || 0);
  const visibleExchanges = getAllExchanges()
    .map((exchange) => `${exchange}:${state.settings.visibleExchanges[exchange] !== false ? 1 : 0}`)
    .join('|');
  return `${state.crossOpportunityRevision}__${query}__${minSpread}__${minAiScore}__${maxAiEntryWaitMinutes}__${maxAiHoldingHours}__${visibleExchanges}`;
}

function getSortedCrossRows() {
  const rows = getFilteredCross();
  const sort = state.sorts.cross || { key: '', direction: 'none' };
  const signature = `${state.filteredCrossCache.signature}__${sort.key || ''}__${sort.direction || 'none'}`;
  if (state.sortedCrossCache.signature === signature) {
    return state.sortedCrossCache.value;
  }
  const value = sort.key && sort.direction !== 'none'
    ? sortRows(rows, 'cross')
    : [...rows].sort((left, right) => {
      const primary = compareBy(left, right, 'profitabilityScore', 'desc');
      return primary || compareBy(left, right, 'score', 'desc');
    });
  state.sortedCrossCache = { signature, value };
  return value;
}

function pickMetric(...values) {
  for (const value of values) {
    if (value == null) {
      continue;
    }
    if (Number.isFinite(Number(value))) {
      return Number(value);
    }
  }
  return values.at(-1) ?? 0;
}

function computeZScore(values, latestValue) {
  if (!values.length) {
    return 0;
  }
  const std = standardDeviation(values);
  if (!std) {
    return 0;
  }
  return (Number(latestValue || 0) - average(values)) / std;
}

function percentileRank(values, target) {
  if (!values.length) {
    return 0;
  }
  const lower = values.filter((value) => value < target).length;
  const equal = values.filter((value) => value === target).length;
  return clamp(((lower + equal * 0.5) / values.length) * 100, 0, 100);
}

function computeTimeframeAlignment(shortZScore, longZScore) {
  if (!Number.isFinite(shortZScore) || !Number.isFinite(longZScore)) {
    return 42;
  }
  const shortStrength = clamp(Math.abs(shortZScore) / 3, 0, 1);
  const longStrength = clamp(Math.abs(longZScore) / 3, 0, 1);
  const closeness = 1 - clamp(Math.abs(shortStrength - longStrength), 0, 1);
  const signAgreement = shortZScore * longZScore >= 0 ? 1.0 : 0.35;
  return Math.round(clamp((shortStrength * 0.45 + longStrength * 0.35 + closeness * 0.2) * signAgreement * 100, 0, 100));
}

function inferSpreadRegime({ sampleCount, latestAbs, recentAvg, previousAvg, stdAbs, meanAbs, slopePerHour, lifecycle }) {
  if (sampleCount < 4) {
    return 'forming';
  }
  if (latestAbs <= 0.15) {
    return 'compressed';
  }
  const noiseRatio = stdAbs / Math.max(meanAbs || latestAbs || 0.2, 0.2);
  if (lifecycle === 'converging' && slopePerHour < 0) {
    return noiseRatio < 0.55 ? 'clean-convergence' : 'stable-carry';
  }
  if (lifecycle === 'widening') {
    return noiseRatio > 0.95 ? 'volatile-expansion' : 'widening-trend';
  }
  if (noiseRatio > 0.9 || Math.abs(recentAvg - previousAvg) < Math.max(0.06, latestAbs * 0.05)) {
    return 'noisy-range';
  }
  return 'stable-carry';
}

function scoreSpreadRegime(regime) {
  if (regime === 'clean-convergence') {
    return 92;
  }
  if (regime === 'stable-carry') {
    return 66;
  }
  if (regime === 'forming') {
    return 44;
  }
  if (regime === 'noisy-range') {
    return 34;
  }
  if (regime === 'widening-trend') {
    return 26;
  }
  if (regime === 'volatile-expansion') {
    return 18;
  }
  return 12;
}

function scoreAnomaly(zScore, percentile, timeframeAlignment) {
  const zComponent = clamp(((Math.abs(Number(zScore || 0)) - 0.5) / 2.8) * 100, 0, 100);
  const percentileComponent = clamp(Number(percentile || 0), 0, 100);
  const alignmentComponent = clamp(Number(timeframeAlignment || 0), 0, 100);
  return Math.round(clamp(zComponent * 0.46 + percentileComponent * 0.34 + alignmentComponent * 0.2, 0, 100));
}

function computeReversionProbability({ zScore, percentile, stabilityScore, regime, meanReversionHours, latestAbs, timeframeAlignment }) {
  if (latestAbs <= 0.15) {
    return 0.12;
  }
  const anomalyBase = 1 / (1 + Math.exp(-(Math.abs(Number(zScore || 0)) - 1.1) * 1.6));
  const percentileBase = clamp(Number(percentile || 0) / 100, 0, 1);
  const stabilityBase = clamp(Number(stabilityScore || 0) / 100, 0, 1);
  const alignmentBase = clamp(Number(timeframeAlignment || 0) / 100, 0, 1);
  let probability = anomalyBase * 0.42 + percentileBase * 0.2 + stabilityBase * 0.16 + alignmentBase * 0.12;
  if (regime === 'clean-convergence') {
    probability += 0.14;
  } else if (regime === 'stable-carry') {
    probability += 0.08;
  } else if (regime === 'noisy-range') {
    probability -= 0.08;
  } else if (regime === 'widening-trend') {
    probability -= 0.16;
  } else if (regime === 'volatile-expansion') {
    probability -= 0.22;
  }
  if (Number.isFinite(meanReversionHours)) {
    probability += meanReversionHours <= 12 ? 0.08 : meanReversionHours <= 36 ? 0.04 : meanReversionHours > 72 ? -0.08 : 0;
  }
  return clamp(probability, 0.05, 0.98);
}

function formatRegimeLabel(regime) {
  return translateRegime(regime);
}

function scoreSpreadQuality(netSpread) {
  const spread = Math.abs(Number(netSpread || 0));
  if (spread <= 0.05) {
    return 8;
  }
  if (spread <= 8) {
    return clamp(spread * 12, 20, 100);
  }
  return clamp(100 - (spread - 8) * 1.8, 18, 88);
}

function scoreOpportunityProfitability({ netAfterExecution, safeSizeUsd, executionRealityScore, healthQuality, marketPermission, overlapPenalty }) {
  const netEdge = clamp(Number(netAfterExecution || 0) * 22, -20, 100);
  const depthValue = Math.max(1, Number(safeSizeUsd || 0));
  const depthScore = clamp(Math.log10(depthValue) * 16 - 8, 0, 100);
  const executionScore = clamp(Number(executionRealityScore || 0), 0, 100);
  const healthScore = clamp(Number(healthQuality || 0), 0, 100);
  const permissionScore = marketPermission.mode === 'open' ? 100 : marketPermission.mode === 'cautious' ? 64 : 32;
  const overlapScore = clamp(100 - Number(overlapPenalty || 0) * 4, 0, 100);
  return Math.round(clamp(
    netEdge * 0.34
      + depthScore * 0.18
      + executionScore * 0.18
      + healthScore * 0.12
      + permissionScore * 0.1
      + overlapScore * 0.08,
    5,
    100,
  ));
}

function renderReliabilityChip(item) {
  return `<span class="chip score-chip ${escapeHtml(scoreClass(item.score))}">${escapeHtml(String(item.score))}</span>`;
}

function getOpportunityById(id) {
  return getCrossOpportunityRows().find((item) => item.id === id) || null;
}

function scoreFundingQuality(fundingCarry, fundingA, fundingB) {
  const carry = Number(fundingCarry || 0);
  const delta = Math.abs(Number(fundingB || 0) - Number(fundingA || 0));
  return clamp(55 + carry * 350 + delta * 900, 10, 100);
}

function scoreLiquidityQuality(minVolume) {
  const volume = Math.max(0, Number(minVolume || 0));
  if (!volume) {
    return 5;
  }
  const normalized = Math.log10(volume + 1);
  return clamp((normalized - 3.5) * 28, 5, 100);
}

function scoreExecutionDifficulty(maxBidAsk, minVolume, item) {
  const spreadPenalty = clamp(Number(maxBidAsk || 0) * 120, 0, 65);
  const liquidityPenalty = clamp(55 - scoreLiquidityQuality(minVolume) * 0.5, 0, 40);
  const healthPenalty = [item.exchangeA, item.exchangeB].reduce((sum, exchange) => {
    const status = getExchangeHealth(exchange)?.status || 'ok';
    if (status === 'failed') {
      return sum + 25;
    }
    if (status === 'quarantined') {
      return sum + 35;
    }
    return sum;
  }, 0);
  return clamp(spreadPenalty + liquidityPenalty + healthPenalty, 5, 100);
}

function scoreMeanReversion(hours, lifecycle) {
  if (lifecycle === 'converging' && Number.isFinite(hours)) {
    return clamp(100 - hours * 1.4, 20, 100);
  }
  if (lifecycle === 'stable') {
    return 52;
  }
  if (lifecycle === 'widening') {
    return 24;
  }
  if (lifecycle === 'new') {
    return 46;
  }
  return 18;
}

function inferLifecycleFromCurrent(netSpread) {
  return Math.abs(Number(netSpread || 0)) <= 0.15 ? 'expired' : 'new';
}

function buildExecutionInsights(item, derived, history, executionDifficulty) {
  const liquidityA = liquidityLegScore(item.volumeA, item.spreadA);
  const liquidityB = liquidityLegScore(item.volumeB, item.spreadB);
  const entryFirst = liquidityA >= liquidityB ? item.exchangeA : item.exchangeB;
  const lowerSlippageExchange = Number(item.spreadA || 0) <= Number(item.spreadB || 0) ? item.exchangeA : item.exchangeB;
  const unhealthyExchanges = [item.exchangeA, item.exchangeB].filter((exchange) => {
    const status = getExchangeHealth(exchange)?.status || 'ok';
    return status !== 'ok';
  });
  const fundingRisk = derived.fundingCarry < 0 ? 'elevated' : Math.abs(derived.fundingCarry) > 0.08 ? 'supportive' : 'neutral';
  const suggestedExit = clamp(Math.abs(derived.netSpread) * 0.18, 0.05, 0.35);
  const entryZone = `${formatNumber(Math.max(Number(state.settings.minSpread || 0), derived.netSpread * 0.82), 2)}% to ${formatNumber(derived.netSpread, 2)}%`;
  const exitZone = `0.00% to ${formatNumber(suggestedExit, 2)}%`;
  const notes = [
    t('execution.enterFirst', { exchange: entryFirst }),
    t('execution.safer', { exchange: lowerSlippageExchange }),
    fundingRisk === 'elevated'
      ? t('execution.fundingAdverse')
      : fundingRisk === 'supportive'
        ? t('execution.fundingSupportive')
        : t('execution.fundingNeutral'),
  ];
  if (unhealthyExchanges.length) {
    notes.push(pickLocalized(
      `Health warning: ${unhealthyExchanges.join(', ')} has degraded adapter status.`,
      `Предупреждение по биржам: ${unhealthyExchanges.join(', ')} имеют деградировавший статус адаптера.`,
    ));
  }
  if ((history.meanReversionHours || 0) > 48) {
    notes.push(pickLocalized(
      'Estimated convergence is slow. Capital can stay tied up for an extended period.',
      'Ожидаемая сходимость медленная. Капитал может оставаться связанным заметно дольше.',
    ));
  }
  return {
    entryFirst,
    lowerSlippageExchange,
    fundingRisk,
    unhealthyExchanges,
    entryZone,
    exitZone,
    difficultyLabel: executionDifficultyLabel(executionDifficulty),
    notes,
  };
}

function liquidityLegScore(volume, bidAskSpread) {
  return scoreLiquidityQuality(volume) - clamp(Number(bidAskSpread || 0) * 110, 0, 40);
}

function getExchangeHealth(exchange) {
  return state.adapterHealth?.adapters?.[exchange] || state.metrics?.exchange_summary?.find((item) => item.exchange === exchange) || null;
}

function buildRouteKey(exchangeA, exchangeB, pairA) {
  const exchanges = [String(exchangeA || ''), String(exchangeB || '')].sort((left, right) => left.localeCompare(right));
  return `${String(pairA || '')}::${exchanges.join('::')}`;
}

function computeHealthQuality(item) {
  const healthRows = [item.exchangeA, item.exchangeB]
    .map((exchange) => getExchangeHealth(exchange))
    .filter(Boolean);
  if (!healthRows.length) {
    return 48;
  }
  return clamp(average(healthRows.map((health) => {
    const status = String(health.status || 'ok');
    const coverage = computeCoverageRatio(health);
    const statusBase = status === 'ok' ? 90 : status === 'quarantined' ? 22 : status === 'failed' ? 14 : 46;
    return clamp(statusBase * 0.65 + coverage * 0.35, 8, 100);
  })), 8, 100);
}

function computeDataFreshnessScore(updatedAt) {
  const rawValue = updatedAt || state.metrics?.generated_at || state.lastRefreshAt;
  if (!rawValue) {
    // SWR: if we have fresh cached data, return a reasonable default instead of 42
    const swrCrossEntry = state.swrCache.cross;
    if (swrCrossEntry && Date.now() - swrCrossEntry.updatedAt < 60000) {
      return 90;
    }
    return 42;
  }
  const timestamp = new Date(rawValue).getTime();
  if (!Number.isFinite(timestamp)) {
    return 42;
  }
  // SWR: use the more recent of data timestamp or SWR cache timestamp
  const swrCrossEntry = state.swrCache.cross;
  const effectiveTimestamp = swrCrossEntry
    ? Math.max(timestamp, swrCrossEntry.updatedAt)
    : timestamp;
  const ageMinutes = Math.max(0, (Date.now() - effectiveTimestamp) / 60000);
  if (ageMinutes <= 1) {
    return 100;
  }
  if (ageMinutes <= 15) {
    return clamp(100 - ageMinutes * 2.2, 72, 100);
  }
  if (ageMinutes <= 60) {
    return clamp(74 - (ageMinutes - 15) * 0.7, 38, 74);
  }
  return clamp(38 - (ageMinutes - 60) * 0.2, 8, 38);
}

function getMarketPermissionProfile() {
  const analytics = state.metrics?.analytics_summary || {};
  const summary = state.metrics?.summary || {};
  const elevatedVolatilityShare = Number(analytics.elevated_volatility_share || 0);
  const adverseFundingShare = Number(analytics.adverse_funding_share || 0);
  const elevatedEventRiskShare = Number(analytics.elevated_event_risk_share || 0);
  const trackedExchanges = Math.max(1, Number(summary.tracked_exchanges || 1));
  const healthyShare = Number(summary.healthy_exchanges || trackedExchanges) / trackedExchanges;
  const backdrop = String(analytics.risk_backdrop || 'watch');

  if (backdrop === 'stressed' || elevatedVolatilityShare >= 0.32 || elevatedEventRiskShare >= 0.04 || healthyShare < 0.6) {
    return {
      mode: 'blocked',
      label: pickLocalized('Capital protection', 'Режим защиты капитала'),
      action: pickLocalized('Pause aggressive entries and reduce size until the backdrop stabilizes.', 'Отложите агрессивные входы и сократите размер, пока рыночный фон не стабилизируется.'),
      reason: pickLocalized('Market backdrop is hostile: elevated volatility or event stress can break convergence assumptions.', 'Рыночный фон враждебный: повышенная волатильность или событийный стресс ломают сценарий схождения.'),
      tone: 'negative',
      score: 28,
      multiplier: 0.78,
      allowAggressive: false,
    };
  }

  if (backdrop === 'watch' || elevatedVolatilityShare >= 0.16 || adverseFundingShare >= 0.03 || healthyShare < 0.85) {
    return {
      mode: 'cautious',
      label: pickLocalized('Selective tape', 'Выборочный режим'),
      action: pickLocalized('Only the cleanest setups deserve capital and size should stay disciplined.', 'Капитал получают только самые чистые связки, а размер нужно держать дисциплинированным.'),
      reason: pickLocalized('Backdrop is mixed: allow trades only when execution and reversion remain clearly aligned.', 'Фон смешанный: сделки допустимы только там, где исполнение и возврат к норме явно подтверждают друг друга.'),
      tone: 'warning',
      score: 62,
      multiplier: 0.9,
      allowAggressive: false,
    };
  }

  return {
    mode: 'open',
    label: pickLocalized('Open tape', 'Открытый режим'),
    action: pickLocalized('Normal selective execution is allowed. Still prefer the strongest routes.', 'Нормальное выборочное исполнение допустимо. Но приоритет всё равно у самых сильных маршрутов.'),
    reason: pickLocalized('Volatility, funding drag and exchange health remain contained enough for active scanning.', 'Волатильность, давление funding и здоровье бирж остаются достаточно спокойными для активного сканирования.'),
    tone: 'positive',
    score: 92,
    multiplier: 1,
    allowAggressive: true,
  };
}

function computeHalfLifeHours(history, meanReversionHours) {
  if (Number.isFinite(Number(meanReversionHours))) {
    return Number(clamp(Number(meanReversionHours) * 0.5, 0.25, 84).toFixed(2));
  }
  const latestAbs = Number(history.latestAbs || 0);
  const slopePerHour = Number(history.slopePerHour || 0);
  if (latestAbs > 0 && slopePerHour < 0) {
    return Number(clamp((latestAbs * 0.5) / Math.abs(slopePerHour), 0.25, 84).toFixed(2));
  }
  return null;
}

function computeExecutionReality(item, derived, executionDifficulty, marketPermission, healthQuality) {
  const deposit = Math.max(1, Number(state.settings.deposit || DEFAULT_SETTINGS.deposit));
  const leverage = Math.max(1, Number(state.settings.leverage || DEFAULT_SETTINGS.leverage));
  const notional = deposit * leverage;
  const minVolume = Math.max(0, Math.min(Number(item.volumeA || 0), Number(item.volumeB || 0)));
  const maxBidAsk = Math.max(Number(item.spreadA || 0), Number(item.spreadB || 0));
  const maxQuoteAgeSeconds = Math.max(0, Number(item.maxQuoteAgeSeconds || 0));
  const quoteSkewSeconds = Math.max(0, Number(item.quoteSkewSeconds || 0));
  const healthMultiplier = clamp(healthQuality / 100, 0.32, 1);
  const permissionMultiplier = marketPermission.mode === 'open' ? 1 : marketPermission.mode === 'cautious' ? 0.55 : 0.24;
  // Depth multipliers match Python execution.py (0.00035 / 0.0002 / 0.00008).
  const depthMultiplier = maxBidAsk <= 0.05 ? 0.00035 : maxBidAsk <= 0.1 ? 0.0002 : 0.00008;
  const freshnessMultiplier = clamp(1 - maxQuoteAgeSeconds / 20, 0.22, 1);
  const skewMultiplier = clamp(1 - quoteSkewSeconds / 3, 0.18, 1);
  const effectiveDepthUsd = Math.max(0, minVolume * depthMultiplier * freshnessMultiplier * skewMultiplier);
  const safeSizeUsd = minVolume <= 0 ? 0 : Math.round(clamp(effectiveDepthUsd * healthMultiplier * permissionMultiplier, 100, Math.max(250, effectiveDepthUsd)));
  const positionPressure = notional / Math.max(1, safeSizeUsd);
  // deterministicCostPercent = round-trip fees (entry + exit) + latency - carry.
  // Mirrors Python compute_execution_reality with closingFeePercent included.
  const deterministicCostPercent = Math.max(0,
    Number(derived.openingFeePercent || 0)
    + Number(derived.closingFeePercent || 0)
    + Number(derived.latencyReservePercent || 0)
    - Number(derived.fundingCarry || 0)
  );
  const expectedSlippagePercent = Number(clamp(
    maxBidAsk * 0.58 + positionPressure * 0.16 + executionDifficulty * 0.006 + deterministicCostPercent * 0.18,
    0.02,
    4.5,
  ).toFixed(3));
  const netAfterExecution = Number((Number(derived.netSpread || 0) - expectedSlippagePercent).toFixed(4));
  const effectiveProfit = Number((((notional * netAfterExecution) / 100)).toFixed(2));
  const score = Math.round(clamp(
    100 - expectedSlippagePercent * 24 - executionDifficulty * 0.34 - deterministicCostPercent * 18 + (healthQuality - 50) * 0.15 + (marketPermission.mode === 'open' ? 6 : marketPermission.mode === 'cautious' ? -4 : -12),
    5,
    100,
  ));
  return {
    score,
    notional,
    effectiveDepthUsd: Number(effectiveDepthUsd.toFixed(2)),
    safeSizeUsd,
    positionPressure: Number(positionPressure.toFixed(2)),
    expectedSlippagePercent,
    deterministicCostPercent: Number(deterministicCostPercent.toFixed(4)),
    openingFeePercent: Number(Number(derived.openingFeePercent || 0).toFixed(4)),
    latencyReservePercent: Number(Number(derived.latencyReservePercent || 0).toFixed(4)),
    expectedFundingCarryPercent: Number(Number(derived.fundingCarry || 0).toFixed(4)),
    netAfterExecution,
    effectiveProfit,
    requiresScaling: safeSizeUsd > 0 && notional > safeSizeUsd,
  };
}

function buildCrossOverlapIndex(cross) {
  const index = new Map();
  for (const row of cross) {
    const key = String(row.pairA || row.pair || '');
    if (!index.has(key)) index.set(key, []);
    index.get(key).push(row);
  }
  return index;
}

function computeOverlapStats(item) {
  const pairKey = String(item.pairA || item.pair || '');
  const pairRows = state.crossOverlapIndex ? (state.crossOverlapIndex.get(pairKey) ?? []) : state.cross.filter((row) => String(row.pairA || row.pair || '') === pairKey);
  const sameDirectionCount = pairRows.filter((row) => String(row.direction || '') === String(item.direction || '')).length;
  const sameVenueCount = pairRows.filter((row) => buildRouteKey(row.exchangeA, row.exchangeB, row.pairA || row.pair) === buildRouteKey(item.exchangeA, item.exchangeB, item.pairA)).length;
  const overlapCount = Math.max(0, pairRows.length - 1);
  const overlapPenalty = clamp(overlapCount * 4 + Math.max(0, sameDirectionCount - 1) * 2 + Math.max(0, sameVenueCount - 1) * 5, 0, 22);
  const status = overlapPenalty >= 14 ? 'negative' : overlapPenalty >= 6 ? 'warning' : 'positive';
  const label = status === 'negative'
    ? pickLocalized('crowded', 'перегружена')
    : status === 'warning'
      ? pickLocalized('clustered', 'кластерная')
      : pickLocalized('clean', 'чистая');
  const reason = overlapPenalty >= 14
    ? pickLocalized('Several near-identical setups are competing for the same market move. Treat them as one risk bucket.', 'Несколько почти одинаковых связок конкурируют за одно и то же движение рынка. Их лучше считать одним кластером риска.')
    : overlapPenalty >= 6
      ? pickLocalized('This setup shares risk with a visible cluster. Prefer the cleanest route instead of stacking all variants.', 'Эта связка делит риск с заметным кластером. Лучше выбрать самый чистый маршрут, а не набирать все варианты сразу.')
      : pickLocalized('Overlap with the current shortlist is limited.', 'Перекрытие с текущим shortlist ограничено.');
  return {
    overlapCount,
    sameDirectionCount,
    sameVenueCount,
    overlapPenalty,
    status,
    label,
    reason,
  };
}

function classifyReliability(score) {
  if (score >= 88) {
    return 'A+';
  }
  if (score >= 78) {
    return 'A';
  }
  if (score >= 66) {
    return 'B';
  }
  if (score >= 54) {
    return 'C';
  }
  return 'D';
}

function reliabilityClassName(reliabilityClass) {
  if (reliabilityClass === 'A+') {
    return 'reliability-a-plus';
  }
  if (reliabilityClass === 'A') {
    return 'reliability-a';
  }
  if (reliabilityClass === 'B') {
    return 'reliability-b';
  }
  if (reliabilityClass === 'C') {
    return 'reliability-c';
  }
  return 'reliability-d';
}

function buildReliabilityReasons({ marketPermission, executionReality, overlapStats, healthQuality, dataFreshnessScore, reversionProbability, netAfterExecution }) {
  const reasons = [];
  if (marketPermission.mode === 'open') {
    reasons.push(pickLocalized('market backdrop allows active execution', 'рыночный фон допускает активное исполнение'));
  } else if (marketPermission.mode === 'cautious') {
    reasons.push(pickLocalized('market backdrop is selective', 'рыночный фон требует выборочного входа'));
  } else {
    reasons.push(pickLocalized('market backdrop is blocking aggressive entries', 'рыночный фон блокирует агрессивные входы'));
  }
  if (executionReality.score >= 72 && netAfterExecution > 0) {
    reasons.push(pickLocalized('execution remains realistic after slippage', 'исполнение остаётся реалистичным даже после учёта проскальзывания'));
  } else if (netAfterExecution <= 0) {
    reasons.push(pickLocalized('execution frictions erase the edge', 'издержки исполнения съедают преимущество'));
  }
  if (healthQuality >= 75) {
    reasons.push(pickLocalized('both venues look operationally healthy', 'обе биржи выглядят операционно здоровыми'));
  }
  if (dataFreshnessScore <= 35) {
    reasons.push(pickLocalized('data is aging and needs confirmation', 'данные стареют и требуют перепроверки'));
  }
  if (overlapStats.overlapPenalty >= 10) {
    reasons.push(pickLocalized('risk overlaps with a crowded cluster', 'риск перекрывается с перегруженным кластером'));
  }
  if (Number(reversionProbability || 0) >= 0.65) {
    reasons.push(pickLocalized('reversion probability is supportive', 'вероятность возврата поддерживает идею'));
  }
  return reasons.slice(0, 4);
}

function getFilteredCross() {
  const query = (state.settings.search || '').toUpperCase();
  const minAiScore = Math.max(0, Number(state.settings.minAiScore || 0));
  const maxAiEntryWaitMinutes = Math.max(0, Number(state.settings.maxAiEntryWaitMinutes || 0));
  const maxAiHoldingHours = Math.max(0, Number(state.settings.maxAiHoldingHours || 0));
  const rows = getCrossOpportunityRows();
  const signature = buildFilteredCrossSignature();
  if (state.filteredCrossCache.signature === signature) {
    return state.filteredCrossCache.value;
  }
  const value = rows
    .filter((item) => isExchangeVisible(item.exchangeA) && isExchangeVisible(item.exchangeB))
    .filter((item) => (query ? `${item.pairA}${item.exchangeA}${item.exchangeB}`.toUpperCase().includes(query) : true))
    .filter((item) => item.netSpread >= Number(state.settings.minSpread || 0))
    .filter((item) => (minAiScore > 0 && item.aiScore != null ? item.aiScore >= minAiScore : true))
    .filter((item) => {
      const liveEntryWaitMinutes = getLiveEntryWaitMinutes(item);
      return maxAiEntryWaitMinutes > 0 && liveEntryWaitMinutes != null
        ? liveEntryWaitMinutes <= maxAiEntryWaitMinutes
        : true;
    })
    .filter((item) => (maxAiHoldingHours > 0 && item.holdingHoursRecommended != null ? item.holdingHoursRecommended <= maxAiHoldingHours : true));
  state.filteredCrossCache = { signature, value };
  return value;
}

function getFilteredFunding() {
  const query = (state.settings.search || '').toUpperCase();
  return state.funding
    .filter((item) => isExchangeVisible(item.exchangeA) && isExchangeVisible(item.exchangeB))
    .filter((item) => (query ? `${item.pair}${item.exchangeA}${item.exchangeB}`.toUpperCase().includes(query) : true));
}

function sortRows(rows, scope) {
  const sort = state.sorts[scope];
  if (!sort?.key || sort.direction === 'none') {
    return rows;
  }
  return [...rows].sort((left, right) => compareBy(left, right, sort.key, sort.direction));
}

function ensurePaginationNode(scope, anchorNode) {
  if (!anchorNode) {
    return null;
  }
  const nodeId = `pagination-${scope}`;
  let node = document.querySelector(`#${nodeId}`);
  if (!node) {
    node = document.createElement('div');
    node.id = nodeId;
    node.className = 'table-pagination hidden';
    anchorNode.insertAdjacentElement('afterend', node);
  } else if (node.previousElementSibling !== anchorNode) {
    anchorNode.insertAdjacentElement('afterend', node);
  }
  return node;
}

let _mql960 = null;
let _mql768 = null;

function isTableVirtualizationEnabled() {
  _mql960 ??= window.matchMedia('(max-width: 960px)');
  return !_mql960.matches;
}

function getPageSizeForScope(scope) {
  _mql768 ??= window.matchMedia('(max-width: 768px)');
  if (_mql768.matches) {
    return MOBILE_TABLE_PAGE_SIZES[scope] || TABLE_PAGE_SIZE;
  }
  return TABLE_PAGE_SIZE;
}

function ensureVirtualScrollBinding(scope, shell, renderFn) {
  if (!shell || !state.virtualization[scope]) {
    return;
  }
  const virtualState = state.virtualization[scope];
  if (virtualState.boundShell === shell) {
    return;
  }

  virtualState.boundShell = shell;
  shell.addEventListener('scroll', () => {
    virtualState.scrollTop = shell.scrollTop;
    if (virtualState.frameId) {
      return;
    }
    virtualState.frameId = window.requestAnimationFrame(() => {
      virtualState.frameId = 0;
      renderFn();
    });
  }, { passive: true });
}

function getVirtualRows(scope, rows, shell) {
  if (!isTableVirtualizationEnabled() || !shell) {
    return {
      rows,
      startIndex: 0,
      endIndex: rows.length,
      beforeHeight: 0,
      afterHeight: 0,
    };
  }

  const rowHeight = VIRTUAL_ROW_HEIGHT[scope] || 40;
  const scrollTop = Math.max(0, Number(state.virtualization[scope]?.scrollTop || shell.scrollTop || 0));
  const viewportHeight = Math.max(rowHeight * 8, shell.clientHeight || rowHeight * 10);
  const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - VIRTUAL_ROW_OVERSCAN);
  const endIndex = Math.min(rows.length, Math.ceil((scrollTop + viewportHeight) / rowHeight) + VIRTUAL_ROW_OVERSCAN);

  return {
    rows: rows.slice(startIndex, endIndex),
    startIndex,
    endIndex,
    beforeHeight: startIndex * rowHeight,
    afterHeight: Math.max(0, (rows.length - endIndex) * rowHeight),
  };
}

function renderVirtualSpacer(colspan, height) {
  if (!height) {
    return '';
  }
  return `<tr class="virtual-spacer-row" aria-hidden="true"><td colspan="${colspan}" style="height:${height}px;padding:0;border:0;"></td></tr>`;
}

function renderTableWindowSummary(scope, totalRows, visibleCount, startIndex, endIndex, anchorNode) {
  const paginationNode = ensurePaginationNode(scope, anchorNode);
  if (!paginationNode) {
    return;
  }
  if (!isTableVirtualizationEnabled()) {
    paginationNode.classList.add('hidden');
    paginationNode.innerHTML = '';
    return;
  }
  paginationNode.classList.remove('hidden');
  paginationNode.innerHTML = `
    <div class="table-pagination-summary">${escapeHtml(pickLocalized(`Virtualized ${startIndex + 1}-${Math.max(startIndex, endIndex)} of ${totalRows}`, `Виртуализация ${startIndex + 1}-${Math.max(startIndex, endIndex)} из ${totalRows}`))}</div>
    <div class="table-pagination-actions">
      <span>${escapeHtml(pickLocalized(`${visibleCount} rows in DOM`, `${visibleCount} строк в DOM`))}</span>
    </div>
  `;
}

function getPaginatedRows(scope, rows, anchorNode, pageSize = getPageSizeForScope(scope)) {
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const currentPage = Math.min(Math.max(1, Number(state.tablePages[scope] || 1)), totalPages);
  const startIndex = totalRows ? (currentPage - 1) * pageSize : 0;
  const pageRows = rows.slice(startIndex, startIndex + pageSize);
  state.tablePages[scope] = currentPage;

  const paginationNode = ensurePaginationNode(scope, anchorNode);
  if (paginationNode) {
    if (totalRows <= pageSize) {
      paginationNode.classList.add('hidden');
      paginationNode.innerHTML = '';
    } else {
      const endIndex = Math.min(totalRows, startIndex + pageRows.length);
      paginationNode.classList.remove('hidden');
      paginationNode.innerHTML = `
        <div class="table-pagination-summary">${escapeHtml(pickLocalized(`Showing ${startIndex + 1}-${endIndex} of ${totalRows}`, `Показаны ${startIndex + 1}-${endIndex} из ${totalRows}`))}</div>
        <div class="table-pagination-actions">
          <button class="ghost-button" type="button" data-page-scope="${escapeHtml(scope)}" data-page-action="prev" ${currentPage <= 1 ? 'disabled' : ''}>${escapeHtml(pickLocalized('Previous', 'Назад'))}</button>
          <span>${escapeHtml(pickLocalized(`Page ${currentPage} of ${totalPages}`, `Страница ${currentPage} из ${totalPages}`))}</span>
          <button class="ghost-button" type="button" data-page-scope="${escapeHtml(scope)}" data-page-action="next" ${currentPage >= totalPages ? 'disabled' : ''}>${escapeHtml(pickLocalized('Next', 'Далее'))}</button>
        </div>
      `;
      paginationNode.querySelectorAll('[data-page-scope]').forEach((button) => {
        button.addEventListener('click', () => {
          const direction = button.dataset.pageAction === 'prev' ? -1 : 1;
          state.tablePages[scope] = Math.min(totalPages, Math.max(1, currentPage + direction));
          renderCurrentTab();
        });
      });
    }
  }

  return pageRows;
}

function compareBy(left, right, key, direction) {
  const multiplier = direction === 'asc' ? 1 : -1;
  const a = left[key];
  const b = right[key];
  if (typeof a === 'number' && typeof b === 'number') {
    return (a - b) * multiplier;
  }
  return String(a ?? '').localeCompare(String(b ?? '')) * multiplier;
}

function toggleSort(scope, key) {
  const current = state.sorts[scope];
  if (current.key !== key) {
    state.sorts[scope] = { key, direction: 'desc' };
  } else if (current.direction === 'desc') {
    state.sorts[scope] = { key, direction: 'asc' };
  } else if (current.direction === 'asc') {
    state.sorts[scope] = { key, direction: 'none' };
  } else {
    state.sorts[scope] = { key, direction: 'desc' };
  }
  if (scope === state.activeTab) {
    scheduleCurrentTabRender();
  }
}

function renderCrossTable() {
  const crossStatus = getDatasetStatus('cross');
  if (crossStatus.loading || (!crossStatus.loaded && !crossStatus.error)) {
    resetTabContent(dom.crossTable, dom.crossMobileList);
    showPanelState(dom.crossEmpty, getTabLoadingMessage('cross'));
    return;
  }
  if (crossStatus.error) {
    resetTabContent(dom.crossTable, dom.crossMobileList);
    showPanelState(dom.crossEmpty, crossStatus.error);
    return;
  }
  const rows = getSortedCrossRows();
  const isDesktopVirtualized = isTableVirtualizationEnabled();
  const shell = dom.crossTable?.parentElement;
  const mobileRows = isDesktopVirtualized ? [] : getPaginatedRows('cross', rows, shell || dom.crossMobileList);
  const desktopRows = isDesktopVirtualized ? rows : mobileRows;
  if (shell) {
    shell.classList.toggle('virtualized-shell', isDesktopVirtualized);
  }
  ensureVirtualScrollBinding('cross', shell, () => {
    if (state.activeTab === 'cross') {
      renderCrossTable();
    }
  });
  const virtualWindow = getVirtualRows('cross', desktopRows, shell);
  const visibleColumns = getVisibleCrossColumns();
  const headers = visibleColumns.map((column) => ({ key: column.key, label: column.header, hint: column.hint || '' }));
  renderHeader(dom.crossTable, headers, 'cross');
  dom.crossEmpty.classList.toggle('hidden', rows.length > 0);
  dom.crossTable.querySelector('tbody').innerHTML = `${renderVirtualSpacer(headers.length, isDesktopVirtualized ? virtualWindow.beforeHeight : 0)}${virtualWindow.rows
    .map(
      (item) => `
        <tr class="opportunity-row" tabindex="0" data-detail-id="${escapeHtml(item.id)}">
          ${visibleColumns.map((column) => renderCrossCell(column, item)).join('')}
        </tr>
      `,
    )
    .join('')}${renderVirtualSpacer(headers.length, isDesktopVirtualized ? virtualWindow.afterHeight : 0)}`;
  if (dom.crossMobileList) {
    dom.crossMobileList.innerHTML = isDesktopVirtualized ? '' : mobileRows.map((item) => renderCrossMobileCard(item)).join('');
  }
  const visibleRows = isDesktopVirtualized ? virtualWindow.rows : mobileRows;
  wireCrossDetailInteractions(visibleRows);
  if (isDesktopVirtualized) {
    renderTableWindowSummary('cross', rows.length, virtualWindow.rows.length, virtualWindow.startIndex, virtualWindow.endIndex, shell);
  }
  hydrateVisibleCrossAi(visibleRows);
}

function renderCrossMobileCard(item) {
  const fundingDelta = Number(item.fundingB || 0) - Number(item.fundingA || 0);
  const signalTone = item.netSpread >= 0 ? 'value-positive' : 'value-negative';
  const tradeAlertModel = getTradeAlertModel(item);
  return `
    <button class="cross-mobile-card" type="button" data-detail-id="${escapeHtml(item.id)}">
      <div class="cross-mobile-topline">
        <div>
          <strong class="cross-mobile-pair">${escapeHtml(item.pairA)}</strong>
          <p class="cross-mobile-route">${escapeHtml(item.exchangeA)} vs ${escapeHtml(item.exchangeB)}</p>
        </div>
        ${renderReliabilityChip(item)}
      </div>
      <div class="cross-mobile-grid">
        <article class="cross-mobile-metric">
          <span>${escapeHtml(t('cross.mobile.netSpread'))}</span>
          <strong class="${escapeHtml(signalTone)}">${escapeHtml(formatNumber(item.netSpread, 3))}%</strong>
        </article>
        <article class="cross-mobile-metric">
          <span>${escapeHtml(t('cross.mobile.roi'))}</span>
          <strong class="${escapeHtml(valueClass(item.roi))}">${escapeHtml(formatNumber(item.roi, 3))}%</strong>
        </article>
        <article class="cross-mobile-metric">
          <span>${escapeHtml(t('cross.mobile.reversion'))}</span>
          <strong class="${escapeHtml(probabilityClass(item.reversionProbability))}">${escapeHtml(formatNumber(item.reversionProbability * 100, 0))}%</strong>
        </article>
        <article class="cross-mobile-metric">
          <span>${escapeHtml(t('cross.mobile.profit'))}</span>
          <strong class="${escapeHtml(valueClass(item.profit))}">${escapeHtml(formatCurrency(item.profit))}</strong>
        </article>
      </div>
      <div class="cross-mobile-trade-status">
        <span class="chip risk-chip risk-${escapeHtml(tradeAlertModel.tone)}" data-cross-mobile-trade-title>${escapeHtml(tradeAlertModel.title)}</span>
        <span data-cross-mobile-trade-timer>${escapeHtml(tradeAlertModel.timerLabel)}</span>
      </div>
      <p class="cross-mobile-position" data-cross-mobile-position>${escapeHtml(`${t('cross.mobile.positionState')}: ${tradeAlertModel.positionLabel}`)}</p>
      <div class="cross-mobile-chips">
        <span class="chip cross-mobile-direction-chip">${escapeHtml(item.direction)}</span>
        <span class="chip lifecycle-chip lifecycle-${escapeHtml(item.lifecycle)}">${escapeHtml(translateLifecycle(item.lifecycle))}</span>
        <span class="chip regime-chip regime-${escapeHtml(item.regime)}">${escapeHtml(item.regimeLabel)}</span>
      </div>
      <div class="cross-mobile-footer cross-mobile-footer-compact">
        <span class="chip">${escapeHtml(t('cross.mobile.fundingA'))} <strong class="${escapeHtml(valueClass(item.fundingA))}">${escapeHtml(formatNumber(item.fundingA, 4))}%</strong></span>
        <span class="chip">${escapeHtml(t('cross.mobile.fundingB'))} <strong class="${escapeHtml(valueClass(item.fundingB))}">${escapeHtml(formatNumber(item.fundingB, 4))}%</strong></span>
        <span class="chip">${escapeHtml(t('cross.mobile.fundingDelta'))} <strong class="${escapeHtml(valueClass(fundingDelta))}">${escapeHtml(formatNumber(fundingDelta, 4))}%</strong></span>
      </div>
    </button>
  `;
}

function getVisibleCrossColumns() {
  const visibleColumns = CROSS_COLUMN_DEFS.filter((column) => state.settings.visibleCrossColumns[column.key] !== false);
  return visibleColumns.length ? visibleColumns : CROSS_COLUMN_DEFS;
}

function renderCrossCell(column, item) {
  const tradeAlertModel = column.key === 'tradeAlert' || column.key === 'positionState' ? getTradeAlertModel(item) : null;
  if (column.key === 'exchangeA') {
    return buildCell(column.label, escapeHtml(item.exchangeA));
  }
  if (column.key === 'pairA') {
    return buildCell(column.label, `<span class="chip">${escapeHtml(item.pairA)}</span>`);
  }
  if (column.key === 'exchangeB') {
    return buildCell(column.label, escapeHtml(item.exchangeB));
  }
  if (column.key === 'direction') {
    return buildCell(column.label, escapeHtml(item.direction));
  }
  if (column.key === 'score') {
    return buildCell(column.label, renderReliabilityChip(item), scoreClass(item.score));
  }
  if (column.key === 'aiScore') {
    return buildCell(
      column.label,
      item.aiScore == null
        ? `<span class="chip score-chip">${escapeHtml(item.aiLoading ? pickLocalized('Loading', 'Загрузка') : pickLocalized('Pending', 'Ожидание'))}</span>`
        : `<span class="chip score-chip ${escapeHtml(aiScoreClass(item.aiScore))}">${escapeHtml(String(item.aiScore))}</span>`,
      aiScoreClass(item.aiScore),
    );
  }
  if (column.key === 'aiForecast') {
    return buildCell(
      column.label,
      `<span class="chip ${escapeHtml(aiForecastClass(item.aiForecastDirection))}">${escapeHtml(buildAiForecastText(item))}</span>`,
      aiForecastClass(item.aiForecastDirection),
    );
  }
  if (column.key === 'entryWaitMinutes') {
    const liveEntryWaitMinutes = getLiveEntryWaitMinutes(item);
    return buildCell(
      column.label,
      liveEntryWaitMinutes == null ? pickLocalized('n/a', 'н/д') : formatEtaMinutes(liveEntryWaitMinutes),
      liveEntryWaitMinutes != null && liveEntryWaitMinutes <= 5 ? 'value-positive' : liveEntryWaitMinutes != null && liveEntryWaitMinutes <= 20 ? 'value-warning' : '',
      `data-column-key="${escapeHtml(column.key)}"`,
    );
  }
  if (column.key === 'holdingHoursRecommended') {
    return buildCell(
      column.label,
      item.holdingHoursRecommended == null
        ? pickLocalized('n/a', 'н/д')
        : `${formatNumber(item.holdingHoursRecommended, item.holdingHoursRecommended >= 10 ? 0 : 1)} ${pickLocalized('h', 'ч')}`,
      item.holdingHoursRecommended != null && item.holdingHoursRecommended <= 8 ? 'value-positive' : item.holdingHoursRecommended != null && item.holdingHoursRecommended <= 24 ? 'value-warning' : '',
    );
  }
  if (column.key === 'exitTarget') {
    return buildCell(column.label, escapeHtml(item.exitTarget || pickLocalized('Pending', 'Ожидание')));
  }
  if (column.key === 'tradeAlert') {
    return buildCell(
      column.label,
      `<span class="chip risk-chip risk-${escapeHtml(tradeAlertModel.tone)}">${escapeHtml(tradeAlertModel.title)}</span><br><small>${escapeHtml(tradeAlertModel.timerLabel)}</small>`,
      '',
      `data-column-key="${escapeHtml(column.key)}"`,
    );
  }
  if (column.key === 'positionState') {
    return buildCell(column.label, escapeHtml(tradeAlertModel.positionLabel), '', `data-column-key="${escapeHtml(column.key)}"`);
  }
  if (column.key === 'lifecycle') {
    return buildCell(column.label, `<span class="chip lifecycle-chip lifecycle-${escapeHtml(item.lifecycle)}">${escapeHtml(translateLifecycle(item.lifecycle))}</span>`);
  }
  if (column.key === 'regime') {
    return buildCell(column.label, `<span class="chip regime-chip regime-${escapeHtml(item.regime)}">${escapeHtml(item.regimeLabel)}</span>`);
  }
  if (column.key === 'zScore') {
    return buildCell(column.label, formatNumber(item.zScore, 2), anomalyClass(item.zScore));
  }
  if (column.key === 'percentile') {
    return buildCell(column.label, `${formatNumber(item.percentile, 0)}%`, percentileClass(item.percentile));
  }
  if (column.key === 'reversionProbability') {
    return buildCell(column.label, `${formatNumber(item.reversionProbability * 100, 0)}%`, probabilityClass(item.reversionProbability));
  }
  if (column.key === 'spread') {
    return buildCell(column.label, formatNumber(item.spread, 3), valueClass(item.spread));
  }
  if (column.key === 'netSpread') {
    return buildCell(column.label, formatNumber(item.netSpread, 3), valueClass(item.netSpread));
  }
  if (column.key === 'expectedFundingCarryPercent') {
    return buildCell(column.label, formatNumber(item.expectedFundingCarryPercent, 4), valueClass(item.expectedFundingCarryPercent));
  }
  if (column.key === 'openingFeePercent') {
    return buildCell(column.label, formatNumber(item.openingFeePercent, 4), 'value-warning');
  }
  if (column.key === 'latencyReservePercent') {
    return buildCell(column.label, formatNumber(item.latencyReservePercent, 4), 'value-warning');
  }
  if (column.key === 'annualizedNetEdge') {
    return buildCell(column.label, formatNumber(item.annualizedNetEdge, 1), valueClass(item.annualizedNetEdge));
  }
  if (column.key === 'roi') {
    return buildCell(column.label, formatNumber(item.roi, 3), valueClass(item.roi));
  }
  if (column.key === 'profit') {
    return buildCell(column.label, formatCurrency(item.profit), valueClass(item.profit));
  }
  if (column.key === 'volumeA') {
    return buildCell(column.label, escapeHtml(item.volumeAFormatted || formatCompact(item.volumeA)));
  }
  if (column.key === 'volumeB') {
    return buildCell(column.label, escapeHtml(item.volumeBFormatted || formatCompact(item.volumeB)));
  }
  if (column.key === 'fundingA') {
    return buildCell(column.label, formatNumber(item.fundingA, 4), valueClass(item.fundingA));
  }
  if (column.key === 'fundingB') {
    return buildCell(column.label, formatNumber(item.fundingB, 4), valueClass(item.fundingB));
  }
  if (column.key === 'spreadA') {
    return buildCell(column.label, formatNumber(item.spreadA, 4));
  }
  if (column.key === 'spreadB') {
    return buildCell(column.label, formatNumber(item.spreadB, 4));
  }
  return '';
}

function formatOptionalNumber(value, digits = 4) {
  return value == null ? pickLocalized('n/a', 'н/д') : formatNumber(value, digits);
}

function renderFundingTable() {
  const fundingStatus = getDatasetStatus('funding');
  if (fundingStatus.loading || (!fundingStatus.loaded && !fundingStatus.error)) {
    resetTabContent(dom.fundingTable, dom.fundingMobileList);
    showPanelState(dom.fundingEmpty, getTabLoadingMessage('funding'));
    return;
  }
  if (fundingStatus.error) {
    resetTabContent(dom.fundingTable, dom.fundingMobileList);
    showPanelState(dom.fundingEmpty, fundingStatus.error);
    return;
  }
  const rows = sortRows(getFilteredFunding(), 'funding');
  const isDesktopVirtualized = isTableVirtualizationEnabled();
  const shell = dom.fundingTable?.parentElement;
  const mobileRows = isDesktopVirtualized ? [] : getPaginatedRows('funding', rows, shell || dom.fundingMobileList);
  const desktopRows = isDesktopVirtualized ? rows : mobileRows;
  if (shell) {
    shell.classList.toggle('virtualized-shell', isDesktopVirtualized);
  }
  ensureVirtualScrollBinding('funding', shell, () => {
    if (state.activeTab === 'funding') {
      renderFundingTable();
    }
  });
  const virtualWindow = getVirtualRows('funding', desktopRows, shell);
  const formatPercentCell = (value, digits = 4) => formatOptionalNumber(value, digits);
  const headers = [
    { key: 'exchangeA', label: t('cross.label.exchangeA'), hint: TABLE_HEADER_HINTS.funding.exchangeA },
    { key: 'exchangeB', label: t('cross.label.exchangeB'), hint: TABLE_HEADER_HINTS.funding.exchangeB },
    { key: 'pair', label: t('cross.label.pairA'), hint: TABLE_HEADER_HINTS.funding.pair },
    { key: 'fundingA', label: t('cross.label.fundingA'), hint: TABLE_HEADER_HINTS.funding.fundingA },
    { key: 'fundingB', label: t('cross.label.fundingB'), hint: TABLE_HEADER_HINTS.funding.fundingB },
    { key: 'difference_abs', label: 'Abs diff %', hint: TABLE_HEADER_HINTS.funding.difference_abs },
    { key: 'difference_rel', label: 'Rel diff %', hint: TABLE_HEADER_HINTS.funding.difference_rel },
    { key: 'expectedFundingCarryPercent', label: t('funding.label.expectedFundingCarryPercent'), hint: TABLE_HEADER_HINTS.funding.expectedFundingCarryPercent },
    { key: 'openingFeePercent', label: t('funding.label.openingFeePercent'), hint: TABLE_HEADER_HINTS.funding.openingFeePercent },
    { key: 'latencyReservePercent', label: t('funding.label.latencyReservePercent'), hint: TABLE_HEADER_HINTS.funding.latencyReservePercent },
    { key: 'expectedNetCarryPercent', label: t('funding.label.expectedNetCarryPercent'), hint: TABLE_HEADER_HINTS.funding.expectedNetCarryPercent },
    { key: 'annualizedFundingEdge', label: t('funding.label.annualizedFundingEdge'), hint: TABLE_HEADER_HINTS.funding.annualizedFundingEdge },
    { key: 'volumeA', label: t('cross.label.volumeA'), hint: TABLE_HEADER_HINTS.funding.volumeA },
    { key: 'volumeB', label: t('cross.label.volumeB'), hint: TABLE_HEADER_HINTS.funding.volumeB },
    { key: 'recommended_action', label: t('funding.hint.recommended_action'), hint: TABLE_HEADER_HINTS.funding.recommended_action },
  ];
  renderHeader(dom.fundingTable, headers, 'funding');
  dom.fundingEmpty.classList.toggle('hidden', rows.length > 0);
  if (dom.fundingMobileList) {
    dom.fundingMobileList.innerHTML = isDesktopVirtualized ? '' : mobileRows.map((item) => renderFundingMobileCard(item)).join('');
  }
  dom.fundingTable.querySelector('tbody').innerHTML = `${renderVirtualSpacer(headers.length, isDesktopVirtualized ? virtualWindow.beforeHeight : 0)}${virtualWindow.rows
    .map(
      (item) => `
        <tr>
          ${buildCell(t('cross.label.exchangeA'), escapeHtml(item.exchangeA))}
          ${buildCell(t('cross.label.exchangeB'), escapeHtml(item.exchangeB))}
          ${buildCell(t('cross.label.pairA'), `<span class="chip">${escapeHtml(item.pair)}</span>`)}
          ${buildCell(t('cross.label.fundingA'), formatNumber(item.fundingA, 4), valueClass(item.fundingA))}
          ${buildCell(t('cross.label.fundingB'), formatNumber(item.fundingB, 4), valueClass(item.fundingB))}
          ${buildCell('Abs diff %', formatNumber(item.difference_abs, 4), valueClass(item.difference_abs))}
          ${buildCell('Rel diff %', formatNumber(item.difference_rel, 2))}
          ${buildCell(t('funding.label.expectedFundingCarryPercent'), formatPercentCell(item.expectedFundingCarryPercent, 4), valueClass(item.expectedFundingCarryPercent || 0))}
          ${buildCell(t('funding.label.openingFeePercent'), formatPercentCell(item.openingFeePercent, 4), item.openingFeePercent == null ? '' : 'value-warning')}
          ${buildCell(t('funding.label.latencyReservePercent'), formatPercentCell(item.latencyReservePercent, 4), item.latencyReservePercent == null ? '' : 'value-warning')}
          ${buildCell(t('funding.label.expectedNetCarryPercent'), formatPercentCell(item.expectedNetCarryPercent, 4), valueClass(item.expectedNetCarryPercent || 0))}
          ${buildCell(t('funding.label.annualizedFundingEdge'), formatPercentCell(item.annualizedFundingEdge, 1), valueClass(item.annualizedFundingEdge || 0))}
          ${buildCell(t('cross.label.volumeA'), escapeHtml(item.volumeAFormatted || formatCompact(item.volumeA)))}
          ${buildCell(t('cross.label.volumeB'), escapeHtml(item.volumeBFormatted || formatCompact(item.volumeB)))}
          ${buildCell(t('funding.hint.recommended_action'), escapeHtml(item.recommended_action))}
        </tr>
      `,
    )
    .join('')}${renderVirtualSpacer(headers.length, isDesktopVirtualized ? virtualWindow.afterHeight : 0)}`;
  if (isDesktopVirtualized) {
    renderTableWindowSummary('funding', rows.length, virtualWindow.rows.length, virtualWindow.startIndex, virtualWindow.endIndex, shell);
  }
}

function renderFundingMobileCard(item) {
  const netCarry = item.expectedNetCarryPercent ?? item.difference_abs;
  return `
    <article class="mobile-data-card">
      <div class="mobile-data-head">
        <div>
          <strong class="mobile-data-title">${escapeHtml(item.pair)}</strong>
          <p class="mobile-data-subtitle">${escapeHtml(item.exchangeA)} -> ${escapeHtml(item.exchangeB)}</p>
        </div>
        <span class="chip score-chip">${escapeHtml(formatOptionalNumber(netCarry, 4))}${netCarry == null ? '' : '%'}</span>
      </div>
      <div class="mobile-data-grid">
        <article class="mobile-data-metric">
          <span>${escapeHtml(t('cross.label.fundingA'))}</span>
          <strong class="${escapeHtml(valueClass(item.fundingA))}">${escapeHtml(formatNumber(item.fundingA, 4))}%</strong>
        </article>
        <article class="mobile-data-metric">
          <span>${escapeHtml(t('cross.label.fundingB'))}</span>
          <strong class="${escapeHtml(valueClass(item.fundingB))}">${escapeHtml(formatNumber(item.fundingB, 4))}%</strong>
        </article>
        <article class="mobile-data-metric">
          <span>${escapeHtml(t('funding.label.expectedNetCarryPercent'))}</span>
          <strong class="${escapeHtml(valueClass(netCarry || 0))}">${escapeHtml(formatOptionalNumber(netCarry, 4))}${netCarry == null ? '' : '%'}</strong>
        </article>
        <article class="mobile-data-metric">
          <span>${escapeHtml(t('funding.label.annualizedFundingEdge'))}</span>
          <strong class="${escapeHtml(valueClass(item.annualizedFundingEdge || 0))}">${escapeHtml(formatOptionalNumber(item.annualizedFundingEdge, 1))}${item.annualizedFundingEdge == null ? '' : '%'}</strong>
        </article>
      </div>
      <div class="mobile-data-footer">
        <span>${escapeHtml(t('funding.label.expectedFundingCarryPercent'))}: <strong>${escapeHtml(formatOptionalNumber(item.expectedFundingCarryPercent, 4))}${item.expectedFundingCarryPercent == null ? '' : '%'}</strong></span>
        <span>${escapeHtml(t('funding.label.openingFeePercent'))}: <strong>${escapeHtml(formatOptionalNumber(item.openingFeePercent, 4))}${item.openingFeePercent == null ? '' : '%'}</strong></span>
        <span>${escapeHtml(t('funding.label.latencyReservePercent'))}: <strong>${escapeHtml(formatOptionalNumber(item.latencyReservePercent, 4))}${item.latencyReservePercent == null ? '' : '%'}</strong></span>
        <span>${escapeHtml(t('cross.label.volumeA'))}: <strong>${escapeHtml(item.volumeAFormatted || formatCompact(item.volumeA))}</strong></span>
        <span>${escapeHtml(t('cross.label.volumeB'))}: <strong>${escapeHtml(item.volumeBFormatted || formatCompact(item.volumeB))}</strong></span>
        <span>${escapeHtml(t('funding.hint.recommended_action'))}: <strong>${escapeHtml(item.recommended_action)}</strong></span>
      </div>
    </article>
  `;
}

function renderHeader(table, headers, scope) {
  const fp = scope + '|' + headers.map((h) => {
    const c = Array.isArray(h) ? { key: h[0], label: h[1] } : h;
    return c.key + ':' + c.label;
  }).join(',');
  if (table._headerFp === fp) {
    return;
  }
  table._headerFp = fp;
  table.querySelector('thead').innerHTML = `<tr>${headers
    .map((header) => {
      const config = Array.isArray(header)
        ? { key: header[0], label: header[1], hint: '' }
        : { key: header.key, label: header.label, hint: header.hint || '' };
      const buttonClasses = ['ghost-button'];
      if (config.hint) {
        buttonClasses.push('tooltip-trigger', 'tooltip-trigger-button');
      }
      const tooltipAttrs = config.hint
        ? ` data-tooltip="${escapeHtml(config.hint)}" aria-label="${escapeHtml(buildTooltipAriaLabel(config.label, config.hint))}"`
        : ` aria-label="${escapeHtml(config.label)}"`;
      return `<th><button class="${buttonClasses.join(' ')}" type="button" data-sort-scope="${scope}" data-sort-key="${config.key}"${tooltipAttrs}>${renderTooltipButtonContent(config.label, config.hint)}</button></th>`;
    })
    .join('')}</tr>`;
  table.querySelectorAll('[data-sort-scope]').forEach((button) => {
    button.addEventListener('click', () => toggleSort(scope, button.dataset.sortKey));
  });
}

function wireCrossDetailInteractions(_rows) {
  // Detail interactions are handled by delegated listeners set up in bindEvents().
}

function renderAiDetailCards(item) {
  const aiEndpointAvailable = hasAiApi();
  const liveEntryWaitMinutes = getLiveEntryWaitMinutes(item);
  const economicsMetrics = [
    {
      label: pickLocalized('Entry window', 'Окно входа'),
      value: liveEntryWaitMinutes == null ? null : formatEtaMinutes(liveEntryWaitMinutes),
      className: liveEntryWaitMinutes == null ? '' : liveEntryWaitMinutes <= 5 ? 'value-positive' : liveEntryWaitMinutes <= 20 ? 'value-warning' : 'risk-negative',
    },
    {
      label: pickLocalized('Hold horizon', 'Горизонт удержания'),
      value: item.holdingHoursRecommended == null ? null : `${formatNumber(item.holdingHoursRecommended, item.holdingHoursRecommended >= 10 ? 0 : 1)} ${pickLocalized('h', 'ч')}`,
      className: item.holdingHoursRecommended == null ? '' : item.holdingHoursRecommended <= 8 ? 'value-positive' : item.holdingHoursRecommended <= 24 ? 'value-warning' : 'risk-negative',
    },
    {
      label: pickLocalized('Funding carry', 'Funding carry'),
      value: item.expectedFundingCarryBps == null ? null : `${formatNumber(item.expectedFundingCarryBps, 1)} bps`,
      className: valueClass(item.expectedFundingCarryBps || 0),
    },
    {
      label: pickLocalized('Funding drag', 'Funding-давление'),
      value: item.expectedFundingCostBps == null ? null : `${formatNumber(item.expectedFundingCostBps, 1)} bps`,
      className: item.expectedFundingCostBps == null ? '' : item.expectedFundingCostBps <= 2 ? 'value-positive' : item.expectedFundingCostBps <= 10 ? 'value-warning' : 'risk-negative',
    },
    {
      label: pickLocalized('Net return', 'Net return'),
      value: item.expectedNetReturnBps == null ? null : `${formatNumber(item.expectedNetReturnBps, 1)} bps`,
      className: valueClass(item.expectedNetReturnBps || 0),
    },
    {
      label: pickLocalized('Dynamic cost', 'Динамическая издержка'),
      value: item.aiDynamicCostBps == null ? null : `${formatNumber(item.aiDynamicCostBps, 1)} bps`,
      className: item.aiDynamicCostBps == null ? '' : item.aiDynamicCostBps <= 5 ? 'value-positive' : item.aiDynamicCostBps <= 15 ? 'value-warning' : 'risk-negative',
    },
    {
      label: pickLocalized('Deterministic cost', 'Детерминированная издержка'),
      value: item.aiDeterministicCostPercent == null ? null : `${formatNumber(item.aiDeterministicCostPercent, 4)}%`,
      className: item.aiDeterministicCostPercent == null ? '' : item.aiDeterministicCostPercent <= 0.08 ? 'value-positive' : item.aiDeterministicCostPercent <= 0.18 ? 'value-warning' : 'risk-negative',
    },
    {
      label: pickLocalized('Net carry', 'Чистый carry'),
      value: item.aiExpectedNetCarryPercent == null ? null : `${formatNumber(item.aiExpectedNetCarryPercent, 4)}%`,
      className: valueClass(item.aiExpectedNetCarryPercent || 0),
    },
    {
      label: pickLocalized('Annualized edge', 'Годовой edge'),
      value: item.aiAnnualizedEdgePercent == null ? null : `${formatNumber(item.aiAnnualizedEdgePercent, 1)}%`,
      className: valueClass(item.aiAnnualizedEdgePercent || 0),
    },
  ].filter((metric) => metric.value != null);
  const economicsCard = economicsMetrics.length ? `
    <section class="detail-note-card ai-economics-card">
      <h3>${escapeHtml(pickLocalized('AI execution economics', 'AI-экономика исполнения'))}</h3>
      <div class="ai-economics-grid">
        ${economicsMetrics.map((metric) => `
          <article class="ai-economics-metric">
            <span>${escapeHtml(metric.label)}</span>
            <strong class="${escapeHtml(metric.className || '')}">${escapeHtml(metric.value)}</strong>
          </article>
        `).join('')}
      </div>
    </section>
  ` : '';

  const factors = item.aiFactors?.length
    ? item.aiFactors.map((factor) => `<li>${escapeHtml(String(factor))}</li>`).join('')
    : `<li>${escapeHtml(item.aiRecommendation || pickLocalized('No AI factors available yet.', 'AI-факторы пока недоступны.'))}</li>`;

  const adviceModal = item.aiAdviceModal && typeof item.aiAdviceModal === 'object' ? item.aiAdviceModal : null;
  const adviceModalHighlights = Array.isArray(adviceModal?.highlights) ? adviceModal.highlights : [];

  const adviceSections = [
    {
      title: pickLocalized('Entry and hold plan', 'План входа и удержания'),
      lines: [...(item.aiAdviceEntryFocus || []), ...(item.aiAdviceHoldingFocus || [])],
    },
    {
      title: pickLocalized('Exit target', 'Цель выхода'),
      lines: item.aiAdviceExitFocus || [],
    },
    {
      title: pickLocalized('Risk watch', 'Контроль рисков'),
      lines: item.aiAdviceRiskWatch || [],
    },
    {
      title: pickLocalized('Next actions', 'Следующие действия'),
      lines: item.aiAdviceNextActions || [],
    },
  ].filter((section) => section.lines.length);
  const resolvedAdviceSections = Array.isArray(adviceModal?.sections) && adviceModal.sections.length
    ? adviceModal.sections.map((section) => ({
      title: section.id === 'entry'
        ? pickLocalized('Entry and hold plan', 'План входа и удержания')
        : section.id === 'holding'
          ? pickLocalized('Hold plan', 'План удержания')
          : section.id === 'exit'
            ? pickLocalized('Exit target', 'Цель выхода')
            : section.id === 'risk'
              ? pickLocalized('Risk watch', 'Контроль рисков')
              : section.id === 'actions'
                ? pickLocalized('Next actions', 'Следующие действия')
                : String(section.title || ''),
      lines: Array.isArray(section.items) ? section.items : [],
    })).filter((section) => section.lines.length)
    : adviceSections;
  const hasEmbeddedAiContent = Boolean(
    economicsMetrics.length
    || item.aiAdviceSummary
    || (adviceModalHighlights || []).length
    || resolvedAdviceSections.length
    || item.aiScore != null
    || item.aiForecastDirection
    || item.whaleIndex != null
    || item.liquidationRisk
    || (item.smartRoutes || []).length
    || (item.aiFactors || []).length
  );

  if (!aiEndpointAvailable && !hasEmbeddedAiContent) {
    return `
      ${economicsCard}
      <section class="detail-note-card">
        <h3>${escapeHtml(pickLocalized('AI metrics', 'AI-метрики'))}</h3>
        <ul>
          <li>${escapeHtml(pickLocalized('AI endpoints are unavailable on this host. Leave the field empty to use the built-in free backend endpoints, or set a compatible self-hosted API base URL.', 'AI endpoint-ы недоступны на этом хосте. Оставьте поле пустым для встроенных бесплатных backend endpoint-ов или укажите базовый URL совместимого self-hosted API.'))}</li>
        </ul>
      </section>
    `;
  }

  if (item.aiLoading && !hasEmbeddedAiContent) {
    return `
      ${economicsCard}
      <section class="detail-note-card">
        <h3>${escapeHtml(pickLocalized('AI metrics', 'AI-метрики'))}</h3>
        <ul>
          <li>${escapeHtml(pickLocalized('AI metrics are loading for this opportunity.', 'AI-метрики для этой связки загружаются.'))}</li>
        </ul>
      </section>
    `;
  }
  const adviceVerdictLabel = item.aiAdviceVerdict
    ? item.aiAdviceVerdict === 'enter-soon'
      ? pickLocalized('Enter soon', 'Можно входить скоро')
      : item.aiAdviceVerdict === 'wait'
        ? pickLocalized('Wait', 'Подождать')
        : item.aiAdviceVerdict === 'observe'
          ? pickLocalized('Observe', 'Наблюдать')
          : item.aiAdviceVerdict
    : pickLocalized('Pending', 'Ожидание');
  const adviceChipClass = item.aiAdviceVerdict === 'enter-soon'
    ? 'ai-score-high'
    : item.aiAdviceVerdict === 'wait'
      ? 'risk-negative'
      : 'ai-forecast-stable';
    const briefingTitle = adviceModal?.headline || pickLocalized('AI trade briefing', 'AI-разбор сделки');
    const briefingSummary = adviceModal?.summary || item.aiAdviceSummary || pickLocalized('Detailed neural advice is not available yet for this pair.', 'Подробный нейросовет по этой паре пока недоступен.');
    const briefingHighlightMarkup = adviceModalHighlights.length
      ? `
        <div class="ai-economics-grid">
          ${adviceModalHighlights.map((highlight) => {
            const className = highlight.tone === 'positive'
              ? 'value-positive'
              : highlight.tone === 'warning'
                ? 'value-warning'
                : highlight.tone === 'negative'
                  ? 'risk-negative'
                  : '';
            return `
              <article class="ai-economics-metric">
                <span>${escapeHtml(highlight.label)}</span>
                <strong class="${escapeHtml(className)}">${escapeHtml(highlight.value)}</strong>
              </article>
            `;
          }).join('')}
        </div>
      `
      : '';

  const whaleLines = item.whaleRecent?.length
    ? item.whaleRecent.slice(0, 3).map((entry) => `
        <li class="whale-event-item">
          <strong>${escapeHtml(`${formatNumber(entry.amount || 0, 2)}M`)}</strong>
          <span>${escapeHtml(entry.exchange || 'n/a')}</span>
          <span>${escapeHtml(formatDateTime(entry.time))}</span>
        </li>
      `).join('')
    : `<li>${escapeHtml(pickLocalized('No recent large-flow events detected.', 'Свежих крупных потоков не обнаружено.'))}</li>`;

  const routeMarkup = item.smartRoutes?.length
    ? item.smartRoutes.slice(0, 3).map((route) => `
        <article class="ai-route-card ${route.recommended ? 'is-recommended' : ''}">
          <div class="ai-route-head">
            <strong>${escapeHtml(`${route.exchangeA}/${route.exchangeB}`)}</strong>
            <span class="chip ${route.recommended ? 'ai-score-high' : 'ai-forecast-stable'}">${escapeHtml(route.recommended ? pickLocalized('Recommended', 'Рекомендуется') : pickLocalized(`Score ${formatNumber(route.score, 2)}`, `Оценка ${formatNumber(route.score, 2)}`))}</span>
          </div>
          <div class="ai-route-grid">
            <div class="ai-route-metric">
              <span>${escapeHtml(pickLocalized('Expected PnL', 'Ожидаемый PnL'))}</span>
              <strong>${escapeHtml(formatCurrency(route.expected_profit))}</strong>
            </div>
            <div class="ai-route-metric">
              <span>${escapeHtml(pickLocalized('Slippage', 'Проскальзывание'))}</span>
              <strong>${escapeHtml(formatNumber(route.expected_slippage_percent, 3))}%</strong>
            </div>
            <div class="ai-route-metric">
              <span>${escapeHtml(pickLocalized('Latency', 'Латентность'))}</span>
              <strong>${escapeHtml(formatNumber(route.estimated_execution_ms, 0))} ms</strong>
            </div>
            <div class="ai-route-metric">
              <span>${escapeHtml(pickLocalized('Within user limit', 'Укладывается в лимит'))}</span>
              <strong>${escapeHtml(route.within_user_slippage ? pickLocalized('yes', 'да') : pickLocalized('no', 'нет'))}</strong>
            </div>
          </div>
        </article>
      `).join('')
    : `<div class="ai-route-card is-empty"><div class="ai-route-grid"><div class="ai-route-metric"><span>${escapeHtml(pickLocalized('Routing status', 'Статус маршрутизации'))}</span><strong>${escapeHtml(pickLocalized('No route recommendations yet.', 'Рекомендаций по маршрутам пока нет.'))}</strong></div></div></div>`;

  const dangerLevels = item.liquidationDangerLevels?.length
    ? item.liquidationDangerLevels.map((value) => formatNumber(value, 3)).join(', ')
    : pickLocalized('n/a', 'н/д');
  const liveUpdateNotice = item.aiLoading
    ? `
      <section class="detail-note-card">
        <h3>${escapeHtml(pickLocalized('AI live update', 'AI live-обновление'))}</h3>
        <ul>
          <li>${escapeHtml(pickLocalized('Snapshot advice is already shown below while live Qwen and the remaining AI metrics continue to refresh in the background.', 'Ниже уже показан snapshot-совет, а live Qwen и остальные AI-метрики продолжают обновляться в фоне.'))}</li>
        </ul>
      </section>
    `
    : '';

  return `
    ${economicsCard}
    ${liveUpdateNotice}
    ${!aiEndpointAvailable ? `
      <section class="detail-note-card">
        <h3>${escapeHtml(pickLocalized('AI snapshot mode', 'AI snapshot-режим'))}</h3>
        <ul>
          <li>${escapeHtml(pickLocalized('Live AI endpoints are unavailable right now, so the card shows the latest embedded AI/Qwen snapshot that came with the dataset.', 'Live AI endpoint-ы сейчас недоступны, поэтому карточка показывает последний встроенный AI/Qwen snapshot из набора данных.'))}</li>
        </ul>
      </section>
    ` : ''}
    <section class="detail-note-card risk-filter-card">
      <h3>${escapeHtml(briefingTitle)}</h3>
      <p class="detail-note-copy"><span class="chip ${escapeHtml(adviceChipClass)}">${escapeHtml(adviceVerdictLabel)}</span> ${escapeHtml(briefingSummary)}</p>
      ${briefingHighlightMarkup}
      ${resolvedAdviceSections.length ? resolvedAdviceSections.map((section) => `
        <p class="detail-note-copy"><strong>${escapeHtml(section.title)}:</strong></p>
        <ul>
          ${section.lines.map((line) => `<li>${escapeHtml(String(line))}</li>`).join('')}
        </ul>
      `).join('') : ''}
    </section>
    <section class="detail-note-card">
      <h3>${escapeHtml(pickLocalized('AI confidence factors', 'Факторы AI confidence'))}</h3>
      <ul>${factors}</ul>
    </section>
    <section class="detail-note-card">
      <h3>${escapeHtml(pickLocalized('Whale and liquidation view', 'Whale и liquidation overview'))}</h3>
      <ul class="detail-note-list">
        <li>${escapeHtml(`${pickLocalized('Whale index', 'Индекс whale-потока')}: ${item.whaleIndex == null ? pickLocalized('pending', 'ожидание') : `${formatNumber(item.whaleIndex, 0)}/100`}`)}</li>
        <li>${escapeHtml(`${pickLocalized('Liquidation risk', 'Риск ликвидаций')}: ${item.liquidationRisk || pickLocalized('pending', 'ожидание')}`)}</li>
        <li>${escapeHtml(`${pickLocalized('Danger levels', 'Опасные уровни')}: ${dangerLevels}`)}</li>
      </ul>
      <ul class="detail-note-list detail-note-list-muted">${whaleLines}</ul>
    </section>
    <section class="detail-note-card ai-route-section">
      <h3>${escapeHtml(pickLocalized('Smart routes', 'Умные маршруты'))}</h3>
      <div class="ai-route-list">${routeMarkup}</div>
    </section>
  `;
}

function renderIndicatorAnalyticsCard(item) {
  const analytics = item?.indicatorAnalytics && typeof item.indicatorAnalytics === 'object' ? item.indicatorAnalytics : null;
  if (!analytics) {
    return '';
  }
  const correlationStateLabel = analytics.correlationState === 'high'
    ? pickLocalized('High', 'Высокая')
    : analytics.correlationState === 'medium'
      ? pickLocalized('Medium', 'Средняя')
      : analytics.correlationState === 'low'
        ? pickLocalized('Low', 'Низкая')
        : pickLocalized('n/a', 'н/д');
  const liquidityLabel = analytics.liquidityLevel === 'high'
    ? pickLocalized('High', 'Высокая')
    : analytics.liquidityLevel === 'medium'
      ? pickLocalized('Medium', 'Средняя')
      : analytics.liquidityLevel === 'low'
        ? pickLocalized('Low', 'Низкая')
        : pickLocalized('n/a', 'н/д');
  const squeezeLabel = analytics.squeezeState === 'compression'
    ? pickLocalized('Compression', 'Сжатие')
    : analytics.squeezeState === 'expansion'
      ? pickLocalized('Expansion', 'Расширение')
      : pickLocalized('Neutral', 'Нейтрально');
  const regressionDirectionLabel = analytics.regressionDirection === 'UP'
    ? pickLocalized('Up', 'Рост')
    : analytics.regressionDirection === 'DOWN'
      ? pickLocalized('Down', 'Снижение')
      : analytics.regressionDirection === 'STABLE'
        ? pickLocalized('Stable', 'Стабильно')
        : pickLocalized('n/a', 'н/д');
  const highlightMetrics = [
    {
      label: pickLocalized('Spread RSI', 'RSI спреда'),
      value: analytics.spreadRsi == null ? null : formatNumber(analytics.spreadRsi, 1),
      className: analytics.spreadRsi == null ? '' : analytics.spreadRsi >= 70 ? 'risk-negative' : analytics.spreadRsi <= 30 ? 'value-positive' : '',
    },
    {
      label: pickLocalized('Extremity index', 'Индекс экстремальности'),
      value: analytics.extremityIndex == null ? null : `${formatNumber(analytics.extremityIndex, 0)}/100`,
      className: analytics.extremityIndex == null ? '' : analytics.extremityIndex >= 75 ? 'risk-negative' : analytics.extremityIndex >= 55 ? 'value-warning' : '',
    },
    {
      label: pickLocalized('Leg correlation', 'Корреляция ног'),
      value: analytics.exchangeCorrelation == null ? correlationStateLabel : `${formatNumber(analytics.exchangeCorrelation, 2)} · ${correlationStateLabel}`,
      className: analytics.exchangeCorrelation == null ? '' : analytics.exchangeCorrelation >= 0.95 ? 'value-positive' : analytics.exchangeCorrelation >= 0.8 ? 'value-warning' : 'risk-negative',
    },
    {
      label: pickLocalized('Liquidity regime', 'Режим ликвидности'),
      value: analytics.liquidityScore == null ? liquidityLabel : `${liquidityLabel} · ${formatNumber(analytics.liquidityScore, 0)}/100`,
      className: analytics.liquidityLevel === 'high' ? 'value-positive' : analytics.liquidityLevel === 'medium' ? 'value-warning' : 'risk-negative',
    },
    {
      label: pickLocalized('Regression bias', 'Регрессионный уклон'),
      value: regressionDirectionLabel,
      className: analytics.regressionDirection === 'DOWN' ? 'value-positive' : analytics.regressionDirection === 'UP' ? 'risk-negative' : '',
    },
    {
      label: pickLocalized('Squeeze state', 'Состояние BB'),
      value: squeezeLabel,
      className: analytics.squeezeState === 'compression' ? 'value-warning' : analytics.squeezeState === 'expansion' ? 'risk-negative' : '',
    },
  ].filter((metric) => metric.value != null);

  const notes = [];
  if (analytics.regressionForecast != null) {
    const band = analytics.regressionLower != null && analytics.regressionUpper != null
      ? ` (${formatNumber(analytics.regressionLower, 3)}% - ${formatNumber(analytics.regressionUpper, 3)}%)`
      : '';
    notes.push(pickLocalized(
      `Next-step regression on service net spread points to ${formatNumber(analytics.regressionForecast, 3)}%${band}.`,
      `Линейная регрессия по сервисному net spread даёт ориентир ${formatNumber(analytics.regressionForecast, 3)}%${band}.`,
    ));
  }
  if (analytics.supportLevel != null && analytics.resistanceLevel != null) {
    notes.push(pickLocalized(
      `Historical support/resistance on the active spread window: ${formatNumber(analytics.supportLevel, 3)}% / ${formatNumber(analytics.resistanceLevel, 3)}%.`,
      `Исторические support/resistance на активном окне спреда: ${formatNumber(analytics.supportLevel, 3)}% / ${formatNumber(analytics.resistanceLevel, 3)}%.`,
    ));
  }
  if (analytics.signalNoiseRatio != null) {
    notes.push(pickLocalized(
      `Signal-to-noise ratio is ${formatNumber(analytics.signalNoiseRatio, 2)}. Higher means the move is cleaner relative to its own recent noise.`,
      `Отношение сигнал/шум сейчас ${formatNumber(analytics.signalNoiseRatio, 2)}. Чем выше, тем чище движение относительно собственного недавнего шума.`,
    ));
  }
  if (analytics.hurst != null) {
    notes.push(pickLocalized(
      `Approximate Hurst exponent is ${formatNumber(analytics.hurst, 2)}. Values below 0.5 lean mean-reverting, above 0.5 lean persistent.`,
      `Приближённый показатель Хёрста равен ${formatNumber(analytics.hurst, 2)}. Значения ниже 0.5 ближе к возврату к среднему, выше 0.5 ближе к персистентности.`,
    ));
  }
  if (analytics.skewness != null || analytics.kurtosis != null) {
    notes.push(pickLocalized(
      `Distribution shape: skew ${formatNumber(analytics.skewness, 2)}, kurtosis ${formatNumber(analytics.kurtosis, 2)}.`,
      `Форма распределения: skew ${formatNumber(analytics.skewness, 2)}, kurtosis ${formatNumber(analytics.kurtosis, 2)}.`,
    ));
  }

  return `
    <section class="detail-note-card">
      <h3>${escapeHtml(pickLocalized('Service-adapted Pine analytics', 'Сервисная Pine-аналитика'))}</h3>
      <p class="detail-note-copy">${escapeHtml(pickLocalized('This block ports the useful parts of the reference TradingView study into backend calculations on net spread history, so the service can use them in snapshots, Qwen context and the detail modal.', 'Этот блок переносит полезные части референсного TradingView-индикатора в backend-расчёты по истории net spread, чтобы сервис мог использовать их в snapshot, контексте Qwen и detail modal.'))}</p>
      ${highlightMetrics.length ? `
        <div class="ai-economics-grid">
          ${highlightMetrics.map((metric) => `
            <article class="ai-economics-metric">
              <span>${escapeHtml(metric.label)}</span>
              <strong class="${escapeHtml(metric.className || '')}">${escapeHtml(metric.value)}</strong>
            </article>
          `).join('')}
        </div>
      ` : ''}
      ${notes.length ? `
        <ul class="detail-note-list">
          ${notes.map((line) => `<li>${escapeHtml(line)}</li>`).join('')}
        </ul>
      ` : ''}
    </section>
  `;
}

function buildOpportunityNextSteps(item, exchangeRisks) {
  if (Array.isArray(item.tradePlan?.manualSteps) && item.tradePlan.manualSteps.length) {
    return item.tradePlan.manualSteps;
  }
  const steps = [];
  const degradedExchange = exchangeRisks.some((note) => /ошибка|карантин|failed|quarantined/i.test(note));
  const lowLiquidity = Math.min(Number(item.volumeA || 0), Number(item.volumeB || 0)) < 250000;
  const hardExecution = Number(item.executionDifficulty || 0) >= 70;

  if (degradedExchange || lowLiquidity || hardExecution) {
    steps.push(pickLocalized(
      'Do not rush into market entry yet. First wait for cleaner execution conditions or reduce the intended size materially.',
      'Пока не входите агрессивно по рынку. Сначала дождитесь более чистого исполнения или заметно уменьшите размер позиции.',
    ));
  } else {
    steps.push(pickLocalized(
      'The setup can stay in your active shortlist: execution conditions are acceptable enough for the next check.',
      'Связку можно оставить в активном коротком списке: условия исполнения пока достаточно чистые для следующей проверки.',
    ));
  }

  if (Number(item.reversionProbability || 0) >= 0.65 && item.lifecycle !== 'widening') {
    steps.push(pickLocalized(
      `If you plan an entry, keep it only while net spread remains inside ${item.execution.entryZone}. Chasing above that zone weakens the asymmetry.`,
      `Если планируете вход, держите его только пока чистый спред остаётся в зоне ${item.execution.entryZone}. Догонять выше этой зоны значит ухудшать асимметрию сделки.`,
    ));
  } else {
    steps.push(pickLocalized(
      'For now the better action is observation, not entry: let the spread stop widening or wait for a stronger reversion signal.',
      'Пока лучше наблюдать, а не входить: дождитесь, чтобы спред перестал расширяться, или чтобы сигнал на возврат стал сильнее.',
    ));
  }

  steps.push(pickLocalized(
    `Plan the exit in advance: start reducing risk as the spread returns toward ${item.execution.exitZone} instead of waiting for a perfect zero.`,
    `План выхода лучше фиксировать заранее: начинайте сокращать риск, когда спред возвращается в ${item.execution.exitZone}, а не ждите идеального нуля.`,
  ));

  steps.push(pickLocalized(
    'If the base setup still looks valid after these checks, open the dedicated AI review for this pair to confirm timing, expected return and route quality.',
    'Если после этих проверок базовая структура всё ещё выглядит рабочей, откройте отдельный AI-разбор этой пары, чтобы подтвердить тайминг входа, ожидаемую доходность и маршрут исполнения.',
  ));

  return steps;
}

function toneFromChecklistStatus(status) {
  if (status === 'ok') {
    return 'positive';
  }
  if (status === 'attention') {
    return 'warning';
  }
  return 'negative';
}

function formatChecklistStatus(status) {
  if (status === 'ok') {
    return pickLocalized('OK', 'OK');
  }
  if (status === 'attention') {
    return pickLocalized('Attention', 'Внимание');
  }
  return pickLocalized('Block', 'Блок');
}

function formatTradePlanAction(action, fallback = '') {
  if (action === 'ready') {
    return pickLocalized('Ready for manual plan', 'Готово для ручного плана');
  }
  if (action === 'reduce') {
    return pickLocalized('Reduced-size plan', 'План с уменьшенным размером');
  }
  if (action === 'watch') {
    return pickLocalized('Review and wait', 'Проверить и подождать');
  }
  if (action === 'blocked') {
    return pickLocalized('Watch only', 'Только наблюдение');
  }
  return fallback || pickLocalized('Review manually', 'Проверить вручную');
}

function renderConfirmationCard(item) {
  const bundle = item.confirmations;
  const signals = Array.isArray(bundle?.items) ? bundle.items : [];
  if (!signals.length) {
    return '';
  }
  const rule = bundle.rule || {};
  return `
    <section class="detail-note-card risk-filter-card">
      <h3>${escapeHtml(pickLocalized('Signal confirmations', 'Подтверждения сигнала'))}</h3>
      <p class="detail-note-copy">${escapeHtml(bundle.summary || '')} ${escapeHtml(`${rule.positiveCount || 0}/${rule.selected || 0} ${pickLocalized('selected confirmations are positive.', 'выбранных подтверждений положительны.')}`)}</p>
      <div class="risk-filter-grid">
        ${signals.map((signal) => `
          <article class="risk-filter-item">
            <div class="risk-filter-head">
              <span class="chip risk-chip risk-${escapeHtml(signal.status || 'warning')}">${escapeHtml(signal.selected ? pickLocalized('In rule', 'В правиле') : pickLocalized('Extra context', 'Доп. контекст'))}</span>
              <strong class="risk-filter-status">${escapeHtml(String(signal.label || pickLocalized('Confirmation', 'Подтверждение')))}</strong>
            </div>
            <p>${escapeHtml(String(signal.reason || ''))}</p>
          </article>
        `).join('')}
      </div>
    </section>
  `;
}

function renderChecklistCard(item) {
  const checklist = item.preTradeChecklist;
  const entries = Array.isArray(checklist?.items) ? checklist.items : [];
  if (!entries.length) {
    return '';
  }
  const tone = toneFromChecklistStatus(checklist.status);
  return `
    <section class="detail-note-card risk-filter-card">
      <h3>${escapeHtml(pickLocalized('Pre-trade checklist', 'Чеклист перед входом'))}</h3>
      <p class="detail-note-copy"><span class="chip risk-chip risk-${escapeHtml(tone)}">${escapeHtml(formatChecklistStatus(checklist.status))}</span> ${escapeHtml(String(checklist.summary || ''))}</p>
      <div class="risk-filter-grid">
        ${entries.map((entry) => `
          <article class="risk-filter-item">
            <div class="risk-filter-head">
              <span class="chip risk-chip risk-${escapeHtml(toneFromChecklistStatus(entry.status))}">${escapeHtml(formatChecklistStatus(entry.status))}</span>
              <strong class="risk-filter-status">${escapeHtml(String(entry.label || pickLocalized('Check', 'Проверка')))}</strong>
            </div>
            <p>${escapeHtml(String(entry.reason || ''))}</p>
          </article>
        `).join('')}
      </div>
    </section>
  `;
}

function renderTradeTrackerCard(item) {
  const tradeAlertModel = getTradeAlertModel(item);
  const position = getTrackedPosition(item);
  const isWatched = isTrackedTrade(item);
  const trackerSummary = position
    ? t('trade.enteredAt', { value: formatDateTime(position.enteredAt) })
    : isWatched
      ? t('trade.watchStatusOn')
      : t('trade.watchStatusOff');
  return `
    <section class="detail-note-card detail-action-card trade-tracker-card">
      <h3>${escapeHtml(t('hero.smartNotices'))}</h3>
      <p class="detail-note-copy"><span class="chip risk-chip risk-${escapeHtml(tradeAlertModel.tone)}">${escapeHtml(tradeAlertModel.title)}</span> ${escapeHtml(trackerSummary)}</p>
      <div class="ai-route-grid">
        <article class="ai-route-metric">
          <span>${escapeHtml(t('trade.routeLabel'))}</span>
          <strong>${escapeHtml(tradeAlertModel.positionLabel)}</strong>
        </article>
        <article class="ai-route-metric">
          <span>${escapeHtml(t('detail.holdingTimer'))}</span>
          <strong>${escapeHtml(tradeAlertModel.timerLabel)}</strong>
        </article>
        <article class="ai-route-metric">
          <span>${escapeHtml(t('cross.label.direction'))}</span>
          <strong>${escapeHtml(item.direction || pickLocalized('n/a', 'н/д'))}</strong>
        </article>
        <article class="ai-route-metric">
          <span>${escapeHtml(t('cross.label.exitTarget'))}</span>
          <strong>${escapeHtml(item.exitTarget || pickLocalized('Pending', 'Ожидание'))}</strong>
        </article>
      </div>
      <div class="trade-tracker-actions">
        ${isWatched
          ? `<button class="ghost-button" type="button" data-track-watch-off="${escapeHtml(item.id)}">${escapeHtml(t('trade.unwatchAction'))}</button>`
          : `<button class="action-button" type="button" data-track-watch-on="${escapeHtml(item.id)}">${escapeHtml(t('trade.watchAction'))}</button>`}
        ${position
          ? `<button class="ghost-button" type="button" data-track-exit="${escapeHtml(item.id)}">${escapeHtml(t('trade.exitAction'))}</button>`
          : `<button class="ghost-button" type="button" data-track-entry="${escapeHtml(item.id)}">${escapeHtml(t('trade.enterAction'))}</button>`}
      </div>
    </section>
  `;
}

function renderTradePlanCard(item) {
  const plan = item.tradePlan;
  if (!plan) {
    return '';
  }
  const profile = item.riskProfile || {};
  const warnings = Array.isArray(plan.warnings) ? plan.warnings : [];
  const steps = Array.isArray(plan.manualSteps) ? plan.manualSteps : [];
  const actionTone = plan.action === 'blocked' ? 'negative' : plan.action === 'reduce' || plan.action === 'watch' ? 'warning' : 'positive';
  return `
    <section class="detail-note-card risk-filter-card detail-action-card">
      <h3>${escapeHtml(pickLocalized('Manual trade plan', 'Ручной торговый план'))}</h3>
      <p class="detail-note-copy"><span class="chip risk-chip risk-${escapeHtml(actionTone)}">${escapeHtml(formatTradePlanAction(plan.action, plan.actionLabel))}</span> ${escapeHtml(String(profile.description || ''))}</p>
      <div class="ai-route-grid">
        <article class="ai-route-metric">
          <span>${escapeHtml(pickLocalized('Risk profile', 'Риск-профиль'))}</span>
          <strong>${escapeHtml(String(profile.label || pickLocalized('n/a', 'н/д')))}</strong>
        </article>
        <article class="ai-route-metric">
          <span>${escapeHtml(pickLocalized('Suggested notional', 'Рекомендуемый размер'))}</span>
          <strong>${escapeHtml(formatCurrency(plan.suggestedNotionalUsd || 0))}</strong>
        </article>
        <article class="ai-route-metric">
          <span>${escapeHtml(pickLocalized('Capital risk', 'Риск на капитал'))}</span>
          <strong>${escapeHtml(formatNumber(plan.capitalRiskPercent || 0, 2))}%</strong>
        </article>
        <article class="ai-route-metric">
          <span>${escapeHtml(pickLocalized('Risk:Reward', 'Риск/прибыль'))}</span>
          <strong>${escapeHtml(formatNumber(plan.riskReward || 0, 2))}</strong>
        </article>
        <article class="ai-route-metric">
          <span>${escapeHtml(pickLocalized('Protective stop spread', 'Защитный stop по спреду'))}</span>
          <strong>${escapeHtml(formatNumber(plan.protectiveStopSpread || 0, 3))}%</strong>
        </article>
        <article class="ai-route-metric">
          <span>${escapeHtml(pickLocalized('Take-profit spread', 'Целевой spread для выхода'))}</span>
          <strong>${escapeHtml(formatNumber(plan.takeProfitSpread || 0, 3))}%</strong>
        </article>
      </div>
      <ul>
        <li>${escapeHtml(`${pickLocalized('Entry zone', 'Зона входа')}: ${plan.entryZone || pickLocalized('n/a', 'н/д')}`)}</li>
        <li>${escapeHtml(`${pickLocalized('Exit zone', 'Зона выхода')}: ${plan.exitZone || pickLocalized('n/a', 'н/д')}`)}</li>
        <li>${escapeHtml(`${pickLocalized('Minimum profile R:R', 'Минимальный R:R профиля')}: ${formatNumber(profile.minRiskReward || 0, 2)}`)}</li>
      </ul>
      ${warnings.length ? `
        <ul>
          ${warnings.map((warning) => `<li>${escapeHtml(String(warning))}</li>`).join('')}
        </ul>
      ` : ''}
      ${steps.length ? `
        <ul>
          ${steps.map((step) => `<li>${escapeHtml(String(step))}</li>`).join('')}
        </ul>
      ` : ''}
    </section>
  `;
}

function renderAiRedirectCard(item) {
  return `
    <section class="detail-note-card detail-action-card">
      <h3>${escapeHtml(pickLocalized('AI review is in a separate tab', 'AI-разбор вынесен в отдельную вкладку'))}</h3>
      <p>${escapeHtml(pickLocalized(
        'The pair card now shows only the base trading structure. Open AI Analytics only after spread, execution and risk filters already look acceptable.',
        'В карточке пары теперь остаётся только базовая торговая структура. Переходите в AI Аналитику только после того, как спред, исполнение и ограничения риска уже выглядят приемлемо.',
      ))}</p>
      <button class="action-button" type="button" data-open-ai-focus="${escapeHtml(item.id)}">${escapeHtml(pickLocalized('Open AI review for this pair', 'Открыть AI-разбор по этой паре'))}</button>
    </section>
  `;
}

function focusAiAnalyticsOpportunity(opportunityId, { closeCurrentModal = true } = {}) {
  state.aiFocusedOpportunityId = opportunityId || null;
  if (closeCurrentModal) {
    closeModal();
  }
  switchTab('ai');
}

function cancelPendingDetailRender() {
  state.detailRenderToken += 1;
  if (state.detailOverviewFrameId) {
    window.cancelAnimationFrame(state.detailOverviewFrameId);
    state.detailOverviewFrameId = 0;
  }
  if (state.detailNotesFrameId) {
    window.cancelAnimationFrame(state.detailNotesFrameId);
    state.detailNotesFrameId = 0;
  }
}

function renderDetailOverviewMarkup(item) {
  return getDetailMetricItems(item)
    .map(
      ({ label, hint, value }) => `
        <article class="metric-box detail-metric-box">
          ${renderMetricLabel(label, hint)}
          <strong>${escapeHtml(String(value))}</strong>
        </article>
      `,
    )
    .join('');
}

function renderDetailNotesMarkup(item) {
  const healthA = getExchangeHealth(item.exchangeA);
  const healthB = getExchangeHealth(item.exchangeB);
  const exchangeRisks = [
    buildExchangeRiskLine(item.exchangeA, healthA),
    buildExchangeRiskLine(item.exchangeB, healthB),
  ].filter(Boolean);
  const riskNotes = buildRiskNotes(item, exchangeRisks);
  const nextSteps = buildOpportunityNextSteps(item, exchangeRisks);

  return `
    ${renderTradeTrackerCard(item)}
    <section class="detail-note-card detail-action-card">
      <h3>${escapeHtml(t('detail.nextStep'))}</h3>
      <ul>
        ${nextSteps.map((note) => `<li>${escapeHtml(note)}</li>`).join('')}
      </ul>
    </section>
    ${renderTradePlanCard(item)}
    ${renderConfirmationCard(item)}
    ${renderChecklistCard(item)}
    <section class="detail-note-card">
      <h3>${escapeHtml(t('detail.executionNotes'))}</h3>
      <ul>
        ${item.execution.notes.map((note) => `<li>${escapeHtml(note)}</li>`).join('')}
      </ul>
    </section>
    <section class="detail-note-card">
      <h3>${escapeHtml(t('detail.riskNotes'))}</h3>
      <ul>
        ${riskNotes.map((note) => `<li>${escapeHtml(note)}</li>`).join('')}
      </ul>
    </section>
    ${renderIndicatorAnalyticsCard(item)}
    ${renderAiDetailCards(item)}
    ${renderAiRedirectCard(item)}
    <section class="detail-note-card risk-filter-card">
      <h3>${escapeHtml(t('detail.riskFilters'))}</h3>
      <div class="risk-filter-grid">
        ${Object.entries(item.riskFlags || {})
          .filter(([key]) => key !== 'marketPermission' && key !== 'overlap')
          .map(([key, flag]) => `
          <article class="risk-filter-item">
            <div class="risk-filter-head">
              <span class="chip risk-chip risk-${escapeHtml(flag.status || 'warning')}">${escapeHtml(formatRiskFilterLabel(key))}</span>
              <strong class="risk-filter-status">${escapeHtml(String(flag.label || t('common.na')))}</strong>
            </div>
            <p>${escapeHtml(String(flag.reason || ''))}</p>
          </article>
        `).join('')}
      </div>
    </section>
  `;
}

function renderDetailLoadingState() {
  dom.detailOverview.setAttribute('aria-busy', 'true');
  dom.detailNotes.setAttribute('aria-busy', 'true');
  dom.detailOverview.innerHTML = `
    <article class="metric-box detail-metric-box detail-loading-card">
      <span>${escapeHtml(pickLocalized('Opening pair brief', 'Открываем разбор пары'))}</span>
      <strong>${escapeHtml(pickLocalized('Core metrics will appear first.', 'Сначала появятся базовые метрики.'))}</strong>
    </article>
  `;
  dom.detailNotes.innerHTML = `
    <section class="detail-note-card detail-loading-card">
      <h3>${escapeHtml(pickLocalized('Loading detail blocks', 'Загружаем блоки детализации'))}</h3>
      <p class="detail-note-copy">${escapeHtml(pickLocalized('Trade plan, checklist and AI notes are rendered right after the modal paint.', 'Торговый план, чеклист и AI-блоки дорисуются сразу после первого кадра модалки.'))}</p>
    </section>
  `;
}

function openOpportunityDetail(item) {
  cancelPendingDetailRender();
  state.activeDetailId = item.id;
  const renderToken = state.detailRenderToken;

  dom.modal.classList.remove('hidden');
  dom.modal.setAttribute('aria-hidden', 'false');
  syncOverlayState();
  dom.modalTitle.textContent = `${item.pairA} · ${item.exchangeA} vs ${item.exchangeB}`;
  dom.modalSubtitle.textContent = `${t('detail.rankingScore')} ${item.score}/100 · ${item.regimeLabel} · ${translateLifecycle(item.lifecycle)} · ${item.execution.difficultyLabel} ${t('detail.executionDifficulty').toLowerCase()}`;
  renderDetailLoadingState();

  state.detailOverviewFrameId = window.requestAnimationFrame(() => {
    state.detailOverviewFrameId = window.requestAnimationFrame(() => {
      state.detailOverviewFrameId = 0;
      if (state.detailRenderToken !== renderToken || state.activeDetailId !== item.id || dom.modal.classList.contains('hidden')) {
        return;
      }
      dom.detailOverview.innerHTML = renderDetailOverviewMarkup(item);
      dom.detailOverview.setAttribute('aria-busy', 'false');

      state.detailNotesFrameId = window.requestAnimationFrame(() => {
        state.detailNotesFrameId = 0;
        if (state.detailRenderToken !== renderToken || state.activeDetailId !== item.id || dom.modal.classList.contains('hidden')) {
          return;
        }
        dom.detailNotes.innerHTML = renderDetailNotesMarkup(item);
        dom.detailNotes.setAttribute('aria-busy', 'false');
        dom.detailNotes.querySelector('[data-open-ai-focus]')?.addEventListener('click', () => {
          focusAiAnalyticsOpportunity(item.id);
        });
        dom.detailNotes.querySelector('[data-track-watch-on]')?.addEventListener('click', () => {
          watchTrackedTrade(item);
        });
        dom.detailNotes.querySelector('[data-track-watch-off]')?.addEventListener('click', () => {
          unwatchTrackedTrade(item);
        });
        dom.detailNotes.querySelector('[data-track-entry]')?.addEventListener('click', () => {
          markTrackedTradeEntry(item);
        });
        dom.detailNotes.querySelector('[data-track-exit]')?.addEventListener('click', () => {
          closeTrackedTrade(item);
        });
      });
    });
  });

  void ensureOpportunityAiData(item);
  if (!getDatasetStatus('adapterHealth').loaded && !getDatasetStatus('adapterHealth').loading) {
    void loadDataset('adapterHealth');
  }
  void ensureCrossHistoryAnalytics([item.id]);
}

function formatRiskFilterLabel(key) {
  if (key === 'volatility') {
    return t('risk.volatility');
  }
  if (key === 'liquidity') {
    return t('risk.liquidity');
  }
  if (key === 'funding') {
    return t('risk.funding');
  }
  if (key === 'eventRisk') {
    return t('risk.newsStyle');
  }
  return key;
}

function buildExchangeRiskLine(exchange, health) {
  if (!health) {
    return t('exchange.noTelemetry', { exchange });
  }
  const coverage = computeCoverageRatio(health);
  const baseStatus = translateHealthStatus(health.status || 'unknown');
  if (health.status === 'ok' && !health.last_error) {
    return t('exchange.base', { exchange, status: baseStatus, coverage: formatNumber(coverage, 1) });
  }
  return t('exchange.baseError', {
    exchange,
    status: baseStatus,
    coverage: formatNumber(coverage, 1),
    error: String(health.last_error || 'API degradation detected.'),
  });
}

function buildRiskNotes(item, exchangeRisks) {
  const notes = [];
  const maxBidAsk = Math.max(Number(item.spreadA || 0), Number(item.spreadB || 0));
  const minVolume = Math.min(Number(item.volumeA || 0), Number(item.volumeB || 0));
  if (maxBidAsk > 0.2) {
    notes.push(t('risk.noteWideSpread'));
  }
  if (minVolume < 250000) {
    notes.push(t('risk.noteLowLiquidity'));
  }
  if (item.lifecycle === 'widening') {
    notes.push(t('risk.noteWidening'));
  }
  if (item.regime === 'noisy-range' || item.regime === 'volatile-expansion') {
    notes.push(t('risk.noteNoisy'));
  }
  if (item.meanReversionHours && item.meanReversionHours > 72) {
    notes.push(t('risk.noteLongHold'));
  }
  if (!notes.length && !exchangeRisks.some((note) => note.includes(t('status.failed')) || note.includes(t('status.quarantined')))) {
    notes.push(t('risk.noteClean'));
  }
  return notes;
}

function normalizeRiskFlags(riskFlags) {
  if (!riskFlags || typeof riskFlags !== 'object' || Array.isArray(riskFlags)) {
    return null;
  }
  return riskFlags;
}

function buildOpportunityRiskFlags(item, derived, analytics) {
  const maxBidAsk = Math.max(Number(item.spreadA || 0), Number(item.spreadB || 0));
  const minVolume = Math.min(Number(item.volumeA || 0), Number(item.volumeB || 0));
  const fundingDelta = Math.abs(Number(item.fundingB || 0) - Number(item.fundingA || 0));
  const degradedExchange = [item.exchangeA, item.exchangeB].some((exchange) => {
    const status = getExchangeHealth(exchange)?.status || 'ok';
    return status !== 'ok';
  });

  const volatility = analytics.regime === 'volatile-expansion' || Math.abs(analytics.zScore) >= 2.8
    ? {
        status: 'negative',
        label: pickLocalized('high', 'высокая'),
        reason: pickLocalized(
          `${translateRegime(analytics.regime)} regime with Z-score ${formatNumber(analytics.zScore, 2)}.`,
          `Режим спреда ${translateRegime(analytics.regime)} при Z-оценке ${formatNumber(analytics.zScore, 2)}.`,
        ),
      }
    : analytics.regime === 'widening-trend' || analytics.regime === 'noisy-range' || Math.abs(analytics.zScore) >= 1.6
      ? {
          status: 'warning',
          label: pickLocalized('elevated', 'повышенная'),
          reason: pickLocalized(
            `Percentile ${formatNumber(analytics.percentile, 0)}% and unstable spread behavior.`,
            `Percentile ${formatNumber(analytics.percentile, 0)}% и нестабильное поведение спреда.`,
          ),
        }
      : {
          status: 'positive',
          label: pickLocalized('contained', 'сдержанная'),
          reason: pickLocalized(
            'Spread volatility is contained relative to its own history.',
            'Волатильность спреда сдержана относительно собственной истории.',
          ),
        };

  const liquidity = minVolume < 250000 || maxBidAsk > 0.2
    ? {
        status: 'negative',
        label: pickLocalized('thin', 'тонкая'),
        reason: pickLocalized(
          `Min venue volume ${formatCompact(minVolume)} and max bid/ask ${formatNumber(maxBidAsk, 3)}%.`,
          `Минимальный объём площадки ${formatCompact(minVolume)} и max bid/ask ${formatNumber(maxBidAsk, 3)}%.`,
        ),
      }
    : minVolume < 1000000 || maxBidAsk > 0.1
      ? {
          status: 'warning',
          label: pickLocalized('watch', 'под наблюдением'),
          reason: pickLocalized(
            `Liquidity is workable but not clean: ${formatCompact(minVolume)} / ${formatNumber(maxBidAsk, 3)}%.`,
            `Ликвидность рабочая, но не чистая: ${formatCompact(minVolume)} / ${formatNumber(maxBidAsk, 3)}%.`,
          ),
        }
      : {
          status: 'positive',
          label: pickLocalized('healthy', 'здоровая'),
          reason: pickLocalized(
            'Both legs show acceptable liquidity and spread width.',
            'Обе ноги показывают приемлемую ликвидность и ширину спреда.',
          ),
        };

  const funding = fundingDelta >= 0.01 || Math.min(Number(item.fundingA || 0), Number(item.fundingB || 0)) <= -0.005
    ? {
        status: 'negative',
        label: pickLocalized('adverse', 'неблагоприятный'),
        reason: pickLocalized(
          `Funding skew is material at ${formatNumber(fundingDelta, 4)}% across venues.`,
          `Перекос ставок существенный: ${formatNumber(fundingDelta, 4)}% между площадками.`,
        ),
      }
    : fundingDelta >= 0.003 || Math.max(Math.abs(Number(item.fundingA || 0)), Math.abs(Number(item.fundingB || 0))) >= 0.003
      ? {
          status: 'warning',
          label: pickLocalized('mixed', 'смешанный'),
          reason: pickLocalized(
            `Funding is non-trivial at ${formatNumber(item.fundingA, 4)}% / ${formatNumber(item.fundingB, 4)}%.`,
            `Влияние ставок уже ощутимо: ${formatNumber(item.fundingA, 4)}% / ${formatNumber(item.fundingB, 4)}%.`,
          ),
        }
      : {
          status: 'positive',
          label: pickLocalized('neutral', 'нейтральный'),
          reason: pickLocalized(
            'Funding carry is small relative to the current spread.',
            'Доход от удержания мал по сравнению с текущим спредом.',
          ),
        };

  const eventRisk = degradedExchange || ((analytics.regime === 'widening-trend' || analytics.regime === 'volatile-expansion') && analytics.timeframeAlignment >= 70 && Math.abs(analytics.zScore) >= 2.2)
    ? {
        status: 'negative',
        label: pickLocalized('news-style', 'событийный'),
        reason: pickLocalized(
          'Proxy stress signal: abrupt widening and/or degraded exchange health.',
          'Прокси-сигнал стресса: резкое расширение и/или деградация здоровья биржи.',
        ),
      }
    : analytics.anomalyScore >= 70 || analytics.regime === 'noisy-range'
      ? {
          status: 'warning',
          label: pickLocalized('headline-like', 'похож на новостной'),
          reason: pickLocalized(
            'Proxy event risk is elevated, but no direct news feed is connected.',
            'Прокси-риск события повышен, но прямой новостной поток не подключён.',
          ),
        }
      : {
          status: 'positive',
          label: pickLocalized('calm', 'спокойный'),
          reason: pickLocalized(
            'No news-style stress proxy is active in the latest snapshot.',
            'В последнем снимке нет активного news-style прокси-стресса.',
          ),
        };

  return {
    volatility,
    liquidity,
    funding,
    eventRisk,
  };
}

function renderHeatmapPage() {
  const crossStatus = getDatasetStatus('cross');
  if (crossStatus.loading || (!crossStatus.loaded && !crossStatus.error)) {
    resetTabContent(null, null, dom.heatmapSummary, dom.heatmapGrid);
    showPanelState(dom.heatmapEmpty, getTabLoadingMessage('heatmap'));
    return;
  }
  if (crossStatus.error) {
    resetTabContent(null, null, dom.heatmapSummary, dom.heatmapGrid);
    showPanelState(dom.heatmapEmpty, crossStatus.error);
    return;
  }
  const rows = getSortedCrossRows().slice(0, 36);
  void ensureCrossHistoryAnalytics(rows.map((item) => item.id));
  const analytics = state.metrics?.analytics_summary;
  if (dom.heatmapEmpty) {
    dom.heatmapEmpty.classList.toggle('hidden', rows.length > 0);
  }
  if (!rows.length) {
    if (dom.heatmapSummary) {
      dom.heatmapSummary.innerHTML = '';
    }
    if (dom.heatmapGrid) {
      dom.heatmapGrid.innerHTML = '';
    }
    return;
  }

  const strongest = rows.reduce((best, row) => (row.anomalyScore > (best?.anomalyScore ?? -1) ? row : best), null);
  const avgReversion = average(rows.map((row) => row.reversionProbability)) * 100;
  const alignedCount = rows.filter((row) => row.timeframeAlignment >= 70).length;
  const convergingCount = rows.filter((row) => row.regime === 'clean-convergence').length;
  const rawCross = Number(analytics?.total_cross || rows.length || 0);
  const publishedCross = Number(analytics?.published_cross || rows.length || 0);
  const marketSummary = analytics && Number(analytics.total_cross || 0) > 0
    ? [
        {
          label: t('heatmap.marketVolatility'),
          value: translateVolatilityRegime(analytics.market_volatility_regime) || t('common.na'),
          detail: t('hero.elevated', { value: Number(analytics.elevated_volatility_cross || 0), total: rawCross }),
        },
        {
          label: t('heatmap.backdrop'),
          value: translateRiskBackdrop(analytics.risk_backdrop) || t('common.na'),
          detail: t('hero.published', { published: publishedCross, total: rawCross, regime: translateRegime(analytics.dominant_regime || 'forming'), share: formatNumber(Number(analytics.dominant_regime_share || 0) * 100, 0) }),
        },
      ]
    : [];

  dom.heatmapSummary.innerHTML = [
    ...marketSummary,
    {
      label: t('heatmap.strongest'),
      value: strongest ? `${strongest.pairA} · ${strongest.exchangeA}/${strongest.exchangeB}` : analytics?.strongest_anomaly_label || t('common.na'),
      detail: strongest
        ? t('heatmap.anomaly', { value: formatNumber(strongest.anomalyScore, 0) })
        : analytics?.strongest_anomaly_score
          ? t('heatmap.anomaly', { value: formatNumber(analytics.strongest_anomaly_score, 0) })
          : t('common.na'),
    },
    {
      label: t('heatmap.reversion'),
      value: `${formatNumber(avgReversion, 0)}%`,
      detail: analytics?.average_reversion_probability != null
        ? t('heatmap.marketAvg', { value: formatNumber(Number(analytics.average_reversion_probability || 0) * 100, 0) })
        : t('heatmap.tiles', { count: publishedCross }),
    },
    {
      label: t('heatmap.aligned'),
      value: `${alignedCount}/${rows.length}`,
      detail: analytics?.aligned_anomaly_cross != null
        ? t('heatmap.market', { value: Number(analytics.aligned_anomaly_cross || 0), total: rawCross })
        : t('heatmap.displayedTiles'),
    },
    {
      label: t('heatmap.cleanConvergence'),
      value: `${convergingCount}/${rows.length}`,
      detail: analytics?.clean_convergence_cross != null
        ? t('heatmap.market', { value: Number(analytics.clean_convergence_cross || 0), total: rawCross })
        : t('heatmap.displayedTiles'),
    },
  ]
    .map(
      (card) => `
        <article class="metric-box">
          <span>${escapeHtml(String(card.label))}</span>
          <strong>${escapeHtml(String(card.value))}</strong>
          <small>${escapeHtml(String(card.detail || ''))}</small>
        </article>
      `,
    )
    .join('');

  dom.heatmapGrid.innerHTML = rows
    .map((item) => {
      const heat = getHeatIntensity(item);
      return `
        <button class="heat-tile" type="button" data-detail-id="${escapeHtml(item.id)}" style="--heat-tone:${heat};">
          <div class="heat-tile-head">
            <strong>${escapeHtml(item.pairA)}</strong>
            ${renderReliabilityChip(item)}
          </div>
          <p class="heat-tile-route">${escapeHtml(item.exchangeA)} vs ${escapeHtml(item.exchangeB)}</p>
          <div class="heat-tile-metrics">
            <span><strong>${escapeHtml(t('heatmap.net'))}</strong>${escapeHtml(formatNumber(item.netSpread, 2))}%</span>
            <span><strong>${escapeHtml(t('heatmap.z'))}</strong>${escapeHtml(formatNumber(item.zScore, 2))}</span>
            <span><strong>${escapeHtml(t('heatmap.pct'))}</strong>${escapeHtml(formatNumber(item.percentile, 0))}%</span>
            <span><strong>${escapeHtml(t('heatmap.rev'))}</strong>${escapeHtml(formatNumber(item.reversionProbability * 100, 0))}%</span>
          </div>
          <div class="heat-tile-footer">
            <span class="chip regime-chip regime-${escapeHtml(item.regime)}">${escapeHtml(item.regimeLabel)}</span>
            <span class="heat-tile-anomaly">${escapeHtml(t('heatmap.anomaly', { value: String(item.anomalyScore) }))}</span>
          </div>
        </button>
      `;
    })
    .join('');

  dom.heatmapGrid.querySelectorAll('[data-detail-id]').forEach((tile) => {
    tile.addEventListener('click', () => {
      const item = rows.find((row) => row.id === tile.dataset.detailId);
      if (item) {
        openOpportunityDetail(item);
      }
    });
  });
}

function renderHealthPage() {
  const healthStatus = getDatasetStatus('adapterHealth');
  if (healthStatus.loading || (!healthStatus.loaded && !healthStatus.error)) {
    resetTabContent(dom.healthTable, dom.healthMobileList, dom.healthSummary, dom.healthDiagnostics);
    showPanelState(dom.healthEmpty, getTabLoadingMessage('health'));
    return;
  }
  if (healthStatus.error) {
    resetTabContent(dom.healthTable, dom.healthMobileList, dom.healthSummary, dom.healthDiagnostics);
    showPanelState(dom.healthEmpty, healthStatus.error);
    return;
  }

  const adapters = state.adapterHealth?.adapters || {};
  const rows = Object.entries(adapters).map(([exchange, health]) => buildHealthRow(exchange, health));
  const sorted = sortRows(rows, 'health');
  const pagedRows = getPaginatedRows('health', sorted, dom.healthMobileList || dom.healthTable?.parentElement);
  renderHeader(
    dom.healthTable,
    [
      { key: 'exchange', label: t('common.exchange'), hint: TABLE_HEADER_HINTS.health.exchange },
      { key: 'statusRank', label: t('common.status'), hint: TABLE_HEADER_HINTS.health.status },
      { key: 'last_success_at', label: t('common.lastSuccess'), hint: TABLE_HEADER_HINTS.health.lastSuccess },
      { key: 'coverageRatio', label: t('common.coverage'), hint: TABLE_HEADER_HINTS.health.coverage },
      { key: 'apiAlert', label: t('common.apiAlert'), hint: TABLE_HEADER_HINTS.health.apiAlert },
      { key: 'excludedReason', label: t('common.excludedWhy'), hint: TABLE_HEADER_HINTS.health.excluded },
    ],
    'health',
  );
  if (dom.healthEmpty) {
    dom.healthEmpty.classList.toggle('hidden', sorted.length > 0);
    if (!sorted.length) {
      dom.healthEmpty.textContent = t('panel.health.empty');
    }
  }
  if (dom.healthMobileList) {
    dom.healthMobileList.innerHTML = pagedRows.map((item) => renderHealthMobileCard(item)).join('');
  }
  if (!sorted.length) {
    dom.healthSummary.innerHTML = '';
    dom.healthTable.querySelector('tbody').innerHTML = '';
    return;
  }
  dom.healthTable.querySelector('tbody').innerHTML = pagedRows
    .map(
      (item) => `
        <tr>
          ${buildCell(t('common.exchange'), escapeHtml(item.exchange))}
          ${buildCell(t('common.status'), `<span class="chip lifecycle-chip lifecycle-${escapeHtml(item.statusTone)}">${escapeHtml(item.statusLabel)}</span>`, item.statusClass)}
          ${buildCell(t('common.lastSuccess'), escapeHtml(item.lastSuccessLabel))}
          ${buildCell(t('common.coverage'), `${formatNumber(item.coverageRatio, 1)}%`, item.coverageRatio >= 90 ? 'value-positive' : item.coverageRatio >= 60 ? 'value-warning' : 'value-negative')}
          ${buildCell(t('common.apiAlert'), escapeHtml(item.apiAlert), item.apiAlertClass)}
          ${buildCell(t('common.excludedWhy'), escapeHtml(item.excludedReason))}
        </tr>
      `,
    )
    .join('');

  const healthy = rows.filter((item) => item.status === 'ok').length;
  const degraded = rows.filter((item) => item.status !== 'ok').length;
  const avgCoverage = rows.length ? average(rows.map((item) => item.coverageRatio)) : 0;
  const alertSummary = getSystemAlertSummary();
  const quoteAge = state.metrics?.analytics_summary?.quote_age || {};
  dom.healthSummary.innerHTML = [
    [t('health.summaryHealthy'), healthy],
    [t('health.summaryDegraded'), degraded],
    [t('health.summaryCoverage'), `${formatNumber(avgCoverage, 1)}%`],
    [t('health.summaryAlerts'), Number(alertSummary.active_count || 0)],
    [t('health.summaryQuoteAge'), quoteAge.available ? `${formatNumber(Number(quoteAge.p95_seconds || 0), 0)}s` : t('common.na')],
    [t('health.summaryUpdated'), state.adapterHealth?.updated_at ? formatDateTime(state.adapterHealth.updated_at) : t('common.na')],
  ]
    .map(
      ([label, value]) => `
        <article class="metric-box">
          <span>${escapeHtml(String(label))}</span>
          <strong>${escapeHtml(String(value))}</strong>
        </article>
      `,
    )
    .join('');
  renderHealthDiagnostics();
}

function renderHealthMobileCard(item) {
  const coverageClass = item.coverageRatio >= 90 ? 'value-positive' : item.coverageRatio >= 60 ? 'value-warning' : 'value-negative';
  return `
    <article class="mobile-data-card mobile-data-card-compact">
      <div class="mobile-data-head">
        <div>
          <strong class="mobile-data-title">${escapeHtml(item.exchange)}</strong>
          <p class="mobile-data-subtitle">${escapeHtml(t('common.lastSuccess'))}: ${escapeHtml(item.lastSuccessLabel)}</p>
        </div>
        <span class="chip lifecycle-chip lifecycle-${escapeHtml(item.statusTone)}">${escapeHtml(item.statusLabel)}</span>
      </div>
      <div class="mobile-data-grid">
        <article class="mobile-data-metric">
          <span>${escapeHtml(t('common.coverage'))}</span>
          <strong class="${escapeHtml(coverageClass)}">${escapeHtml(formatNumber(item.coverageRatio, 1))}%</strong>
        </article>
        <article class="mobile-data-metric">
          <span>${escapeHtml(t('common.apiAlert'))}</span>
          <strong class="${escapeHtml(item.apiAlertClass)}">${escapeHtml(item.apiAlert)}</strong>
        </article>
      </div>
      <div class="mobile-data-footer">
        <span>${escapeHtml(t('common.excludedWhy'))}: <strong>${escapeHtml(item.excludedReason)}</strong></span>
      </div>
    </article>
  `;
}

function buildHealthRow(exchange, health) {
  const coverageRatio = computeCoverageRatio(health);
  const status = String(health?.status || 'unknown');
  const statusRank = status === 'quarantined' ? 3 : status === 'failed' ? 2 : status === 'ok' ? 1 : 0;
  const statusTone = status === 'ok' ? 'stable' : status === 'failed' ? 'widening' : status === 'quarantined' ? 'expired' : 'new';
  const apiAlert = status === 'ok'
    ? t('health.normal')
    : health?.quarantined_until
      ? t('health.quarantined')
      : t('health.degraded');
  const excludedReason = health?.quarantined_until
    ? t('health.until', { value: formatDateTime(health.quarantined_until) })
    : health?.last_error
      ? String(health.last_error)
      : t('health.active');
  return {
    exchange,
    status,
    statusRank,
    statusTone,
    statusLabel: translateHealthStatus(status),
    statusClass: status === 'ok' ? 'value-positive' : 'value-negative',
    last_success_at: health?.last_success_at || '',
    lastSuccessLabel: health?.last_success_at ? formatDateTime(health.last_success_at) : t('common.never'),
    coverageRatio,
    apiAlert,
    apiAlertClass: status === 'ok' ? 'value-positive' : health?.quarantined_until ? 'value-negative' : 'value-warning',
    excludedReason,
  };
}

function computeCoverageRatio(health) {
  const marketsLoaded = Number(health?.markets_loaded ?? health?.markets_collected ?? 0);
  const pairsLoaded = Number(health?.pairs_loaded ?? health?.pairs_discovered ?? 0);
  if (!pairsLoaded) {
    return 0;
  }
  return clamp((marketsLoaded / pairsLoaded) * 100, 0, 100);
}

function average(values) {
  if (!values.length) {
    return 0;
  }
  return values.reduce((sum, value) => sum + Number(value || 0), 0) / values.length;
}

function standardDeviation(values) {
  if (values.length < 2) {
    return 0;
  }
  const mean = average(values);
  const variance = average(values.map((value) => (Number(value || 0) - mean) ** 2));
  return Math.sqrt(variance);
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function scoreClass(score) {
  if (score >= 75) {
    return 'value-positive';
  }
  if (score >= 45) {
    return 'value-warning';
  }
  return 'value-negative';
}

function marketPermissionChipClass(permission) {
  if (!permission) {
    return 'ai-forecast-stable';
  }
  if (permission.mode === 'open') {
    return 'ai-score-high';
  }
  if (permission.mode === 'cautious') {
    return 'ai-score-medium';
  }
  return 'ai-score-low';
}

function anomalyClass(zScore) {
  if (Math.abs(Number(zScore || 0)) >= 2.4) {
    return 'value-positive';
  }
  if (Math.abs(Number(zScore || 0)) >= 1.4) {
    return 'value-warning';
  }
  return 'value-negative';
}

function percentileClass(percentile) {
  if (Number(percentile || 0) >= 90) {
    return 'value-positive';
  }
  if (Number(percentile || 0) >= 70) {
    return 'value-warning';
  }
  return 'value-negative';
}

function probabilityClass(probability) {
  if (Number(probability || 0) >= 0.7) {
    return 'value-positive';
  }
  if (Number(probability || 0) >= 0.45) {
    return 'value-warning';
  }
  return 'value-negative';
}

function getHeatIntensity(item) {
  const intensity = clamp(item.anomalyScore / 100, 0.08, 1);
  const hue = item.regime === 'clean-convergence'
    ? 'rgba(76, 175, 158, 0.22)'
    : item.regime === 'widening-trend' || item.regime === 'volatile-expansion'
      ? 'rgba(242, 139, 130, 0.22)'
      : 'rgba(143, 184, 255, 0.22)';
  return `linear-gradient(180deg, ${hue}, rgba(255,255,255,${(intensity * 0.08).toFixed(3)}))`;
}

function executionDifficultyLabel(score) {
  if (score >= 70) {
    return t('detail.execution.high');
  }
  if (score >= 40) {
    return t('detail.execution.medium');
  }
  return t('detail.execution.low');
}

async function loadHistorySnapshot(file) {
  if (state.historyCache.has(file)) {
    return state.historyCache.get(file);
  }
  const payload = await fetchJson(buildDataUrl(`./data/history/${file}`));
  const list = Array.isArray(payload) ? payload : [];
  state.historyCache.set(file, list);
  return list;
}

function closeModal() {
  cancelPendingDetailRender();
  dom.modal.classList.add('hidden');
  dom.modal.setAttribute('aria-hidden', 'true');
  state.activeDetailId = null;
  dom.modalSubtitle.textContent = '';
  dom.detailOverview.setAttribute('aria-busy', 'false');
  dom.detailNotes.setAttribute('aria-busy', 'false');
  dom.detailOverview.innerHTML = '';
  dom.detailNotes.innerHTML = '';
  syncOverlayState();
}

function median(values) {
  const numericValues = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right);
  if (!numericValues.length) {
    return 0;
  }
  const mid = Math.floor(numericValues.length / 2);
  if (numericValues.length % 2 === 0) {
    return (numericValues[mid - 1] + numericValues[mid]) / 2;
  }
  return numericValues[mid];
}

function getAiPredictions(snapshot) {
  if (snapshot?.predictions && typeof snapshot.predictions === 'object') {
    return snapshot.predictions;
  }
  if (snapshot?.prediction && Number.isFinite(Number(snapshot.prediction.horizon_minutes))) {
    return { [Number(snapshot.prediction.horizon_minutes)]: snapshot.prediction };
  }
  return {};
}

function getAiPrediction(snapshot, horizonMinutes) {
  const predictions = getAiPredictions(snapshot);
  return predictions[horizonMinutes] || snapshot?.prediction || null;
}

function formatEtaMinutes(minutes) {
  if (!Number.isFinite(Number(minutes))) {
    return pickLocalized('n/a', 'н/д');
  }
  const value = Math.max(1, Number(minutes));
  if (value >= 60) {
    return `${formatNumber(value / 60, 1)} ${pickLocalized('h', 'ч')}`;
  }
  return `${formatNumber(value, 0)} ${pickLocalized('min', 'мин')}`;
}

function getLiveEntryWaitMinutes(item) {
  const waitMinutes = numberOrNull(item.entryWaitMinutes);
  if (waitMinutes == null || waitMinutes <= 0) {
    return waitMinutes;
  }

  const snapshotUpdatedAt = item.aiSnapshot?.updatedAt || item.updated_at;
  const snapshotUpdatedMs = Number.isFinite(Date.parse(snapshotUpdatedAt)) ? Date.parse(snapshotUpdatedAt) : null;
  if (snapshotUpdatedMs == null) {
    return waitMinutes;
  }

  const elapsedMinutes = (Date.now() - snapshotUpdatedMs) / 60000;
  if (elapsedMinutes <= 0) {
    return waitMinutes;
  }
  return Math.max(0, Number((waitMinutes - elapsedMinutes).toFixed(1)));
}

function formatSignedPercent(value, digits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return pickLocalized('n/a', 'н/д');
  }
  const prefix = numeric > 0 ? '+' : '';
  return `${prefix}${formatNumber(numeric, digits)}%`;
}

function buildPredictionChip(prediction) {
  if (!prediction) {
    return pickLocalized('pending', 'ожидание');
  }
  const direction = prediction.direction === 'UP' ? '↑' : prediction.direction === 'DOWN' ? '↓' : '→';
  return `${direction} ${formatNumber(Number(prediction.probability || 0) * 100, 0)}%`;
}

let crossRenderScheduled = false;

function scheduleCrossRender() {
  if (crossRenderScheduled) {
    return;
  }
  crossRenderScheduled = true;
  requestAnimationFrame(() => {
    crossRenderScheduled = false;
    if (dom && state.activeTab) {
      renderCurrentTab();
    }
  });
}

function scheduleAiAnalyticsDeferredRender() {
  if (state.aiDeferredRenderTimer) {
    window.clearTimeout(state.aiDeferredRenderTimer);
  }
  state.aiDeferredRenderPending = true;
  state.aiDeferredRenderTimer = window.setTimeout(() => {
    state.aiDeferredRenderTimer = null;
    state.aiDeferredRenderPending = false;
    if (state.activeTab === 'ai') {
      renderAiAnalyticsPage();
    }
  }, 100);
}

function renderAiPlaybook() {
  if (!dom.aiPlaybook) {
    return;
  }
  const cards = getCurrentLanguage() === 'ru'
    ? [
        {
          title: '1. Что сюда попадает',
          body: 'Сюда попадают только те связки, которые уже прошли базовый отбор в таблице спредов: спред после комиссий выше вашего порога, исполнение выглядит приемлемо, а по биржам нет явного красного флага.',
        },
        {
          title: '2. Когда вход сейчас',
          body: 'Входить сейчас имеет смысл только тогда, когда ожидаемая доходность положительная, вероятность успеха высокая, а риск ликвидации и проскальзывания не съедают идею.',
        },
        {
          title: '3. Когда ждать',
          body: 'Если AI показывает слабую ожидаемую доходность, длинное время до схождения или высокое давление крупных потоков и ликвидаций, правильное действие обычно не вход, а ожидание более чистой структуры.',
        },
        {
          title: '4. Что делать после решения',
          body: 'Если оставляете идею в работе, заранее зафиксируйте: где вход по спреду после комиссий, где частично сокращать риск и на какой бирже открывать первую ногу.',
        },
      ]
    : [
        {
          title: '1. What belongs here',
          body: 'Only setups that already passed the basic spread review: net spread above threshold, acceptable execution and no obvious exchange red flag.',
        },
        {
          title: '2. When to enter now',
          body: 'Enter now only when expected return is positive, success probability is strong and liquidation plus slippage risk are not destroying the edge.',
        },
        {
          title: '3. When to wait',
          body: 'If AI shows weak return, long ETA or high liquidation or whale stress, the correct action is usually to wait instead of forcing a trade.',
        },
        {
          title: '4. What to lock in next',
          body: 'If the idea stays alive, decide in advance where the entry is valid, where risk should be reduced and which exchange leg should be opened first.',
        },
      ];

  dom.aiPlaybook.innerHTML = cards.map((card) => `
    <article class="detail-note-card ai-playbook-card">
      <h3>${escapeHtml(card.title)}</h3>
      <p>${escapeHtml(card.body)}</p>
    </article>
  `).join('');
}

function renderAiFocusBanner(rows) {
  if (!dom.aiFocusBanner) {
    return;
  }
  const focusedItem = state.aiFocusedOpportunityId
    ? rows.find((item) => item.id === state.aiFocusedOpportunityId) || null
    : null;

  if (!focusedItem) {
    dom.aiFocusBanner.classList.add('hidden');
    dom.aiFocusBanner.innerHTML = '';
    return;
  }

  dom.aiFocusBanner.classList.remove('hidden');
  dom.aiFocusBanner.innerHTML = `
    <div>
      <strong>${escapeHtml(pickLocalized('Focused AI review', 'Фокусный AI-разбор'))}</strong>
      <p>${escapeHtml(pickLocalized(
        `You came here from ${focusedItem.pairA}. Read this row first, then compare it with the rest of the shortlist.`,
        `Вы пришли сюда из карточки ${focusedItem.pairA}. Сначала разберите именно эту связку, а уже потом сравнивайте её с остальными кандидатами из короткого списка.`,
      ))}</p>
    </div>
    <div class="ai-focus-actions">
      <button class="ghost-button" type="button" data-ai-focus-detail="${escapeHtml(focusedItem.id)}">${escapeHtml(pickLocalized('Open pair card', 'Открыть карточку пары'))}</button>
      <button class="ghost-button" type="button" data-ai-focus-clear="true">${escapeHtml(pickLocalized('Clear focus', 'Снять фокус'))}</button>
    </div>
  `;

  dom.aiFocusBanner.querySelector('[data-ai-focus-clear]')?.addEventListener('click', () => {
    state.aiFocusedOpportunityId = null;
    renderAiAnalyticsPage();
  });
  dom.aiFocusBanner.querySelector('[data-ai-focus-detail]')?.addEventListener('click', () => {
    openOpportunityDetail(focusedItem);
  });
}

function liquidationRiskTone(riskPercent) {
  if (riskPercent >= 18) {
    return 'value-negative';
  }
  if (riskPercent >= 8) {
    return 'value-warning';
  }
  return 'value-positive';
}

function aiDecisionClass(decision) {
  if (decision === 'enter-now') {
    return 'ai-score-high';
  }
  if (decision === 'scale-in') {
    return 'ai-score-medium';
  }
  return 'ai-score-low';
}

function translateAiDecision(decision) {
  if (decision === 'enter-now') {
    return pickLocalized('Enter now', 'Можно входить сейчас');
  }
  if (decision === 'scale-in') {
    return pickLocalized('Scale in carefully', 'Входить постепенно');
  }
  return pickLocalized('Wait for cleaner setup', 'Подождать более чистый вход');
}

function buildAiForecastSvg(item) {
  const snapshot = item.aiSnapshot || getAiSnapshot(item.id);
  const currentNetSpread = Number(item.netSpread || 0);
  const predictionPoints = AI_PREDICTION_HORIZON_OPTIONS
    .map((horizon) => getAiPrediction(snapshot, horizon))
    .filter(Boolean)
    .map((prediction) => ({
      horizon: Number(prediction.horizon_minutes || 0),
      value: currentNetSpread + Number(prediction.predicted_change || 0),
      interval: Array.isArray(prediction.confidence_interval) ? prediction.confidence_interval : [],
    }))
    .sort((left, right) => left.horizon - right.horizon);

  if (!predictionPoints.length) {
    return `<div class="ai-chart-caption">${escapeHtml(pickLocalized('Waiting for multi-horizon forecast data.', 'Ожидание прогноза по нескольким горизонтам.'))}</div>`;
  }

  const points = [{ horizon: 0, value: currentNetSpread, interval: [currentNetSpread, currentNetSpread] }, ...predictionPoints];
  const rangeValues = points.flatMap((point) => {
    const low = point.horizon === 0 ? point.value : currentNetSpread + Number(point.interval[0] || 0);
    const high = point.horizon === 0 ? point.value : currentNetSpread + Number(point.interval[1] || 0);
    return [point.value, low, high];
  });
  const rawMinValue = Math.min(...rangeValues);
  const rawMaxValue = Math.max(...rangeValues);
  const width = 320;
  const height = 150;
  const padding = 18;
  const chartPadding = Math.max(0.04, (rawMaxValue - rawMinValue) * 0.12);
  const minValue = rawMinValue - chartPadding;
  const maxValue = rawMaxValue + chartPadding;
  const span = Math.max(0.12, maxValue - minValue);
  const xScale = (horizon) => padding + ((width - padding * 2) * horizon) / Math.max(15, points.at(-1)?.horizon || 15);
  const yScale = (value) => height - padding - ((value - minValue) / span) * (height - padding * 2);
  const linePath = points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${xScale(point.horizon).toFixed(2)} ${yScale(point.value).toFixed(2)}`).join(' ');
  const upper = points.map((point) => {
    if (point.horizon === 0) {
      return [xScale(point.horizon), yScale(point.value)];
    }
    const upperValue = Number(item.netSpread || 0) + Number(point.interval[1] || 0);
    return [xScale(point.horizon), yScale(upperValue)];
  });
  const lower = [...points].reverse().map((point) => {
    if (point.horizon === 0) {
      return [xScale(point.horizon), yScale(point.value)];
    }
    const lowerValue = Number(item.netSpread || 0) + Number(point.interval[0] || 0);
    return [xScale(point.horizon), yScale(lowerValue)];
  });
  const bandPath = [...upper, ...lower]
    .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point[0].toFixed(2)} ${point[1].toFixed(2)}`)
    .join(' ');
  const currentLine = yScale(currentNetSpread);
  const zeroLine = 0 >= minValue && 0 <= maxValue ? yScale(0) : null;

  return `
    <svg class="ai-forecast-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
      ${zeroLine == null ? '' : `<line class="ai-forecast-grid-line" x1="${padding}" y1="${zeroLine.toFixed(2)}" x2="${width - padding}" y2="${zeroLine.toFixed(2)}"></line>`}
      <line class="ai-forecast-baseline" x1="${padding}" y1="${currentLine.toFixed(2)}" x2="${width - padding}" y2="${currentLine.toFixed(2)}"></line>
      <path class="ai-forecast-band" d="${bandPath} Z"></path>
      <path class="ai-forecast-line" d="${linePath}"></path>
      ${points.map((point) => `<circle class="ai-forecast-point" cx="${xScale(point.horizon).toFixed(2)}" cy="${yScale(point.value).toFixed(2)}" r="3.2"></circle>`).join('')}
    </svg>
  `;
}

function buildAiAnalyticsItemSignature(item, snapshot) {
  const predictionsSignature = AI_PREDICTION_HORIZON_OPTIONS
    .map((horizon) => {
      const prediction = getAiPrediction(snapshot, horizon);
      if (!prediction) {
        return `${horizon}:na`;
      }
      const interval = Array.isArray(prediction.confidence_interval)
        ? prediction.confidence_interval.map((value) => formatNumber(value, 4)).join(',')
        : '';
      return [
        horizon,
        prediction.direction || '',
        formatNumber(Number(prediction.probability || 0), 4),
        formatNumber(Number(prediction.predicted_change || 0), 4),
        interval,
      ].join(':');
    })
    .join('|');

  return [
    item.id,
    item.pairA,
    item.exchangeA,
    item.exchangeB,
    normalizeAiPredictionHorizon(state.settings.aiPredictionHorizon),
    Number(state.settings.leverage || DEFAULT_SETTINGS.leverage),
    formatNumber(Number(item.netSpread || 0), 4),
    formatNumber(Number(item.executionDifficulty || 0), 2),
    formatNumber(Number(item.fundingCarry || 0), 4),
    item.liquidationRisk || '',
    formatNumber(Number(item.whaleIndex || 0), 2),
    formatNumber(Number(item.reversionProbability || 0), 4),
    formatNumber(Number(item.meanReversionHours || 0), 3),
    item.regime || '',
    formatNumber(Number(item.aiScore ?? item.score ?? 0), 2),
    formatNumber(Number(item.history?.stdAbs || 0), 4),
    formatNumber(Math.min(Number(item.volumeA || 0), Number(item.volumeB || 0)), 0),
    Array.isArray(item.smartRoutes)
      ? item.smartRoutes
        .map((route) => [
          route.exchangeA || '',
          route.exchangeB || '',
          formatNumber(Number(route.expected_profit || 0), 3),
          formatNumber(Number(route.expected_slippage_percent || 0), 3),
          route.within_user_slippage ? '1' : '0',
          route.recommended ? '1' : '0',
        ].join(':'))
        .join('|')
      : '',
    snapshot?.loading ? '1' : '0',
    snapshot?.error || '',
    snapshot?.updatedAt || '',
    snapshot?.requestedHorizon || '',
    predictionsSignature,
  ].join('~');
}

function buildAiAnalyticsItem(item) {
  const snapshot = getAiSnapshot(item.id);
  const signature = buildAiAnalyticsItemSignature(item, snapshot);
  const cached = state.aiAnalyticsItemCache.get(item.id);
  if (cached?.signature === signature) {
    return cached.value;
  }
  const selectedHorizon = normalizeAiPredictionHorizon(state.settings.aiPredictionHorizon);
  const prediction = getAiPrediction(snapshot, selectedHorizon);
  const execution = snapshot?.execution || {};
  const entry = execution.entry || {};
  const exit = execution.exit || {};
  const holding = execution.holding || {};
  const leverage = Math.max(1, Number(state.settings.leverage || DEFAULT_SETTINGS.leverage));
  const currentAbsSpread = Math.max(0.01, Math.abs(Number(item.netSpread || 0)));
  const predictedChange = Number.isFinite(Number(prediction?.predicted_change)) ? Number(prediction.predicted_change) : Number(item.aiPredictedChange || 0);
  const route = Array.isArray(item.smartRoutes) && item.smartRoutes.length ? item.smartRoutes.find((entry) => entry.recommended) || item.smartRoutes[0] : null;
  const expectedConvergence = prediction?.direction === 'DOWN'
    ? Math.max(0.02, Math.abs(predictedChange))
    : prediction?.direction === 'STABLE'
      ? currentAbsSpread * 0.12
      : -Math.max(0.01, Math.abs(predictedChange) * 0.6);
  const executionPenalty = clamp((Number(item.executionDifficulty || 50) / 100) * 0.22, 0.03, 0.22);
  const routePenalty = route ? clamp(Number(route.expected_slippage_percent || 0) * 0.25, 0.02, 0.32) : 0.08;
  const fundingBoost = clamp(Number(item.fundingCarry || 0), -0.2, 0.2);
  const expectedNetReturnPct = Number.isFinite(Number(holding.expected_net_return_bps)) ? Number(holding.expected_net_return_bps) / 100 : null;
  const expectedReturnPct = Number((clamp(expectedNetReturnPct ?? ((expectedConvergence - executionPenalty - routePenalty + fundingBoost) * leverage), -8, 12)).toFixed(3));

  const liquidationBase = item.liquidationRisk === 'high' ? 11 : item.liquidationRisk === 'medium' ? 6 : 2.8;
  const whalePenalty = item.whaleIndex >= 70 ? 6 : item.whaleIndex >= 50 ? 2.5 : 0;
  const volatilityPenalty = clamp((Number(item.history?.stdAbs || currentAbsSpread) / Math.max(0.12, currentAbsSpread)) * 4.5, 1, 10);
  const liquidationRiskPct = Number(clamp((Number.isFinite(Number(execution.risk_score)) ? Number(execution.risk_score) * 100 : (liquidationBase + Math.max(0, leverage - 1) * 0.75 + whalePenalty + volatilityPenalty)), 1, 45).toFixed(1));

  const successBase =
    (Number(item.reversionProbability || 0.5) * 100) * 0.38
    + Number(item.aiScore ?? item.score ?? 50) * 0.24
    + (Number(prediction?.probability || 0.5) * 100) * 0.2
    + (route?.within_user_slippage ? 8 : 0)
    + (item.regime === 'clean-convergence' ? 6 : item.regime === 'widening-trend' ? -5 : 0);
  const successProbabilityPct = Number(clamp((Number.isFinite(Number(execution.success_probability)) ? Number(execution.success_probability) * 100 : successBase - liquidationRiskPct * 0.45 - Math.max(0, leverage - 5) * 1.8), 5, 95).toFixed(0));

  let etaMinutes = Number.isFinite(Number(exit.eta_minutes)) ? Number(exit.eta_minutes) : null;
  if (etaMinutes == null && Number.isFinite(Number(item.meanReversionHours))) {
    etaMinutes = clamp(Number(item.meanReversionHours) * 60, 5, 24 * 60);
  } else if (prediction?.direction === 'DOWN' && Math.abs(predictedChange) > 0.001) {
    const speedPerMinute = Math.abs(predictedChange) / Math.max(1, Number(prediction.horizon_minutes || selectedHorizon));
    etaMinutes = clamp(currentAbsSpread / Math.max(0.0005, speedPerMinute), 5, 24 * 60);
  }

  const thresholdCandidate = Number(clamp(
    Number.isFinite(Number(entry.expected_spread_pct))
      ? Number(entry.expected_spread_pct)
      : currentAbsSpread * (0.52 + successProbabilityPct / 260) - Number(item.history?.stdAbs || 0) * 0.28,
    0.05,
    Math.max(0.08, currentAbsSpread),
  ).toFixed(2));

  let decision = 'wait';
  const entryWaitMinutes = Number.isFinite(Number(entry.wait_minutes)) ? Number(entry.wait_minutes) : item.entryWaitMinutes;
  const holdingHoursRecommended = Number.isFinite(Number(holding.recommended_hours)) ? Number(holding.recommended_hours) : item.holdingHoursRecommended;
  const exitTarget = Number.isFinite(Number(exit.target_spread_pct))
    ? `${exit.exit_type || pickLocalized('target', 'цель')} · ${formatNumber(exit.target_spread_pct, 3)}%`
    : item.exitTarget;
  if (expectedReturnPct >= 0.3 && successProbabilityPct >= 68 && liquidationRiskPct <= 12 && (entryWaitMinutes == null || entryWaitMinutes <= 5)) {
    decision = 'enter-now';
  } else if (expectedReturnPct > 0 && successProbabilityPct >= 55) {
    decision = 'scale-in';
  }

  const whaleAlert = item.whaleIndex >= 70
    ? pickLocalized('High whale stress', 'Высокое давление крупных потоков')
    : item.whaleIndex >= 50
      ? pickLocalized('Watch whale flow', 'Следить за крупными переводами')
      : pickLocalized('Whale flow calm', 'Крупные потоки спокойны');

  const value = {
    ...item,
    aiSnapshot: snapshot,
    selectedPrediction: prediction,
    expectedReturnPct,
    successProbabilityPct,
    liquidationRiskPct,
    etaMinutes,
    thresholdCandidate,
    decision,
    whaleAlert,
    route,
    entryWaitMinutes,
    holdingHoursRecommended,
    exitTarget,
  };

  state.aiAnalyticsItemCache.set(item.id, { signature, value });
  return value;
}

function computeAiThresholdRecommendationFromAnalytics(analyticsRows = []) {
  if (!analyticsRows.length) {
    return {
      value: Number(state.settings.minSpread || DEFAULT_SETTINGS.minSpread),
      eligibleCount: 0,
      reasons: [pickLocalized('waiting for tradable rows', 'ожидание подходящих строк')],
    };
  }

  const eligible = analyticsRows.filter((item) => item.expectedReturnPct > 0 && item.successProbabilityPct >= 55);
  const referenceRows = eligible.length ? eligible : analyticsRows;
  const value = Number(clamp(median(referenceRows.map((item) => item.thresholdCandidate)), 0.05, 3).toFixed(2));
  const avgVolatility = average(referenceRows.map((item) => Number(item.history?.stdAbs || 0)));
  const avgDepth = average(referenceRows.map((item) => Math.min(Number(item.volumeA || 0), Number(item.volumeB || 0))));
  const avgFundingDelta = average(referenceRows.map((item) => Math.abs(Number(item.fundingB || 0) - Number(item.fundingA || 0))));
  const reasons = [];

  reasons.push(
    avgDepth >= 5_000_000
      ? pickLocalized('liquidity is supportive', 'ликвидность поддерживает исполнение')
      : pickLocalized('liquidity is mixed', 'ликвидность смешанная'),
  );
  reasons.push(
    avgVolatility <= 0.25
      ? pickLocalized('volatility is contained', 'волатильность сдержанная')
      : pickLocalized('volatility still needs buffer', 'волатильности нужен запас'),
  );
  reasons.push(
    avgFundingDelta <= 0.006
      ? pickLocalized('funding drag is restrained', 'давление ставок умеренное')
      : pickLocalized('funding drag is elevated', 'давление ставок повышено'),
  );

  return {
    value,
    eligibleCount: eligible.length,
    reasons,
  };
}

function getAiAnalyticsSourceRows() {
  const minAiScore = Math.max(0, Number(state.settings.minAiScore || 0));
  const sortedRows = getSortedCrossRows();
  const focusedRow = state.aiFocusedOpportunityId
    ? sortedRows.find((item) => item.id === state.aiFocusedOpportunityId) || null
    : null;
  const filteredRows = sortedRows.filter((item) => {
    if (focusedRow && item.id === focusedRow.id) {
      return false;
    }
    return minAiScore > 0 && item.aiScore != null ? item.aiScore >= minAiScore : true;
  });
  return [focusedRow, ...filteredRows].filter(Boolean).slice(0, AI_ANALYTICS_CARD_LIMIT);
}

function getAiAnalyticsBundle(rows = getAiAnalyticsSourceRows()) {
  const analyticsRows = rows.map((item) => buildAiAnalyticsItem(item));
  const signature = rows
    .map((item) => {
      const cached = state.aiAnalyticsItemCache.get(item.id);
      return `${item.id}:${cached?.signature || ''}`;
    })
    .join('||');
  if (state.aiAnalyticsBundleCache.signature === signature && state.aiAnalyticsBundleCache.value) {
    return state.aiAnalyticsBundleCache.value;
  }

  const sortedRows = analyticsRows.slice().sort((left, right) => {
    if (right.expectedReturnPct !== left.expectedReturnPct) {
      return right.expectedReturnPct - left.expectedReturnPct;
    }
    return right.successProbabilityPct - left.successProbabilityPct;
  });
  const value = {
    rows,
    analyticsRows: sortedRows,
    recommendation: computeAiThresholdRecommendationFromAnalytics(sortedRows),
  };
  state.aiAnalyticsBundleCache = { signature, value };
  return value;
}

function computeAiThresholdRecommendation(rows = getAiAnalyticsSourceRows()) {
  return getAiAnalyticsBundle(rows).recommendation;
}

function renderAiThresholdNotes() {
  const recommendation = computeAiThresholdRecommendation();
  const currentThreshold = Number(state.settings.minSpread || DEFAULT_SETTINGS.minSpread);
  const text = recommendation.eligibleCount > 0
    ? `${pickLocalized('AI threshold for this tab suggests', 'Рекомендованный порог входа для AI-вкладки')} ${formatNumber(recommendation.value, 2)}% · ${pickLocalized('cross threshold now', 'текущий порог спреда в таблице')} ${formatNumber(currentThreshold, 2)}% · ${recommendation.reasons.join(' · ')}`
    : pickLocalized('AI threshold is waiting for enough tradable rows inside the AI tab.', 'Рекомендация по AI-порогу появится, когда во вкладке AI накопится достаточно пригодных связок.');
  if (dom.aiThresholdNote) {
    dom.aiThresholdNote.textContent = text;
  }
  if (dom.settingsAiThresholdNote) {
    dom.settingsAiThresholdNote.textContent = text;
  }
}

function applyAiSuggestedThreshold() {
  const recommendation = computeAiThresholdRecommendation();
  state.settings.minSpread = recommendation.value;
  invalidateAiAnalyticsCaches();
  invalidateCrossOpportunityCaches();
  persistSettings();
  if (dom.settingsForm?.elements?.minSpread) {
    dom.settingsForm.elements.minSpread.value = recommendation.value;
  }
  renderAll();
}

function renderAiAnalyticsPage() {
  const crossStatus = getDatasetStatus('cross');
  if (crossStatus.loading || getDatasetStatus('metrics').loading || (!crossStatus.loaded && !crossStatus.error)) {
    resetTabContent(null, null, dom.aiSummary, dom.aiAnalyticsList);
    showPanelState(dom.aiEmpty, getTabLoadingMessage('ai'));
    return;
  }
  if (crossStatus.error || getDatasetStatus('metrics').error) {
    resetTabContent(null, null, dom.aiSummary, dom.aiAnalyticsList);
    showPanelState(dom.aiEmpty, crossStatus.error || getDatasetStatus('metrics').error);
    return;
  }
  if (state.aiDeferredRenderPending) {
    resetTabContent(null, null, dom.aiSummary, dom.aiAnalyticsList);
    showPanelState(dom.aiEmpty, getTabLoadingMessage('ai'));
    return;
  }
  renderAiPlaybook();
  const rows = getAiAnalyticsSourceRows();
  renderAiFocusBanner(rows);
  hydrateVisibleCrossAi(rows);
  void ensureCrossHistoryAnalytics(rows.map((item) => item.id));
  const { analyticsRows, recommendation } = getAiAnalyticsBundle(rows);

  if (dom.aiEmpty) {
    dom.aiEmpty.classList.toggle('hidden', analyticsRows.length > 0);
  }
  if (!analyticsRows.length) {
    if (dom.aiSummary) {
      dom.aiSummary.innerHTML = '';
    }
    if (dom.aiAnalyticsList) {
      dom.aiAnalyticsList.innerHTML = '';
    }
    return;
  }

  const bestRow = analyticsRows[0];
  const fastestRow = analyticsRows.filter((item) => Number.isFinite(Number(item.etaMinutes))).sort((left, right) => left.etaMinutes - right.etaMinutes)[0] || bestRow;
  const whaleAlerts = analyticsRows.filter((item) => item.whaleIndex >= 50).length;
  const safestRow = analyticsRows.slice().sort((left, right) => left.liquidationRiskPct - right.liquidationRiskPct)[0] || bestRow;

  dom.aiSummary.innerHTML = [
    {
      label: pickLocalized('AI threshold', 'Рекомендуемый порог входа'),
      value: `${formatNumber(recommendation.value, 2)}%`,
      detail: `${pickLocalized('eligible rows', 'подходящих связок')}: ${recommendation.eligibleCount}`,
    },
    {
      label: pickLocalized('Best expected return', 'Лучшая ожидаемая доходность'),
      value: `${bestRow.pairA} · ${formatSignedPercent(bestRow.expectedReturnPct, 2)}`,
      detail: `${pickLocalized('success', 'вероятность успеха')} ${bestRow.successProbabilityPct}%`,
    },
    {
      label: pickLocalized('Fastest convergence', 'Самая быстрая сходимость'),
      value: `${fastestRow.pairA} · ${formatEtaMinutes(fastestRow.etaMinutes)}`,
      detail: getLiveEntryWaitMinutes(fastestRow) != null
        ? `${translateAiDecision(fastestRow.decision)} · ${pickLocalized('entry', 'вход')} ${formatEtaMinutes(getLiveEntryWaitMinutes(fastestRow))}`
        : translateAiDecision(fastestRow.decision),
    },
    {
      label: pickLocalized('Whale / liquidation watch', 'Крупные потоки и риск ликвидации'),
      value: `${whaleAlerts}/${analyticsRows.length}`,
      detail: `${pickLocalized('lowest risk', 'минимальный риск')} ${safestRow.pairA} · ${formatNumber(safestRow.liquidationRiskPct, 1)}%`,
    },
  ].map((card) => `
      <article class="metric-box">
        <span>${escapeHtml(String(card.label))}</span>
        <strong>${escapeHtml(String(card.value))}</strong>
        <small>${escapeHtml(String(card.detail || ''))}</small>
      </article>
    `).join('');

  dom.aiAnalyticsList.innerHTML = analyticsRows.map((item) => {
    return `
      <article class="ai-analytics-card ${item.whaleIndex >= 70 || item.liquidationRiskPct >= 18 ? 'is-alert' : ''}">
        <div class="ai-analytics-head">
          <div>
            <strong>${escapeHtml(item.pairA)}</strong>
            <div class="ai-analytics-route">${escapeHtml(item.exchangeA)} vs ${escapeHtml(item.exchangeB)}</div>
          </div>
          <span class="chip ${escapeHtml(aiDecisionClass(item.decision))}">${escapeHtml(translateAiDecision(item.decision))}</span>
        </div>

        <div class="ai-threshold-bar">
          <span>${escapeHtml(pickLocalized('Suggested entry threshold', 'Рекомендуемый порог входа'))}</span>
          <strong>${escapeHtml(formatNumber(item.thresholdCandidate, 2))}%</strong>
        </div>

        <div class="ai-analytics-grid">
          <div class="ai-analytics-metric">
            <span>${escapeHtml(pickLocalized('Expected return', 'Ожидаемая доходность'))}</span>
            <strong class="${escapeHtml(valueClass(item.expectedReturnPct))}">${escapeHtml(formatSignedPercent(item.expectedReturnPct, 2))}</strong>
          </div>
          <div class="ai-analytics-metric">
            <span>${escapeHtml(pickLocalized('Success probability', 'Вероятность успеха'))}</span>
            <strong class="${escapeHtml(probabilityClass(item.successProbabilityPct / 100))}">${escapeHtml(`${formatNumber(item.successProbabilityPct, 0)}%`)}</strong>
          </div>
          <div class="ai-analytics-metric">
            <span>${escapeHtml(pickLocalized('ETA to convergence', 'Оценка времени до схождения'))}</span>
            <strong>${escapeHtml(formatEtaMinutes(item.etaMinutes))}</strong>
          </div>
          <div class="ai-analytics-metric">
            <span>${escapeHtml(pickLocalized('Entry wait', 'Ожидание до входа'))}</span>
            <strong>${escapeHtml(getLiveEntryWaitMinutes(item) == null ? pickLocalized('n/a', 'н/д') : formatEtaMinutes(getLiveEntryWaitMinutes(item)))}</strong>
          </div>
          <div class="ai-analytics-metric">
            <span>${escapeHtml(pickLocalized('Holding horizon', 'Горизонт удержания'))}</span>
            <strong>${escapeHtml(item.holdingHoursRecommended == null ? pickLocalized('n/a', 'н/д') : `${formatNumber(item.holdingHoursRecommended, item.holdingHoursRecommended >= 10 ? 0 : 1)} ${pickLocalized('h', 'ч')}`)}</strong>
          </div>
          <div class="ai-analytics-metric">
            <span>${escapeHtml(pickLocalized('Liquidation risk', 'Риск ликвидации'))}</span>
            <strong class="${escapeHtml(liquidationRiskTone(item.liquidationRiskPct))}">${escapeHtml(`${formatNumber(item.liquidationRiskPct, 1)}%`)}</strong>
          </div>
        </div>

        <div class="ai-chart-shell">
          ${buildAiForecastSvg(item)}
          <div class="ai-chart-caption">${escapeHtml(pickLocalized('Projected net spread path with confidence band for 5, 10 and 15 minute scenarios.', 'Ожидаемая траектория спреда после комиссий с диапазоном неопределённости для сценариев на 5, 10 и 15 минут.'))}</div>
        </div>

        <div class="ai-horizon-list">
          ${AI_PREDICTION_HORIZON_OPTIONS.map((horizon) => {
            const horizonPrediction = getAiPrediction(item.aiSnapshot, horizon);
            return `<span class="chip ai-horizon-chip ${escapeHtml(aiForecastClass(horizonPrediction?.direction || null))}">${escapeHtml(`${horizon} мин`)} ${escapeHtml(buildPredictionChip(horizonPrediction))}</span>`;
          }).join('')}
        </div>

        <div class="ai-analytics-footer">
          <span class="ai-chart-caption">${escapeHtml(`${pickLocalized('Whale', 'Крупные потоки')}: ${item.whaleAlert}`)}</span>
          <span class="ai-chart-caption">${escapeHtml(`${pickLocalized('Exit target', 'Цель выхода')}: ${item.exitTarget || pickLocalized('Pending', 'Ожидание')}`)}</span>
          <span class="ai-chart-caption">${escapeHtml(item.route ? `${pickLocalized('Best route', 'Лучший маршрут исполнения')}: ${item.route.exchangeA}/${item.route.exchangeB}` : pickLocalized('Route suggestion pending', 'Подсказка по маршруту исполнения появится после AI-расчёта'))}</span>
          <button class="ghost-button" type="button" data-detail-id="${escapeHtml(item.id)}">${escapeHtml(pickLocalized('Open detail', 'Открыть детали'))}</button>
        </div>
      </article>
    `;
  }).join('');

  dom.aiAnalyticsList.querySelectorAll('[data-detail-id]').forEach((button) => {
    button.addEventListener('click', () => {
      const item = analyticsRows.find((entry) => entry.id === button.dataset.detailId);
      if (item) {
        openOpportunityDetail(item);
      }
    });
  });
}

function getGlossaryEntries() {
  return Array.isArray(state.glossary) ? state.glossary : [];
}

function getGlossaryCategories(entries = getGlossaryEntries()) {
  return [...new Set(entries.map((entry) => String(entry.category || '').trim()).filter(Boolean))].sort((left, right) => left.localeCompare(right));
}

function getFilteredGlossaryEntries() {
  const query = String(state.glossarySearch || '').trim().toLowerCase();
  const category = String(state.glossaryCategory || 'all');
  return getGlossaryEntries().filter((entry) => {
    const entryCategory = String(entry.category || '').trim();
    if (category !== 'all' && entryCategory !== category) {
      return false;
    }
    if (!query) {
      return true;
    }
    const haystack = [
      entry.term,
      entry.category,
      entry.definition,
      entry.formula,
      entry.interpretation,
      entry.action,
      Array.isArray(entry.related) ? entry.related.join(' ') : '',
    ].join(' ').toLowerCase();
    return haystack.includes(query);
  });
}

function renderGlossaryPage() {
  const glossaryStatus = getDatasetStatus('glossary');
  if (glossaryStatus.loading || (!glossaryStatus.loaded && !glossaryStatus.error)) {
    resetTabContent(null, null, null, dom.glossaryList);
    showPanelState(dom.glossaryEmpty, getTabLoadingMessage('glossary'));
    return;
  }
  if (glossaryStatus.error) {
    resetTabContent(null, null, null, dom.glossaryList);
    showPanelState(dom.glossaryEmpty, glossaryStatus.error);
    return;
  }

  const categories = getGlossaryCategories();
  if (dom.glossaryCategory) {
    const currentCategory = String(state.glossaryCategory || 'all');
    dom.glossaryCategory.innerHTML = [`<option value="all">${escapeHtml(pickLocalized('All categories', 'Все категории'))}</option>`, ...categories.map((category) => `<option value="${escapeHtml(category)}">${escapeHtml(category)}</option>`)].join('');
    dom.glossaryCategory.value = categories.includes(currentCategory) ? currentCategory : 'all';
    state.glossaryCategory = dom.glossaryCategory.value;
  }
  if (dom.glossarySearch && dom.glossarySearch.value !== state.glossarySearch) {
    dom.glossarySearch.value = state.glossarySearch;
  }

  const entries = getFilteredGlossaryEntries();
  if (dom.glossaryEmpty) {
    dom.glossaryEmpty.classList.toggle('hidden', entries.length > 0);
    if (!entries.length) {
      dom.glossaryEmpty.textContent = t('panel.glossary.empty');
    }
  }
  if (!dom.glossaryList) {
    return;
  }
  dom.glossaryList.innerHTML = entries.map((entry) => `
    <article class="detail-note-card glossary-card">
      <div class="ai-analytics-head glossary-card__head">
        <div>
          <strong>${escapeHtml(String(entry.term || ''))}</strong>
          <div class="ai-analytics-route">${escapeHtml(String(entry.category || pickLocalized('General', 'Общее')))}</div>
        </div>
        ${entry.short ? `<span class="chip score-chip">${escapeHtml(String(entry.short))}</span>` : ''}
      </div>
      <p>${escapeHtml(String(entry.definition || ''))}</p>
      ${entry.formula ? `<div class="ai-chart-caption"><strong>${escapeHtml(pickLocalized('Formula', 'Формула'))}:</strong> ${escapeHtml(String(entry.formula))}</div>` : ''}
      ${entry.interpretation ? `<div class="ai-chart-caption"><strong>${escapeHtml(pickLocalized('How to read', 'Как читать'))}:</strong> ${escapeHtml(String(entry.interpretation))}</div>` : ''}
      ${entry.action ? `<div class="ai-chart-caption"><strong>${escapeHtml(pickLocalized('Action', 'Действие'))}:</strong> ${escapeHtml(String(entry.action))}</div>` : ''}
      ${Array.isArray(entry.related) && entry.related.length ? `<div class="ai-horizon-list">${entry.related.map((item) => `<span class="chip ai-horizon-chip">${escapeHtml(String(item))}</span>`).join('')}</div>` : ''}
    </article>
  `).join('');
}

function renderQualityBadges() {
  if (!dom.qualityBadges) {
    return;
  }
  const summary = state.metrics?.summary || {};
  const alertSummary = getSystemAlertSummary();
  const adaptivePublication = state.metrics?.analytics_summary?.adaptive_publication || {};
  const quoteAge = state.metrics?.analytics_summary?.quote_age || {};
  const visibleExchanges = Object.entries(state.settings.visibleExchanges).filter(([, visible]) => visible !== false).length;
  const allExchanges = getAllExchanges().length || visibleExchanges || 0;
  const crossRows = getFilteredCross();
  const filteredRows = crossRows.length;
  const hotAnomalies = crossRows.filter((item) => item.anomalyScore >= 75).length;
  const badges = [
    {
      tone: visibleExchanges >= Math.max(1, Math.ceil(allExchanges * 0.6)) ? 'positive' : 'warning',
      text: t('quality.visibleExchanges', { visible: visibleExchanges, total: allExchanges }),
    },
    {
      tone: filteredRows >= 10 ? 'positive' : filteredRows > 0 ? 'warning' : 'negative',
      text: t('quality.crossHits', { count: filteredRows }),
    },
    state.dataOrigin === 'ephemeral-filtered'
      ? {
          tone: 'warning',
          text: t('quality.snapshotOriginEphemeral'),
        }
      : null,
    {
      tone: hotAnomalies >= 8 ? 'positive' : hotAnomalies > 0 ? 'warning' : 'negative',
      text: t('quality.hotAnomalies', { count: hotAnomalies }),
    },
    {
      tone: Number(summary.quarantined_exchanges || 0) === 0 ? 'positive' : 'warning',
      text: t('quality.quarantined', { count: Number(summary.quarantined_exchanges || 0) }),
    },
    {
      tone: alertSummaryTone(alertSummary),
      text: t('quality.systemAlerts', { count: Number(alertSummary.active_count || 0) }),
    },
    adaptivePublication.mode && adaptivePublication.mode !== 'open'
      ? {
          tone: adaptivePublication.mode === 'blocked' ? 'negative' : 'warning',
          text: t('quality.publicationProfile', { mode: translatePublicationMode(adaptivePublication.mode) }),
        }
      : null,
    quoteAge.available
      ? {
          tone: Number(quoteAge.p95_seconds || 0) >= 90 ? 'warning' : 'positive',
          text: t('quality.quoteAge', { value: formatNumber(Number(quoteAge.p95_seconds || 0), 0) }),
        }
      : null,
    hasRefreshApi() && state.refreshState?.status
      ? {
          tone: state.refreshState.status === 'failed' ? 'negative' : state.refreshState.status === 'completed' ? 'positive' : 'warning',
          text: t('quality.refreshStatus', { status: t(`status.${state.refreshState.status}`) }),
        }
      : null,
    hasRefreshApi() && state.refreshState?.scope
      ? {
          tone: state.refreshState.status === 'failed' ? 'negative' : state.refreshState.status === 'completed' ? 'positive' : 'warning',
          text: t('quality.refreshScope', { scope: state.refreshState.scope }),
        }
      : null,
  ].filter(Boolean);

  dom.qualityBadges.innerHTML = badges
    .map((badge) => `<span class="quality-badge ${escapeHtml(badge.tone)}">${escapeHtml(badge.text)}</span>`)
    .join('');
}

function renderHeroRiskSummary() {
  if (!dom.heroRiskSummary) {
    return;
  }

  const visibleRows = getFilteredCross();
  const analytics = state.metrics?.analytics_summary || {};
  if ((!analytics || !Number(analytics.total_cross || 0)) && !visibleRows.length) {
    dom.heroRiskSummary.innerHTML = '';
    return;
  }

  const totalCross = Math.max(1, Number(analytics.total_cross || 0));
  const publishedCross = Math.max(0, Number(analytics.published_cross || 0));
  const elevatedVolatilityShare = Number(analytics.elevated_volatility_share || 0);
  const adverseFundingShare = Number(analytics.adverse_funding_share || 0);
  const elevatedEventRiskShare = Number(analytics.elevated_event_risk_share || 0);
  const dominantRegimeShare = Number(analytics.dominant_regime_share || 0);
  const alertSummary = getSystemAlertSummary();
  const adaptivePublication = analytics.adaptive_publication || {};
  const cards = [
    {
      label: t('hero.volatility'),
      value: translateVolatilityRegime(analytics.market_volatility_regime) || t('common.na'),
      detail: t('hero.elevated', { value: Number(analytics.elevated_volatility_cross || 0), total: totalCross }),
      tone: analytics.market_volatility_regime === 'stressed' ? 'negative' : analytics.market_volatility_regime === 'mixed' ? 'warning' : 'positive',
    },
    {
      label: t('hero.backdrop'),
      value: translateRiskBackdrop(analytics.risk_backdrop) || t('common.na'),
      detail: t('hero.published', { published: publishedCross, total: totalCross, regime: translateRegime(analytics.dominant_regime || 'forming'), share: formatNumber(dominantRegimeShare * 100, 0) }),
      tone: analytics.risk_backdrop === 'stressed' ? 'negative' : analytics.risk_backdrop === 'watch' ? 'warning' : 'positive',
    },
    {
      label: t('hero.fundingDrag'),
      value: t('hero.flagged', { value: formatNumber(adverseFundingShare * 100, 0) }),
      detail: t('hero.pairsAdverse', { count: Number(analytics.adverse_funding_cross || 0) }),
      tone: adverseFundingShare >= 0.05 ? 'negative' : adverseFundingShare > 0.01 ? 'warning' : 'positive',
    },
    {
      label: t('hero.eventStress'),
      value: t('hero.flagged', { value: formatNumber(elevatedEventRiskShare * 100, 0) }),
      detail: t('hero.proxies', { count: Number(analytics.elevated_event_risk_cross || 0) }),
      tone: elevatedEventRiskShare >= 0.05 ? 'negative' : elevatedEventRiskShare > 0.01 ? 'warning' : 'positive',
    },
    {
      label: t('hero.systemAlerts'),
      value: Number(alertSummary.active_count || 0) > 0 ? t('hero.alertsActive', { count: Number(alertSummary.active_count || 0) }) : t('hero.alertsClear'),
      detail: t('hero.alertTopSeverity', { severity: translateAlertSeverity(alertSummary.highest_severity || 'info') }),
      tone: alertSummaryTone(alertSummary),
    },
    {
      label: t('hero.publicationProfile'),
      value: translatePublicationMode(adaptivePublication.mode || 'open'),
      detail: t('hero.publicationThresholds', {
        spread: formatNumber(Number(adaptivePublication.min_net_spread || 0), 2),
        volume: formatCompact(Number(adaptivePublication.min_volume || 0)),
        bidAsk: formatNumber(Number(adaptivePublication.max_bid_ask || 0), 2),
      }),
      tone: adaptivePublication.mode === 'blocked' ? 'negative' : adaptivePublication.mode === 'cautious' ? 'warning' : 'positive',
    },
  ];

  dom.heroRiskSummary.innerHTML = cards
    .map(
      (card) => `
        <article class="metric-box metric-${escapeHtml(card.tone)}">
          <span>${escapeHtml(card.label)}</span>
          <strong>${escapeHtml(String(card.value))}</strong>
          <small>${escapeHtml(card.detail)}</small>
        </article>
      `,
    )
    .join('');
}

function renderHealthDiagnostics() {
  if (!dom.healthDiagnostics) {
    return;
  }
  const alerts = getSystemAlertsList();
  const alertSummary = getSystemAlertSummary();
  const adaptivePublication = state.metrics?.analytics_summary?.adaptive_publication || {};
  const quoteAge = state.metrics?.analytics_summary?.quote_age || {};
  const publicationTone = adaptivePublication.mode === 'blocked' ? 'negative' : adaptivePublication.mode === 'cautious' ? 'warning' : 'positive';
  const quoteAgeTone = Number(quoteAge.p95_seconds || 0) >= 180 ? 'negative' : Number(quoteAge.p95_seconds || 0) >= 90 ? 'warning' : 'positive';

  dom.healthDiagnostics.innerHTML = `
    <article class="detail-note-card health-diagnostic-card detail-action-card">
      <h3>${escapeHtml(pickLocalized('System alerts', 'Системные алерты'))}</h3>
      <p>${escapeHtml(Number(alertSummary.active_count || 0) > 0
        ? pickLocalized('Active alerts are generated by backend quality gates and adapter/runtime checks.', 'Активные алерты формируются backend quality-gates и проверками адаптеров/рантайма.')
        : pickLocalized('No active system alerts in the latest metrics snapshot.', 'В последнем срезе метрик активных системных алертов нет.'))}</p>
      <div class="health-diagnostic-metrics">
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Active', 'Активно'))}</span>
          <strong class="${escapeHtml(alertSummaryTone(alertSummary))}">${escapeHtml(String(Number(alertSummary.active_count || 0)))}</strong>
        </article>
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Highest severity', 'Максимальная серьёзность'))}</span>
          <strong>${escapeHtml(translateAlertSeverity(alertSummary.highest_severity || 'info'))}</strong>
        </article>
      </div>
      ${alerts.length ? `
        <ul class="inline-note-list">
          ${alerts.slice(0, 5).map((alert) => `<li>${escapeHtml(formatSystemAlertLine(alert))}</li>`).join('')}
        </ul>
      ` : ''}
    </article>
    <article class="detail-note-card health-diagnostic-card">
      <h3>${escapeHtml(pickLocalized('Publication profile', 'Профиль публикации'))}</h3>
      <p>${escapeHtml(pickLocalized('These thresholds decide how selective the published opportunity feed is under current market conditions.', 'Эти пороги определяют, насколько выборочно публикуется поток возможностей при текущем рыночном фоне.'))}</p>
      <div class="health-diagnostic-metrics">
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Mode', 'Режим'))}</span>
          <strong class="${escapeHtml(`value-${publicationTone}`)}">${escapeHtml(translatePublicationMode(adaptivePublication.mode || 'open'))}</strong>
        </article>
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Published', 'Опубликовано'))}</span>
          <strong>${escapeHtml(`${Number(state.metrics?.analytics_summary?.published_cross || state.metrics?.summary?.cross_opportunities || 0)}/${Number(state.metrics?.analytics_summary?.total_cross || state.cross.length || 0)}`)}</strong>
        </article>
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Min net spread', 'Мин. net spread'))}</span>
          <strong>${escapeHtml(`${formatNumber(Number(adaptivePublication.min_net_spread || 0), 2)}%`)}</strong>
        </article>
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Min venue volume', 'Мин. объём площадки'))}</span>
          <strong>${escapeHtml(formatCompact(Number(adaptivePublication.min_volume || 0)))}</strong>
        </article>
      </div>
    </article>
    <article class="detail-note-card health-diagnostic-card">
      <h3>${escapeHtml(pickLocalized('Quote freshness', 'Свежесть котировок'))}</h3>
      <p>${escapeHtml(quoteAge.available
        ? pickLocalized('Quote age shows how old the source quotes are before cross computations are published.', 'Возраст котировок показывает, насколько стары исходные котировки до публикации cross-расчётов.')
        : pickLocalized('Quote-age telemetry is not available in this snapshot yet.', 'Телеметрия возраста котировок пока недоступна в этом снимке.'))}</p>
      <div class="health-diagnostic-metrics">
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Average age', 'Средний возраст'))}</span>
          <strong class="${escapeHtml(`value-${quoteAgeTone}`)}">${escapeHtml(quoteAge.available ? `${formatNumber(Number(quoteAge.avg_seconds || 0), 0)}s` : pickLocalized('n/a', 'н/д'))}</strong>
        </article>
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('P95 age', 'Возраст p95'))}</span>
          <strong class="${escapeHtml(`value-${quoteAgeTone}`)}">${escapeHtml(quoteAge.available ? `${formatNumber(Number(quoteAge.p95_seconds || 0), 0)}s` : pickLocalized('n/a', 'н/д'))}</strong>
        </article>
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Stale share', 'Доля stale'))}</span>
          <strong>${escapeHtml(quoteAge.available ? `${formatNumber(Number(quoteAge.stale_share || 0) * 100, 1)}%` : pickLocalized('n/a', 'н/д'))}</strong>
        </article>
        <article class="health-diagnostic-metric">
          <span>${escapeHtml(pickLocalized('Metrics generated', 'Срез метрик'))}</span>
          <strong>${escapeHtml(state.metrics?.generated_at ? formatDateTime(state.metrics.generated_at) : pickLocalized('n/a', 'н/д'))}</strong>
        </article>
      </div>
    </article>
  `;
}

function handleExport(scope, format) {
  const rows = getExportRows(scope);
  if (!rows.length) {
    setRefreshStatus(t('refresh.noRows', { scope }), true);
    return;
  }

  const payload = format === 'csv' ? toCsv(rows) : JSON.stringify(rows, null, 2);
  const blob = new Blob([payload], { type: format === 'csv' ? 'text/csv;charset=utf-8' : 'application/json;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `${scope}-${new Date().toISOString().slice(0, 19).replace(/[T:]/g, '-')}.${format}`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function getExportRows(scope) {
  if (scope === 'cross') {
    return getSortedCrossRows();
  }
  if (scope === 'funding') {
    return sortRows(getFilteredFunding(), 'funding');
  }
  return [];
}

function toCsv(rows) {
  if (!rows.length) {
    return '';
  }
  const headers = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const escapeCsv = (value) => {
    const text = String(value ?? '');
    if (/[",\n]/.test(text)) {
      return `"${text.replaceAll('"', '""')}"`;
    }
    return text;
  };

  return [
    headers.join(','),
    ...rows.map((row) => headers.map((header) => escapeCsv(row[header])).join(',')),
  ].join('\n');
}

function parseSnapshotTimestamp(value) {
  const normalized = value.includes('T') ? value : `${value.replace(/-/g, '-').replace(/-(\d{2})-(\d{2})$/, 'T$1:$2')}:00Z`;
  return new Date(normalized).getTime();
}

function formatDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat('ru-RU', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZone: 'Europe/Moscow',
  }).format(date) + ' МСК';
}

function formatNumber(value, digits = 2) {
  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue.toFixed(digits) : 'n/a';
}

function formatCurrency(value) {
  const numericValue = Number(value);
  return Number.isFinite(numericValue)
    ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(numericValue)
    : 'n/a';
}

function formatCompact(value) {
  if (value == null) return 'n/a';
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return 'n/a';
  }
  return new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 }).format(numericValue);
}

function valueClass(value) {
  if (value > 0) return 'value-positive';
  if (value < 0) return 'value-negative';
  return 'value-warning';
}

function isSafeUrl(url) {
  return typeof url === 'string' && /^https?:\/\//i.test(url.trim());
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function setupPwa() {
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register(`./sw.js?v=${ASSET_VERSION}`, { updateViaCache: 'none' })
      .then((registration) => registration.update())
      .catch(() => {});
  }
}
