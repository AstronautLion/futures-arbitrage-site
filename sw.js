const CACHE_NAME = 'fas-cache-v47';
const ASSET_VERSION = '20260411-04';
const STATIC_ASSETS = [
  './',
  './index.html',
  `./styles.css?v=${ASSET_VERSION}`,
  `./script.js?v=${ASSET_VERSION}`,
  './manifest.json',
  './assets/icon-192.svg',
  './assets/icon-512.svg',
];

// Deduplication set: track URLs already being fetched to avoid parallel duplicates
const pendingFetches = new Set();

function isApiRequest(url) {
  return url.pathname.startsWith('/api/');
}

function isDataRequest(url) {
  return url.pathname.startsWith('/data/') || url.pathname.endsWith('.json');
}

function normalizeUrl(url) {
  // Strip query params for cache key dedup (styles.css?v=X → styles.css)
  return url.origin + url.pathname;
}

async function networkFirst(request, fallbackToIndex = false) {
  const cache = await caches.open(CACHE_NAME);
  const normalizedKey = normalizeUrl(new URL(request.url));
  try {
    // Deduplicate concurrent fetches for the same resource
    if (pendingFetches.has(normalizedKey)) {
      const cached = await cache.match(request);
      if (cached) return cached;
    }
    pendingFetches.add(normalizedKey);
    let response;
    try {
      response = await fetch(request);
    } finally {
      pendingFetches.delete(normalizedKey);
    }
    if (response.ok) {
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await cache.match(request);
    if (cached) {
      return cached;
    }
    if (fallbackToIndex) {
      const shell = await cache.match('./index.html');
      if (shell) {
        return shell;
      }
    }
    return new Response('Offline', { status: 503 });
  }
}

async function cacheFirst(request) {
  const cache = await caches.open(CACHE_NAME);
  const cached = await cache.match(request);
  if (cached) {
    // Stale-while-revalidate: update cache in background, never block on fetch
    const normalizedKey = normalizeUrl(new URL(request.url));
    if (!pendingFetches.has(normalizedKey)) {
      pendingFetches.add(normalizedKey);
      fetch(request).then((response) => {
        if (response.ok) cache.put(request, response.clone());
      }).catch(() => undefined)
      .finally(() => pendingFetches.delete(normalizedKey));
    }
    return cached;
  }

  const response = await fetch(request);
  if (response.ok) {
    cache.put(request, response.clone());
  }
  return response;
}

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS)));
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))),
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') {
    return;
  }

  const requestUrl = new URL(event.request.url);
  if (requestUrl.origin !== self.location.origin) {
    return;
  }

  if (isApiRequest(requestUrl)) {
    return;
  }

  if (event.request.mode === 'navigate') {
    event.respondWith(networkFirst(event.request, true));
    return;
  }

  if (isDataRequest(requestUrl)) {
    event.respondWith(networkFirst(event.request));
    return;
  }

  event.respondWith(cacheFirst(event.request));
});
