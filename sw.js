const CACHE_NAME = 'fas-cache-v22';
const ASSET_VERSION = '20260322-1';
const STATIC_ASSETS = [
  './',
  './index.html',
  `./styles.css?v=${ASSET_VERSION}`,
  `./script.js?v=${ASSET_VERSION}`,
  './manifest.json',
  './assets/icon-192.svg',
  './assets/icon-512.svg',
];

function isApiRequest(url) {
  return url.pathname.startsWith('/api/');
}

function isDataRequest(url) {
  return url.pathname.startsWith('/data/') || url.pathname.endsWith('.json');
}

async function networkFirst(request, fallbackToIndex = false) {
  const cache = await caches.open(CACHE_NAME);
  try {
    const response = await fetch(request);
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
    throw new Error(`No cache entry for ${request.url}`);
  }
}

async function cacheFirst(request) {
  const cache = await caches.open(CACHE_NAME);
  const cached = await cache.match(request);
  if (cached) {
    void fetch(request).then((response) => {
      if (response.ok) {
        cache.put(request, response.clone());
      }
    }).catch(() => undefined);
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
