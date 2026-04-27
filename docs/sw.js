// Service Worker — Financeiro Brian
const CACHE = 'fin-v12';
const STATIC = [
  'https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js',
  'https://cdn.sheetjs.com/xlsx-0.20.1/package/dist/xlsx.full.min.js',
];

self.addEventListener('install', e => {
  self.skipWaiting();
  e.waitUntil(
    caches.open(CACHE).then(c =>
      Promise.allSettled(STATIC.map(url => c.add(url).catch(() => {})))
    )
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = e.request.url;

  // APIs externas: sempre rede, sem cache
  if (url.includes('supabase.co') || url.includes('brapi.dev') || url.includes('bcb.gov.br')) return;

  // HTML da app: network-first (garante sempre versão mais recente)
  if (url.includes('/financeiro-brian/') && (url.endsWith('/') || url.includes('.html'))) {
    e.respondWith(
      fetch(e.request)
        .then(r => {
          if (r.ok) caches.open(CACHE).then(c => c.put(e.request, r.clone()));
          return r;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  // CDN (Chart.js, SheetJS): cache-first (raramente mudam)
  if (url.includes('cdn.jsdelivr') || url.includes('cdn.sheetjs') || url.includes('cdnjs.cloudflare')) {
    e.respondWith(
      caches.match(e.request).then(cached => {
        if (cached) return cached;
        return fetch(e.request).then(r => {
          if (r.ok) caches.open(CACHE).then(c => c.put(e.request, r.clone()));
          return r;
        });
      })
    );
    return;
  }
});
