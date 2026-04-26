// Service Worker — Financeiro Brian
const CACHE = 'fin-v6';
const STATIC = [
  '/financeiro-brian/',
  '/financeiro-brian/index.html',
  // CDN — chart.js
  'https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js',
  // CDN — SheetJS
  'https://cdn.sheetjs.com/xlsx-0.20.1/package/dist/xlsx.full.min.js',
];

self.addEventListener('install', e => {
  self.skipWaiting();
  e.waitUntil(
    caches.open(CACHE).then(c =>
      Promise.allSettled(STATIC.map(url => c.add(url).catch(()=>{})))
    )
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
  ).then(() => self.clients.claim()));
});

self.addEventListener('fetch', e => {
  // Supabase e APIs externas: sempre rede, sem cache
  if (e.request.url.includes('supabase.co') ||
      e.request.url.includes('brapi.dev') ||
      e.request.url.includes('bcb.gov.br')) return;

  // App shell + CDN: cache primeiro, depois rede em background
  e.respondWith(
    caches.match(e.request).then(cached => {
      const net = fetch(e.request).then(r => {
        if (r.ok && (e.request.url.includes('/financeiro-brian/') || e.request.url.includes('cdn.jsdelivr') || e.request.url.includes('cdn.sheetjs'))) {
          caches.open(CACHE).then(c => c.put(e.request, r.clone()));
        }
        return r;
      }).catch(() => cached);
      return cached || net;
    })
  );
});
