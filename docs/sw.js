// Service Worker — Financeiro Brian
const CACHE = 'fin-v1';
const STATIC = [
  '/financeiro-brian/',
  '/financeiro-brian/index.html',
];

self.addEventListener('install', e => {
  self.skipWaiting();
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC).catch(()=>{})));
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

  // App shell: cache primeiro, depois rede em background
  e.respondWith(
    caches.match(e.request).then(cached => {
      const net = fetch(e.request).then(r => {
        if (r.ok) caches.open(CACHE).then(c => c.put(e.request, r.clone()));
        return r;
      }).catch(() => cached);
      return cached || net;
    })
  );
});
