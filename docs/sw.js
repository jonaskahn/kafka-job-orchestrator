/* global self, caches */
const CACHE_NAME = "kjo-docs-v1";
const urlsToCache = [
  "./",
  "./index.html",
  "./manifest.json",
  "https://cdn.jsdelivr.net/npm/daisyui@4.4.24/dist/full.min.css",
  "https://cdn.tailwindcss.com",
  "https://cdn.jsdelivr.net/npm/gsap@3.13.0/dist/gsap.min.js",
  "https://unpkg.com/lucide@latest",
];

// Install event - cache resources
self.addEventListener("install", event => {
  event.waitUntil(
    caches
      .open(CACHE_NAME)
      .then(cache => {
        console.log("Cache opened");
        return cache.addAll(urlsToCache);
      })
      .catch(error => {
        console.log("Cache installation failed:", error);
      })
  );
});

// Fetch event - serve from cache, fallback to network
self.addEventListener("fetch", event => {
  event.respondWith(
    caches
      .match(event.request)
      .then(response => {
        // Return cached version or fetch from network
        return response || fetch(event.request);
      })
      .catch(() => {
        // Return a fallback page if both cache and network fail
        if (event.request.destination === "document") {
          return caches.match("./index.html");
        }
      })
  );
});

// Activate event - clean up old caches
self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.map(cacheName => {
          if (cacheName !== CACHE_NAME) {
            console.log("Deleting old cache:", cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
});
