import threading
import random
import logging

class ProxyManager:
    def __init__(self, proxy_tester):
        self.proxy_tester = proxy_tester
        self.lock = threading.Lock()
        self.proxies = []
        self.failed_proxies = set()
        self.refresh_proxies()

    def refresh_proxies(self):
        logging.info("Obteniendo proxies nuevos...")
        new_proxies = self.proxy_tester.get_working_proxies() or []
        with self.lock:
            self.proxies = new_proxies
            self.failed_proxies.clear()
        logging.info(f"{len(new_proxies)} proxies disponibles.")

    def get_proxy(self):
        with self.lock:
            available = [p for p in self.proxies if p not in self.failed_proxies]
            if not available:
                logging.warning("No quedan proxies disponibles, refrescando lista...")
                self.refresh_proxies()
                available = self.proxies
                if not available:
                    logging.error("No hay proxies disponibles después de refrescar.")
                    return None
            proxy = random.choice(available)
            return proxy

    def mark_failed(self, proxy):
        with self.lock:
            self.failed_proxies.add(proxy)
