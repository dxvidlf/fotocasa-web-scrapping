import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd

PROXY_LIST_URL = "https://free-proxy-list.net/"
TEST_URL = "https://web.gw.fotocasa.es/v2/propertysearch/search/propertycoordinates?combinedLocationIds=724,1,29,0,0,0,0,0,0&culture=es-ES&includePurchaseTypeFacets=true&isMap=false&isNewConstructionPromotions=false&latitude=36.72&longitude=-4.41491&pageNumber=1&platformId=1&propertyTypeId=2&size=30&sortOrderDesc=true&sortType=scoring&transactionTypeId=1"
TIMEOUT = 5
MAX_PROXIES = 50  # Máximo número de proxies a testear

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ProxyTester:
    def __init__(self):
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        return session

    def _make_request(self, url: str, proxies: Optional[Dict] = None, timeout: int = TIMEOUT) -> Optional[requests.Response]:
        try:
            response = self.session.get(url, proxies=proxies, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed: {e}")
            return None

    def fetch_proxy_list(self) -> List[Dict]:
        """Obtiene y filtra la lista de proxies"""
        response = self._make_request(PROXY_LIST_URL, timeout=15)
        if not response:
            logging.error("No se pudo obtener la lista de proxies.")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'table table-striped table-bordered'})
        if not table:
            logging.warning("No se encontró la tabla de proxies en el HTML.")
            return []

        proxies = []
        for row in table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) >= 8:
                proxy_data = {
                    'ip': cols[0].text.strip(),
                    'port': cols[1].text.strip(),
                    'country_code': cols[2].text.strip(),
                    'country': cols[3].text.strip(),
                    'anonymity': cols[4].text.strip(),
                    'google': cols[5].text.strip(),
                    'https': cols[6].text.strip(),
                    'last_checked': cols[7].text.strip(),
                    'working': None,
                    'response_time': None,
                    'tested_at': None
                }
                if proxy_data['https'] == "yes" and proxy_data['anonymity'] in ['anonymous', 'elite proxy']:
                    proxies.append(proxy_data)

        logging.info(f"{len(proxies)} proxies filtrados encontrados.")
        return proxies

    def test_proxy(self, proxy: Dict) -> Dict:
        proxy_url = f"http://{proxy['ip']}:{proxy['port']}"
        proxy_config = {'http': proxy_url}
        result = proxy.copy()
        start_time = time.time()

        response = self._make_request(TEST_URL, proxies=proxy_config)
        elapsed = time.time() - start_time

        if response and response.status_code == 200:
            result.update({
                'working': True,
                'response_time': round(elapsed, 2),
                'tested_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            result['working'] = False

        return result

    def test_proxy_list(self, proxies: List[Dict]) -> Optional[List[str]]:
        if not proxies:
            logging.warning("Lista de proxies vacía.")
            return None

        limited_proxies = proxies[:MAX_PROXIES]
        working_proxies = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(limited_proxies), 20)) as executor:
            future_to_proxy = {executor.submit(self.test_proxy, proxy): proxy for proxy in limited_proxies}
            for future in concurrent.futures.as_completed(future_to_proxy):
                try:
                    tested_proxy = future.result()
                    if tested_proxy['working']:
                        working_proxies.append(tested_proxy)
                except Exception as e:
                    logging.error(f"Error al testear proxy: {e}")

        if working_proxies:
            df = pd.DataFrame(working_proxies)
            df['ip:port'] = df.apply(lambda x: f"{x['ip']}:{x['port']}", axis=1)
            return df['ip:port'].to_list()
        else:
            return None

    def get_working_proxies(self) -> Optional[List[str]]:
        """Obtiene una lista de proxies funcionales en formato ip:port"""
        proxies = self.fetch_proxy_list()
        if not proxies:
            logging.error("No se pudieron obtener proxies.")
            return None

        working_proxies = self.test_proxy_list(proxies)
        if working_proxies:
            return working_proxies
        else:
            logging.warning("No hay proxies disponibles.")
            return None

# if __name__ == "__main__":
#     tester = ProxyTester()
#     working_proxies = tester.get_working_proxies()
#     print(working_proxies)
