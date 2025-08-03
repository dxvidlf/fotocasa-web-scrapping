import requests
import pandas as pd
import math
import warnings
from tqdm import tqdm
import os
import logging
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests.exceptions
import time
from modules.stop_locator import StopLocator
from utils.get_provinces_info import get_provinces_info
from utils.insert_ads_from_df import insert_ads_from_df
from utils.get_next_page import get_next_page
from utils.update_heartbeat import update_heartbeat
from utils.set_total_pages_on_province import set_total_pages_on_province
from utils.update_current_page_on_province import update_current_page_on_province
from utils.set_province_as_fetched import set_province_as_fetched

warnings.filterwarnings('ignore', category=FutureWarning)

LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("FotocasaDataFetcher")
logger.setLevel(logging.DEBUG)

if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
    log_file = os.path.join(LOG_DIR, "fotocasa_fetcher.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))
    logger.addHandler(console_handler)

class FotocasaDataFetcher:
    def __init__(self, max_empty_consecutive_dfs: int = 10, max_consecutive_bad_inserts: int = 3, proxy_manager=None):
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.fotocasa.es",
            "Referer": "https://www.fotocasa.es/",
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
            ),
        }
        self.stop_locator = StopLocator()
        self.provinces_info = get_provinces_info()
        self.logger = logger
        self.consecutive_empty_dfs = 0
        self.MAX_EMPTY_DFS = max_empty_consecutive_dfs
        self.consecutive_bad_inserts = 0
        self.MAX_CONSECUTIVE_BAD_INSERTS = max_consecutive_bad_inserts
        self.proxy_manager = proxy_manager

    def _request_with_proxy(self, method, url, **kwargs):
        # Intentamos usar proxies rotativos hasta 5 veces
        for attempt in range(5):
            proxy = self.proxy_manager.get_proxy() if self.proxy_manager else None
            proxies = None
            if proxy:
                proxies = {
                    'http': f'http://{proxy}',
                    'https': f'http://{proxy}'
                }
            try:
                response = requests.request(method, url, proxies=proxies, timeout=5, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if proxy:
                    self.proxy_manager.mark_failed(proxy)
                    self.logger.warning(f"Proxy {proxy} falló, intentando otro proxy...")
                else:
                    self.logger.warning(f"Request falló sin proxy: {e}")
                time.sleep(1)

    def _build_params(self, ids, lat, lon, next_page):
        return {
            "combinedLocationIds": ids,
            "culture": "es-ES",
            "includePurchaseTypeFacets": "true",
            "isMap": "false",
            "isNewConstructionPromotions": "false",
            "latitude": lat,
            "longitude": lon,
            "pageNumber": next_page,
            "platformId": 1,
            "propertyTypeId": 2,
            "size": 30,
            "sortOrderDesc": "true",
            "sortType": "scoring",
            "transactionTypeId": 1
        }

    def _safe(self, d, key, cast=None):
        try:
            val = d.get(key)
            return cast(val) if cast and val is not None else val
        except:
            return None

    def _parse_v1(self, ad):
        loc = ad.get('location', {})
        trans = ad.get('transaction', {})
        return {
            'id': self._safe(ad, 'propertyId', int),
            'propertySubtype': self._safe(ad, 'propertySubtype', int),
            'price': self._safe(trans, 'price', int),
            'bathrooms': self._safe(ad, 'baths', int),
            'conservationStatus': ad.get('conservationStatus'),
            'surface': self._safe(ad, 'surface', int),
            'rooms': self._safe(ad, 'rooms', int),
            'zipCode': self._safe(ad, 'zipCode', int),
            'orientation': ad.get('orientation'),
            'floorType': ad.get('floorType'),
            'antiquity': ad.get('antiquity'),
            'ccaa': loc.get('level1Name'),
            'province': loc.get('level2Name'),
            'municipality': loc.get('level5Name'),
            'latitude': self._safe(loc, 'latitude', float),
            'longitude': self._safe(loc, 'longitude', float)
        }

    def _parse_v2(self, ad):
        def safe_int(val):
            try:
                return int(val)
            except:
                return None

        features = {f['key']: safe_int(f['value'][0]) for f in ad.get('features', []) if f.get('value')}
        return {
            'id': ad.get('propertyId'),
            **{k: features.get(k) for k in [
                'terrace', 'swimming_pool', 'parking', 'garden',
                'heater', 'air_conditioner', 'elevator', 'balcony'
            ]}
        }

    retry_on_requests = retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True
    )

    @retry_on_requests
    def _get_v1(self, ids, lat, lon, next_page):
        url = "https://web.gw.fotocasa.es/v1/search/ads"
        payload = {
            "combinedLocations": [ids],
            "includePurchaseTypeFacets": True,
            "isMap": False,
            "latitude": lat,
            "longitude": lon,
            "pageNumber": next_page,
            "propertyType": 2,
            "sortOrderDesc": True,
            "sortType": "scoring",
            "transactionType": 1,
            "size": 30
        }
        res = self._request_with_proxy("POST", url, headers=self.headers, json=payload)
        data = res.json()

        items = data.get("items", [])
        total_items = data.get("totalItems", 0)
        size = data.get("next_page", {}).get("size", len(items) or 30)

        if not isinstance(items, list):
            self.logger.warning(f"[V1] 'items' malformado en página {next_page}")
            items = []

        return items, total_items, size

    @retry_on_requests
    def _get_v2(self, ids, lat, lon, next_page):
        url = "https://web.gw.fotocasa.es/v2/propertysearch/search/propertycoordinates"
        res = self._request_with_proxy("GET", url, headers=self.headers, params=self._build_params(ids, lat, lon, next_page))
        data = res.json()

        if 'propertyCoordinates' in data and len(data['propertyCoordinates']) > 0:
            return data['propertyCoordinates']
        else:
            self.logger.warning(f"[V2] Respuesta vacía o malformada en página {next_page}")
            return []

    def _add_distances(self, df):
        for idx, row in df.iterrows():
            if pd.notnull(row.latitude) and pd.notnull(row.longitude):
                dists = self.stop_locator.find_nearest(row.latitude, row.longitude)
                for k, v in dists.items():
                    df.at[idx, f"{k}_distance"] = v['distance_m']
            else:
                for k in ['bus', 'metro', 'train']:
                    df.at[idx, f"{k}_distance"] = None
        return df

    def _write_error_log(self, province_name, next_page, df1, df2):
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"logs/error_{province_name}_{next_page}_{now}.log"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"Error en provincia: {province_name}\n")
            f.write(f"Página: {next_page}\n")
            f.write(f"Timestamp: {now}\n\n")
            f.write("DataFrame df1 (v1) contenido:\n")
            f.write(df1.to_string())
            f.write("\n\nDataFrame df2 (v2) contenido:\n")
            f.write(df2.to_string())

    def fetch_ads_from_province(self, province_index):
        province = self.provinces_info[province_index-1]
        ids, lat, lon = province['ids'], province['latitude'], province['longitude']

        try:
            first_items, total, size = self._get_v1(ids, lat, lon, next_page=1)
        except Exception as e:
            self.logger.error(f"Error inicial en _get_v1 para ids={ids}: {e}")
            return

        if not first_items:
            self.logger.warning(f"No se encontraron anuncios iniciales para ids={ids}")
            return

        total_pages = math.ceil(total / size)
        set_total_pages_on_province(province_index,total_pages)
        
        next_page = get_next_page(province_index)

        if next_page >= total_pages:
            set_province_as_fetched(province_index)
            return True
        
        with tqdm(total=total_pages, desc=f"{province['nombre']} ({province_index})", leave=False, initial=next_page) as pbar:
            while next_page <= total_pages:
                try:
                    items_v1, _, _ = self._get_v1(ids, lat, lon, next_page)
                    items_v2 = self._get_v2(ids, lat, lon, next_page)
                except Exception as e:
                    self.logger.error(f"Error en petición página {next_page} para provincia {self._parse_v1(first_items[0]).get('province')}: {e}")
                    self._handle_empty_or_error(next_page, self._parse_v1(first_items[0]).get('province'))
                    continue
                
                df1 = pd.DataFrame([self._parse_v1(ad) for ad in items_v1]).set_index('id') if items_v1 else pd.DataFrame()
                df2 = pd.DataFrame([self._parse_v2(ad) for ad in items_v2]).set_index('id') if items_v2 else pd.DataFrame()

                if not df1.empty and not df2.empty:
                    df = pd.merge(df1, df2, left_index=True, right_index=True, how='inner')
                    df = self._add_distances(df)
                    df['page_number'] = next_page

                    if insert_ads_from_df(input_df=df) == 0:
                        self.consecutive_bad_inserts += 1
                    else:
                        self.consecutive_bad_inserts += 0

                    if self.consecutive_bad_inserts >= self.MAX_CONSECUTIVE_BAD_INSERTS:  # Parada temprana por detección de replicación de anuncios
                        set_province_as_fetched(province_index)
                        pbar.update(pbar.total - pbar.n)
                        return True
                    
                    self.consecutive_empty_dfs = 0
                    update_current_page_on_province(province_index,next_page)
                    next_page += 1
                    pbar.update(1)
                else:
                    self.logger.warning(f"DataFrames vacíos en provincia {self._parse_v1(first_items[0]).get('province')} página {next_page}. Generando log...")
                    self._write_error_log(self._parse_v1(first_items[0]).get('province'), next_page, df1, df2)
                    self.consecutive_empty_dfs += 1
                    if self.consecutive_empty_dfs >= self.MAX_EMPTY_DFS:
                        raise RuntimeError(f"Demasiados errores consecutivos en {self._parse_v1(first_items[0]).get('province')} (página {next_page}), abortando ejecución.")
                
                update_heartbeat()

        set_province_as_fetched(province_index)
        return True
    
# Example usage
if __name__ == "__main__":
    FotocasaDataFetcher().fetch_ads_from_province(province_index=1)

