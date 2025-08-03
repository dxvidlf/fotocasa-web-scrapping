from concurrent.futures import ThreadPoolExecutor, as_completed
from modules.fotocasa_data_fetcher import FotocasaDataFetcher
from modules.proxy_tester import ProxyTester
from modules.proxy_manager import ProxyManager
from utils.get_provinces_info import get_provinces_info
from utils.check_province_status import check_province_status

def fetch_all_provinces(proxy_manager, max_workers=5):

    remaining_provinces = list(range(1, len(get_provinces_info()) + 1))

    while remaining_provinces:
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in remaining_provinces:
                if not check_province_status(i):
                    fetcher = FotocasaDataFetcher(proxy_manager=proxy_manager)
                    future = executor.submit(fetcher.fetch_ads_from_province, i)
                    futures[future] = i

            # Provincias que fallaron en esta ronda
            failed_provinces = []
            for future in as_completed(futures):
                i = futures[future]
                try:
                    success = future.result()
                    if not success:
                        failed_provinces.append(i)
                except Exception as e:
                    print(f"❌ Error inesperado en provincia {i}: {e}")
                    failed_provinces.append(i)
        
        remaining_provinces = failed_provinces

if __name__ == "__main__":
    tester = ProxyTester()
    proxy_manager = ProxyManager(tester)
    fetch_all_provinces(proxy_manager, max_workers=10)
