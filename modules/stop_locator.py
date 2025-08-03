import osmium as osm
from geopy.distance import geodesic
from rtree import index
import os
from modules.transport_downloader import TransportDownloader

BASED_DIR = os.path.join(os.getcwd(), "assets",'public_transport')

class StopLocator:
    def __init__(self):
        self.pbf_files = [
            os.path.join(BASED_DIR, "spain-transporte-publico.osm.pbf"),
            os.path.join(BASED_DIR, "canary-islands-transporte-publico.osm.pbf")
        ]
        self._ensure_data_available()
        self.stops = self._build_spatial_index()

    def _ensure_data_available(self):
        if not all(os.path.exists(p) for p in self.pbf_files):
            print("Downloading and filtering transport data...")
            TransportDownloader().get_transport_data(BASED_DIR)

        # Re-verifica después de la descarga
        for pbf_file in self.pbf_files:
            if not os.path.exists(pbf_file):
                raise FileNotFoundError(f"Required PBF file not found: {pbf_file}")

    def _build_spatial_index(self):
        idx = {t: index.Index() for t in ['bus', 'train', 'tram']}
        coords = {t: {} for t in ['bus', 'train', 'tram']}
        counters = {t: 0 for t in ['bus', 'train', 'tram']}

        class Handler(osm.SimpleHandler):
            def node(self, n):
                lat, lon = n.location.lat, n.location.lon
                if n.tags.get('highway') == 'bus_stop' or n.tags.get('public_transport') == 'bus':
                    t = 'bus'
                elif n.tags.get('railway') in ['station', 'halt'] or n.tags.get('public_transport') == 'train':
                    t = 'train'
                elif n.tags.get('railway') == 'tram_stop' or n.tags.get('public_transport') == 'tram':
                    t = 'tram'
                else:
                    return

                i = counters[t]
                idx[t].insert(i, (lon, lat, lon, lat))
                coords[t][i] = (lat, lon)
                counters[t] += 1

        handler = Handler()
        for pbf_file in self.pbf_files:
            handler.apply_file(pbf_file)

        return {t: {'index': idx[t], 'coords': coords[t]} for t in ['bus', 'train', 'tram']}

    def find_nearest(self, lat, lon, radius_km=5):
        results = {}
        point = (lat, lon)
        deg_radius = radius_km / 111.32

        for t in ['bus', 'train', 'tram']:
            idx = self.stops[t]['index']
            coords = self.stops[t]['coords']
            candidates = list(idx.nearest((lon - deg_radius, lat - deg_radius, lon + deg_radius, lat + deg_radius), 5))

            if not candidates:
                results[t] = None
                continue

            closest = min(
                ((geodesic(point, coords[i]).meters, coords[i]) for i in candidates),
                key=lambda x: x[0]
            )
            results[t] = {'distance_m': round(closest[0]), 'coordinates': closest[1]}

        return results

# # Ejemplo de uso
# if __name__ == "__main__":
#     locator = StopLocator()
#     nearest_stops = locator.find_nearest(27.940335643166776, -15.56964137086057)  # Coordenadas en Canarias
#     print("Nearest Stops:")
#     for transport_type, data in nearest_stops.items():
#         if data:
#             print(f"{transport_type.capitalize()}: {data['distance_m']:.1f} m - Coord: {data['coordinates']}")
#         else:
#             print(f"{transport_type.capitalize()}: Not found within radius")
