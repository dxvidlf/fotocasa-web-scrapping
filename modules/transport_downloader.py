import osmium as osm
import requests
import os

class TransportDownloader(osm.SimpleHandler):
    def __init__(self, writer=None):
        super().__init__()
        self.writer = writer

    def node(self, n):
        tags = n.tags
        if 'public_transport' in tags or 'railway' in tags or tags.get('highway') == 'bus_stop' or tags.get('amenity') == 'bus_station':
            self.writer.add_node(n)

    def way(self, w):
        if any(k in w.tags for k in ['railway', 'route', 'public_transport']):
            self.writer.add_way(w)

    def relation(self, r):
        if r.tags.get('route') in ['bus', 'tram', 'train', 'subway']:
            self.writer.add_relation(r)

    def download_file(self, url, path):
        if not os.path.exists(path):
            print(f"Downloading: {url}")
            r = requests.get(url)
            if r.ok:
                with open(path, 'wb') as f:
                    f.write(r.content)
                print(f"Downloaded: {os.path.basename(path)}")
            else:
                print(f"Failed to download {url} ({r.status_code})")

    def filter_transport(self, input_path, output_path):
        if not os.path.exists(output_path):
            writer = osm.SimpleWriter(output_path)
            handler = TransportDownloader(writer)
            handler.apply_file(input_path)
            writer.close()
            print(f"Filtered: {os.path.basename(output_path)}")

    def get_transport_data(self, assets_dir):
        os.makedirs(assets_dir, exist_ok=True)

        regions = {
            "canary-islands": "https://download.geofabrik.de/africa/canary-islands-latest.osm.pbf",
            "spain": "https://download.geofabrik.de/europe/spain-latest.osm.pbf"
        }

        osm_dir = os.path.join(os.path.dirname(assets_dir), 'osm')
        os.makedirs(osm_dir, exist_ok=True)

        for region, url in regions.items():
            raw = os.path.join(osm_dir, f"{region}-latest.osm.pbf")
            filtered = os.path.join(assets_dir, f"{region}-transporte-publico.osm.pbf")
            self.download_file(url, raw)
            self.filter_transport(raw, filtered)


# if __name__ == "__main__":
#     assets_dir = os.path.join(os.getcwd(), "assets")
#     TransportDownloader().get_transport_data(assets_dir)
#     print("✅ Transport data downloaded and filtered successfully.")
