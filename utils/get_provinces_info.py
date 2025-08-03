import requests
import json
from pathlib import Path
from tqdm import tqdm

def get_provinces_info():
    path = Path("assets/provinces_info.json")
    if path.exists():
        # print("Province information already exists. Skipping fetch.")
        return json.loads(path.read_text(encoding="utf-8"))
    print("Fetching province information...")
    provincias = [
        {"ruta": ruta, "nombre": nombre} for ruta, nombre in [
            ("araba-alava", "Araba/Álava"), ("albacete", "Albacete"), ("alicante", "Alicante/Alacant"),
            ("almeria", "Almería"), ("asturias", "Asturias"), ("avila", "Ávila"),
            ("badajoz", "Badajoz"), ("barcelona", "Barcelona"), ("burgos", "Burgos"),
            ("caceres", "Cáceres"), ("cadiz", "Cádiz"), ("cantabria", "Cantabria"),
            ("castellon", "Castellón/Castelló"), ("ciudad-real", "Ciudad Real"),
            ("cordoba", "Córdoba"), ("cuenca", "Cuenca"), ("girona", "Girona"),
            ("granada", "Granada"), ("guadalajara", "Guadalajara"), ("gipuzkoa", "Gipuzkoa"),
            ("huelva", "Huelva"), ("huesca", "Huesca"), ("illes-balears", "Islas Baleares"),
            ("jaen", "Jaén"), ("a-coruna", "A Coruña"), ("la-rioja", "La Rioja"),
            ("las-palmas", "Las Palmas"), ("leon", "León"), ("lleida", "Lleida"),
            ("lugo", "Lugo"), ("madrid", "Comunidad de Madrid"), ("malaga", "Málaga"),
            ("murcia", "Región de Murcia"), ("navarra", "Navarra"), ("ourense", "Ourense"),
            ("palencia", "Palencia"), ("pontevedra", "Pontevedra"), ("salamanca", "Salamanca"),
            ("santa-cruz-de-tenerife", "Santa Cruz de Tenerife"), ("segovia", "Segovia"),
            ("sevilla", "Sevilla"), ("soria", "Soria"), ("tarragona", "Tarragona"),
            ("teruel", "Teruel"), ("toledo", "Toledo"), ("valencia", "Valencia/València"),
            ("valladolid", "Valladolid"), ("bizkaia", "Bizkaia"), ("zamora", "Zamora"),
            ("zaragoza", "Zaragoza"), ("ceuta", "Ceuta"), ("melilla", "Melilla")
        ]
    ]
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.fotocasa.es",
        "Referer": "https://www.fotocasa.es/",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    }
    for provincia in tqdm(provincias, desc="Fetching province data", unit="province"):
        url = f"https://web.gw.fotocasa.es/v2/propertysearch/urllocationsegments?location={provincia['ruta']}-provincia&zone=todas-las-zonas"
        try:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            data = r.json()
            provincia["ids"] = data.get("ids", [])
            coords = data.get("coordinates", {})
            provincia["latitude"] = coords.get("latitude")
            provincia["longitude"] = coords.get("longitude")
        except Exception as e:
            print(f"❌ Error con {provincia['nombre']}: {e}")
            provincia.update({"ids": [], "latitude": None, "longitude": None})
    path.parent.mkdir(parents=True, exist_ok=True)
    provincias = sorted(provincias, key=lambda x: int(x["ids"].split(',')[2]))
    with path.open("w", encoding="utf-8") as f:
        json.dump(provincias, f, ensure_ascii=False, indent=2)
    return provincias