import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import os

# Archivos sacados de https://centrodedescargas.cnig.es/CentroDescargas/resultados-busqueda
peninbal_path = os.path.join(os.getcwd(),'assets','shp',"recintos_municipales_inspire_peninbal_etrs89.shp")
canarias_path = os.path.join(os.getcwd(),'assets','shp',"recintos_municipales_inspire_canarias_regcan95.shp")
ceuta_path = os.path.join(os.getcwd(),'assets','shp',"zonaneutral Marruecos-Ceuta.shp")
melilla_path = os.path.join(os.getcwd(),'assets','shp',"Zona Neutral Marruecos-Melilla.shp")

def get_city_from_coordinates(latitude,longitude):
    gdf_total = gpd.GeoDataFrame(pd.concat([gpd.read_file(peninbal_path), gpd.read_file(canarias_path).to_crs(epsg=4258),gpd.read_file(ceuta_path),gpd.read_file(melilla_path)], ignore_index=True), crs="EPSG:4258")
    punto = Point(longitude, latitude) 
    for _, row in gdf_total.iterrows():
        if row.geometry.contains(punto):
            return row["NAMEUNIT"]
    return None

# # Example usage
# if __name__ == '__main__':
#     print(get_city_from_coordinates(36.72985333462365, -4.433723694409191))
