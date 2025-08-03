from database.postgresqldb import PostgreSQLDB, RawSQL
from modules.location_matcher import LocationMatcher
from utils.get_city_from_coordinates import get_city_from_coordinates
import pandas as pd

db = PostgreSQLDB()
lm = LocationMatcher()

FLOOR_TYPE_MAP = {
    'FIRST_FLOOR': 1,
    'INTERMEDIATE_FLOOR': 2,
    'TOP_FLOOR': 3
}

def safe_int(value):
    return int(value) if pd.notna(value) else None

def safe_float(value):
    return float(value) if pd.notna(value) else None

def safe_bool(value) -> bool:
    return bool(int(value)) if pd.notna(value) else False

def insert_ads_from_df(input_df: pd.DataFrame):
    "Inserts the rows from input_df and returns the correct inserted rows."

    required_fields = ['price', 'ccaa', 'province', 'municipality', 'longitude', 'latitude']

    status_list = []

    for idx, row in input_df.iterrows():
        missing_fields = [field for field in required_fields if pd.isna(row.get(field))]
        
        if missing_fields:
            continue

        if safe_int(row.get('propertySubtype',1)) == 9: # Anuncio de parcela/terreno, no aplica
            continue

        city_params = lm.match_location(row['ccaa'], row['province'], row['municipality'])

        if city_params['guess'] is None:
            # Posiblemente en municipality esté un nombre no oficial (urbanización, barrio...) en lugar del nombre de a ciudad
            city_name = get_city_from_coordinates(latitude=row['latitude'], longitude=row['longitude']) 

            if city_name:
                city_params = lm.match_location(row['ccaa'], row['province'], city_name)

            if city_params['guess'] is None:
                print(f"No city guess available for ad_id {idx}")
                continue

        floor_type_raw = row.get('floorType')
        floor_type = FLOOR_TYPE_MAP.get(floor_type_raw, None)

        input_params = {
            'table': 'ads_data',
            'values': {
                'ad_id': safe_int(idx),  # 'id' comes from index
                'page_number': safe_int(row.get('page_number')),
                'price': safe_int(row.get('price')),
                'surface': safe_int(row.get('surface')),
                'rooms': safe_int(row.get('rooms')),
                'bathrooms': safe_int(row.get('bathrooms')),
                'zip_code': safe_int(row.get('zipCode')),
                'location': RawSQL(f"ST_SetSRID(ST_MakePoint({row['longitude']}, {row['latitude']}), 4326)"),
                'conservation_status': safe_int(row.get('conservationStatus')),
                'antiquity': safe_int(row.get('antiquity')),
                'floor_type': floor_type,
                'orientation': safe_int(row.get('orientation')),
                'terrace': safe_bool(row.get('terrace')),
                'parking': safe_bool(row.get('parking')),
                'elevator': safe_bool(row.get('elevator')),
                'swimming_pool': safe_bool(row.get('swimming_pool')),
                'garden': safe_bool(row.get('garden')),
                'air_conditioner': safe_bool(row.get('air_conditioner')),
                'heater': safe_bool(row.get('heater')),
                'balcony': safe_bool(row.get('balcony')),
                'bus_distance': safe_float(row.get('bus_distance')),
                'train_distance': safe_float(row.get('train_distance')),
                'tram_distance': safe_float(row.get('tram_distance')),
                'city_id': int(city_params['guess_id'])
            }
        }
        status, _ = db.insert(params=input_params)
        status_list.append(status)

    return sum(status_list)

# # Example usage
# if __name__ == "__main__":
#     df_alava = pd.read_csv(os.path.join(os.getcwd(), 'old', 'fotocasa_data_Araba_Álava.csv'))
#     insert_ads_from_df(input_df=df_alava)
