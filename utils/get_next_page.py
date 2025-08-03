from database.postgresqldb import PostgreSQLDB, RawSQL

db = PostgreSQLDB()

def get_next_page(province_id: int = -1):
    cities = db.select({
        'table': 'cities',
        'fields': ['city_id'],
        'filters': {
            'where': {
                'field': 'province_id',
                'operator': '=',
                'value': province_id
            }
        }
    })
    city_ids = [city['city_id'] for city in cities]

    ads_query = {
        'table': 'ads_data',
        'fields': [RawSQL("MAX(page_number)")],
        'filters': {
            'where': [{
                'field': 'city_id',
                'operator': 'IN',
                'value': city_ids
            }]
        }
    }
    result = db.select(ads_query)
    last_page = result[0]['max']

    return last_page + 1 if last_page is not None else 1

# # Example usage
# if __name__=='__main__':
#     print(get_last_inserted_page(province_id = 1)