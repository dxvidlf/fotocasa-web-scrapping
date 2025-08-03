from database.postgresqldb import PostgreSQLDB

db = PostgreSQLDB()

def set_province_as_fetched(province_index: int):
    params = {
        'table': 'provinces',
        'values': {
            'is_fetched': True
        },
        'filters': {
            'where': {
                'field': 'province_id',
                'operator': '=',
                'value': province_index
            }
        }
    }
    db.update(params)

# # Example usage
# set_province_as_fetched(province_index=1)