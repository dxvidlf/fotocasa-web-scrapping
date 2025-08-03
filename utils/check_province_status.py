from database.postgresqldb import PostgreSQLDB

db = PostgreSQLDB()

def check_province_status(province_index: int = -1) -> bool:
    return db.select(params={
        'table': 'provinces',
        'fields': ['is_fetched'],
        'filters': {
            'where': {
                'field': 'province_id',
                'operator': '=',
                'value': province_index
            }
        }
    })[0]['is_fetched']

# # Example usage
# print(check_province_status(11))