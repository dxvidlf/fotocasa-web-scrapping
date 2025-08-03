from database.postgresqldb import PostgreSQLDB, RawSQL

db = PostgreSQLDB()

def check_global_status() -> bool:
    fetched_count = db.select(params={
        'table': 'provinces',
        'fields': [RawSQL("COUNT(is_fetched) AS count")],
        'filters': {
            'where': {
                'field': 'is_fetched',
                'operator': '=',
                'value': True
            }
        }
    })[0]['count']

    total_count = db.select(params={
        'table': 'provinces',
        'fields': [RawSQL("COUNT(province_id) AS count")]
    })[0]['count']

    return fetched_count == total_count

# # Example usage
# print(check_global_status())