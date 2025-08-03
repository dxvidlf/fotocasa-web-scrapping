from database.postgresqldb import PostgreSQLDB

db = PostgreSQLDB()

def set_total_pages_on_province(province_index: int, total_pages: int = 0):
    params = {
        'table': 'provinces',
        'values': {
            'total_pages': total_pages
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
# set_total_pages_on_province(province_index=1, total_pages=100)