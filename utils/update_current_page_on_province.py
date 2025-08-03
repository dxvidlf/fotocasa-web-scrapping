from database.postgresqldb import PostgreSQLDB

db = PostgreSQLDB()

def update_current_page_on_province(province_index: int = -1, current_page: int = -1):
    params = {
        'table': 'provinces',
        'values': {
            'fetched_pages': current_page
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

# # # Example usage
# update_current_page_on_province(province_index=1, current_page=100)