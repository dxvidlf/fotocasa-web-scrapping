from pathlib import Path
import pandas as pd
from tqdm import tqdm
from database.postgresqldb import PostgreSQLDB, RawSQL

base_df = pd.read_csv(Path('assets/ccaa_province_city.csv'))
db = PostgreSQLDB()

def insert_rows(df, table_name):
    """Insert rows into the given table displaying a progress bar."""
    for row in tqdm(df.itertuples(index=False), total=len(df), desc=f"Inserting {table_name}"):
        db.insert({'table': table_name, 'values': row._asdict()})

def ensure_base_db_structure():
    """
    Checks that the CCAA, provinces, and cities tables are complete
    according to the CSV. Inserts missing data if necessary.
    """
    tables = ['ccaas', 'provinces', 'cities']
    db_counts = {
        table: db.select({"table": table, "fields": [RawSQL("COUNT(*) AS total")]} )[0]['total']
        for table in tables
    }
    csv_counts = {
        'ccaas': base_df['ccaa_id'].nunique(),
        'provinces': base_df['province_id'].nunique(),
        'cities': base_df['city_id'].nunique()
    }
    if any(csv_counts[t] != db_counts[t] for t in tables):
        print("Ingesting autonomous communities, provinces, and cities into the database...")
        insert_rows(base_df[['ccaa_id', 'ccaa_name']].drop_duplicates(), 'ccaas')
        insert_rows(base_df[['province_id', 'province_name', 'ccaa_id']].drop_duplicates(), 'provinces')
        insert_rows(base_df[['city_id', 'city_name', 'province_id']], 'cities')
        print("Base structure ingested!")

# # Example usage
# if __name__ == "__main__":
#     ensure_base_db_structure()