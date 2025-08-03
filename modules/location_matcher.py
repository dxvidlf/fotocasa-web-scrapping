import os
import re
import unicodedata
import pandas as pd
from thefuzz import process

class LocationMatcher:
    def __init__(self, reference_csv_path: str = None, threshold: int = 60):
        if reference_csv_path is None:
            reference_csv_path = os.path.join('assets', 'ccaa_province_city.csv')
        self.df_reference = pd.read_csv(reference_csv_path)
        self.threshold = threshold
        self._cache = {}

    def _normalize(self, text: str) -> str:
        text = unicodedata.normalize('NFD', str(text).lower())
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        return ''.join(sorted(re.split(r'[\s\-\/]+', text)))

    def _best_match(self, target: str, candidates: list[str]) -> tuple:
        norm_target = self._normalize(target)
        norm_candidates = {self._normalize(c): c for c in candidates}
        
        if not norm_candidates:
            return None, 0
            
        result = process.extractOne(norm_target, list(norm_candidates.keys()))
        if not result:
            return None, 0
            
        match, score = result 
        return (norm_candidates[match], score) if score >= self.threshold else (None, 0)

    def _get_unique_id(self, column: str, value: str, filter_cols: dict = None) -> int:
        df = self.df_reference
        if filter_cols:
            for col, val in filter_cols.items():
                df = df[df[col] == val]
        ids = df[df[column] == value][f"{column.split('_')[0]}_id"].unique()
        return int(ids[0]) if ids.size > 0 else None

    def match_location(self, ccaa: str = None, province: str = None, city: str = None) -> dict:
        if not ccaa:
            return {'guess': None, 'guess_id': None, 'score': 0}

        # Match CCAA
        ccaa_guess, score = self._best_match(ccaa, self.df_reference['ccaa_name'].dropna().unique())
        if not ccaa_guess:
            return {'guess': None, 'guess_id': None, 'score': 0}
        
        if not province:
            ccaa_id = self._get_unique_id('ccaa_name', ccaa_guess)
            return {'guess': ccaa_guess, 'guess_id': ccaa_id, 'score': score}

        # Match Province
        province_candidates = self.df_reference[self.df_reference['ccaa_name'] == ccaa_guess]['province_name'].dropna().unique()
        province_guess, score = self._best_match(province, province_candidates)
        if not province_guess:
            return {'guess': None, 'guess_id': None, 'score': 0}
        
        if not city:
            province_id = self._get_unique_id('province_name', province_guess, {'ccaa_name': ccaa_guess})
            return {'guess': province_guess, 'guess_id': province_id, 'score': score}

        # Match City
        city_candidates = self.df_reference[
            (self.df_reference['ccaa_name'] == ccaa_guess) & 
            (self.df_reference['province_name'] == province_guess)
        ]['city_name'].dropna().unique()
        
        city_guess, score = self._best_match(city, city_candidates)
        if not city_guess:
            return {'guess': None, 'guess_id': None, 'score': 0}

        city_id = self._get_unique_id('city_name', city_guess, {
            'ccaa_name': ccaa_guess,
            'province_name': province_guess
        })
        return {'guess': city_guess, 'guess_id': city_id, 'score': score}

# if __name__ == "__main__":
#     matcher = LocationMatcher()
    
#     input_path = os.path.join(os.getcwd(),'old', 'fotocasa_data_Araba_Álava.csv')
#     df = pd.read_csv(input_path, usecols=['ccaa', 'province', 'municipality'])
    
#     results = df.apply(
#         lambda row: pd.Series(
#             matcher.match_location(
#                 ccaa=row['ccaa'],
#                 province=row['province'], 
#                 city=row['municipality']
#             )
#         ), 
#         axis=1
#     )
    
#     df = pd.concat([df, results], axis=1)
#     df.to_csv('estimations.csv', index=False)