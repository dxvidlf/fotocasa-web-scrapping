from functools import lru_cache
import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv 

APP_ROOT = os.path.dirname(os.path.dirname(__file__))
ENVIRONMENTS = {
    'development': '.env',
}

@lru_cache
def get_env_filename():
    return os.path.join(APP_ROOT, ENVIRONMENTS.get('development'))

load_dotenv(dotenv_path=get_env_filename(), override=True)

class EnvironmentSettings(BaseSettings):
    DB_NAME: str
    DB_IP: str
    DB_PORT: int
    DB_USERNAME: str
    DB_PASSWORD: str

    model_config = SettingsConfigDict()

@lru_cache
def get_environment_variables():
    return EnvironmentSettings()
