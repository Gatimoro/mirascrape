from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SUPABASE_URL: str = ""
    SUPABASE_KEY: str = ""

    REQUEST_DELAY_MIN: float = 4.0
    REQUEST_DELAY_MAX: float = 6.0
    MAX_RETRIES: int = 3

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
