from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="allow"
    )

    kafka_bootstrap_servers: str = ""
    kafka_sasl_mechanism: str = ""
    kafka_producer_username: str = ""
    kafka_producer_password: str = ""
    kafka_security_protocol: str = ""

    kafka_consumer_username: str = ""
    kafka_consumer_password: str = ""

    kafka_client_username: str = ""
    kafka_client_password: str = ""

    es_host: str = ""
    es_username: str = ""
    es_password: str = ""


settings = Settings()
