import psycopg2
import requests
from contextlib import contextmanager
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 5432
    database: str = "nyc_crashes"
    user: str = ""
    password: str = ""

    def _conn_kwargs(self) -> dict:
        kwargs = {"host": self.host, "port": self.port, "dbname": self.database}
        if self.user:
            kwargs["user"] = self.user
        if self.password:
            kwargs["password"] = self.password
        return kwargs

    @contextmanager
    def get_cursor(self):
        conn = psycopg2.connect(**self._conn_kwargs())
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "snowfall_sum",
    "snow_depth_max",
    "windspeed_10m_max",
    "windgusts_10m_max",
    "weathercode",
]


class OpenMeteoResource(ConfigurableResource):
    base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    latitude: float = 40.7128
    longitude: float = -74.0060
    timezone: str = "America/New_York"

    def fetch(self, start_date: str, end_date: str) -> dict:
        response = requests.get(
            self.base_url,
            params={
                "latitude": self.latitude,
                "longitude": self.longitude,
                "start_date": start_date,
                "end_date": end_date,
                "daily": ",".join(DAILY_VARIABLES),
                "timezone": self.timezone,
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["daily"]
