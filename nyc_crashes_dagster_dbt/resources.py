import psycopg2
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
