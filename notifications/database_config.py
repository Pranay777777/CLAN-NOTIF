import os
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

load_dotenv()

class PostgresConfig:
    """Manages dynamic configurations for Account 14 from PostgreSQL."""

    def __init__(self, account_id: int = 14):
        self.account_id = account_id
        self.conn_params = {
            "host": os.getenv("PG_HOST"),
            "port": int(os.getenv("PG_PORT", 5432)),
            "dbname": os.getenv("PG_DATABASE"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "connect_timeout": 10,
        }
        self._engine: Engine | None = None

    def _get_engine(self) -> Engine:
        if self._engine is not None:
            return self._engine

        url = URL.create(
            drivername="postgresql+psycopg2",
            username=self.conn_params["user"],
            password=self.conn_params["password"],
            host=self.conn_params["host"],
            port=self.conn_params["port"],
            database=self.conn_params["dbname"],
        )
        self._engine = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={"connect_timeout": self.conn_params["connect_timeout"]},
        )
        return self._engine

    @staticmethod
    def _to_code(name: str) -> str:
        return str(name or "").lower().replace(" ", "_").strip()

    def get_active_kii_rows(self) -> List[Dict[str, Any]]:
        """Fetches full active KII rows used for indicator classification."""
        stmt = text(
            """
            SELECT
                id,
                account_id,
                kii_name,
                status,
                created_at,
                modified_at,
                role_id,
                default_target,
                rank,
                daily_target,
                star_kii,
                color_value
            FROM public.kii_master
            WHERE account_id = :account_id
              AND status = 1
            ORDER BY id
            """
        )

        with self._get_engine().connect() as conn:
            rows = conn.execute(stmt, {"account_id": self.account_id}).mappings().all()
        return [dict(row) for row in rows]

    def get_indicators(self) -> List[Dict[str, str]]:
        """Fetches active lead indicators (KIIs) for the account."""
        indicators = []
        for row in self.get_active_kii_rows():
            raw_name = str(row.get("kii_name") or "").strip()
            code_name = self._to_code(raw_name)
            if not code_name:
                continue
            indicators.append(
                {
                    "id": row["id"],
                    "name": raw_name,
                    "code": code_name,
                }
            )
        return indicators

    def get_indicator_label_map(self) -> Dict[str, str]:
        """Returns a code->display_name map for active KIIs."""
        indicators = self.get_indicators()
        return {str(ind["code"]): str(ind["name"]) for ind in indicators if ind.get("code")}

    def get_activity_mapping(self) -> Dict[str, Tuple[str, ...]]:
        """Fetches KII->activity mappings from explicit DB relations only."""
        mapping = {}
        stmt = text(
            """
            SELECT km.kii_name, ma.activity_method
            FROM kii_master km
            JOIN kii_ldms_activity_relation lar ON km.id = lar.kii_id
            JOIN ldms_md_activities ma ON lar.ldms_activity_id = ma.id
            WHERE km.account_id = :account_id
              AND km.status = 1
              AND ma.status = 1
            """
        )
        with self._get_engine().connect() as conn:
            rows = conn.execute(stmt, {"account_id": self.account_id}).all()

        for kii_name, method in rows:
            code_name = self._to_code(str(kii_name))
            if code_name not in mapping:
                mapping[code_name] = []
            mapping[code_name].append(str(method))
        
        # Convert lists to tuples and unique strings
        return {k: tuple(sorted(set(v))) for k, v in mapping.items()}

if __name__ == "__main__":
    config = PostgresConfig()
    print("--- Dynamic Indicators ---")
    indicator_rows = config.get_active_kii_rows()
    for row in indicator_rows:
        print(row)
    
    print("\n--- Activity Mappings ---")
    mapping = config.get_activity_mapping()
    for code, methods in mapping.items():
        print(f"{code}: {methods}")
