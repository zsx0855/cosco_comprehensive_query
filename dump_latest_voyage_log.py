from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict
import psycopg2
from psycopg2.extras import RealDictCursor

from kingbase_config import get_kingbase_config


def fetch_latest_voyage(uuid: str) -> Dict[str, Any]:
    cfg = get_kingbase_config()
    try:
        connection = psycopg2.connect(**cfg)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = (
                "SELECT * FROM lng.voyage_risk_log WHERE uuid = %s ORDER BY request_time DESC LIMIT 1"
            )
            cursor.execute(sql, (uuid,))
            row = cursor.fetchone()
            return dict(row) if row else {}
    finally:
        if 'connection' in locals():
            connection.close()


def dump_to_file(uuid: str) -> str:
    rec = fetch_latest_voyage(uuid)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = f"voyage_log_{uuid}_{ts}.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(rec, f, ensure_ascii=False, indent=2)
    return out_path


def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: python dump_latest_voyage_log.py <uuid>")
        sys.exit(1)
    uuid = sys.argv[1]
    path = dump_to_file(uuid)
    print(f"Saved latest voyage risk log to: {path}")


if __name__ == "__main__":
    main()


