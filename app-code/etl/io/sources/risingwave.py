from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Iterator
from etl.io.base import DataSource, DataReader

@dataclass
class RisingWaveSource(DataSource):
    """
    Configuration for reading from RisingWave (Postgres-protocol).
    """
    host: str
    user: str
    password: str
    database: str
    query: str
    port: int = 4566
    
    def open(self) -> 'RisingWaveReader':
        return RisingWaveReader(self)


class RisingWaveReader(DataReader):
    """
    Runtime reader for RisingWave using Psycopg2.
    """
    REQUIRED_DEPENDENCIES = ["psycopg2"]

    def __init__(self, source: RisingWaveSource):
        self.source = source
        self._conn = None
        self._cursor = None

    def _connect(self):
        if self._conn:
            return
            
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
        except ImportError:
            raise ImportError("RisingWaveReader requires 'psycopg2' or 'psycopg2-binary'. Please install it.")

        self._conn = psycopg2.connect(
            host=self.source.host,
            port=self.source.port,
            user=self.source.user,
            password=self.source.password,
            dbname=self.source.database
        )
        self._cursor = self._conn.cursor(cursor_factory=RealDictCursor)

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        self._cursor = None
        self._conn = None

    def read_batch(self, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """
        Yields batches of results from the query.
        """
        self._connect()
        
        try:
            self._cursor.execute(self.source.query)
            
            while True:
                rows = self._cursor.fetchmany(batch_size)
                if not rows:
                    break
                # rows is a list of RealDictRow, convert to pure dict if needed or keep as is
                yield [dict(r) for r in rows]
                
        except Exception as e:
            print(f"[RisingWaveReader] Error executing query: {e}")
            raise
