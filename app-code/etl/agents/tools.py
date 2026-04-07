from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from etl.io.sinks.timescaledb import TimescaleDBSink
from etl.io.sinks.milvus import MilvusSink

class TimescaleTool:
    """
    Allows an Agent to query the TimescaleDB Operational Store.
    Reuses the connection logic from TimescaleDBSink configuration.
    """
    def __init__(self, config: TimescaleDBSink):
        self.config = config
        self._conn = None
        self._cursor = None

    def _connect(self):
        if self._conn:
            return
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        self._conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            dbname=self.config.database
        )
        self._cursor = self._conn.cursor(cursor_factory=RealDictCursor)

    def run(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a read-only query. 
        In a real prod system, this should have safeguards against injection/DROP.
        """
        self._connect()
        try:
            self._cursor.execute(query)
            # Commit not needed for Select, but good practice to reset state
            self._conn.commit() 
            results = self._cursor.fetchall()
            return [dict(row) for row in results]
        except Exception as e:
            if self._conn:
                self._conn.rollback()
            raise RuntimeError(f"Timescale Query Error: {e}")

class MilvusTool:
    """
    Allows an Agent to search the Milvus Vector DB (RAG).
    Reuses MilvusSink configuration.
    """
    def __init__(self, config: MilvusSink, embedding_func=None):
        self.config = config
        self.embedding_func = embedding_func # Function to convert text -> vector
        self._collection = None

    def _connect(self):
        if self._collection:
            return
            
        from pymilvus import connections, Collection
        
        conn_args = {"uri": self.config.uri}
        if self.config.token: conn_args["token"] = self.config.token
        
        connections.connect(alias="default", **conn_args)
        self._collection = Collection(self.config.collection_name)
        self._collection.load() # Load into memory for search

    def search(self, query: str, top_k: int = 5, output_fields: List[str] = None):
        """
        Semantic Search.
        req: self.embedding_func must be set to convert query str -> vector.
        """
        if not self.embedding_func:
            raise ValueError("MilvusTool requires an `embedding_func` to search by text.")
            
        self._connect()
        
        vector = self.embedding_func(query)
        
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        results = self._collection.search(
            data=[vector], 
            anns_field="vector", # Assumption: vector field name
            param=search_params, 
            limit=top_k, 
            output_fields=output_fields or ["content"]
        )
        
        # Normalize result
        hits = []
        for hit in results[0]:
            hits.append({
                "id": hit.id,
                "distance": hit.distance,
                "entity": {k: hit.entity.get(k) for k in output_fields or ["content"]}
            })
        return hits
