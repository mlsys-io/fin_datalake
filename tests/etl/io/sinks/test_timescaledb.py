import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import pyarrow as pa

from etl.io.sinks.timescaledb import TimescaleDBSink, TimescaleDBWriter

class TestTimescaleDB(unittest.TestCase):
    
    def setUp(self):
        self.sink = TimescaleDBSink(
            host="localhost",
            user="admin",
            password="pwd",
            database="mydb",
            table_name="my_table"
        )
        self.writer = self.sink.open()

    @patch("etl.io.sinks.timescaledb.psycopg2")
    def test_connect(self, mock_pg):
        self.writer._connect()
        mock_pg.connect.assert_called_once_with(
            host="localhost", port=5432, user="admin", password="pwd", dbname="mydb"
        )

    @patch("etl.io.sinks.timescaledb.psycopg2")
    @patch("psycopg2.extras.execute_values")
    def test_write_dataframe(self, mock_execute_values, mock_pg):
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Test Data
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        
        # Act
        self.writer.write_batch(df)
        
        # Assert
        self.assertTrue(mock_execute_values.called)
        args, _ = mock_execute_values.call_args
        cursor_arg, sql_arg, data_arg = args
        
        self.assertEqual(cursor_arg, mock_cursor)
        self.assertIn("INSERT INTO public.my_table", sql_arg)
        self.assertIn('("id","val")', sql_arg)
        self.assertEqual(len(data_arg), 2)
        mock_conn.commit.assert_called_once()

    @patch("etl.io.sinks.timescaledb.psycopg2")
    @patch("psycopg2.extras.execute_values")
    def test_write_list_dicts(self, mock_execute_values, mock_pg):
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Test Data
        data = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        
        # Act
        self.writer.write_batch(data)
        
        # Assert
        self.assertTrue(mock_execute_values.called)
        # Check that it extracted columns correctly
        args, _ = mock_execute_values.call_args
        sql_arg = args[1]
        self.assertIn('("id","val")', sql_arg)

if __name__ == "__main__":
    unittest.main()
