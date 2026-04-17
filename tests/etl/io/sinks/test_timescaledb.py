import unittest
import sys
import types
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

    def test_connect(self):
        fake_psycopg2 = types.ModuleType("psycopg2")
        fake_psycopg2.connect = MagicMock(return_value=MagicMock(cursor=MagicMock(return_value=MagicMock())))
        fake_extras = types.ModuleType("psycopg2.extras")
        fake_extras.execute_values = MagicMock()

        with patch.dict(sys.modules, {"psycopg2": fake_psycopg2, "psycopg2.extras": fake_extras}):
            self.writer._connect()

        fake_psycopg2.connect.assert_called_once_with(
            host="localhost", port=5432, user="admin", password="pwd", dbname="mydb"
        )

    def test_write_dataframe(self):
        fake_psycopg2 = types.ModuleType("psycopg2")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        fake_psycopg2.connect = MagicMock(return_value=mock_conn)
        fake_extras = types.ModuleType("psycopg2.extras")
        fake_extras.execute_values = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Test Data
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})

        with patch.dict(sys.modules, {"psycopg2": fake_psycopg2, "psycopg2.extras": fake_extras}):
            self.writer.write_batch(df)

        # Assert
        self.assertTrue(fake_extras.execute_values.called)
        args, _ = fake_extras.execute_values.call_args
        cursor_arg, sql_arg, data_arg = args

        self.assertEqual(cursor_arg, mock_cursor)
        self.assertIn("INSERT INTO public.my_table", sql_arg)
        self.assertIn('("id","val")', sql_arg)
        self.assertEqual(len(data_arg), 2)
        mock_conn.commit.assert_called_once()

    def test_write_list_dicts(self):
        fake_psycopg2 = types.ModuleType("psycopg2")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        fake_psycopg2.connect = MagicMock(return_value=mock_conn)
        fake_extras = types.ModuleType("psycopg2.extras")
        fake_extras.execute_values = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Test Data
        data = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]

        with patch.dict(sys.modules, {"psycopg2": fake_psycopg2, "psycopg2.extras": fake_extras}):
            self.writer.write_batch(data)

        # Assert
        self.assertTrue(fake_extras.execute_values.called)
        # Check that it extracted columns correctly
        args, _ = fake_extras.execute_values.call_args
        sql_arg = args[1]
        self.assertIn('("id","val")', sql_arg)

if __name__ == "__main__":
    unittest.main()
