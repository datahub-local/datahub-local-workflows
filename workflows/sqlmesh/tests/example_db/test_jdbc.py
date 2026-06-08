"""Unit tests for the write_jdbc helper — no Spark session required."""
import os
from unittest.mock import MagicMock

import pytest


def _make_df_mock():
    df = MagicMock()
    df.write.format.return_value.options.return_value.mode.return_value.save = MagicMock()
    return df


def _jdbc_options(df_mock) -> dict:
    return df_mock.write.format.return_value.options.call_args[1]


class TestWriteJdbc:
    def test_h2_driver_skips_prepare_query(self, monkeypatch):
        monkeypatch.setenv("EXAMPLE_DB_URL", "jdbc:h2:mem:testdb")
        monkeypatch.setenv("EXAMPLE_DB_USER", "sa")
        monkeypatch.setenv("EXAMPLE_DB_PASSWORD", "")
        monkeypatch.delenv("EXAMPLE_DB_DRIVER", raising=False)

        from models.jdbc import write_jdbc

        df = _make_df_mock()
        write_jdbc(df, table="my_table")

        opts = _jdbc_options(df)
        assert "prepareQuery" not in opts
        assert opts["url"] == "jdbc:h2:mem:testdb"
        assert opts["user"] == "sa"
        assert opts["dbtable"] == "public.my_table"

    def test_postgresql_driver_includes_prepare_query(self, monkeypatch):
        monkeypatch.setenv("EXAMPLE_DB_URL", "jdbc:postgresql://localhost:5432/mydb")
        monkeypatch.setenv("EXAMPLE_DB_USER", "pguser")
        monkeypatch.setenv("EXAMPLE_DB_PASSWORD", "secret")
        monkeypatch.setenv("EXAMPLE_DB_DRIVER", "org.postgresql.Driver")

        from models.jdbc import write_jdbc

        df = _make_df_mock()
        write_jdbc(df, table="my_table")

        opts = _jdbc_options(df)
        assert "prepareQuery" in opts
        assert "CREATE SCHEMA IF NOT EXISTS public" in opts["prepareQuery"]

    def test_custom_schema_in_dbtable(self, monkeypatch):
        monkeypatch.setenv("EXAMPLE_DB_URL", "jdbc:h2:mem:testdb")
        monkeypatch.setenv("EXAMPLE_DB_USER", "sa")
        monkeypatch.setenv("EXAMPLE_DB_PASSWORD", "")
        monkeypatch.setenv("EXAMPLE_DB_SCHEMA", "myschema")
        monkeypatch.delenv("EXAMPLE_DB_DRIVER", raising=False)

        from models.jdbc import write_jdbc

        df = _make_df_mock()
        write_jdbc(df, table="my_table")

        opts = _jdbc_options(df)
        assert opts["dbtable"] == "myschema.my_table"

    def test_missing_url_raises(self, monkeypatch):
        monkeypatch.delenv("EXAMPLE_DB_URL", raising=False)
        monkeypatch.setenv("EXAMPLE_DB_USER", "sa")

        from models.jdbc import write_jdbc

        df = _make_df_mock()
        with pytest.raises(KeyError):
            write_jdbc(df, table="my_table")

    def test_missing_user_raises(self, monkeypatch):
        monkeypatch.setenv("EXAMPLE_DB_URL", "jdbc:h2:mem:testdb")
        monkeypatch.delenv("EXAMPLE_DB_USER", raising=False)

        from models.jdbc import write_jdbc

        df = _make_df_mock()
        with pytest.raises(KeyError):
            write_jdbc(df, table="my_table")
