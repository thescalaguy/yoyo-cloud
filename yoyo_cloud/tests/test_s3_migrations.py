from unittest import mock
from unittest.mock import PropertyMock

import pytest

from yoyo_cloud import read_s3_migrations


class TestS3Migrations:

    @mock.patch("s3fs.S3FileSystem.ls")
    def test_it_reads_migrations_from_s3(
        self,
        mock_ls,
        s3_files,
    ):
        mock_ls.return_value = s3_files
        migration_list = read_s3_migrations(paths=["s3://some-path/"])
        assert len(migration_list) == 1
        assert len(migration_list.post_apply) == 1

    @mock.patch(
        "yoyo_cloud.s3_migrations.S3Migration.rollback_source",
        new_callable=PropertyMock,
    )
    @mock.patch(
        "yoyo_cloud.s3_migrations.S3Migration._source",
        new_callable=PropertyMock,
    )
    @mock.patch("s3fs.S3FileSystem.ls")
    def test_it_applies_migrations_from_s3(
        self,
        mock_ls,
        mock_source,
        mock_rollback_source,
        s3_files,
        sqlite_backend,
        create_table_sql,
        table_name,
    ):
        # -- Setup test environment
        mock_rollback_source.return_value = None
        mock_ls.return_value = s3_files[:1]
        mock_source.return_value = create_table_sql

        # -- Read and apply migrations
        migration_list = read_s3_migrations(paths=["s3://some-path/"])
        sqlite_backend.apply_migrations(migration_list)

        # -- Verify migrations
        cursor = sqlite_backend.cursor()
        cursor.execute(f"SELECT count(1) FROM {table_name}")

        assert cursor.fetchone() == (0,)
        assert len(migration_list) == 1
        assert len(migration_list.post_apply) == 0
