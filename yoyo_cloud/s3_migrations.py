# Copyright 2024 Fasih Khatib
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import types
from functools import cached_property
from itertools import chain, zip_longest
from typing import Type

import s3fs
from yoyo.exceptions import BadMigration
from yoyo.migrations import (
    Migration,
    StepCollector,
    MigrationList,
)

from yoyo_cloud.migrations import CloudMigration


class S3Migration(CloudMigration):
    """Represents a migration to run from S3."""

    def load(self):
        assert self.is_raw_sql(), f"{self.path} does not point to a valid SQL file."

        if self.loaded:
            return

        collector = StepCollector(migration=self)
        self.source = self._source
        self.module = types.ModuleType(self.path)

        self.module.step = collector.add_step  # type: ignore
        self.module.group = collector.add_step_group  # type: ignore
        self.module.transaction = collector.add_step_group  # type: ignore
        self.module.__yoyo_collector__ = collector  # type: ignore

        # -- Parse the contents of the SQL file
        directives, leading_comment, statements = self._parse_sql_migration(
            sql=self._source
        )

        # -- Parse the content of the rollback SQL file, if present
        _, _, rollback_statements = self._parse_sql_migration(
            sql=self.rollback_source,
        )

        statements_with_rollback = zip_longest(
            statements, rollback_statements, fillvalue=None
        )

        # -- Add steps to the collector
        for s, r in statements_with_rollback:
            collector.add_step(s, r)

        self.module.__doc__ = leading_comment

        setattr(
            self.module,
            "__transactional__",
            {"true": True, "false": False}[
                directives.get("transactional", "true").lower()
            ],
        )

        setattr(
            self.module,
            "__depends__",
            {d for d in directives.get("depends", "").split() if d},
        )

        depends = getattr(self.module, "__depends__", [])

        if isinstance(depends, (str, bytes)):
            depends = [depends]

        self._depends = {self.__all_migrations.get(id, None) for id in depends}

        self.use_transactions = getattr(self.module, "__transactional__", True)

        if None in self._depends:
            raise BadMigration("Could not resolve dependencies in {}".format(self.path))

        self.steps = collector.create_steps(self.use_transactions)

    @cached_property
    def _source(self) -> str:
        return self._read_file_from_s3(path=self.path)

    @cached_property
    def rollback_source(self) -> str | None:
        try:
            return self._read_file_from_s3(path=self.rollback_path)
        except FileNotFoundError:
            return None

    @cached_property
    def _s3(self) -> s3fs.S3FileSystem:
        key, secret = _aws_key_and_secret()
        return s3fs.S3FileSystem(key=key, secret=secret)

    def _read_file_from_s3(self, path: str):
        with self._s3.open(path=path, mode="r") as f:
            return str(f.read())


class S3PostApplyHookMigration(S3Migration): ...


def read_s3_migrations(paths: list[str]) -> MigrationList:
    """
    Reads migrations from directories in S3.
    :param paths: A list of paths pointing to directories in S3.
    :return: A list of migrations to apply.
    """
    key, secret = _aws_key_and_secret()
    s3 = s3fs.S3FileSystem(key=key, secret=secret)
    migrations: dict[str, MigrationList] = dict()

    for path in paths:
        # -- Remove any trailing slashes
        path = path[:-1] if path[-1] == "/" else path

        for file in s3.ls(path=path):

            # -- Ignore any file which does not end with ".sql" extension
            if not (file.endswith(".sql")):
                continue

            # -- Ignore any rollback file
            if file.endswith(".rollback.sql"):
                continue

            filename, extension = os.path.splitext(os.path.basename(file))
            migration_class: Type[Migration] = S3Migration

            if filename.startswith("post-apply"):
                migration_class = S3PostApplyHookMigration

            migration = migration_class(
                id=filename,
                path=f"{path}/{filename}{extension}",
                source_dir=path,
            )

            ml = migrations.setdefault(path, MigrationList())

            if migration_class is S3PostApplyHookMigration:
                ml.post_apply.append(migration)
            else:
                ml.append(migration)

    merged_migrations = MigrationList(
        chain(*migrations.values()),
        chain(*(m.post_apply for m in migrations.values())),
    )

    return merged_migrations


def _aws_key_and_secret() -> tuple[str | None, str | None]:
    return (
        os.environ.get("AWS_ACCESS_KEY_ID"),
        os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )
