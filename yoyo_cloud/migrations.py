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


import abc
import os
from functools import cached_property

import sqlparse
from yoyo.migrations import (
    Migration,
    DirectivesType,
    parse_metadata_from_sql_comments,
)


class CloudMigration(abc.ABC, Migration):

    def _parse_sql_migration(self, sql: str | None) -> tuple[
        DirectivesType,
        str,
        list[str],
    ]:
        if not sql:
            return {}, "", []

        directives: DirectivesType = {}
        leading_comment = ""
        statements = sqlparse.split(sql=sql)
        if statements:
            (
                directives,
                leading_comment,
                sql,
            ) = parse_metadata_from_sql_comments(statements[0])
            statements[0] = sql
        statements = [s for s in statements if s.strip()]
        return directives, leading_comment, statements

    @property
    def rollback_path(self) -> str:
        return os.path.splitext(self.path)[0] + ".rollback.sql"

    @abc.abstractmethod
    @cached_property
    def _source(self) -> str:
        raise NotImplemented

    @abc.abstractmethod
    @cached_property
    def rollback_source(self) -> str | None:
        raise NotImplemented
