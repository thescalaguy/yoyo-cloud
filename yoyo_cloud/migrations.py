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
