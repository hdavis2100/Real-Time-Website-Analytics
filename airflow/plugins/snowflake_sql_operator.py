from __future__ import annotations

"""Minimal wrapper for running one or more Snowflake statements in a task."""

from collections.abc import Sequence

from airflow.sdk import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeSqlOperator(BaseOperator):
    template_fields: Sequence[str] = ("sql",)
    ui_color = "#D9F0FF"
    custom_operator_name = "Snowflake SQL"

    def __init__(
        self,
        *,
        sql: str | list[str],
        snowflake_conn_id: str = "snowflake_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.snowflake_conn_id = snowflake_conn_id

    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        statements = self.sql if isinstance(self.sql, list) else [self.sql]
        # Strip empty statements so large templated SQL lists can stay readable
        # without tripping over blank lines.
        rendered_statements = [stmt.strip() for stmt in statements if stmt and stmt.strip()]

        self.log.info(
            "Running %s Snowflake statement(s) with connection %s",
            len(rendered_statements),
            self.snowflake_conn_id,
        )

        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                for idx, statement in enumerate(rendered_statements, start=1):
                    self.log.info(
                        "Executing Snowflake statement %s/%s",
                        idx,
                        len(rendered_statements),
                    )
                    cursor.execute(statement)
                    self.log.info("Snowflake query id: %s", cursor.sfqid)
            conn.commit()
        finally:
            conn.close()

        return {"statements_executed": len(rendered_statements)}
