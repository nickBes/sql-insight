from sql_insight.core.observer import ExpressionObserver
from sql_insight.core.utils import (
    get_expressions_by_recursive_path,
    get_parent,
    get_expressions_by_path,
)
from sqlglot.expressions import (
    Where,
    Column,
    Condition,
    Join,
    Table,
    Select,
    From,
    With,
)


class PartitionObserver(ExpressionObserver):
    where_columns: list[Column] = []
    join_columns: list[Column] = []

    @ExpressionObserver.register(Where)
    def get_where_columns(self, where_expression: Where):
        self.where_columns += get_expressions_by_recursive_path(
            where_expression, (Condition, Column)
        )

    @ExpressionObserver.register(Join)
    def get_join_columns(self, join_expression: Join):
        self.join_columns += get_expressions_by_recursive_path(
            join_expression, (Condition, Column)
        )

    def get_column_expression_tables(self, column_expression: Column) -> list[Table]:
        related_query = get_parent(column_expression, Select)

        if not related_query:
            return []

        tables: list[Table] = [
            *get_expressions_by_path(related_query, (From, Table)),
            *get_expressions_by_path(related_query, (Join, Table)),
            *get_expressions_by_path(related_query, (With, Table)),
        ]

        if column_expression.table != "":
            for table in tables:
                if (
                    table.alias == column_expression.table
                    or table.name == column_expression.table
                ):
                    return [table]

        return tables
