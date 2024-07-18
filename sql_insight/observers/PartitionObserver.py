from dataclasses import dataclass
from sql_insight.core.observer import ExpressionObserver
from sql_insight.observers.utils import get_column_expression_tables
from sql_insight.core.utils import get_expressions_by_recursive_path
from sqlglot.expressions import Where, Column, Condition, Join


@dataclass
class PartitionCandidate:
    column: str
    possible_tables: list[str]


class PartitionObserver(ExpressionObserver):
    columns: list[Column] = []
    partition_candidates: list[PartitionCandidate] = []

    @ExpressionObserver.on_exit(Where)
    def get_where_columns(self, where_expression: Where):
        self.columns += get_expressions_by_recursive_path(
            where_expression, (Condition, Column)
        )

    @ExpressionObserver.on_exit(Join)
    def get_join_columns(self, join_expression: Join):
        self.columns += get_expressions_by_recursive_path(
            join_expression, (Condition, Column)
        )
