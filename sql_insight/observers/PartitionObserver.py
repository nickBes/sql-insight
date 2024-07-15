from dataclasses import dataclass
from sql_insight.core.observer import ExpressionObserver
from sql_insight.observers.utils import get_column_expression_tables
from sql_insight.core.utils import get_expressions_by_recursive_path
from sqlglot.expressions import Where, Column, Condition, Join, Table


@dataclass
class PartitionCandidate:
    column: str
    possible_tables: list[str]


class PartitionObserver(ExpressionObserver):
    partition_candidates: list[PartitionCandidate] = []

    @ExpressionObserver.register(Where)
    def get_where_columns(self, where_expression: Where):
        for column in get_expressions_by_recursive_path(
            where_expression, (Condition, Column)
        ):
            self.partition_candidates.append(
                PartitionCandidate(
                    column.name,
                    [table.name for table in get_column_expression_tables(column)],
                )
            )

    @ExpressionObserver.register(Join)
    def get_join_columns(self, join_expression: Join):
        for column in get_expressions_by_recursive_path(
            join_expression, (Condition, Column)
        ):
            self.partition_candidates.append(
                PartitionCandidate(
                    column.name,
                    [table.name for table in get_column_expression_tables(column)],
                )
            )
