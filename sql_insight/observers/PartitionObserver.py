from sql_insight.core.observer import ExpressionObserver
from sql_insight.core.utils import get_expressions_by_recursive_path
from sqlglot.expressions import Where, Column, Condition, Join


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
