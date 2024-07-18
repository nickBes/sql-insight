from sql_insight.core.observer import ExpressionObserver
from sqlglot.expressions import (
    Expression,
    CTE,
    Subquery,
    DerivedTable,
    From,
    Join,
    Table,
    Star,
    Query,
    Alias,
    Column,
)
from sql_insight.core.utils import get_expressions_by_path, get_first_expression_by_path


class ObjectTrackingObserver(ExpressionObserver):
    """
    Initializes tracking of query objects (CTEs, Subuqeries and tables)
    via the next format:

    {
        "expressionId": {
            "columnA": ["expressionId", "expressionId2"],
        }
    }
    """

    def __init__(self):
        self.object_map: dict[Expression, dict[str, list[Expression]]] = dict()

    def __str__(self) -> str:
        result = "{"

        for expression, columns in self.object_map.items():
            result += f"\n\t{expression.sql()}: " + "{"

            for column, expressions in columns.items():
                result += (
                    f"\n\t\t{column}: [{', '.join([exp.sql() for exp in expressions])}]"
                )

            result += "\n\t}"

        return result + "\n}"

    @ExpressionObserver.on_exit(CTE)
    def track_ctes(self, cte_expression: CTE):
        self.track_derived_table(cte_expression)

    @ExpressionObserver.on_exit(Subquery)
    def track_subqueries(self, subquery_expression: Subquery):
        self.track_derived_table(subquery_expression)

    def track_derived_table(self, derived_table_expression: DerivedTable):
        query_expression = derived_table_expression.this

        if not isinstance(query_expression, Query):
            return

        simple_tables = [
            *get_expressions_by_path(query_expression, (From, Table)),
            *get_expressions_by_path(query_expression, (Join, Table)),
        ]

        subquery_tables = [
            *get_expressions_by_path(query_expression, (From, Subquery)),
            *get_expressions_by_path(query_expression, (Join, Subquery)),
        ]

        column_map: dict[str, list[Expression]] = dict()

        for child in query_expression.iter_expressions():
            if isinstance(child, Star):
                column_map["*"] = [*simple_tables, *subquery_tables]
                break
            else:
                column_name = child.output_name

                if not column_name:
                    continue

                table_name: str | None = None

                if isinstance(child, Column):
                    table_name = child.table
                elif isinstance(child, Alias):
                    if isinstance(child.this, Column):
                        table_name = child.this.table
                    elif isinstance(child.this, Subquery):
                        column_map[column_name] = [child.this]
                        continue

                if not table_name:
                    column_map[column_name] = [*simple_tables, *subquery_tables]
                else:
                    if column_name not in column_map:
                        column_map[column_name] = []

                    for table in simple_tables:
                        if table_name == table.name or table_name == table.alias:
                            column_map[column_name].append(table)
                            break

                    for table in subquery_tables:
                        if table_name == table.name or table_name == table.alias:
                            column_map[column_name].append(table)
                            break

        self.object_map[derived_table_expression] = column_map
