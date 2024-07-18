from sql_insight.core.observer import ExpressionWalker
from sql_insight.core.utils import get_expressions_by_path
from sql_insight.observers.PartitionObserver import PartitionObserver
from sql_insight.observers.ObjectTrackingObserver import ObjectTrackingObserver
from sqlglot import parse_one
from sqlglot.expressions import Query, From, Table, Join, Subquery, With, CTE


partition = PartitionObserver()
object_tracker = ObjectTrackingObserver()
expression = parse_one(
    """
            with boses as (
                select id, name from (
                    select * from employees e
                    inner join employees b
                    on b.id = e.bossId
                ) where salary > 50000
            )
            select name, age from boses b
            inner join people p
            on b.id = p.id
            where age < 28 and name = 'yossi'
    """,
    dialect="trino",
)

walker = ExpressionWalker(expression, object_tracker, partition)
walker.walk()

for column in partition.columns:
    query_expression = column.find_ancestor(Query)

    if query_expression:
        simple_tables = [
            *get_expressions_by_path(query_expression, (From, Table)),
            *get_expressions_by_path(query_expression, (Join, Table)),
        ]

        subquery_tables = [
            *get_expressions_by_path(query_expression, (From, Subquery)),
            *get_expressions_by_path(query_expression, (Join, Subquery)),
        ]

        cte_tables = get_expressions_by_path(query_expression, (With, CTE))

        filtered_simple_tables = []

        for table in simple_tables:
            is_cte = False

            for cte in cte_tables:
                if cte.alias == table.name:
                    is_cte = True

            if not is_cte:
                filtered_simple_tables.append(table)

        print(
            column.name,
            object_tracker.get_all_column_deps(
                column.name, [*filtered_simple_tables, *cte_tables, *subquery_tables]
            ),
        )


"""
    Question the observer strategy, as you can simply loop over the expressions you're looking for

    Pro:
        you can use multiple observers for single query, however not sure how necessary it is....
"""
