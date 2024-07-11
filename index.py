from sql_insight.core.observer import ExpressionWalker
from sql_insight.observers.PartitionObserver import PartitionObserver
from sqlglot import parse_one


partition = PartitionObserver()
expression = parse_one(
    """
            with abc as (
                select * from alpha a, gamma g, (
                    select * from delta
                ) d
                inner join beta b
                on a.id = b.id
                where a.id between 10 and 20
            ),
            efg as (
                select * from beta
                where id = 2
            )
            select a, b from abc, efg
            where a = 5 and (b > 5 or c in (1, 2, 3)) 
        """,
    dialect="trino",
)

walker = ExpressionWalker(expression, partition)
walker.walk()

[
    print([y.name for y in partition.get_column_expression_tables(x)], x)
    for x in partition.where_columns
]
