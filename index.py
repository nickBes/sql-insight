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
print(repr(expression))
[print(partition) for partition in partition.partition_candidates]

"""
    Question the observer strategy, as you can simply loop over the expressions you're looking for

    Pro:
        you can use multiple observers for single query, however not sure how necessary it is....
"""
