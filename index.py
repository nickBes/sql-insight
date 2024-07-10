from core.observer import ExpressionObserver, ExpressionWalker
from sqlglot.expressions import Column, Table
from sqlglot import parse_one


class LineageObserver(ExpressionObserver):
    @ExpressionObserver.register(Column)
    def observe_column_exp(self, column_exp: Column):
        print(column_exp)

    @ExpressionObserver.register(Table)
    def observe_table_exp(self, table_exp: Table):
        print(table_exp)


lineage = LineageObserver()
expression = parse_one(
    """
            with abc as (
                select * from alpha a, gamma g, (
                    select * from delta
                ) d
                inner join beta b
                on a.id = b.id
            ),
            efg as (
                select * from beta
            )
            select * from abc, efg
        """,
    dialect="trino",
)
print(repr(expression))
walker = ExpressionWalker(expression, lineage)
walker.walk()
