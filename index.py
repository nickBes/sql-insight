from core.observer import ExpressionObserver, ExpressionWalker
from sqlglot.expressions import Column, Table
from sqlglot import parse_one


class LineageObserver(ExpressionObserver):
    @ExpressionObserver.observe(Column)
    def observe_column_exp(self, column_exp: Column):
        print(column_exp)

    @ExpressionObserver.observe(Table)
    def observe_table_exp(self, table_exp: Table):
        print(table_exp)


lineage = LineageObserver()
expression = parse_one("select a , b, c from table, tabl2", dialect="trino")
walker = ExpressionWalker(expression, lineage)
walker.walk()
