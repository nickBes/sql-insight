from sqlglot.expressions import Column, Table, Select, From, Join, With, CTE, Subquery
from sql_insight.core.utils import get_expressions_by_path, get_parent


def get_column_expression_tables(
    column_expression: Column,
) -> list[Table]:
    related_query = get_parent(column_expression, Select)

    if not related_query:
        return []

    simple_tables = [
        *get_expressions_by_path(related_query, (From, Table)),
        *get_expressions_by_path(related_query, (Join, Table)),
    ]

    subquery_tables = [
        *get_expressions_by_path(related_query, (From, Subquery)),
        *get_expressions_by_path(related_query, (Join, Subquery)),
    ]

    cte_tables = get_expressions_by_path(related_query, (With, CTE))

    filtered_simple_tables: list[Table] = []

    for table in simple_tables:
        is_cte = False

        for cte in cte_tables:
            if cte.alias != table.name:
                is_cte = True

        if not is_cte:
            filtered_simple_tables.append(table)

    if column_expression.table != "":
        for table in filtered_simple_tables:
            if (
                table.alias == column_expression.table
                or table.name == column_expression.table
            ):
                return [table]

        for subquery in subquery_tables:
            if subquery.alias == column_expression.table:
                return list(subquery.find_all(Table))

        for cte in cte_tables:
            if cte.alias == column_expression.table:
                return list(cte.find_all(Table))

    all_tables = [*filtered_simple_tables]

    for subquery in subquery_tables:
        all_tables += subquery.find_all(Table)

    for cte in cte_tables:
        all_tables += cte.find_all(Table)

    return all_tables
