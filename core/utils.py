from sqlglot import Expression
from typing import TypeVar, TypeAlias
from collections import deque

E = TypeVar("E", bound=Expression)
ExpressionPath: TypeAlias = tuple[*tuple[type[Expression], ...], type[E]]


def get_expressions_by_path(start: Expression, path: ExpressionPath[E]) -> list[E]:
    queue: deque[Expression] = deque([start])

    for expression_type in path:
        if not queue:
            return []

        matching_expression_queue: deque[Expression] = deque()

        while queue:
            current_expression = queue.popleft()

            for child_expression in current_expression.iter_expressions():
                if isinstance(child_expression, expression_type):
                    matching_expression_queue.append(child_expression)

        queue = matching_expression_queue

    # if reached the end the queue should be of type list[RC]
    return queue  # type: ignore


def get_first_expression_by_path(
    start: Expression, path: ExpressionPath[E]
) -> E | None:
    expressions = get_expressions_by_path(start, path)

    if expressions:
        return expressions[0]
    else:
        return None
