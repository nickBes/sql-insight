from typing import TypeVar, Callable, TypeAlias
from sqlglot.expressions import Expression

E = TypeVar("E", bound=Expression)
SelfExpressionObserver = TypeVar("SelfExpressionObserver", bound="ExpressionObserver")
ObserverMethod: TypeAlias = Callable[[SelfExpressionObserver, E], None]


class ExpressionObserver:
    observer_method_names: dict[str, str] = dict()

    @classmethod
    def observe(cls, expression_kind: type[Expression]):
        def register_observer_method(
            observer_method: ObserverMethod[SelfExpressionObserver, E],
        ) -> ObserverMethod[SelfExpressionObserver, E]:
            cls.observer_method_names[expression_kind.__name__] = (
                observer_method.__name__
            )

            return observer_method

        return register_observer_method


class ExpressionWalker:
    def __init__(self, expression: Expression, *observers: ExpressionObserver) -> None:
        self.expression = expression
        self.observers = observers

    def walk(self):
        for expression in self.expression.bfs():
            for observer in self.observers:
                observer_method_name = observer.observer_method_names.get(
                    expression.__class__.__name__
                )

                if observer_method_name:
                    observer_method = observer.__getattribute__(observer_method_name)

                    if observer_method:
                        observer_method(expression)
