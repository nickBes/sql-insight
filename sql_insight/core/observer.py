from typing import TypeVar, Callable, TypeAlias
from sqlglot.expressions import Expression

E = TypeVar("E", bound=Expression)
SelfExpressionObserver = TypeVar("SelfExpressionObserver", bound="ExpressionObserver")
ObserverMethod: TypeAlias = Callable[[SelfExpressionObserver, E], None]


class ExpressionObserver:
    on_exit_method_names: dict[str, str] = dict()
    on_enter_method_names: dict[str, str] = dict()

    @classmethod
    def on_exit(cls, expression_kind: type[Expression]):
        def register_on_exit_method(
            observer_method: ObserverMethod[SelfExpressionObserver, E],
        ) -> ObserverMethod[SelfExpressionObserver, E]:
            cls.on_exit_method_names[expression_kind.__name__] = (
                observer_method.__name__
            )

            return observer_method

        return register_on_exit_method

    @classmethod
    def on_enter(cls, expression_kind: type[Expression]):
        def register_on_enter_method(
            observer_method: ObserverMethod[SelfExpressionObserver, E],
        ) -> ObserverMethod[SelfExpressionObserver, E]:
            cls.on_enter_method_names[expression_kind.__name__] = (
                observer_method.__name__
            )

            return observer_method

        return register_on_enter_method


class ExpressionWalker:
    def __init__(self, expression: Expression, *observers: ExpressionObserver) -> None:
        self.expression = expression
        self.observers = observers

    def __walk_recursive(self, expression: Expression):
        on_enter_methods = []
        on_exit_methods = []

        for observer in self.observers:
            on_enter_method_name = observer.on_enter_method_names.get(
                expression.__class__.__name__
            )

            on_exit_method_name = observer.on_exit_method_names.get(
                expression.__class__.__name__
            )

            if on_enter_method_name:
                on_enter_methods.append(observer.__getattribute__(on_enter_method_name))

            if on_exit_method_name:
                on_exit_methods.append(observer.__getattribute__(on_exit_method_name))

        for on_enter_method in on_enter_methods:
            on_enter_method(expression)

        for child in expression.iter_expressions():
            self.__walk_recursive(child)

        for on_exit_method in on_exit_methods:
            on_exit_method(expression)

    def walk(self):
        self.__walk_recursive(self.expression)
