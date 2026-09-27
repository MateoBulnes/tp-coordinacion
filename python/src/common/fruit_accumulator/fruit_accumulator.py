from ..fruit_item import FruitItem


def _as_records(items):
    return [[item.fruit, item.amount] for item in items]


class FruitAccumulator:
    """Acumula pares (fruta, cantidad) y arma tops."""

    def __init__(self):
        self._items_by_fruit = {}

    def add(self, records):
        for [fruit, amount] in records:
            item = FruitItem(fruit, amount)
            current = self._items_by_fruit.get(fruit)
            self._items_by_fruit[fruit] = item if current is None else current + item

    def items(self):
        return _as_records(self._items_by_fruit.values())

    def top(self, size):
        ordered = sorted(self._items_by_fruit.values())
        ordered.reverse()
        return _as_records(ordered[:size])
