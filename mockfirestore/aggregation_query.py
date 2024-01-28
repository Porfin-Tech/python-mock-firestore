from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mockfirestore.query import Query


class AggregationValue:
    value: Any

    def __init__(self, value: Any):
        self.value = value


class AggregationQuery:
    def __init__(self, query: "Query"):
        self.query = query

    async def get(self) -> int:
        return [[AggregationValue(len([_ async for _ in self.query.stream()]))]]
