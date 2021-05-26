from typing import Union

Value = Union[int, str, float]

ColValue = tuple[str, Value]

UpdateList = tuple[str, Value, Value]

ColValueUpdateList = Union[ColValue, UpdateList]
