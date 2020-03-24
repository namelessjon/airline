import datetime as dt
from collections import defaultdict
from contextlib import contextmanager
import json
import time
from typing import (
    Dict,
    Any,
    Union,
    Optional,
    DefaultDict
)

from .format_exception import format_exception

Numeric = Union[int, float]
TIMER_PREFIX = 'timers.'
ROLLUP_PREFIX = 'rollup.'


class Event:
    def __init__(self, data: Dict[str, Any] = {}, created_at=dt.datetime.utcnow(), client=None):
        self._data: Dict[str, Any] = {}
        self._client = client

        if client:
            self.dataset = client.dataset
        else:
            self.dataset = ''
        self.created_at = created_at
        self.add(data=data)
        self._rollup_fields: DefaultDict[str, Numeric] = defaultdict(int)
        self._timer_fields: DefaultDict[str, float] = defaultdict(float)

    def add(self, data: Dict[str, Any]):
        self._data.update(data)

    def add_field(self, name: str, value: Any):
        self._data[name] = value

    def add_rollup_field(self, name: str, value: Numeric):
        self._rollup_fields[name] += value

    @contextmanager
    def add_timer_field(self, name: str):
        try:
            start = time.perf_counter()
            yield
        finally:
            done = time.perf_counter()
            self._timer_fields[name] += (done - start) * 1000

    def attach_exception(self, err: Optional[BaseException] = None, prefix: str = 'exception'):
        self.add(format_exception(err, prefix))

    def __str__(self):
        return json.dumps({
            "data": self._data,
            "rollups": self._rollup_fields,
            "timers": self._timer_fields
        })

    def send(self):
        if self._client:
            self._client.send(self)
        else:
            raise RuntimeError("Can't send, no client!")

    def rollup_fields(self) -> Dict[str, Numeric]:
        return {_rollup_name(k): v for k, v in self._rollup_fields.items()}

    def fields(self) -> Dict[str, Any]:
        return {**self._data, **self.rollup_fields(), **self.timer_fields()}

    def timer_fields(self) -> Dict[str, float]:
        return {_timer_name(k): round(v, 3) for k, v in self._timer_fields.items()}


def _rollup_name(name: str):
    if name.startswith(ROLLUP_PREFIX):
        return name
    else:
        return ROLLUP_PREFIX + name


def _timer_name(name: str):
    if not name.startswith(TIMER_PREFIX):
        name = TIMER_PREFIX + name

    if not name.endswith('_ms'):
        name = name + '_ms'
    return name
