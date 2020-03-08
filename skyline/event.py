import datetime as dt
from collections import defaultdict

TIMER_PREFIX = 'timers.'
ROLLUP_PREFIX = 'rollup.'

class Event:
    def __init__(self, data={}, created_at=dt.datetime.utcnow(), client=None):
        self._data = {}
        self._client = client

        if client:
            self.dataset = client.dataset
        else:
            self.dataset = ''
        self.created_at = created_at
        self.add(data=data)
        self._rollup_fields = defaultdict(int)

    def add(self, data):
        self._data.update(data)

    def add_field(self, name, value):
        self._data[name] = value

    def add_rollup_field(self, name, value):
        self._rollup_fields[name] += value
    
    def __str__(self):
        return json.dumps({"data": self._data, "rollups": self._rollup_fields})

    def send(self):
        if self._client:
            self._client.send(self)
        else:
            raise RuntimeError("Can't send, no client!")

    def rollup_fields(self):
        return {_rollup_name(k):v for k,v in self._rollup_fields.items()}

    def fields(self):
        return {**self._data, **self.rollup_fields()}

def _rollup_name(name: str):
    if name.startswith(ROLLUP_PREFIX) or name.startswith(TIMER_PREFIX):
        return name
    else:
        return ROLLUP_PREFIX + name

def _timer_name(name: str):
    if not name.startswith(TIMER_PREFIX):
        name = TIMER_PREFIX + name

    if not name.endswith('_ms'):
        name = name + '_ms'
    return name