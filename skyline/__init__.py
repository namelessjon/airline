# This code substantially copied from honeycomb's beeline python
# Licensed under the APL

import datetime
from collections import defaultdict
from contextlib import contextmanager
import threading
import functools
import json
import time
import sys

__version__ = '0.1.0'

_SKL = None

TIMER_PREFIX = 'timers.'
ROLLUP_PREFIX = 'rollup.'

def init(dataset=''):
    global _SKL

    if _SKL is None:
        _SKL = ThreadLocalClient(dataset)


def add_context(data):
    '''Similar to add_context_field(), but allows you to add a number of name:value pairs
    to the currently active event at the same time.
    `beeline.add_context({ "first_field": "a", "second_field": "b"})`
    Args:
    - `data`: dictionary of field names (strings) to field values to add
    '''
    if _SKL and _SKL._event:
        _SKL.add_context(data=data)

def add_context_field(name, value):
    ''' Add a field to the currently active event. For example, if you are
    using django and wish to add additional context to the current request
    before it is sent:
    `beeline.add_context_field("my field", "my value")`
    Args:
    - `name`: Name of field to add
    - `value`: Value of new field
    '''
    if _SKL and _SKL._event:
        _SKL.add_context_field(name=name, value=value)

def remove_context_field(name):
    ''' Remove a single field from the current span.
    ```
    beeline.add_context({ "first_field": "a", "second_field": "b"})
    beeline.remove_context_field("second_field")
    Args:
    - `name`: Name of field to remove
    ```
     '''

    if _SKL and _SKL._event:
        _SKL.remove_context_field(name=name)

def add_rollup_field(name, value):
    ''' AddRollupField adds a key/value pair to the current span. If it is called repeatedly
    on the same span, the values will be summed together.  Additionally, this
    field will be summed across all spans and added to the trace as a total. It
    is especially useful for doing things like adding the duration spent talking
    to a specific external service - eg database time. The root span will then
    get a field that represents the total time spent talking to the database from
    all of the spans that are part of the trace.
    Args:
    - `name`: Name of field to add
    - `value`: Numeric (float) value of new field
    '''

    if _SKL and _SKL._event:
        _SKL.add_rollup_field(name=name, value=value)

@contextmanager
def timer(name):
    if _SKL and _SKL._event:
        try:
            start = time.perf_counter()
            yield
        finally:
            done = time.perf_counter()
            _SKL.add_rollup_field(_timer_name(name), (done-start)*1000)
    else:
        yield


def done():
    ''' close the skyline client, flushing any unsent events. '''
    global _SKL
    if _SKL:
        _SKL.done()

    _SKL = None

def evented():
    """Implementation of the traced decorator without async support."""
    def wrapped(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            if _SKL:
                with _SKL.evented():
                    return fn(*args, **kwargs)
            else:
                return fn(*args, **kwargs)

        return inner

    return wrapped


def _log(message, *args):
    if _SKL and _SKL.debug:
        print(message % args)


class Client():
    def __init__(self, dataset, debug=True):
        self.dataset = dataset
        self.debug = debug

    def add_context_field(self, name, value):
        if self._event:
            self._event.add_field(name, value)
        else:
            _log("No event found")

    def add_rollup_field(self, name, value):
        if self._event:
            self._event.add_rollup_field(name, value)
        else:
            _log("No event found")

    @contextmanager
    def evented(self):
        if self._event:
            _log("Event already created")
        else:
            event = self.new_event(data={})
            self._event = event
        start = time.perf_counter()
        try:
            yield
        except Exception as e:
            event.add_field('exception.message', str(e))
            event.add_field('exception.type', e.__class__.__name__)
            raise
        finally:
            done = time.perf_counter()
            event.add_field('duration_ms', (done-start)*1000)
            self.done()

    def done(self):
        self.send(self._event)
        self._event = None

    def new_event(self, data={}):
        return Event(data=data, client=self)

    def send(self, ev):
        '''send accepts an event and writes it to the configured output file'''
        event_time = ev.created_at.isoformat()
        if ev.created_at.tzinfo is None:
            event_time += "Z"

        payload = {
            "time": event_time,
            "dataset": ev.dataset,
            "client": "skyline/" + __version__,
            "data": dots_to_deep(ev.fields()),
        }
        print(json.dumps(payload, indent=2, default=_json_default_handler) + "\n", file=sys.stderr)

class ThreadLocalClient(Client):
    def __init__(self, *args):
        super(ThreadLocalClient, self).__init__(*args)
        self._state = threading.local()

    @property
    def _event(self):
        return getattr(self._state, 'event', None)

    @_event.setter
    def _event(self, new_event):
        self._state.event = new_event


class Event:
    def __init__(self, data={}, created_at=datetime.datetime.utcnow(), client=_SKL):
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
        return json.dumps(self._data)

    def send(self):
        self._client.send(self)

    def rollup_fields(self):
        return {_rollup_name(k):v for k,v in self._rollup_fields.items()}

    def fields(self):
        return {**self._data, **self.rollup_fields()}


def dots_to_deep(dictionary):
    new_dict = {}
    for k, v in dictionary.items():
        *keys, key = k.split('.')
        d = new_dict
        for kk in keys:
            if kk not in d:
                d[kk] = {}
            d = d[kk]
        if not isinstance(d, dict):
            raise RuntimeError("Incorrect nesting specified: key=%s" % (k,))

        if key in d:
            raise RuntimeError("Incorrect nesting specified: key=%s" % (k,))

        d[key] = v

            

    return new_dict


def _timer_name(name: str):
    if not name.startswith(TIMER_PREFIX):
        name = TIMER_PREFIX + name

    if not name.endswith('_ms'):
        name = name + '_ms'
    return name

def _rollup_name(name: str):
    if name.startswith(ROLLUP_PREFIX) or name.startswith(TIMER_PREFIX):
        return name
    else:
        return ROLLUP_PREFIX + name


def _json_default_handler(obj):
    if isinstance(obj, Event):
        return obj.fields()
    try:
        return str(obj)
    except TypeError:
        return repr(obj)