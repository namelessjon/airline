from contextlib import contextmanager
import json
import time
import sys

from .event import Event
from .version import __version__


class Client():
    def __init__(self, dataset, debug=False):
        self.dataset = dataset
        self.debug = debug

    def add_context_field(self, name, value):
        if self._event:
            self._event.add_field(name, value)
        else:
            self.log("No event found")

    def add_context(self, data):
        if self._event:
            self._event.add(data=data)
        else:
            self.log("No event found")

    def add_rollup_field(self, name, value):
        if self._event:
            self._event.add_rollup_field(name, value)
        else:
            self.log("No event found")

    @contextmanager
    def evented(self):
        if self._event:
            self.log("Event already created")
        else:
            event = self.new_event(data={})
            self._event = event
        start = time.perf_counter()
        try:
            yield
        except Exception as e:
            event.add_field('status', 'ERROR')
            event.add_field('exception.message', str(e))
            event.add_field('exception.type', e.__class__.__name__)
            raise
        finally:
            done = time.perf_counter()
            duration = (done - start) * 1000
            event.add_field('duration_ms', round(duration, 3))
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
            "client": "airline/" + __version__,
            "data": dots_to_deep(ev.fields()),
        }
        if self.debug:
            indent = 2
        else:
            indent = None

        print(json.dumps(payload, indent=indent, default=_json_default_handler) + "\n", file=sys.stderr)

    def log(self, message, *args):
        if self.debug:
            print(message % args)

    def __repr__(self):
        return "{cls}(dataset={dataset!r}, debug={debug!r})".format(
            cls=self.__class__.__name__,
            dataset=self.dataset,
            debug=self.debug,
        )


def _json_default_handler(obj):
    if isinstance(obj, Event):
        return obj.fields()
    try:
        return str(obj)
    except TypeError:
        return repr(obj)


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
            raise RuntimeError("Incorrect nesting specified: key=%s collides with key=%s" % (k, '.'.join(keys)))

        if key in d:
            raise RuntimeError("Incorrect nesting specified: key=%s would overwrite nesting key" % (k,))

        d[key] = v

    return new_dict
