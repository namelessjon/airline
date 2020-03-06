# This code substantially copied from honeycomb's beeline python
# Licensed under the APL

import datetime
from collections import defaultdict
import uuid
from contextlib import contextmanager
import threading
import functools
import hashlib
import json
import struct
import sys

_SKL = None

MAX_INT32 = 2**32 - 1

def init(dataset=''):
    global _SKL

    if _SKL is None:
        _SKL = Client(dataset)


def add_context(data):
    '''Similar to add_context_field(), but allows you to add a number of name:value pairs
    to the currently active event at the same time.
    `beeline.add_context({ "first_field": "a", "second_field": "b"})`
    Args:
    - `data`: dictionary of field names (strings) to field values to add
    '''
    if _SKL:
        _SKL.tracer_impl.add_context(data=data)

def add_context_field(name, value):
    ''' Add a field to the currently active span. For example, if you are
    using django and wish to add additional context to the current request
    before it is sent to Honeycomb:
    `beeline.add_context_field("my field", "my value")`
    Args:
    - `name`: Name of field to add
    - `value`: Value of new field
    '''
    if _SKL:
        _SKL.tracer_impl.add_context_field(name=name, value=value)

def remove_context_field(name):
    ''' Remove a single field from the current span.
    ```
    beeline.add_context({ "first_field": "a", "second_field": "b"})
    beeline.remove_context_field("second_field")
    Args:
    - `name`: Name of field to remove
    ```
     '''

    if _SKL:
        _SKL.tracer_impl.remove_context_field(name=name)

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

    if _SKL:
        _SKL.tracer_impl.add_rollup_field(name=name, value=value)

def add_trace_field(name, value):
    ''' Similar to `add_context_field` - adds a field to the current span, but
    also to all other future spans in this trace. Trace context fields will be
    propagated to downstream services if using instrumented libraries
    like `requests`.
    Args:
    - `name`: Name of field to add
    - `value`: Value of new field
    '''
    if _SKL:
        _SKL.tracer_impl.add_trace_field(name=name, value=value)

def remove_trace_field(name):
    ''' Removes a trace context field from the current span. This will not
    affect  other existing spans, but will prevent the field from being
    propagated to new spans.
    Args:
    - `name`: Name of field to remove
    '''
    if _SKL:
        _SKL.tracer_impl.remove_trace_field(name=name)

def tracer(name, trace_id=None, parent_id=None):
    '''
    When used in a context manager, creates a span for the contained
    code. If a trace is ongoing, will add a new child span under the currently
    running span. If no trace is ongoing, will start a new trace.
    Example use:
    ```
    with tracer(name="my expensive computation"):
        recursive_fib(100)
    ```
    Args:
    - `name`: a descriptive name for the this trace span, i.e. "database query for user"
    - `trace_id`: the trace_id to use. If None, will be automatically generated if no
       current trace is ongoing. Use this if you want to explicitly resume a trace
       in this application that was initiated in another application, and you have
       the upstream trace_id.
    - `parent_id`: If trace_id is set, will populate the root span's parent
        with this id.
    '''

    if _SKL:
        return _SKL.tracer(name=name, trace_id=trace_id, parent_id=parent_id)

    # if the beeline is not initialized, build a dummy function
    # that will work as a context manager and call that
    @contextmanager
    def _noop_cm():
        yield

    return _noop_cm()

def start_trace(context=None, trace_id=None, parent_span_id=None):
    '''
    Start a trace, returning the root span. To finish the trace, pass the span
    to `finish_trace`. If you are using the beeline middleware plugins, such as for
    django, flask, or AWS lambda, you will want to use `start_span` instead, as
    `start_trace` is called at the start of the request.
    Args:
    - `context`: optional dictionary of event fields to populate the root span with
    - `trace_id`: the trace_id to use. If None, will be automatically generated.
        Use this if you want to explicitly resume trace in this application that was
        initiated in another application, and you have the upstream trace_id.
    - `parent_span_id`: If trace_id is set, will populate the root span's parent
        with this id.
    '''

    if _SKL:
        return _SKL.tracer_impl.start_trace(context=context, trace_id=trace_id, parent_span_id=parent_span_id)

def finish_trace(span):
    ''' Explicitly finish a trace. If you started a trace with `start_trace`, you must call
    this to close the trace and send the root span. If you are using the beeline middleware plugins,
    such as django, flask, or AWS lambda, you can skip this step as the trace will be closed for
    you.
    Args:
    - `span`: Span object that was returned by `start_trace`
    '''

    if _SKL:
        _SKL.tracer_impl.finish_trace(span=span)

def start_span(context=None, parent_id=None):
    '''
    Start a new span and return the span object. Returns None if no trace is active.
    For each `start_span`, there should be one call to `close_span`. Child spans should
    also be closed before parent spans. Closing spans out of order will lead to strange
    results and can break the bookkeeping needed to preserve trace structure. For example:
    ```
    parent_span = beeline.start_span()
    # this span is a child of the last span created
    child_span = beeline.start_span()
    beeline.finish_span(child_span)
    beeline.finish_span(parent_span)
    ```
    Args:
    - `context`: optional dictionary of event fields to populate the span with
    - `parent_id`: ID of parent span - use this only if you have a very good reason to
        do so.
    '''

    if _SKL:
        return _SKL.tracer_impl.start_span(context=context, parent_id=parent_id)

def finish_span(span):
    '''
    Finish the provided span, sending the associated event data to Honeycomb.
    For each `start_span`, there should be one call to `finish_span`.
    Args:
    - `span`: Span object that was returned by `start_trace`
    '''

    if _SKL:
        _SKL.tracer_impl.finish_span(span=span)

def close():
    ''' close the skyline client, flushing any unsent events. '''
    global _SKL
    if _SKL:
        _SKL.close()

    _SKL = None

def traced(name, trace_id=None, parent_id=None):
    '''
    Function decorator to wrap an entire function in a trace span. If no trace
    is active in the current thread, starts a new trace, and the wrapping span
    will be a root span. If a trace is active, creates a child span of the
    existing trace.
    Example use:
    ```
    @traced(name="my_expensive_function")
    def my_func(n):
        recursive_fib(n)
    my_func(100)
    ```
    Args:
    - `name`: a descriptive name for the this trace span, i.e. "function_name". This is required.
    - `trace_id`: the trace_id to use. If None, will be automatically generated.
        Use this if you want to explicitly resume a trace in this application that was
        initiated in another application, and you have the upstream trace_id.
    - `parent_id`: If trace_id is set, will populate the root span's parent
        with this id.
    '''

    return traced_impl(tracer_fn=tracer, name=name, trace_id=trace_id, parent_id=parent_id)


def _log(message, *args):
    if _SKL and _SKL.debug:
        print(message % args)



class Trace(object):
    '''Object encapsulating all state of an ongoing trace.'''
    def __init__(self, trace_id):
        self.id = trace_id
        self.stack = []
        self.fields = {}
        self.rollup_fields = defaultdict(float)

    def copy(self):
        '''Copy the trace state for use in another thread or context.'''
        result = Trace(self.id)
        result.stack = copy.copy(self.stack)
        result.fields = copy.copy(self.fields)
        return result

class Tracer(object):
    def __init__(self, client):
        self._client = client

        self.presend_hook = None
        self.sampler_hook = None

    @contextmanager
    def __call__(self, name, trace_id=None, parent_id=None):
        try:
            span = None
            if self.get_active_trace_id() and trace_id is None:
                span = self.start_span(context={'name': name}, parent_id=parent_id)
                if span:
                    _log('tracer context manager started new span, id = %s',
                        span.id)
            else:
                span = self.start_trace(context={'name': name}, trace_id=trace_id, parent_span_id=parent_id)
                if span:
                    _log('tracer context manager started new trace, id = %s',
                        span.trace_id)
            yield span
        except Exception as e:
            if span:
                span.add_context({
                    "app.exception_type": str(type(e)),
                    "app.exception_string": stringify_exception(e),
                })
            raise
        finally:
            if span:
                if span.is_root():
                    _log('tracer context manager ending trace, id = %s',
                        span.trace_id)
                    self.finish_trace(span)
                else:
                    _log('tracer context manager ending span, id = %s',
                        span.id)
                    self.finish_span(span)
            else:
                _log('tracer context manager span for %s was unexpectedly None', name)

    def start_trace(self, context=None, trace_id=None, parent_span_id=None):
        if trace_id:
            if self._trace:
                _log('warning: start_trace got explicit trace_id but we are already in a trace. '
                    'starting new trace with id = %s', trace_id)
            self._trace = Trace(trace_id)
        else:
            self._trace = Trace(str(uuid.uuid4()))

        # start the root span
        return self.start_span(context=context, parent_id=parent_span_id)

    def start_span(self, context=None, parent_id=None):
        if not self._trace:
            _log('start_span called but no trace is active')
            return None

        span_id = str(uuid.uuid4())
        if parent_id:
            parent_span_id = parent_id
        else:
            parent_span_id = self._trace.stack[-1].id if self._trace.stack else None
        ev = self._client.new_event(data=self._trace.fields)
        if context:
            ev.add(data=context)

        ev.add(data={
            'trace.trace_id': self._trace.id,
            'trace.parent_id': parent_span_id,
            'trace.span_id': span_id,
        })
        is_root = len(self._trace.stack) == 0
        span = Span(trace_id=self._trace.id, parent_id=parent_span_id,
                    id=span_id, event=ev, is_root=is_root)
        self._trace.stack.append(span)

        return span

    def finish_span(self, span):
        # avoid exception if called with None
        if span is None:
            return

        # send the span's event. Even if the stack is in an unhealthy state,
        # it's probably better to send event data than not
        if span.event:
            if self._trace:
                # add the trace's rollup fields to the root span
                if span.is_root():
                    for k, v in self._trace.rollup_fields.items():
                        span.event.add_field(k, v)

                for k, v in span.rollup_fields.items():
                    span.event.add_field(k, v)

                # propagate trace fields that may have been added in later spans
                for k, v in self._trace.fields.items():
                    # don't overwrite existing values because they may be different
                    if k not in span.event.fields():
                        span.event.add_field(k, v)

            duration = datetime.datetime.now() - span.event.start_time
            duration_ms = duration.total_seconds() * 1000.0
            span.event.add_field('duration_ms', duration_ms)

            self._run_hooks_and_send(span)
        else:
            _log('warning: span has no event, was it initialized correctly?')

        if not self._trace:
            _log('warning: span finished without an active trace')
            return

        if span.trace_id != self._trace.id:
            _log('warning: finished span called for span in inactive trace. '
                'current trace_id = %s, span trace_id = %s', self._trace.id, span.trace_id)
            return

        if not self._trace.stack:
            _log('warning: finish span called but stack is empty')
            return

        if self._trace.stack[-1].id != span.id:
            _log('warning: finished span is not the currently active span')
            return

        self._trace.stack.pop()

    def finish_trace(self, span):
        self.finish_span(span)
        self._trace = None

    def get_active_trace_id(self):
        if self._trace:
            return self._trace.id
        return None

    def get_active_span(self):
        if self._trace and self._trace.stack:
            return self._trace.stack[-1]
        return None

    def add_context_field(self, name, value):
        span = self.get_active_span()
        if span:
            span.add_context_field(name=name, value=value)

    def add_context(self, data):
        span = self.get_active_span()
        if span:
            span.add_context(data=data)

    def remove_context_field(self, name):
        span = self.get_active_span()
        if span:
            span.remove_context_field(name=name)

    def add_rollup_field(self, name, value):
        value = float(value)

        span = self.get_active_span()
        if span:
            span.rollup_fields[name] += value

        if not self._trace:
            _log('warning: adding rollup field without an active trace')
            return

        self._trace.rollup_fields["rollup.%s" % name] += value

    def add_trace_field(self, name, value):
        # prefix with app to avoid key conflicts
        # add the app prefix if it's missing

        if (type(name) == str and not name.startswith("app.")) or type(name) != str:
            key = "app.%s" % name
        else:
            key = name

        # also add to current span
        self.add_context_field(key, value)

        if not self._trace:
            _log('warning: adding trace field without an active trace')
            return
        self._trace.fields[key] = value

    def remove_trace_field(self, name):
        key = "app.%s" % name
        self.remove_context_field(key)
        if not self._trace:
            _log('warning: removing trace field without an active trace')
            return
        self._trace.fields.pop(key)

    def register_hooks(self, presend=None, sampler=None):
        self.presend_hook = presend
        self.sampler_hook = sampler

    def _run_hooks_and_send(self, span):
        ''' internal - run any defined hooks on the event and send
        kind of hacky: we fetch the hooks from the beeline, but they are only
        used here. Pass them to the tracer implementation?
        '''
        presampled = False
        if self.sampler_hook:
            _log("executing sampler hook on event ev = %s", span.event.fields())
            keep, new_rate = self.sampler_hook(span.event.fields())
            if not keep:
                _log("skipping event due to sampler hook sampling ev = %s", span.event.fields())
                return
            span.event.sample_rate = new_rate
            presampled = True

        if self.presend_hook:
            _log("executing presend hook on event ev = %s", span.event.fields())
            self.presend_hook(span.event.fields())

        if presampled:
            _log("enqueuing presampled event ev = %s", span.event.fields())
            span.event.send_presampled()
        elif _should_sample(span.trace_id, span.event.sample_rate):
            # if our sampler hook wasn't used, use deterministic sampling
            span.event.send_presampled()

class SynchronousTracer(Tracer):
    def __init__(self, client):
        super(SynchronousTracer, self).__init__(client)
        self._state = threading.local()

    @property
    def _trace(self):
        return getattr(self._state, 'trace', None)

    @_trace.setter
    def _trace(self, new_trace):
        self._state.trace = new_trace

class Span(object):
    ''' Span represents an active span. Should not be initialized directly, but
    through a Tracer object's `start_span` method. '''
    def __init__(self, trace_id, parent_id, id, event, is_root=False):
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.id = id
        self.event = event
        self.event.start_time = datetime.datetime.now()
        self.rollup_fields = defaultdict(float)
        self._is_root = is_root

    def add_context_field(self, name, value):
        self.event.add_field(name, value)

    def add_context(self, data):
        self.event.add(data)

    def remove_context_field(self, name):
        if name in self.event.fields():
            del self.event.fields()[name]

    def is_root(self):
        return self._is_root

def _should_sample(trace_id, sample_rate):
    sample_upper_bound = MAX_INT32 / sample_rate
    # compute a sha1
    sha1 = hashlib.sha1()
    sha1.update(trace_id.encode('utf-8'))
    # convert first 4 digits to int
    value, = struct.unpack('>I', sha1.digest()[:4])
    if value < sample_upper_bound:
        return True
    return False

def traced_impl(tracer_fn, name, trace_id, parent_id):
    """Implementation of the traced decorator without async support."""
    def wrapped(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            with tracer_fn(name=name, trace_id=trace_id, parent_id=parent_id):
                return fn(*args, **kwargs)

        return inner

    return wrapped


class Event:
    def __init__(self, data={}, created_at=datetime.datetime.utcnow(), client=_SKL):
        self._data = {}
        self._client = client
        self.sample_rate = client.sample_rate
        self.dataset = client.dataset
        self.created_at = created_at
        self.add(data=data)

    def add(self, data):
        self._data.update(data)

    def add_field(self, name, value):
        self._data[name] = value
    
    def __str__(self):
        return json.dumps(self._data)

    def send_presampled(self):
        self._client.send(self)

    def fields(self):
        return self._data


class Client():
    def __init__(self, dataset, sample_rate=1):
        self.dataset = dataset
        self.debug = True
        self.sample_rate = sample_rate

        self.tracer_impl = SynchronousTracer(self)

    def tracer(self, name, trace_id=None, parent_id=None):
        return self.tracer_impl(name=name, trace_id=trace_id, parent_id=parent_id)

    def new_event(self, data={}):
        return Event(data=data, client=self)

    def send(self, ev):
        '''send accepts an event and writes it to the configured output file'''
        event_time = ev.created_at.isoformat()
        if ev.created_at.tzinfo is None:
            event_time += "Z"

        # we add dataset and user_agent to the payload
        # if processed by another honeycomb agent (i.e. agentless integrations
        # for AWS), this data will get used to route the event to the right
        # location with appropriate metadata
        payload = {
            "time": event_time,
            "samplerate": ev.sample_rate,
            "dataset": ev.dataset,
            "data": ev.fields(),
        }
        print(json.dumps(payload, default=_json_default_handler) + "\n", file=sys.stderr)

def _json_default_handler(obj):
    try:
        return str(obj)
    except TypeError:
        return repr(obj)