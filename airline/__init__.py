"""
Lightweight wide event logging to bring more observability to lambda functions.

The idea is to build up the full context of a function/script into one wide
event that gets emitted at the end.  This puts all the context in one log
message, making it very easy to run analytics on, find like errors, and a lot
of other things.  A full blown observability platform like honeycomb would be
more informative, and allows for the notion of spans and distributed tracing
(i.e. across different (micro) services and the like).

But this is a start.
"""
from contextlib import contextmanager
import functools
from typing import (
    Dict,
    Any,
    Optional,
)

from .threadlocal_client import ThreadLocalClient
from .version import __version__   # noqa: F401


_ARL = None


def init(dataset: str = '', debug=False):
    global _ARL

    if _ARL is None:
        _ARL = ThreadLocalClient(dataset=dataset, debug=debug)
    else:
        print("Library already initialized: client=%s new_dataset=%r" % (_ARL, dataset))


def add_context(data: Dict[str, Any]):
    '''Similar to add_context_field(), but allows you to add a number of name:value pairs
    to the currently active event at the same time.
    `airline.add_context({ "first_field": "a", "second_field": "b"})`
    Args:
    - `data`: dictionary of field names (strings) to field values to add
    '''
    if _ARL:
        _ARL.add_context(data=data)
    else:
        _log("Client or Event not initialised")


def add_context_field(name: str, value: Any):
    ''' Add a field to the currently active event. For example, if you are
    using django and wish to add additional context to the current request
    before it is sent:
    `airline.add_context_field("my field", "my value")`
    Args:
    - `name`: Name of field to add
    - `value`: Value of new field
    '''
    if _ARL:
        _ARL.add_context_field(name=name, value=value)
    else:
        _log("Client or Event not initialised")


def add_rollup_field(name: str, value: Any):
    ''' AddRollupField adds a key/value pair to the current event. If it is called repeatedly
    on the same event, the values will be summed together.
    Args:
    - `name`: Name of field to add
    - `value`: Numeric (float) value of new field
    '''

    if _ARL:
        _ARL.add_rollup_field(name=name, value=value)
    else:
        _log("Client or Event not initialised")


@contextmanager
def timer(name: str):
    """ Timer yields block (think `with` statement) and counts the time
     taken during that block.  The time is added to the event.  If there
     are multiple invocations with the same name, these will be added up
     over the whole event.
    It is especially useful for doing things like adding the duration spent talking
    to a specific external service - eg database time
    """
    if _ARL:
        with _ARL.add_timer_field(name):
            yield
    else:
        _log("Client or Event not initialised")
        yield


def done():
    ''' close the airline client, flushing any unsent events. '''
    global _ARL
    if _ARL:
        _ARL.done()

    _ARL = None


def evented(**extra_context):
    """Decorator for wrapping a generic function in an event.

    The event will be sent when the function ends, possibly annotated with
    any exception raised."""
    def wrapped(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            if _ARL:
                with _ARL.evented():
                    if extra_context:
                        add_context(extra_context)
                    return fn(*args, **kwargs)
            else:
                return fn(*args, **kwargs)

        return inner

    return wrapped


def timed(add_count=True, **extra_context):
    """
    Decorator to turn a function call into a timed function call.
    The total duration of all calls to the function, and the number
    of calls will be attached as fields to the event.
    Also, additional context can be provided.
    """
    def wrapped(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            if _ARL:
                name = fn.__name__
                timer_name = f"{name}_duration"
                count_name = f"{name}_calls"
                with _ARL.add_timer_field(timer_name):
                    if add_count:
                        _ARL.add_rollup_field(count_name, 1)
                    if extra_context:
                        add_context(extra_context)
                    return fn(*args, **kwargs)
            else:
                return fn(*args, **kwargs)

        return inner

    return wrapped


def attach_exception(err: Optional[BaseException] = None, prefix: str = 'exception'):
    """
    Attach an exception and traceback to the current event with the given prefix
    """
    if _ARL:
        _ARL.attach_exception(err=err, prefix=prefix)
    else:
        _log("Client or Event not initialised")


def set_status(status: str):
    """
    Set the status field on the current event
    """
    add_context_field('status', status)


def success():
    """
    helper methods for consistent success/fail statuses
    """
    set_status('SUCCESS')


def error():
    """
    helper methods for consistent success/fail statuses
    """
    set_status('ERROR')


def _log(message, *args):
    if _ARL and _ARL.debug:
        print(message % args)
