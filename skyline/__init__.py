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

from .threadlocal_client import ThreadLocalClient
from .version import __version__   # noqa: F401


_ARL = None


def init(dataset='', debug=False):
    global _ARL

    if _ARL is None:
        _ARL = ThreadLocalClient(dataset=dataset, debug=debug)
    else:
        print("Library already initialized: client=%s new_dataset=%r" % (_ARL, dataset))


def add_context(data):
    '''Similar to add_context_field(), but allows you to add a number of name:value pairs
    to the currently active event at the same time.
    `airline.add_context({ "first_field": "a", "second_field": "b"})`
    Args:
    - `data`: dictionary of field names (strings) to field values to add
    '''
    if _ARL and _ARL._event:
        _ARL.add_context(data=data)
    else:
        _log("Client or Event not initialised")


def add_context_field(name, value):
    ''' Add a field to the currently active event. For example, if you are
    using django and wish to add additional context to the current request
    before it is sent:
    `airline.add_context_field("my field", "my value")`
    Args:
    - `name`: Name of field to add
    - `value`: Value of new field
    '''
    if _ARL and _ARL._event:
        _ARL.add_context_field(name=name, value=value)
    else:
        _log("Client or Event not initialised")


def remove_context_field(name):
    ''' Remove a single field from the current span.
    ```
    airline.add_context({ "first_field": "a", "second_field": "b"})
    airline.remove_context_field("second_field")
    Args:
    - `name`: Name of field to remove
    ```
     '''

    if _ARL and _ARL._event:
        _ARL.remove_context_field(name=name)
    else:
        _log("Client or Event not initialised")


def add_rollup_field(name, value):
    ''' AddRollupField adds a key/value pair to the current event. If it is called repeatedly
    on the same event, the values will be summed together.
    Args:
    - `name`: Name of field to add
    - `value`: Numeric (float) value of new field
    '''

    if _ARL and _ARL._event:
        _ARL.add_rollup_field(name=name, value=value)
    else:
        _log("Client or Event not initialised")


@contextmanager
def timer(name):
    """ Timer yields block (think `with` statement) and counts the time
     taken during that block.  The time is added to the event.  If there
     are multiple invocations with the same name, these will be added up
     over the whole event.
    It is especially useful for doing things like adding the duration spent talking
    to a specific external service - eg database time
    """
    if _ARL and _ARL._event:
        with _ARL._event.add_timer_field(name):
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


def evented():
    """Decorator for wrapping a generic function in an event.

    The event will be sent when the function ends, possibly annotated with
    any exception raised."""
    def wrapped(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            if _ARL:
                with _ARL.evented():
                    return fn(*args, **kwargs)
            else:
                return fn(*args, **kwargs)

        return inner

    return wrapped


def success():
    """
    helper methods for consistent success/fail statuses
    """
    add_context_field('status', 'SUCCESS')


def error():
    """
    helper methods for consistent success/fail statuses
    """
    add_context_field('status', 'ERROR')


def _log(message, *args):
    if _ARL and _ARL.debug:
        print(message % args)
