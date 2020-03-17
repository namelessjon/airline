import threading

from .client import Client


class ThreadLocalClient(Client):
    def __init__(self, *args, **kwargs):
        super(ThreadLocalClient, self).__init__(*args, **kwargs)
        self._state = threading.local()

    @property
    def _event(self):
        return getattr(self._state, 'event', None)

    @_event.setter
    def _event(self, new_event):
        self._state.event = new_event
