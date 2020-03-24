import sys
import traceback
from typing import (
    Optional
)

FS_NAMES = ('filename', 'lineno', 'name',)


def format_exception(exception: Optional[BaseException] = None, prefix: str = 'exception') -> dict:
    cls, msg, tb = _extract_exception(exception)

    return {
        f'{prefix}.type': cls.__name__,
        f'{prefix}.message': str(msg),
        f'{prefix}.traceback': [_fs_to_dict(i) for i in traceback.extract_tb(tb)],
    }


def _fs_to_dict(fs: traceback.FrameSummary, fs_names=FS_NAMES):
    return {n: getattr(fs, n) for n in fs_names}


def _extract_exception(exception: Optional[BaseException] = None):
    if isinstance(exception, BaseException):
        return exception.__class__, exception, exception.__traceback__
    elif isinstance(exception, tuple):
        return exception
    else:
        return sys.exc_info()
