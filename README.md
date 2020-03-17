# Skyline

Lightweight wide event logging to bring more observability to lambda functions.

This is very strongly inspired by [honeycomb](https://honeycomb.io) and their [beeline](https://github.com/honeycomb/beeline-python) library.

## How?

Use the decorators!

```python
import skyline

skyline.init(dataset='your_app_name')


@skyline.evented()
def some_function(a, b)
    # do things
```

or 

```python
import skyline
from skyline.awslambda import skyline_wrapper


skyline.init(dataset='function_or_app_name')


@skyline_wrapper
def handler(event, context):
    # do things
```


## Wide event logging?

The idea is to build up the full context of a function/script into one wide
event that gets emitted at the end.  This puts all the context in one log
message, making it very easy to run analytics on, find like errors, and a lot
of other things.  A full blown observability platform like honeycomb would be
more informative, and allows for the notion of spans and distributed tracing
(i.e. across different (micro) services and the like).

But this is a start.

Take this:

```python
import time
import random

import skyline

skyline.init(dataset='example')



@skyline.evented()
def main(a, b):
    skyline.add_context_field("a", a)
    skyline.add_context_field("b", b)

    with skyline.timer('processing_a'):
        subfunction1(a)

    with skyline.timer('processing_b'):
        subfunction1(b)


def subfunction1(input):
    time.sleep(random.uniform(0, len(input)))


main("foo", "example_long_thing")
```

And emit this at the end

```json
{
  "time": "2020-03-09T09:49:43.376126Z",
  "dataset": "example",
  "client": "skyline/0.1.0",
  "data": {
    "a": "foo",
    "b": "example_long_thing",
    "duration_ms": 9438.041,
    "timers": {
      "processing_a_ms": 255.11,
      "processing_b_ms": 9182.859
    }
  }
}
```

