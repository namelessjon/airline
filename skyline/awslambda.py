import functools

import skyline


COLD_START = True

def skyline_wrapper(_handler=None, *, add_event=True, add_response=True, init=False):
    '''Skyline decorator for Lambda functions. Expects a handler
    function with the signature:
    `def handler(event, context)`
    Example use:
    ```
    @skyline_wrapper
    def my_handler(event, context):
        # ...
    ```
    '''



    def decorator_skyline(handler):
        @functools.wraps(handler)
        def _skyline_wrapper(event, context):
            global COLD_START

            # don't blow up the world if the skyline has not been initialized
            if not skyline._SKL:
                return handler(event, context)

            try:

                with skyline._SKL.evented():
                    skyline.add_context({
                        "app.function_name": getattr(context, 'function_name', ""),
                        "app.function_version": getattr(context, 'function_version', ""),
                        "app.request_id": getattr(context, 'aws_request_id', ""),
                        "meta.cold_start": COLD_START,
                    })

                    if add_event:
                        skyline.add_context("app.event", event)

                    resp = handler(event, context)

                    if resp is not None and add_response:
                        skyline.add_context_field('app.response', resp)

                    return resp
            finally:
                # This remains false for the lifetime of the module
                COLD_START = False

        return _skyline_wrapper


    if init:
        if isinstance(init, str):
            skyline.init(dataset=init)
        elif isinstance(init, dict):
            skyline.init(**init)
        elif isinstance(init, (tuple, list)):
            skyline.init(*init)
        else:
            print("Don't know how to initialise with init=%r" % init)

    if _handler is None:
        return decorator_skyline
    else:
        return decorator_skyline(_handler)