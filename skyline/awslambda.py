import functools

import airline


COLD_START = True


def airline_wrapper(_handler=None, *, add_event=False, add_response=False):
    '''airline decorator for Lambda functions. Expects a handler
    function with the signature:
    `def handler(event, context)`
    Example use:
    ```
    @airline_wrapper
    def my_handler(event, context):
        # ...
    ```
    '''

    def decorator_airline(handler):
        @functools.wraps(handler)
        def _airline_wrapper(event, context):
            global COLD_START

            # don't blow up the world if the airline has not been initialized
            if not airline._ARL:
                return handler(event, context)

            try:

                with airline._ARL.evented():
                    airline.add_context({
                        "app.function_name": getattr(context, 'function_name', ""),
                        "app.function_version": getattr(context, 'function_version', ""),
                        "app.request_id": getattr(context, 'aws_request_id', ""),
                        "meta.cold_start": COLD_START,
                    })

                    if add_event:
                        airline.add_context("app.event", event)

                    resp = handler(event, context)

                    if resp is not None and add_response:
                        airline.add_context_field('app.response', resp)

                    return resp
            finally:
                # This remains false for the lifetime of the module
                COLD_START = False

        return _airline_wrapper

    if _handler is None:
        return decorator_airline
    else:
        return decorator_airline(_handler)
