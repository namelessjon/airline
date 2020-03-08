import skyline


COLD_START = True

def skyline_wrapper(handler):
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

    def _skyline_wrapper(event, context):
        global COLD_START

        # don't blow up the world if the beeline has not been initialized
        if not skyline._SKL:
            return handler(event, context)

        try:

            with skyline._SKL.evented():
                skyline.add_context({
                    "app.function_name": getattr(context, 'function_name', ""),
                    "app.function_version": getattr(context, 'function_version', ""),
                    "app.request_id": getattr(context, 'aws_request_id', ""),
                    "app.event": event,
                    "meta.cold_start": COLD_START,
                })

                resp = handler(event, context)

                if resp is not None:
                    skyline.add_context_field('app.response', resp)

                return resp
        finally:
            # This remains false for the lifetime of the module
            COLD_START = False

    return _skyline_wrapper