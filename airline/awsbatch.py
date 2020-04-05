import functools
import os

import airline


def airline_wrapper(_handler=None, *, env_vars=None):
    '''airline decorator for a function in a AWS Batch job
    ```
    @airline_wrapper
    def my_handler(event, context):
        # ...
    ```
    '''

    def decorator_airline(handler):
        @functools.wraps(handler)
        def _airline_wrapper(*args, **kwargs):

            # don't blow up the world if the airline has not been initialized
            if not airline._ARL:
                return handler(*args, **kwargs)

            with airline._ARL.evented():
                airline.add_environment_variable('aws.batch.job_id', 'AWS_BATCH_JOB_ID')
                airline.add_environment_variable('aws.batch.compute_environment', 'AWS_BATCH_CE_NAME')
                airline.add_environment_variable('aws.batch.job_queue', 'AWS_BATCH_JQ_NAME')
                airline.add_environment_variable('aws.batch.job_attempt', 'AWS_BATCH_JOB_ATTEMPT')
                array_index = os.getenv('AWS_BATCH_JOB_ARRAY_INDEX')
                if array_index:
                    airline.add_context_field('aws.batch.array_job', True)
                    airline.add_context_field('aws.batch.array_index', array_index)

                if env_vars:
                    for name, var in env_vars.items():
                        airline.add_environment_variable(name, var)

                resp = handler(*args, **kwargs)

                return resp
        return _airline_wrapper

    if _handler is None:
        return decorator_airline
    else:
        return decorator_airline(_handler)
