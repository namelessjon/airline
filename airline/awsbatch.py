import functools
import json
import os
import urllib.request as req
import time
import random

import airline


def airline_wrapper(_handler=None, *, env_vars=None, add_role_info=True):
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

                if add_role_info:
                    _add_role_info()

                resp = handler(*args, **kwargs)

                return resp
        return _airline_wrapper

    if _handler is None:
        return decorator_airline
    else:
        return decorator_airline(_handler)


def _add_role_info():
    uri = os.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    if not uri:
        return

    full_uri = f"169.254.170.2{uri}"
    data = _fetch_parse_uri(full_uri)

    airline.add_context_field('aws.role_arn', data.get('RoleArn'))
    airline.add_context_field('aws.access_key_id', data.get('AccessKeyId'))


def _fetch_parse_uri(uri, attempts=5):
    attempt = 0
    while True:
        try:
            attempt += 1
            with req.urlopen(uri, timeout=1) as f:
                response = json.load(f)
                return response
        except req.URLError:
            if attempt < attempts:
                _sleep(attempt)
            else:
                raise


def _sleep(attempt, cap=60, base=1):
    sleep_time = random.uniform(0, min(cap, base * 2**attempt))
    time.sleep(sleep_time)
