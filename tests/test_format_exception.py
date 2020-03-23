import airline.format_exception


def test_format_exception_works():
    try:
        raise RuntimeError('example')
    except RuntimeError as e:
        v = airline.format_exception.format_exception(e)

        assert v['exception.type'] == "RuntimeError"
        assert v['exception.message'] == 'example'
        assert len(v['exception.traceback']) == 1

        trace = v['exception.traceback']
        first = trace[0]

        assert 'filename' in first
        assert first['lineno'] == 6
        assert first['name'] == 'test_format_exception_works'


def test_format_exception_works_with_no_explicit_exception():
    try:
        raise RuntimeError('example')
    except RuntimeError:
        v = airline.format_exception.format_exception()

        assert v['exception.type'] == "RuntimeError"
        assert v['exception.message'] == 'example'
        assert len(v['exception.traceback']) == 1

        trace = v['exception.traceback']
        first = trace[0]

        assert 'filename' in first
        assert first['lineno'] == 24
        assert first['name'] == 'test_format_exception_works_with_no_explicit_exception'


def test_format_exception_works_with_prefix():
    try:
        raise RuntimeError('example')
    except RuntimeError as e:
        v = airline.format_exception.format_exception(e, prefix='foo')

        assert v['foo.type'] == "RuntimeError"
        assert v['foo.message'] == 'example'
        assert len(v['foo.traceback']) == 1

        trace = v['foo.traceback']
        first = trace[0]

        assert 'filename' in first
        assert first['lineno'] == 42
        assert first['name'] == 'test_format_exception_works_with_prefix'
