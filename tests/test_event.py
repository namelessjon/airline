import pytest

import airline.event


@pytest.fixture
def event():
    return airline.event.Event()


def test_an_added_field_is_in_fields(event):
    event.add_field('foo', 'bar')

    assert event.fields()['foo'] == 'bar'


def test_added_fields_are_in_fields(event):
    event.add({'foo': 'bar', 'baz': 1})

    assert event.fields() == {'foo': 'bar', 'baz': 1}


@pytest.mark.parametrize('name', ['foo', 'rollup.foo'])
def test_rollup_fields_are_added(event, name):
    event.add_rollup_field(name, 1)

    assert event.fields() == {'rollup.foo': 1}


def test_rollup_fields_accumulate(event):
    event.add_rollup_field('foo', 1)
    event.add_rollup_field('foo', 1)

    assert event.fields() == {'rollup.foo': 2}


def test_timers_work(event, mocker):
    mocker.patch('time.perf_counter', side_effect=[0, 5])

    with event.add_timer_field('five'):
        pass

    assert event.fields() == {'timers.five_ms': 5000.0}


@pytest.mark.parametrize('name', ['example', 'timers.example', 'example_ms', 'timers.example_ms'])
def test_timer_fields_are_added(event, name):
    with event.add_timer_field(name):
        pass

    assert 'timers.example_ms' in event.fields()


def test_fields_are_in___str__(event):
    event.add_field('foo', 1)

    assert '"data": {"foo": 1' in str(event)


def test_rollups_are_in___str__(event):
    event.add_rollup_field('foo', 1)

    assert '"rollups": {"foo": 1' in str(event)


def test_timers_are_in___str__(event):
    with event.add_timer_field('foo'):
        pass

    assert '"timers": {"foo": ' in str(event)


def test_send_passes_off_to_client(mocker):
    client = mocker.Mock()

    event = airline.event.Event(client=client)

    event.send()

    client.send.assert_called_with(event)
