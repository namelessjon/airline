import pytest

import airline.client


@pytest.fixture()
def client() -> airline.client.Client:
    client = airline.client.Client('test')
    return client


def test_dataset_is_available(client):
    assert client.dataset == 'test'


def test_a_client_can_be_started(client):
    client.start()

    assert client._event is not None


def test_add_context_field_adds_to_event(client, mocker):
    ev = mocker.MagicMock()
    client.start(ev)

    client.add_context_field('foo', 'bar')

    ev.add_field.assert_called_with('foo', 'bar')


def test_add_context_adds_to_event(client, mocker):
    ev = mocker.MagicMock()
    client.start(ev)

    client.add_context({'foo': 'bar'})

    ev.add.assert_called_with(data={'foo': 'bar'})


def test_add_rollup_field_adds_to_event(client, mocker):
    ev = mocker.MagicMock()
    client.start(ev)

    client.add_rollup_field('foo', 1)

    ev.add_rollup_field.assert_called_with('foo', 1)
