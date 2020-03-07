import skyline as sl
import pytest

def test_dots_to_deep_not_deep():
    assert sl.dots_to_deep({'a': 1}) == {'a':1}

def test_dots_to_deep_one_level():
    assert sl.dots_to_deep({'a.b': 1}) == {'a': {'b': 1}}

def test_dots_to_deep_one_level2():
    assert sl.dots_to_deep({'a.b': 1, 'c': 2}) == {'a': {'b': 1}, 'c': 2}

def test_dots_to_deep_one_level2():
    assert sl.dots_to_deep({'a.b.c': 1, 'c': 2}) == {'a': {'b': {'c': 1}}, 'c': 2}

def test_dots_to_deep_one_warns_on_collision():
    with pytest.raises(RuntimeError):
        sl.dots_to_deep({'a':1, 'a.b': 1, 'c': 2})

def test_dots_to_deep_one_warns_on_collision_no_matter_the_order():
    with pytest.raises(RuntimeError):
        sl.dots_to_deep({'a.b': 1, 'a':1, 'c': 2})