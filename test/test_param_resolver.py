from decorators.processor import ProcessingContext
from utils.param_resolver import resolve_param, resolve_params


def test_resolve_param_kwargs_precedence_over_shared():
    ctx = ProcessingContext()
    ctx.set_shared(['ns', 'x'], 10)

    val = resolve_param('x', {'x': 5}, ctx, namespace='ns', default=0)

    assert val == 5


def test_resolve_param_reads_shared_when_missing_in_kwargs():
    ctx = ProcessingContext()
    ctx.set_shared(['ns', 'x'], 10)

    val = resolve_param('x', {}, ctx, namespace='ns', default=0)

    assert val == 10


def test_resolve_param_keeps_falsy_values_from_kwargs():
    ctx = ProcessingContext()
    ctx.set_shared(['ns', 'x'], 10)

    assert resolve_param('x', {'x': 0}, ctx, namespace='ns', default=99) == 0
    assert resolve_param('x', {'x': ''}, ctx, namespace='ns', default='d') == ''
    assert resolve_param('x', {'x': []}, ctx, namespace='ns', default=[1]) == []


def test_resolve_params_batch():
    ctx = ProcessingContext()
    ctx.set_shared(['ns', 'a'], 1)
    kwargs = {'b': 2}

    out = resolve_params(['a', 'b', 'c'], kwargs, ctx, namespace='ns', defaults={'c': 3})

    assert out == {'a': 1, 'b': 2, 'c': 3}
