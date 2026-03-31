from pathlib import Path

from core.engine import BatchProcessor
from decorators.processor import ProcessingContext, processor


def test_set_get_main_delete_list():
    ctx = ProcessingContext()
    ctx.set_main('df', {'a': 1})

    assert ctx.get_main('df') == {'a': 1}
    assert 'df' in ctx.list_main()

    ctx.delete_main('df')
    assert ctx.get_main('df') is None


def test_get_pipe_and_pipe_log_filtering(tmp_path):
    @processor(name='pipe_capture_for_test')
    def pipe_capture_for_test(path, context, **kwargs):
        return {'count': 2, 'name': path.name}

    src = tmp_path / 'a.txt'
    src.write_text('hello', encoding='utf-8')
    cfg = {'**/*.txt': {'processors': ['pipe_capture_for_test']}}

    bp = BatchProcessor(cfg)
    ctx = bp.run(tmp_path)

    assert ctx.get_pipe('pipe_capture_for_test', 'count') == 2
    assert isinstance(ctx.get_pipe_log('pipe_capture_for_test'), list)
    assert ctx.get_pipe_log('pipe_capture_for_test')


def test_clear_includes_main_pipe():
    ctx = ProcessingContext()
    ctx.set_main('x', 1)
    ctx.pipe['p'] = {'ok': True}
    ctx.pipe_log.append({'proc_name': 'p'})

    ctx.clear()

    assert ctx.main == {}
    assert ctx.pipe == {}
    assert ctx.pipe_log == []
