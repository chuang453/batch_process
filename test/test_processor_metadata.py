from decorators.processor import (
    PROCESSORS,
    PRE_PROCESSORS,
    POST_PROCESSORS,
    TRANSFORMS,
    get_all_processors,
    post_processor,
    pre_processor,
    processor,
    transform,
)


@processor(name='meta_file_proc_test', inputs=['main:df'], outputs=['meta_file_proc_test.rows'])
def meta_file_proc_test(path, context, **kwargs):
    return {'rows': 1}


@pre_processor(name='meta_pre_proc_test', inputs=['cfg:x'], outputs=['meta_pre_proc_test.ok'])
def meta_pre_proc_test(context, **kwargs):
    return {'ok': True}


@post_processor(name='meta_post_proc_test', inputs=['meta_pre_proc_test.ok'], outputs=['meta_post_proc_test.done'])
def meta_post_proc_test(context, **kwargs):
    return {'done': True}


@transform(name='meta_transform_test', inputs=['main:df'], outputs=['main:df'])
def meta_transform_test(df, context, **kwargs):
    return df


def test_processor_metadata_fields_set():
    assert 'meta_file_proc_test' in PROCESSORS
    assert 'meta_pre_proc_test' in PRE_PROCESSORS
    assert 'meta_post_proc_test' in POST_PROCESSORS
    assert 'meta_transform_test' in TRANSFORMS

    assert getattr(PROCESSORS['meta_file_proc_test'], 'processor_inputs', []) == ['main:df']
    assert getattr(PROCESSORS['meta_file_proc_test'], 'processor_outputs', []) == ['meta_file_proc_test.rows']

    assert getattr(PRE_PROCESSORS['meta_pre_proc_test'], 'processor_inputs', []) == ['cfg:x']
    assert getattr(POST_PROCESSORS['meta_post_proc_test'], 'processor_outputs', []) == ['meta_post_proc_test.done']
    assert getattr(TRANSFORMS['meta_transform_test'], 'processor_kind', '') == 'transform'


def test_get_all_processors_exposes_inputs_outputs():
    entries = get_all_processors()
    by_name = {item['name']: item for item in entries}

    assert by_name['meta_file_proc_test']['inputs'] == ['main:df']
    assert by_name['meta_transform_test']['outputs'] == ['main:df']
