from stage2_platform.contracts import DatasetRef, RunManifest, SeriesManifest, normalize_stage


def test_dataset_ref_defaults():
    ref = DatasetRef(name='raw', source_type='file')
    assert ref.source_params == {}
    assert ref.schema == {}
    assert ref.tags == []


def test_normalize_stage_legacy_steps_to_default_series():
    stage = normalize_stage({'name': 's1', 'type': 'data', 'source': 'df', 'steps': [{'head': 1}]})
    assert stage.name == 's1'
    assert stage.source_key == 'df'
    assert len(stage.series) == 1
    assert stage.series[0].name == 'default'
    assert stage.series[0].steps == [{'head': 1}]


def test_run_manifest_model():
    rec = SeriesManifest(series_name='a', input_key='df', output_key='out', status='success')
    man = RunManifest(run_id='r1', project_name='p1', status='done', series_records=[rec])
    assert man.series_records[0].series_name == 'a'
