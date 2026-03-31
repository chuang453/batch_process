from config.loader import is_pipeline_config


def test_is_pipeline_config_true_for_pipeline_list():
    cfg = {'pipeline': [{'type': 'walk', 'config': {}}]}
    assert is_pipeline_config(cfg) is True


def test_is_pipeline_config_false_for_non_pipeline_configs():
    assert is_pipeline_config(None) is False
    assert is_pipeline_config({}) is False
    assert is_pipeline_config({'pipeline': {}}) is False
    assert is_pipeline_config({'**/*.txt': {'processors': ['backup_file']}}) is False
