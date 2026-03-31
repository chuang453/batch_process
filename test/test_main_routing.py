import main


class DummyBatchProcessor:

    def __init__(self, config):
        self.config = config

    def run(self, root):
        return {'runner': 'batch', 'root': root, 'config': self.config}


class DummyPipeline:

    def __init__(self, stages):
        self.stages = stages

    def run(self, root):
        return {'runner': 'pipeline', 'root': root, 'stages': self.stages}


def test_run_pipeline_routes_to_batch(monkeypatch):
    cfg = {'**/*.txt': {'processors': ['backup_file']}}

    monkeypatch.setattr(main, 'load_config', lambda p: cfg)
    monkeypatch.setattr(main, 'load_plugins', lambda: None)
    monkeypatch.setattr(main, 'BatchProcessor', DummyBatchProcessor)
    monkeypatch.setattr(main, 'Pipeline', DummyPipeline)

    out = main.run_pipeline('rootA', 'cfg.yaml')

    assert out['runner'] == 'batch'
    assert out['root'] == 'rootA'


def test_run_pipeline_routes_to_pipeline(monkeypatch):
    cfg = {'pipeline': [{'type': 'walk', 'config': {}}]}

    monkeypatch.setattr(main, 'load_config', lambda p: cfg)
    monkeypatch.setattr(main, 'load_plugins', lambda: None)
    monkeypatch.setattr(main, 'BatchProcessor', DummyBatchProcessor)
    monkeypatch.setattr(main, 'Pipeline', DummyPipeline)

    out = main.run_pipeline('rootB', 'cfg.yaml')

    assert out['runner'] == 'pipeline'
    assert out['root'] == 'rootB'
