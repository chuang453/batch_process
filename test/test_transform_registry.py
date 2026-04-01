import pandas as pd

from stage2_platform.execution.transform_registry import (
    STAGE2_TRANSFORMS,
    apply_transform,
    df_transform,
    register_transform,
)


def test_df_transform_registration_and_apply():
    @df_transform(name='stage2_inc')
    def _inc(df, context, **kwargs):
        out = df.copy()
        out['v'] = out['v'] + 1
        return out

    df = pd.DataFrame({'v': [1, 2]})
    out = apply_transform('stage2_inc', df, context={})
    assert list(out['v']) == [2, 3]


def test_register_transform_and_metadata_flags():
    def _noop(df, context, **kwargs):
        return df

    register_transform('stage2_noop', _noop, vectorized=True, produces_multiple=False)
    assert 'stage2_noop' in STAGE2_TRANSFORMS
    assert getattr(STAGE2_TRANSFORMS['stage2_noop'], 'vectorized') is True
