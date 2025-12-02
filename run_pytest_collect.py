import sys
import traceback

try:
    import pytest
    print('pytest version:', pytest.__version__)
    # run collect-only programmatically
    ret = pytest.main(['--collect-only', '-vv', 'batch_process/test'])
    print('pytest.main returned', ret)
except Exception as e:
    print('EXCEPTION RAISED')
    traceback.print_exc()
    sys.exit(2)
