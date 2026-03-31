"""processors package

Import builtin processors so tests and code can access them as
`processors.builtin_recorders`. Also re-export the processor registries
from `decorators.processor` for convenience.
"""

from decorators.processor import PROCESSORS, PRE_PROCESSORS, POST_PROCESSORS, TRANSFORMS

# import builtin modules (ensure they register their processors)
from . import builtin_recorders
from . import df_transforms
from . import file_ops
from . import plotting

__all__ = [
    "PROCESSORS",
    "PRE_PROCESSORS",
    "POST_PROCESSORS",
    "TRANSFORMS",
    "builtin_recorders",
    "df_transforms",
    "file_ops",
    "plotting",
]
