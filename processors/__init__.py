"""processors package

Import builtin processors so tests and code can access them as
`processors.builtin_recorders`. Also re-export the processor registries
from `decorators.processor` for convenience.
"""

from decorators.processor import PROCESSORS, PRE_PROCESSORS, POST_PROCESSORS

# import builtin modules (ensure they register their processors)
from . import builtin_recorders

__all__ = [
    "PROCESSORS",
    "PRE_PROCESSORS",
    "POST_PROCESSORS",
    "builtin_recorders",
]
