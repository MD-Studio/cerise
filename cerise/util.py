from typing import TYPE_CHECKING, Type

# See https://stackoverflow.com/questions/49959656/
# typing-exit-in-3-5-fails-on-runtime-but-typechecks
if TYPE_CHECKING:
    BaseExceptionType = Type[BaseException]
else:
    BaseExceptionType = None
