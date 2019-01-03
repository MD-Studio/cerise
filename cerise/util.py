from typing import Type, TYPE_CHECKING


# See https://stackoverflow.com/questions/49959656/typing-exit-in-3-5-fails-on-runtime-but-typechecks
if TYPE_CHECKING:
    BaseExceptionType = Type[BaseException]
else:
    BaseExceptionType = None
