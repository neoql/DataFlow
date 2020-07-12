from abc import ABCMeta, abstractmethod, abstractproperty

from typing import Generator, Dict, Any, Sequence


class InStream(metaclass=ABCMeta):
    @abstractmethod
    def iter_items(self) -> Generator[Dict[str, Any]]:
        pass


class OutStream(metaclass=ABCMeta):
    @abstractmethod
    def put_item(self, item: Any):
        pass

    @abstractmethod
    @property
    def requires(self) -> Sequence[str]:
        pass


class Closer(metaclass=ABCMeta):
    @abstractmethod
    def close(self):
        pass


class CtxCloser(Closer, metaclass=ABCMeta):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
