import os

from abc import ABCMeta, abstractmethod
from typing import Generator, Dict, Any, Sequence, Mapping


class InStream(metaclass=ABCMeta):
    @abstractmethod
    def iter_items(self) -> Generator[Dict[str, Any]]:
        pass


class OutStream(metaclass=ABCMeta):
    @abstractmethod
    def put_item(self, item: Dict[str, Any]):
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


class CsvReadStream(InStream, CtxCloser):
    def __init__(self, filename: str, sep: str = ','):
        self._csv = open(filename, 'r')
        self._sep = sep
        self._cols_title = self._read_cols_title()

    def _read_cols_title(self):
        line = self._csv.readline().strip()
        return line.split(self._sep)

    def iter_items(self) -> Generator[Dict[str, Any]]:
        for line in self._csv:
            vals = line.strip().split(self._sep)
            item = {k: v for k, v in zip(self._cols_title, vals)}
            yield item

    def close(self):
        self._csv.close()


class CsvWriteStream(OutStream, CtxCloser):
    def __init__(self,
                 filename: str,
                 cols: Sequence[str],
                 alias: Mapping[str, str] = None,
                 sep: str = ',',
                 max_buf_size: int = 20):
        # our name -> global name
        self._alias = alias if alias else {col: col for col in cols}
        self._requires = cols if alias is None else [alias[col] for col in cols]
        self._cols = cols

        self._sep = sep
        self._csv = self._create_csv(filename)

        self._buf = []
        self._max_buf_size = max_buf_size

        self._write_title()

    # noinspection PyMethodMayBeStatic
    def _create_csv(self, filename: str):
        folder, _ = os.path.split(filename)
        os.makedirs(folder, exist_ok=True)
        return open(filename, 'w')

    def _write_title(self):
        row = self._sep.join(self._cols)
        self._csv.write('{}\n'.format(row))
        self._csv.flush()

    def put_item(self, item: Dict[str, Any]):
        alias = self._alias

        row = [item[alias[col]] for col in self._cols]

        self._buf.append('{}\n'.format(self._sep.join(row)))
        self._check_buf()

    def _check_buf(self):
        if len(self._buf) > self._max_buf_size:
            self.flush()

    @property
    def requires(self) -> Sequence[str]:
        return self._requires

    def flush(self):
        if len(self._buf) > 0:
            self._csv.writelines(self._buf)
            self._buf = []
        self._csv.flush()

    def close(self):
        self.flush()
        self._csv.close()
