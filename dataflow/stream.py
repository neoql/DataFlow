import os

from abc import ABCMeta, abstractmethod
from typing import Iterable, Dict, Any, Sequence, Mapping, Optional


class InStream(metaclass=ABCMeta):
    @abstractmethod
    def iter_items(self) -> Iterable[Dict[str, Any]]:
        pass


class OutStream(metaclass=ABCMeta):
    @abstractmethod
    def put_item(self, item: Dict[str, Any]):
        pass

    @property
    @abstractmethod
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
        self._filename = filename
        self._csv = None
        self._sep = sep
        self._cols_title = None

    def _open_csv(self):
        self._csv = open(self._filename, 'r')

    def _read_cols_title(self):
        line = self._csv.readline().strip()
        return line.split(self._sep)

    def iter_items(self) -> Iterable[Dict[str, Any]]:
        if self._csv is None:
            self._open_csv()

        self._csv.seek(0)
        self._cols_title = self._read_cols_title()
        for i, line in enumerate(self._csv):
            vals = line.strip().split(self._sep)
            item = {k: v for k, v in zip(self._cols_title, vals)}
            yield item

    def close(self):
        if self._csv is None:
            return
        self._csv.close()


class CsvWriteStream(OutStream, CtxCloser):
    def __init__(self,
                 filename: str,
                 cols: Sequence[str],
                 alias: Mapping[str, str] = None,
                 sep: str = ',',
                 inc_id: Optional[str] = None,
                 max_buf_size: int = 20):
        # our name -> global name
        self._alias = alias if alias else {col: col for col in cols}
        self._requires = cols if alias is None else [alias.get(col, col) for col in cols]
        self._cols = cols
        self._inc_id = inc_id

        self._sep = sep
        self._filename = filename
        self._csv = None
        self._line_no = -1

        self._buf = []
        self._max_buf_size = max_buf_size

    def _open_csv(self):
        self._csv = self._create_csv(self._filename)

    # noinspection PyMethodMayBeStatic
    def _create_csv(self, filename: str):
        folder, _ = os.path.split(filename)
        os.makedirs(folder, exist_ok=True)
        return open(filename, 'w')

    def _write_title(self):
        if self._inc_id is None:
            cols = self._cols
        else:
            cols = [self._inc_id] + self._cols
        row = self._sep.join(cols)
        self._csv.write('{}\n'.format(row))
        self._csv.flush()

    def put_item(self, item: Dict[str, Any]):
        if self._csv is None:
            self._open_csv()
            self._write_title()
            self._line_no = 0

        alias = self._alias

        row = [str(item[alias.get(col, col)]) for col in self._cols]

        self._line_no += 1
        if self._inc_id is not None:
            row.insert(0, str(self._line_no))

        self._buf.append('{}\n'.format(self._sep.join(row)))
        self._check_buf()

    def _check_buf(self):
        if len(self._buf) > self._max_buf_size:
            self.flush()

    @property
    def requires(self) -> Sequence[str]:
        return self._requires

    def flush(self):
        if self._csv is None:
            return

        if len(self._buf) > 0:
            self._csv.writelines(self._buf)
            self._buf = []
        self._csv.flush()

    def close(self):
        if self._csv is None:
            return

        self.flush()
        self._csv.close()
