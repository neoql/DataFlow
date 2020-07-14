import heapq
import multiprocessing as mp

from abc import ABCMeta, abstractmethod
from typing import Optional, Callable, Sequence

from .stream import InStream, OutStream
from .pipeline import Pipeline, SerialPipeline
from .flow import BaseDataFlow


class Producer(metaclass=ABCMeta):
    @abstractmethod
    def produce(self, flow: BaseDataFlow, ins: InStream, ous: OutStream):
        pass


class ParallelProducer(Producer):
    def __init__(self,
                 num_workers: int = 0,
                 pipe_cls: Optional[Callable[[BaseDataFlow], Pipeline]] = None):
        self._num_workers = num_workers if num_workers > 0 else mp.cpu_count()
        self._pipe_cls = pipe_cls if pipe_cls is not None else SerialPipeline

    def produce(self,
                flow: BaseDataFlow,
                ins: InStream,
                ous: OutStream,
                keep_order: bool = False,
                pbar: str = 'none'):
        pbar = pbar.lower()
        assert pbar in {'none', 'terminal', 'notebook'}

        mp.set_start_method('spawn')

        inq, ouq = mp.Queue(), mp.Queue()

        read_worker = mp.Process(target=self._read_worker, args=(ins, inq))
        produce_workers = [mp.Process(target=self._produce_worker, args=(flow, ous.requires, inq, ouq))
                           for _ in range(self._num_workers)]
        write_worker = mp.Process(target=self._write_worker, args=(ouq, ous, keep_order, pbar))

        write_worker.start()
        for w in produce_workers:
            w.start()
        read_worker.start()

        read_worker.join()
        for _ in range(len(produce_workers)):
            inq.put((-1, None))

        for w in produce_workers:
            w.join()

        ouq.put((-1, None))
        write_worker.join()

    # noinspection PyMethodMayBeStatic
    def _read_worker(self, ins: InStream, inq: mp.Queue):
        for i, item in enumerate(ins.iter_items()):
            inq.put((i, item))

    def _produce_worker(self,
                        flow: BaseDataFlow,
                        targets: Sequence[str],
                        inq: mp.Queue,
                        ouq: mp.Queue):
        pipe = self._pipe_cls(flow)

        while True:
            n, item = inq.get()
            if n < 0:
                break
            result = pipe.product(targets, item)
            out = {k: v for k, v in zip(targets, result)}
            ouq.put((n, out))

    # noinspection PyMethodMayBeStatic
    def _write_worker(self, ouq: mp.Queue, ous: OutStream, keep_order: bool, pbar_tp: str):
        try:
            # noinspection PyPackageRequirements,PyUnresolvedReferences
            from tqdm import tqdm, tqdm_notebook

            if pbar_tp == 'none':
                pbar_cls = _FakePbar
            elif pbar_tp == 'notebook':
                pbar_cls = tqdm_notebook
            elif pbar_tp == 'terminal':
                pbar_cls = tqdm
            else:
                raise ValueError('un-support pbar: {}'.format(pbar_tp))
        except ImportError:
            pbar_cls = _FakePbar

        buf = []
        offset = 0

        with pbar_cls() as pbar:
            while True:
                n, item = ouq.get()
                if n < 0:
                    break
                if not keep_order:
                    ous.put_item(item)
                else:
                    heapq.heappush(buf, (n, item))
                    while len(buf) > 0 and buf[0][0] == offset:
                        _, item = heapq.heappop(buf)
                        ous.put_item(item)
                        offset += 1
                pbar.update(1)


class _FakePbar(object):
    def __init__(self):
        self.n = 0

    def update(self, n):
        self.n += n

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
