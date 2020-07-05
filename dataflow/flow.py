from __future__ import annotations

from typing import Union, Sequence, Callable, Dict

from .utils import _trans_str_seq


class BaseDataFlow(object):
    def __init__(self):
        self._filters: Dict[str, Filter] = {}
        self._provides: Dict[str, Factory] = {}

    def append_filter(self, fields: Union[str, Sequence[str]], fn: Callable):
        flt = Filter(self, fields, fn)

        for field in flt.fields:
            self._filters[field] = flt

        return flt

    def append_factory(self,
                       requires: Union[str, Sequence[str]],
                       provides: Union[str, Sequence[str]],
                       fn: Callable):
        factory = Factory(self, requires, provides, fn)

        for field in factory.provides:
            self._provides[field] = factory

        return factory

    def get_filter(self, field: str) -> Filter:
        return self._filters[field]

    def get_provides(self, field) -> Factory:
        return self._provides[field]


class Filter(object):
    def __init__(self, flow: BaseDataFlow, fields: Union[str, Sequence[str]], fn: Callable):
        assert flow is not None
        assert fields is not None
        fields = _trans_str_seq(fields)
        assert len(fields) >= 1
        assert callable(fn)

        self._flow = flow
        self._fields = fields
        self._fn = fn

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    @property
    def flow(self):
        return self._flow

    @property
    def fields(self):
        return self._fields


class Factory(object):
    def __init__(self,
                 flow: BaseDataFlow,
                 requires: Union[str, Sequence[str]],
                 provides: Union[str, Sequence[str]],
                 fn: Callable):
        assert flow is not None
        assert requires is not None
        assert provides is not None
        assert callable(fn)

        requires = _trans_str_seq(requires)
        provides = _trans_str_seq(provides)
        assert len(requires) >= 1 and len(provides) >= 1

        self._flow = flow
        self._requires = requires
        self._provides = provides
        self._fn = fn

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    @property
    def flow(self):
        return self._flow

    @property
    def requires(self):
        return self._requires

    @property
    def provides(self):
        return self._provides


class DataFlow(BaseDataFlow):
    def __init__(self):
        super(DataFlow, self).__init__()

    def filter(self, fields: Union[str, Sequence[str]]) -> Filter:
        return lambda fn: self.append_filter(fields, fn)

    def factory(self,
                requires: Union[str, Sequence[str]],
                provides: Union[str, Sequence[str]]) -> Factory:
        return lambda fn: self.append_factory(requires, provides, fn)
