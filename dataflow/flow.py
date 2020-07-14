from __future__ import annotations

from typing import Union, Sequence, Callable, Dict, Any

from .utils import _trans_str_seq


class BaseDataFlow(object):
    def __init__(self):
        self._filters: Dict[str, Filter] = {}
        self._provides: Dict[str, Factory] = {}

        self._fn_op = {}
        self._const = {}

    def append_filter(self,
                      fields: Union[str, Sequence[str]],
                      fn: Callable,
                      g: Union[None, str, Sequence[str]] = None):
        flt = Filter(self, fields, fn, g)

        for field in flt.fields:
            self._filters[field] = flt

        self._fn_op[fn] = flt

        return flt

    def append_factory(self,
                       requires: Union[str, Sequence[str]],
                       provides: Union[str, Sequence[str]],
                       fn: Callable,
                       g: Union[None, str, Sequence[str]] = None):
        factory = Factory(self, requires, provides, fn, g)

        for field in factory.provides:
            self._provides[field] = factory

        self._fn_op[fn] = factory

        return factory

    def get_filter(self, field: str) -> Filter:
        return self._filters[field]

    def get_provides(self, field) -> Factory:
        return self._provides[field]

    def operation(self, fn: Callable) -> Operation:
        return self._fn_op[fn]

    @property
    def const(self) -> Dict[str: Any]:
        return self._const


class Operation(object):
    def __init__(self, flow: BaseDataFlow, fn: Callable, g: Union[None, str, Sequence[str]] = None):
        assert flow is not None
        assert callable(fn)
        self._flow = flow
        self._fn = fn

        if g is None:
            self._require_const = []
        elif isinstance(g, str):
            self._require_const = [g]
        else:
            self._require_const = g

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    @property
    def flow(self):
        return self._flow

    @property
    def require_const(self):
        return self._require_const


class Filter(Operation):
    def __init__(self,
                 flow: BaseDataFlow,
                 fields: Union[str, Sequence[str]],
                 fn: Callable,
                 g: Union[None, str, Sequence[str]] = None):
        super(Filter, self).__init__(flow, fn, g)

        assert fields is not None
        fields = _trans_str_seq(fields)
        assert len(fields) >= 1
        self._fields = fields

    @property
    def fields(self):
        return self._fields


class Factory(Operation):
    def __init__(self,
                 flow: BaseDataFlow,
                 requires: Union[str, Sequence[str]],
                 provides: Union[str, Sequence[str]],
                 fn: Callable,
                 g: Union[None, str, Sequence[str]] = None):
        super(Factory, self).__init__(flow, fn, g)

        assert requires is not None
        assert provides is not None

        requires = _trans_str_seq(requires)
        provides = _trans_str_seq(provides)
        assert len(requires) >= 1 and len(provides) >= 1

        self._requires = requires
        self._provides = provides

    @property
    def requires(self):
        return self._requires

    @property
    def provides(self):
        return self._provides


class DataFlow(BaseDataFlow):
    def __init__(self):
        super(DataFlow, self).__init__()

    def filter(self, fields: Union[str, Sequence[str]],
               g: Union[None, str, Sequence[str]] = None) -> Callable:
        def wrap(fn):
            self.append_filter(fields, fn, g)
            return fn
        return wrap

    def factory(self,
                requires: Union[str, Sequence[str]],
                provides: Union[str, Sequence[str]],
                g: Union[None, str, Sequence[str]] = None) -> Callable:
        def wrap(fn):
            self.append_factory(requires, provides, fn, g)
            return fn
        return wrap
