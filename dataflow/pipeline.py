import typing

from abc import ABCMeta, abstractmethod
from typing import Union, Mapping, Any, Sequence, List, Tuple, AbstractSet, Set, Optional, Dict, Type

from .flow import BaseDataFlow, Filter, Factory, Operation
from .utils import _trans_str_seq


class Pipeline(metaclass=ABCMeta):
    def __init__(self, flow: BaseDataFlow):
        self._flow = flow

    @abstractmethod
    def exec(self,
             fn: Union[Filter, Factory],
             inputs: Mapping[str, Any],
             return_dict: bool = False) -> Any:
        pass

    @abstractmethod
    def product(self, target: Union[str, Sequence[str]], inputs: Mapping[str, Any]) -> Any:
        pass

    @property
    def flow(self):
        return self._flow


class SerialPipeline(Pipeline):
    def __init__(self, flow: BaseDataFlow):
        super(SerialPipeline, self).__init__(flow)

        self._cache_for_route = {}

    def exec(self, op: Type[Operation], inputs: Mapping[str, Any], return_dict: bool = False) -> Any:
        op = typing.cast(Union[Filter, Factory, Operation], op)

        op_type = type(op)

        if op_type == Filter:
            targets = op.fields
        elif op_type == Factory:
            targets = op.provides
        else:
            raise TypeError('unknown operation.'
                            'The current version only supports Filter and Factory')

        result = self.product(targets, inputs)

        if return_dict:
            return {k: v for k, v in zip(targets, result)}
        elif len(targets) == 1:
            return result[0]
        else:
            return result

    def product(self, target: Union[str, Sequence[str]], inputs: Mapping[str, Any]) -> Any:
        assert target is not None and inputs is not None
        targets = _trans_str_seq(target)
        assert len(target) > 0

        _, route = self._get_route(set(targets), inputs.keys())
        vals = self._exec_chain(inputs, route)

        if isinstance(target, str):
            return vals[target]
        else:
            return [vals[t] for t in targets]

    # noinspection PyMethodMayBeStatic
    def _exec_chain(self,
                    inputs: Mapping[str, Any],
                    route: Sequence[Operation]) -> Dict[str, Any]:
        vals = dict(inputs)

        for op in route:
            op = typing.cast(Union[Filter, Factory, Operation], op)
            if isinstance(op, Filter):
                kwargs = {field: vals[field] for field in op.fields}
                result = op(**kwargs)
                result = (result,) if len(op.fields) == 1 else result
                vals.update({field: v for field, v in zip(op.fields, result)})
            elif isinstance(op, Factory):
                kwargs = {field: vals[field] for field in op.requires}
                result = op(**kwargs)
                result = (result,) if len(op.provides) == 1 else result
                vals.update({field: v for field, v in zip(op.provides, result)})
            else:
                raise TypeError('unknown operation.'
                                'The current version only supports Filter and Factory')

        return vals

    def _get_route(self,
                   targets: AbstractSet[str],
                   inputs: AbstractSet[str]) -> Tuple[Set[str], List[Operation]]:
        init_and_route = self._get_cached_route(targets, inputs)

        if init_and_route is not None:
            return init_and_route

        init_and_route = self._search_route(targets, inputs)
        self._cache_route((targets, inputs), init_and_route)

        return init_and_route

    def _get_cached_route(self,
                          targets: AbstractSet[str],
                          inputs: AbstractSet[str]) -> Optional[Tuple[Set[str], List[Operation]]]:
        inputs = tuple(sorted(inputs))
        targets = tuple(sorted(targets))

        return self._cache_for_route.get((inputs, targets), None)

    def _cache_route(self,
                     inputs_and_targets: Tuple[AbstractSet[str], AbstractSet[str]],
                     init_and_route: Tuple[Set[str], Sequence[Operation]]):
        inputs, targets = inputs_and_targets
        inputs = tuple(sorted(inputs))
        targets = tuple(sorted(targets))

        self._cache_for_route[(inputs, targets)] = init_and_route

    def _search_route(self,
                      targets: AbstractSet[str],
                      inputs: AbstractSet[str]) -> Tuple[Set[str], List[Factory]]:
        init, factory_route = self._search_factory_route(targets, inputs)
        init, filter_route = self._search_filter_route(inputs, init)

        return init, filter_route + factory_route

    def _search_filter_route(self,
                             inputs: AbstractSet[str],
                             init: AbstractSet[str]) -> Tuple[Set[str], List[Filter]]:
        route = []
        requires = set(init)
        flow = self.flow

        for field in init:
            try:
                fltr = flow.get_filter(field)
                route += [fltr]
                requires = requires.union(flow.get_filter(field).fields)
            except KeyError:
                pass

        for field in requires:
            if field not in inputs:
                raise TypeError('missing inputs: {}'.format(field))

        return requires, route

    def _search_factory_route(self,
                              targets: AbstractSet[str],
                              inputs: AbstractSet[str]) -> Tuple[Set[str], List[Factory]]:
        init, route = set(), []

        requires = [t for t in targets]
        provided = {field for field in inputs}
        visited = set()
        factories = []

        flow = self.flow

        while len(requires) > 0:
            field = requires[-1]

            if field in inputs:
                init.add(field)

            if field in provided:
                requires.pop()
                continue

            if field in visited:
                raise CircularDependence(field)
            visited.add(field)

            try:
                new_factory = flow.get_provides(field)
                factories.append(new_factory)
                requires += new_factory.requires
            except KeyError:
                raise TypeError('No factory can produce {}.'.format(field))

            while len(factories) > 0:
                factory = factories[-1]

                ready = all(r in provided for r in factory.requires)
                if not ready:
                    break

                route.append(factory)
                provided = provided.union(factory.provides)
                factories.pop()

        return init, route


class CircularDependence(ValueError):
    def __init__(self, field: str):
        super(CircularDependence, self).__init__('{} field circular dependence.'.format(field))
