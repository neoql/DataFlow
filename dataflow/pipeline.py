from abc import ABCMeta, abstractmethod
from typing import Union, Mapping, Any, Sequence, List, Tuple, Set

from .flow import BaseDataFlow, Filter, Factory
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

        self._route_cache = {}

    def exec(self, fn: Union[Filter, Factory], inputs: Mapping[str, Any], return_dict: bool = False) -> Any:
        pass

    def product(self, target: Union[str, Sequence[str]], inputs: Mapping[str, Any]) -> Any:
        assert target is not None and inputs is not None
        targets = _trans_str_seq(target)
        assert len(target) > 0

        init, route = self._search_route(targets, inputs)

    def _search_route(self,
                      targets: Sequence[str],
                      inputs: Mapping[str, Any]) -> Tuple[Set[str], List[Filter]]:
        provided = {field for field in inputs.keys()}
        route = []
        requires = [t for t in targets]
        blocked = {}
        visits = set()
        init = set()

        flow = self.flow

        while len(requires) > 0:
            field = requires.pop()
            if field in inputs:
                continue

            if field in visits:
                raise CircularDependence(field)
            visits.add(field)

            try:
                new_factory = flow.get_provides(field)
                update = [new_factory]
            except KeyError:
                raise KeyError('No factory can produce {}.'.format(field))

            while len(update) > 0:
                # noinspection PyUnresolvedReferences
                factory = update.pop()

                ready = True
                for f in factory.requires:
                    if f not in provided:
                        blocked[f] = factory
                    ready = ready and (f not in provided)

                if ready:
                    route.append(factory)
                    init.union(f for f in factory.requires if f in inputs)
                    provided.union(factory.provides)
                    for f in factory.provides:
                        if f not in blocked:
                            continue
                        # noinspection PyUnresolvedReferences
                        update.append(blocked[f])
                        del blocked[f]
                elif factory is new_factory:
                    requires += [f for f in factory.requires if f not in provided]

        return init, route


class CircularDependence(Exception):
    def __init__(self, field: str):
        super(CircularDependence, self).__init__('{} field circular dependence.'.format(field))
