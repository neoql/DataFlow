import dataflow as dflow
import typing


# noinspection PyTypeChecker
def test_filter():
    flow = dflow.DataFlow()
    print('here')

    @flow.filter("number")
    def number_filter(x: int) -> int:
        return x * x

    @flow.filter("name")
    def name_filter(name: str) -> str:
        return '{}{}'.format(name[0].upper(), name[1:].lower())

    assert number_filter(2) == 4
    assert flow.operation(number_filter)(2) == 4
    assert flow.operation(number_filter).flow is flow
    assert typing.cast(dflow.Filter, flow.operation(number_filter)).fields == ('number',)

    assert name_filter('tOM') == 'Tom'
    assert flow.operation(name_filter)('tOM') == 'Tom'
    assert flow.operation(name_filter).flow is flow
    assert typing.cast(dflow.Filter, flow.operation(name_filter)).fields == ('name',)

    assert flow._filters['name'] == flow.operation(name_filter)
    assert flow._filters['number'] == flow.operation(number_filter)


def test_factory():
    pass
