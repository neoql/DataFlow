import dataflow as dflow


def test_serial_pipeline():
    flow = dflow.DataFlow()

    @flow.factory(requires=['a', 'b'], provides=['e'])
    def multiply(a, b):
        return a * b

    @flow.factory(requires=['c', 'd'], provides=['f'])
    def division(c, d):
        return c / d

    @flow.factory(requires=['e', 'f'], provides=['g'])
    def add(e, f):
        return e + f

    pipe = dflow.SerialPipeline(flow)

    a, b, c, d = 1, 2, 3, 4
    e = a * b
    f = c / d
    g = e + f

    inputs = dict(a=1, b=2, c=3, d=4)
    assert pipe.product('g', inputs) == g
    assert pipe.product(['e', 'f', 'g'], inputs) == [e, f, g]
