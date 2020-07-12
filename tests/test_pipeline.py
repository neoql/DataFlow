import dataflow as dflow


def test_basic_flow():
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

    assert len(pipe._cache_for_route) == 0
    assert pipe.product('g', inputs) == g
    assert len(pipe._cache_for_route) == 1
    assert pipe.product(['e', 'f', 'g'], inputs) == (e, f, g)
    assert len(pipe._cache_for_route) == 2

    assert pipe.exec(add, inputs) == g


def test_circle_flow():
    flow = dflow.DataFlow()

    @flow.factory(requires=['a', 'b'], provides=['c'])
    def compute_c(a, b):
        return a + b

    @flow.factory(requires=['e'], provides=['b'])
    def compute_e(b):
        return b + 1

    @flow.factory(requires=['c', 'd'], provides=['e'])
    def compute_e(c, d):
        return c * d

    pipe = dflow.SerialPipeline(flow)

    err = None
    try:
        pipe.product('e', inputs=dict(a=1, d=2))
    except ValueError as e:
        err = e

    assert type(err) == dflow.CircularDependence


def test_flow_with_filter():
    flow = dflow.DataFlow()

    @flow.factory(requires=['a', 'b'], provides='c')
    def div(a, b):
        return a / b

    @flow.filter('b')
    def filter_zero(b):
        return b if b else 1e-7

    pipe = dflow.SerialPipeline(flow)

    assert pipe.product('c', dict(a=4, b=2)) == 2
    assert pipe.product('c', dict(a=1, b=0)) != float('inf')

    flow = dflow.DataFlow()

    @flow.factory(requires=['a', 'b'], provides='c')
    def div(a, b):
        return a / b

    @flow.filter(['a', 'b'])
    def filter_zero(a, b):
        return a, (b if b else 1e-7)

    pipe = dflow.SerialPipeline(flow)

    assert pipe.product('c', dict(a=0, b=2)) == 0
    assert pipe.product('c', dict(a=0, b=0)) == 0


def test_multi_output():
    flow = dflow.DataFlow()

    @flow.factory(requires='a', provides=['b', 'c'])
    def get_b_c(a):
        return a+1, a+2

    @flow.factory(requires=['b', 'c'], provides='d')
    def get_d(b, c):
        return b * c

    pipe = dflow.SerialPipeline(flow)

    assert pipe.product('d', dict(a=1)) == 6


def test_missing_inputs():
    flow = dflow.DataFlow()

    @flow.factory(requires=['e', 'd'], provides='g')
    def compute_g(e, f):
        return e + f

    @flow.factory(requires=['a', 'f'], provides=['e'])
    def compute_e(a, f):
        return a * f

    @flow.factory(requires=['b', 'c'], provides='f')
    def compute_f(b, c):
        return b - c

    pipe = dflow.SerialPipeline(flow)

    inputs = dict(a=1, b=2, c=3, d=4)
    assert pipe.product('g', inputs) == 3

    del inputs['c']
    err = None
    try:
        pipe.product('g', inputs)
    except TypeError as e:
        err = e

    assert isinstance(err, TypeError)


def test_complex_flow():
    flow = dflow.DataFlow()

    @flow.filter(['a', 'b'])
    def filter_a_b(a, b):
        return a*2, b+1

    @flow.filter('c')
    def filter_c(_):
        return 0

    @flow.factory(requires=['a', 'b'], provides='g')
    def compute_g(a, b):
        return a * b

    @flow.factory(requires=['g', 'b'], provides='h')
    def compute_h(g, b):
        return g + b

    @flow.factory(requires=['c', 'd'], provides='i')
    def compute_i(c, d):
        return c - d

    @flow.factory(requires=['d', 'k'], provides='j')
    def compute_j(d, k):
        return d * k * 2

    @flow.factory(requires=['e', 'f'], provides='k')
    def compute_k(e, f):
        return e * f / 5

    @flow.factory(requires=['h', 'i'], provides='l')
    def compute_l(h, i):
        return h / i

    @flow.factory(requires=['i', 'd', 'j', 'k'], provides=['m', 'n'])
    def compute_m(i, d, j, k):
        return i + d + j, j / k

    inputs = dict(a=1, b=2, c=3, d=4, e=5, f=6)
    a, b, c, d, e, f = 1, 2, 3, 4, 5, 6

    a, b = filter_a_b(a, b)
    c = filter_c(c)

    g = compute_g(a, b)
    h = compute_h(g, b)
    i = compute_i(c, d)
    k = compute_k(e, f)
    j = compute_j(d, k)
    l = compute_l(h, i)
    m, n = compute_m(i, d, j, k)

    pipe = dflow.SerialPipeline(flow)

    assert pipe.product(['l', 'm', 'n'], inputs) == (l, m, n)
