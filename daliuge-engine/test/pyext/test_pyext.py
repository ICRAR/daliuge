import sys, inspect, types, py.test
from pyext import *

def test_overload_argc():
    @overload.argc(1)
    def f(a): return 1
    @overload.argc(2)
    def f(a, b): return 2
    @overload.argc()
    def f(): return 0
    assert f() == 0
    assert f(1) == 1
    assert f(1, 2) == 2
    with py.test.raises(TypeError): f(1, 2, 3)
    assert len(inspect.getargspec(f).args) == 0

def test_overload_args():
    @overload.args(str, int)
    def f(a, b): return str, int
    @overload.args(int)
    def f(a): return int
    @overload.args(str)
    def f(a): return str
    @overload.args()
    def f(): return
    assert f() == None
    assert f(0) == int
    assert f('s') == str
    assert f('s', 0) == (str, int)
    with py.test.raises(TypeError): f(0, 's')
    assert len(inspect.getargspec(f).args) == 0
    class x(object):
        @overload.args(str, is_cls=True)
        def f(self, s): return 1
        @overload.args(int, is_cls=True)
        def f(self, i): return 2
    assert x().f('s') == 1
    assert x().f(1) == 2

def test_module():
    m = RuntimeModule('s', 'doc', x=1, f=2)
    assert m.x == 1
    assert m.f == 2
    assert isinstance(m, types.ModuleType)
    assert m.__doc__ == 'doc'
    m2 = RuntimeModule.from_string('s', 'doc', 'a=7; b=6')
    assert m2.a == 7
    assert m2.b == 6

def test_switch():
    with switch('x'):
        if case('x'): x = 4
        if case('b'): x = 2
        if case(1): x = 3
        if case('a'): x = 1
        if case('x'): x = 0
    assert x == 4
    with switch(1):
        if case.default(): x = 7
    assert x == 7
    with switch(2):
        if case(1,2): x = 9
    assert x == 9
    with switch('x', cstyle=True):
        if case('x'): x = 4
        if case('x'): x = 2; case.quit()
        if case('x'): x = 9
    assert x == 2

def test_annotate():
    @fannotate('r', a='a', b=1, c=2)
    def x(a, b, c): pass
    assert x.__annotations__ == {'a': 'a', 'b': 1, 'c': 2, 'return': 'r'}

def test_unpack():
    t = (1, 2, 3)
    assert safe_unpack(t, 2) == (1, 2)
    assert safe_unpack(t, 4) == (1, 2, 3, None)
    assert safe_unpack(t, 4, fill=0) == (1, 2, 3, 0)

def test_assign():
    assert assign('x', 7) == 7
    assert x == 7
    def f(): pass
    assert assign('f.__annotations__', {'a': 1}) == {'a': 1}
    assert f.__annotations__ == {'a': 1}

def test_compare_and_swap():
    global v
    v = None
    compare_and_swap('v', None, 7)
    assert v == 7
    compare_and_swap('v', None, 8)
    assert v == 7

if sys.version_info.major == 3:
    def test_overload_args_annot():
        def x(a, b): return 0
        x.__annotations__ = {'a': int, 'b': str}
        x = overload.args(None)(x)
        assert x(1, 's') == 0
        with py.test.raises(TypeError): x(1, 2)
