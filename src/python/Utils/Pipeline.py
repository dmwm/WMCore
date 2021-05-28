"""
File       : Pipeline.py
Description: Provides 2 basic classes:
             - Functor:  A class to create function calls from a function object
                         and arbitrary number of arguments
             - Pipeline: A class to provide building blocks for creating functional
                         pipelines for cumulative execution on an arbitrary object
"""

# futures
from __future__ import division, print_function

from builtins import object
from functools import reduce


class Functor(object):
    """
    A simple functor class used to construct a function call which later to be
    applied on an (any type) object.
    NOTE:
        It expects a function in the constructor and an (any type) object
        passed to the run or __call__ methods, which methods once called they
        construct and return the following function:
        func(obj, *args, **kwargs)
    NOTE:
        All the additional arguments which the function may take must be set in
        the __init__ method. If any of them are passed during run time an error
        will be raised.

    :func:
        The function to which the rest of the constructor arguments are about
        to be attached and then the newly created function will be returned.
        - The function needs to take at least one parameter since the object
        passed to the run/__call__ methods will always be put as a first
        argument to the function.

    :Example:

    def adder(a, b, *args, **kwargs):
        if args:
            print("adder args: %s" % args)
        if kwargs:
            print("adder kwargs: %s" % kwargs)
        res = a + b
        return res

    >>> x=Functor(adder, 8, 'foo', bar=True)
    >>> x(2)
    adder args: foo
    adder kwargs: {'bar': True}
    adder res: 10
    10

    >>> x
    <Pipeline.Functor instance at 0x7f319bbaeea8>


    """
    def __init__(self, func, *args, **kwargs):
        """
        The init method for class Functor
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self, obj):
        """
        The call method for class Functor
        """
        return self.run(obj)

    def run(self, obj):
        return self.func(obj, *self.args, **self.kwargs)


class Pipeline(object):
    """
    A simple Functional Pipeline Class: applies a set of functions to an object,
    where the output of every previous function is an input to the next one.
    """
    # NOTE:
    #    Similar and inspiring approaches but yet some different implementations
    #    are discussed in the following two links [1] & [2]. With a quite good
    #    explanation in [1], which helped a lot. All in all at the bottom always
    #    sits the reduce function.
    #    [1]
    #    https://softwarejourneyman.com/python-function-pipelines.html
    #    [2]
    #    https://gitlab.com/mc706/functional-pipeline

    def __init__(self, funcLine=None, name=None):
        """
        :funcLine: A list of functions or Functors of function + arguments (see
                   the Class definition above) that are to be applied sequentially
                   to the object.
                   - If any of the elements of 'funcLine' is a function, a direct
                   function call with the object as an argument is performed.
                   - If any of the elements of 'funcLine' is a Functor, then the
                   first argument of the Functor constructor is the function to
                   be evaluated and the object is passed as a first argument to
                   the function with all the rest of the arguments passed right
                   after it eg. the following Functor in the funcLine:

                   Functor(func, 'foo', bar=True)

                   will result in the following function call later when the
                   pipeline is executed:

                   func(obj, 'foo', bar=True)

        :Example:
            (using the adder function from above and an object of type int)

        >>> pipe = Pipeline([Functor(adder, 5),
                             Functor(adder, 6),
                             Functor(adder, 7, "extraArg"),
                             Functor(adder, 8, update=True)])

        >>> pipe.run(1)
        adder res: 6
        adder res: 12
        adder args: extraArg
        adder res: 19
        adder kwargs: {'update': True}
        adder res: 27
        """
        self.funcLine = funcLine or []
        self.name = name

    def getPipelineName(self):
        """
        __getPipelineName__
        """
        name = self.name or "Unnamed Pipeline"
        return name

    def run(self, obj):
        return reduce(lambda obj, functor: functor(obj), self.funcLine, obj)
