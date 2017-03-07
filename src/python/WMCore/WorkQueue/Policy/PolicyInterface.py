#!/usr/bin/env python
"""
WorkQueue PolicyInterface

"""
from builtins import object
from copy import deepcopy

__all__ = []



class PolicyInterface(object):
    """Interface for policies"""
    def __init__(self, **args):
        self.args = deepcopy(args)
