#!/usr/bin/env python
"""
WorkQueue PolicyInterface

"""
from copy import deepcopy

__all__ = []



class PolicyInterface:
    """Interface for policies"""
    def __init__(self, **args):
        self.args = deepcopy(args)
