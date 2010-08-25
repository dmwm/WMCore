#!/usr/bin/env python
"""
WorkQueue PolicyInterface

"""
__all__ = []
__revision__ = "$Id: PolicyInterface.py,v 1.1 2009/12/02 13:52:44 swakef Exp $"
__version__ = "$Revision: 1.1 $"

class PolicyInterface:
    """Interface for policies"""
    def __init__(self, **args):
        self.args = args
