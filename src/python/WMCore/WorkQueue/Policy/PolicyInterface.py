#!/usr/bin/env python
"""
WorkQueue PolicyInterface

"""
import logging
from copy import deepcopy




class PolicyInterface:
    """Interface for policies"""
    def __init__(self, **args):
        self.logger = args.pop('logger') if args.get('logger') else logging.getLogger()
        self.args = deepcopy(args)
