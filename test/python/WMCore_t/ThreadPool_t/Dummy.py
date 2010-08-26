#!/usr/bin/env python
"""
_Dummy_

A dummy class to mimic the component for tets.
"""

__revision__ = "$Id: Dummy.py,v 1.1 2008/09/25 13:14:02 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"

import logging

class Dummy:
    """
    _Dummy_

    A dummy class to mimic the component for tets.
    """

    def __init__(self):
        logging.debug("I am a dummy object!")
        self.config = None
