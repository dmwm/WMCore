#!/usr/bin/env python
"""
_Dummy_

A dummy class to mimic the component for tets.
"""

__revision__ = "$Id: Dummy.py,v 1.3 2008/09/09 13:50:36 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"

import logging

class Dummy:
    """
    _Dummy_

    A dummy class to mimic the component for tets.
    """

    def __init__(self):
        logging.debug("I am a dummy object!")
        self.config = None
