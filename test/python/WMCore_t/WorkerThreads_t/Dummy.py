#!/usr/bin/env python
"""
_Dummy_

A dummy class to mimic the component for tets.
"""

__revision__ = "$Id: Dummy.py,v 1.1 2009/02/05 22:40:10 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"

import logging

class DBCoreDummy:
    def __init__(self):
        self.dialect = "mysql"

class ConfigDummy:
    def __init__(self):
        self.CoreDatabase = DBCoreDummy()

class Dummy:
    """
    _Dummy_

    A dummy class to mimic the component for tets.
    """

    def __init__(self):
        logging.debug("I am a dummy object!")
        self.config = ConfigDummy()
