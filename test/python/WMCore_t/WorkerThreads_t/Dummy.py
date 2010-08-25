#!/usr/bin/env python
"""
_Dummy_

A dummy class to mimic the component for tets.
"""




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
