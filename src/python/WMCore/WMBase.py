#!/usr/bin/python
"""
_WMBase

Gets the base of the WM source tree
"""





import os.path

def getWMBASE():
    """ returns the root of WMCore install """
    return os.path.normpath( os.path.join(os.path.dirname(__file__), '..', '..','..' ) )