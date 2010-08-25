#!/usr/bin/python
"""
_WMBase

Gets the base of the WM source tree
"""

__revision__ = "$Id: WMBase.py,v 1.1 2010/02/09 01:53:27 meloam Exp $"
__version__ = "$Revision: 1.1 $"


import os.path

def getWMBASE():
    """ returns the root of WMCore install """
    return os.path.normpath( os.path.join(os.path.dirname(__file__), '..', '..','..' ) )