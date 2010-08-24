#!/usr/bin/env python
# pylint: disable-msg = R0903
"""
_SQLFactory_

A factory to create the appropriate WMBS database dialect object
for a given database engine. 

"""

__revision__ = "$Id: Factory.py,v 1.4 2008/05/29 16:39:35 metson Exp $"
__version__ = "$Revision: 1.4 $"

class SQLFactory(object):
    """
    _wmbsSQLFactory_
    
    Factory to create WMBS database instances. Could do something similar else where.
    
    """
    logger = ""
    def __init__(self, logger):
        print "THIS CLASS IS DEPRECATED!!"