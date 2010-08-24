#!/usr/bin/env python
"""
_Proxy_

Component that acts as a proxy between 
different PA instances. The current implementation
directly access the old message service interface using
a rewritten (schema compliant) (old) message service
interface integrated in a queue object. this interface
can be replaced by another transport mechanism should
that be necessary in the future.


"""
__all__ = []
__revision__ = "$Id: __init__.py,v 1.3 2008/09/29 16:10:56 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "fvlingen@caltech.edu"


