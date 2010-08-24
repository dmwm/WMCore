#!/usr/bin/python
"""
_WMExceptions_

List of standard exception ids and their
mappings to a human readable message.

"""

__revision__ = "$Id: WMExceptions.py,v 1.4 2008/08/26 11:05:28 fvlingen Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "fvlingen@caltech.edu"


WMEXCEPTION = {'WMCore-1' : 'Not allowed to instantiate ', \
   'WMCore-2' : 'Problem creating database table ',\
   'WMCORE-3' : 'Could not find in library class ',\
   'WMCORE-4' : 'Problem with loading the class ',\
   'WMCORE-5' : 'Dialect not specified in configuration ',\
   'WMCORE-6' : 'Message name is same as diagnostic message name! ',}
