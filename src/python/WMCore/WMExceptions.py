#!/usr/bin/python
"""
_WMExceptions_

List of standard exception ids and their
mappings to a human readable message.

"""

__revision__ = "$Id: WMExceptions.py,v 1.5 2008/08/29 19:00:15 fvlingen Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "fvlingen@caltech.edu"


WMEXCEPTION = {'WMCore-1' : 'Not allowed to instantiate ', \
   'WMCore-2' : 'Problem creating database table ',\
   'WMCORE-3' : 'Could not find in library class ',\
   'WMCORE-4' : 'Problem with loading the class ',\
   'WMCORE-5' : 'Dialect not specified in configuration ',\
   'WMCORE-6' : 'Message name is same as diagnostic message name! ',\
   'WMCORE-7' : 'Component name is reserved word. ',}
