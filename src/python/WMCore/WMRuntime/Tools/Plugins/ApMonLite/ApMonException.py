#!/usr/bin/python
"""
_ApMonException_

General Exception class for JC modules

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: ApMonException.py,v 1.1 2010/05/17 20:42:29 mnorman Exp $"

import exceptions
import inspect
#import logging

class ApMonException(exceptions.Exception):
    """
    _ApMonException_

    Exception class which works out details of where
    it was raised.

    """
    def __init__(self, message, **data):
        self.name = str(self.__class__.__name__)
        exceptions.Exception.__init__(self, self.name,
                                      message)

        #  //
        # // Init data dictionary with defaults
        #// 
        self.data = {}
        self.data.setdefault("ClassName", None)
        self.data.setdefault("ModuleName", None)
        self.data.setdefault("MethodName", None)
        self.data.setdefault("ClassInstance", None)
        self.data.setdefault("FileName", None)
        self.data.setdefault("LineNumber", None)
        
        self.message = message
        self.data.update(data)

        #  //
        # // Automatically determine the module name
        #//  if not set
        if self['ModuleName'] == None:
            frame = inspect.currentframe()
            lastframe = inspect.getouterframes(frame)[1][0]
            excepModule = inspect.getmodule(lastframe)
            if excepModule != None:
                modName = excepModule.__name__
                self['ModuleName'] = modName

                
        #  //
        # // Find out where the exception came from
        #//
        stack = inspect.stack(1)[1]
        self['FileName'] = stack[1]
        self['LineNumber'] = stack[2]
        self['MethodName'] = stack[3]

        #  //
        # // ClassName if ClassInstance is passed
        #//
        if self['ClassInstance'] != None:
            self['ClassName'] = \
              self['ClassInstance'].__class__.__name__

        #logging.error(str(self))

    def __getitem__(self, key):
        """
        make exception look like a dictionary
        """
        return self.data[key]

    def __setitem__(self, key, value):
        """
        make exception look like a dictionary
        """
        self.data[key] = value
        
    def addInfo(self, **data):
        """
        _addInfo_

        Add key=value information pairs to an
        exception instance
        """
        for key, value in data.items():
            self[key] = value
        return


    def __str__(self):
        """create a string rep of this exception"""
        strg = "%s\n" % self.name
        strg += "Message: %s\n" % self.message
        for key, value in self.data.items():
            strg += "\t%s : %s\n" % (key, value, )
        return strg




