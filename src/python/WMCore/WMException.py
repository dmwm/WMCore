#!/usr/bin/python
"""
_WMException_

General Exception class for WM modules

"""

from builtins import bytes
from future.utils import viewitems

import inspect
import logging
import sys
import traceback

WMEXCEPTION_START_STR = "<@========== WMException Start ==========@>"
WMEXCEPTION_END_STR = "<@---------- WMException End ----------@>"


class WMException(Exception):
    """
    _WMException_

    Exception class which works out details of where
    it was raised.

    """

    def __init__(self, message, errorNo=None, **data):
        self.name = str(self.__class__.__name__)
        if type(message) == bytes:
            # Fix for the unicode encoding issue, see #8056 and #8403
            # interprets this string using utf-8 codec and ignoring any errors
            message = message.decode('utf-8', 'ignore')

        Exception.__init__(self, self.name, message)

        #  //
        # // Init data dictionary with defaults
        # //
        self.data = {}
        self.data.setdefault("ClassName", None)
        self.data.setdefault("ModuleName", None)
        self.data.setdefault("MethodName", None)
        self.data.setdefault("ClassInstance", None)
        self.data.setdefault("FileName", None)
        self.data.setdefault("LineNumber", None)
        if errorNo is None:
            self.data.setdefault("ErrorNr", 0)
        else:
            self.data.setdefault("ErrorNr", errorNo)

        self._message = message
        self.data.update(data)

        #  //
        # // Automatically determine the module name
        # //  if not set
        if self.data['ModuleName'] is None:
            try:
                frame = inspect.currentframe()
                lastframe = inspect.getouterframes(frame)[1][0]
                excepModule = inspect.getmodule(lastframe)
                if excepModule != None:
                    modName = excepModule.__name__
                    self.data['ModuleName'] = modName
            finally:
                frame = None

        # //
        # // Find out where the exception came from
        # //
        try:
            stack = inspect.stack(1)[1]
            self.data['FileName'] = stack[1]
            self.data['LineNumber'] = stack[2]
            self.data['MethodName'] = stack[3]
        finally:
            stack = None

        # //
        # // ClassName if ClassInstance is passed
        # //
        try:
            if self.data['ClassInstance'] != None:
                self.data['ClassName'] = self.data['ClassInstance'].__class__.__name__
        except Exception:
            pass

        # Determine the traceback at time of __init__
        try:
            self.traceback = "\n".join(traceback.format_tb(sys.exc_info()[2]))
        except Exception:
            self.traceback = "WMException error: Couldn't get traceback\n"

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
        for key, value in viewitems(data):
            self[key] = value
        return

    def xml(self):
        """create a xml string rep of this exception"""
        strg = "<Exception>\n"
        strg += "<Object>\n"
        strg += "%s\n" % self.name
        strg += "</Object>\n"
        strg += "<Message>\n"
        strg += self._message
        strg += "</Message>\n"
        strg += "<DataItems>\n"
        for key, value in viewitems(self.data):
            strg += "<DataItem>\n"
            strg += "<Key>\n"
            strg += str(key)
            strg += "</Key>\n"
            strg += "<Value>\n"
            strg += str(value)
            strg += "</Value>\n"
            strg += "</DataItem>\n"
        strg += "</DataItems>\n"
        strg += "</Exception>\n"
        logging.error(strg)
        return strg

    def __str__(self):
        """create a string rep of this exception"""
        # WARNING: Do not change this string - it is used to extract error from log
        strg = WMEXCEPTION_START_STR
        strg += "\nException Class: %s\n" % self.name
        strg += "Message: %s\n" % self._message
        for key, value in viewitems(self.data):
            strg += "\t%s : %s\n" % (key, value,)
        strg += "\nTraceback: \n"
        strg += self.traceback
        strg += '\n'
        strg += WMEXCEPTION_END_STR
        if type(strg) == bytes:
            # Fix for the unicode encoding issue, #8043
            strg = strg.decode('utf-8', 'ignore')
        return strg

    def message(self):
        return self._message
