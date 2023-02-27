#!/usr/bin/env python3
"""
DBSErrors represents generic class to handle DBS Go server errors
"""


import json

from WMCore.Services.DBS.ProdException import ProdException


def formatEx(excepInst):
    """
    _formatEx_

    given a DbdException instance, generate a simple message from it
    """
    msg = "%s:%s %s" % (excepInst.__class__.__name__,
                        excepInst.getErrorMessage(),
                        excepInst.getErrorCode(),
                        )
    return msg


def formatEx3(excepInst):
    """
    _formatEx_

    given a DbdException instance, generate a simple message from it
    """
    msg = "%s:%s" % (excepInst.__class__.__name__, str(excepInst))
    return msg


class DataMgmtError(ProdException):
    """
    _DataMgmtError_

    General Exception from DataMgmt Interface

    """
    def __init__(self, message, errorNo=1000, **data):
        ProdException.__init__(self, message, errorNo, **data)


class DBSWriterError(DataMgmtError):
    """
    _DBSWriterError_

    Generic Exception for DBS Write Error

    """
    def __init__(self, msg, **data):
        DataMgmtError.__init__(self, msg, 1001, **data)


class DBSReaderError(DataMgmtError):
    """
    _DBSReaderError_

    Generic Exception for DBS Read Error

    """
    def __init__(self, msg, **data):
        DataMgmtError.__init__(self, msg, 1002, **data)


class DBSError():
    """
    DBSError provides generic interface for DBS (Go-based) errors
    """
    def __init__(self, ex):
        try:
            if hasattr(ex, "body"):
                # case of DBSClient HTTPError
                self.data = json.loads(ex.body)[0]
            else:
                self.data = json.loads(ex)[0]
        except Exception as exp:
            self.data = str(ex)

    def getHttpCode(self):
        """
        :return: HTTP error code
        """
        if isinstance(self.data, dict):
            return self.data['http']['code']
        return 0

    def getServerCode(self):
        """
        :return: DBS server error code which is defined here
-        https://github.com/dmwm/dbs2go/blob/master/dbs/errors.go
        """
        if isinstance(self.data, dict):
            return self.data['error']['code']
        return 0

    def getMessage(self):
        """
        :return: DBS server error message (consice output, last error in DBS error chain)
        """
        if isinstance(self.data, dict):
            return self.data['error']['message']
        return ""

    def getReason(self):
        """
        :return: DBS server error reason (expanded message)
        """
        if isinstance(self.data, dict):
            return self.data['error']['reason']
        return ""
