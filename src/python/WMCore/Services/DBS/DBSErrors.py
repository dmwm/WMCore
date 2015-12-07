#!/usr/bin/env python


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
    def __init__(self, message, errorNo = 1000 , **data):
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
