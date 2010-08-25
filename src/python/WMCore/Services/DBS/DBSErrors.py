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

    

class DBSWriterError(ProdException):
    """
    _DBSWriterError_

    Generic Exception for DBS Write Error

    """
    def __init__(self, msg, **data):
        DataMgmtError.__init__(self, msg, 1001, **data)



class DBSReaderError(ProdException):
    """
    _DBSReaderError_

    Generic Exception for DBS Read Error

    """
    def __init__(self, msg, **data):
        DataMgmtError.__init__(self, msg, 1002, **data)
    
