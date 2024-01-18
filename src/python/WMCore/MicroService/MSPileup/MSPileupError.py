"""
File       : MSPileupError.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileupError represents MSPileup errors
"""

# system modules
import json

# WMCore modules
from WMCore.WMException import WMException


# MSPileup error codes
MSPILEUP_GENERIC_ERROR = 1
MSPILEUP_INVALID_ERROR = 2
MSPILEUP_DUPLICATE_ERROR = 3
MSPILEUP_NOTFOUND_ERROR = 4
MSPILEUP_DATABASE_ERROR = 5
MSPILEUP_UNIQUE_ERROR = 6
MSPILEUP_SCHEMA_ERROR = 7
MSPILEUP_FRACTION_ERROR = 8


class MSPileupError(WMException):
    """
    MSPileupError represents generic MSPileup error
    """
    def __init__(self, data, msg=""):
        """
        Constructor of MSPileupError class
        """
        super().__init__(self, msg)
        self.data = data
        self.assign(msg)

    def error(self):
        """
        JSON representation of MSPileupError

        :return: MSPileupError representation
        """
        edict = {'data': self.data, 'message': self.msg, 'code': self.code, 'error': 'MSPileupError'}
        return edict

    def __str__(self):
        """
        String representation of MSPileupError

        :return: human readable MSPileupError representation
        """
        return json.dumps(self.error(), indent=4)

    def assign(self, msg="", code=0):
        """
        Assign proper message and error codes

        :param msg: string
        :param code: int
        :return: None
        """
        if msg:
            self.msg = msg
        else:
            self.msg = 'Generic MSPileup error'
        if code > 0:
            self.code = code
        else:
            self.code = MSPILEUP_GENERIC_ERROR


class MSPileupGenericError(MSPileupError):
    """
    Generic MSPileup exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "generic error"
        self.assign(msg=msg, code=MSPILEUP_GENERIC_ERROR)


class MSPileupSchemaError(MSPileupError):
    """
    Schema MSPileup exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "schema error"
        self.assign(msg=msg, code=MSPILEUP_SCHEMA_ERROR)


class MSPileupFractionError(MSPileupError):
    """
    Schema MSPileup exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "container fraction error"
        self.assign(msg=msg, code=MSPILEUP_FRACTION_ERROR)


class MSPileupInvalidDataError(MSPileupError):
    """
    Create MSPileup exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "invalid data"
        self.assign(msg=msg, code=MSPILEUP_INVALID_ERROR)


class MSPileupDuplicateDocumentError(MSPileupError):
    """
    Duplicate document MSPileup exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "duplicate MSPileup document found"
        self.assign(msg=msg, code=MSPILEUP_DUPLICATE_ERROR)


class MSPileupNoKeyFoundError(MSPileupError):
    """
    No document found MSPileup exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "no document found"
        self.assign(msg=msg, code=MSPILEUP_NOTFOUND_ERROR)


class MSPileupDatabaseError(MSPileupError):
    """
    Database error MSPileup exception
    """
    def __init__(self, data, msg="", err=None):
        super().__init__(data, msg)
        msg = "Dataset error"
        if err:
            msg += f" err={err}"
        self.assign(msg=msg, code=MSPILEUP_DATABASE_ERROR)


class MSPileupUniqueConstrainError(MSPileupError):
    """
    UniqueConstrain error MSPileup exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "UniqueConstrainError"
        self.assign(msg=msg, code=MSPILEUP_UNIQUE_ERROR)
