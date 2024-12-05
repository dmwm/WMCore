"""
File       : MSTransferorError.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSTransferorError represents MSTransferor errors
"""

# system modules
import json

# WMCore modules
from WMCore.WMException import WMException


# MSTransferor error codes
MSPILEUP_GENERIC_ERROR = 1
MSPILEUP_STORAGE_ERROR = 2


class MSTransferorError(WMException):
    """
    MSTransferorError represents generic MSTransferor error
    """
    def __init__(self, data, msg=""):
        """
        Constructor of MSTransferorError class
        """
        super().__init__(self, msg)
        self.data = data
        self.assign(msg)

    def error(self):
        """
        JSON representation of MSTransferorError

        :return: MSTransferorError representation
        """
        edict = {'data': self.data, 'message': self.msg, 'code': self.code, 'error': 'MSTransferorError'}
        return edict

    def __str__(self):
        """
        String representation of MSTransferorError

        :return: human readable MSTransferorError representation
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
            self.msg = 'Generic MSTransferor error'
        if code > 0:
            self.code = code
        else:
            self.code = MSPILEUP_GENERIC_ERROR


class MSTransferorGenericError(MSTransferorError):
    """
    Generic MSTransferor exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "generic error"
        self.assign(msg=msg, code=MSPILEUP_GENERIC_ERROR)


class MSTransferorStorageError(MSTransferorError):
    """
    Storage MSTransferor exception
    """
    def __init__(self, data, msg=""):
        super().__init__(data, msg)
        msg = self.msg if self.msg else "storage error"
        self.assign(msg=msg, code=MSPILEUP_STORAGE_ERROR)
