from WMCore.WMException import WMException

class WMSpecFactoryException(WMException):
    """
    _WMSpecFactoryException_

    This exception will be raised by validation functions if
    the code fails validation.  It will then be changed into
    a proper HTTPError in the ReqMgr, with the message you enter
    used as the message for farther up the line.
    """
    pass
