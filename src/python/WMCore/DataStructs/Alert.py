#!/usr/bin/env python
"""
_Alert_

Data structure that contains details about an alert.
"""

__revision__ = "$Id: Alert.py,v 1.1 2008/10/22 21:22:46 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import datetime

from WMCore.DataStructs.WMObject import WMObject

class Alert(WMObject, dict):
    """
    _Alert_
    
    Data structure that contains details about an alert.
    """
    def __init__(self, id = None, component = None, severity = None,
                 message = None, timestamp = None):
        """
        ___init___

        Initialize the four data members:
          id
          component
          severity
          message
          timestamp
        """
        dict.__init__(self)

        self.setdefault("id", id)
        self.setdefault("component", component)
        self.setdefault("severity", severity)
        self.setdefault("message", message)
        self.setdefault("timestamp", timestamp)        

        return

    def setID(self, id):
        """
        _setID_

        Set the ID for this alert.
        """
        pass

    def setComponent(self, component):
        """
        _setComponent_

        Set the name of the component that generated the alert.
        """
        pass

    def setSeverity(self, severity):
        """
        _setSeverity_

        Set the severity of the alert.
        """
        pass

    def setMessage(self, message):
        """
        _setMessage_

        Set the message explaining what the alert means.
        """
        pass

    def setTimestamp(self, timestamp):
        """
        _setTimestamp_

        Set the time at which the alert was generated.
        """
        pass

    def getID(self):
        """
        _getID_

        Retrieve the ID of the alert.
        """
        return self.id

    def getComponent(self):
        """
        _getComponent_

        Retrieve the name of the component that generated the alert.
        """
        return self.component

    def getSeverity(self):
        """
        _getSeverity_

        Retrieve the severity of the alert.
        """
        return self.severity

    def getMessage(self):
        """
        _getMessage_

        Retrieve the message that explains what the alert means.
        """
        return self.message

    def getTimestamp(self):
        """
        _getTimestamp_

        Retrieve the timestamp that signifies when the alert was generated.
        """
        return self.timestamp
