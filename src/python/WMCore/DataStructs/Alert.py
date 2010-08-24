#!/usr/bin/env python
"""
_Alert_

Data structure that contains details about an alert.
"""

__revision__ = "$Id: Alert.py,v 1.2 2008/10/28 16:29:48 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DataStructs.WMObject import WMObject

class Alert(WMObject, dict):
    """
    _Alert_
    
    Data structure that contains details about an alert.
    """
    def __init__(self, alertID = None, component = None, severity = None,
                 message = None, timestamp = None):
        """
        ___init___

        Initialize the five data members:
          alertID
          component
          severity
          message
          timestamp
        """
        dict.__init__(self)
        WMObject.__init__(self)

        self.alertID = alertID
        self.component = component
        self.severity = severity
        self.message = message
        self.timestamp = timestamp        

        return

    def setID(self, alertID):
        """
        _setID_

        Set the ID for this alert.
        """
        self.alertID = alertID
        return

    def setComponent(self, component):
        """
        _setComponent_

        Set the name of the component that generated the alert.
        """
        self.component = component
        return

    def setSeverity(self, severity):
        """
        _setSeverity_

        Set the severity of the alert.
        """
        self.severity = severity
        return

    def setMessage(self, message):
        """
        _setMessage_

        Set the message explaining what the alert means.
        """
        self.message = message
        return

    def setTimestamp(self, timestamp):
        """
        _setTimestamp_

        Set the time at which the alert was generated.
        """
        self.timestamp = timestamp
        return

    def getID(self):
        """
        _getID_

        Retrieve the ID of the alert.
        """
        return self.alertID

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
