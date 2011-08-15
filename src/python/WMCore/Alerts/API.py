#!/usr/bin/env python

"""
API

Home of the Alert system API.

For now this just contains the two functions that are used to build a sendAlert
function.  In the WMComponents, the sendAlert function will handle the sending
of actual messages.

"""
import time
import logging

from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sender import Sender


def setUpAlertsMessaging(compInstance, compName = None):
    """
    Set up Alerts Sender instance, etc.
    Depends on provided configuration (general section 'Alert').
    Should not break anything if such config is not provided.
    
    compInstance is instance of the various agents components which
    set up alerts messaging. Details about the calling components
    are referenced through this variable (e.g. configuration instance).
    compName is string containing name of the component.
    
    Method is made static since not all components' classes
    participating in alerts messaging inherit from this class.
    
    """
    callerClassName = compInstance.__class__.__name__
    if hasattr(compInstance.config, "Alert"):
        # pre-defined values for Alert instances
        comp = compName or callerClassName 
        preAlert = Alert(Type = "WMAgent",
                         Workload = "n/a",
                         Component = comp,
                         Source = callerClassName)
        # create sender instance (sending alert messages)
        sender = Sender(compInstance.config.Alert.address,
                        callerClassName,
                        compInstance.config.Alert.controlAddr)
        sender.register()
        logging.debug("Alerts messaging set up for '%s'" % callerClassName)
        return preAlert, sender
    else:
        logging.debug("Alerts messaging not enabled for '%s'" % callerClassName)
        return None, None
        
        
def getSendAlert(sender, preAlert):
    """
    Common method taking care of sending Alert messages.
    It is silent should not the Alert framework be set up (sender
    would be None).
    preAlert is an Alert instance with predefined information.
    Level of the Alert messages is defined by level variable,
    other details are defined by the args dictionary.
    
    Method is made static since it is also called from classes
    which do not inherit from this class.
    
    """
    def sendAlertFunc(level, **args):
        if sender:
            alert = Alert(**preAlert)
            alert["Timestamp"] = time.time()
            alert["Level"] = level
            alert["Details"] = args
            sender(alert)
    return sendAlertFunc
