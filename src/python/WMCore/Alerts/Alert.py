#!/usr/bin/env python
# encoding: utf-8

"""

Created by Dave Evans on 2011-02-24.
Copyright (c) 2011 Fermilab. All rights reserved.

"""



class Alert(dict):
    """
    Alert structure - alert message instance.
    
    """
    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("Level", 0)
        self.setdefault("Source", None)
        self.setdefault("Type", None)
        self.setdefault("Workload", None)
        self.setdefault("Component", None)
        self.setdefault("Details", {})
        self.setdefault("Timestamp", None)
        self.update(args)


    level = property(lambda x: x.get("Level"))

        
    
class RegisterMsg(dict):
    """
    Control message to register senders with Receiver instance.
    
    """
    
    key = u"Register"
    
    def __init__(self, label):
        dict.__init__(self)
        self[self.key] = label
        


class UnregisterMsg(dict):
    """
    Control message to unregister senders with Receiver instance.
    
    """
    
    key = u"Unregister"
    
    def __init__(self, label):
        dict.__init__(self)
        self[self.key] = label
        
        
        
class ShutdownMsg(dict):
    """
    Control message to shutdown the Receiver instance.
    
    """
    
    key = u"Shutdown"
    
    def __init__(self):
        dict.__init__(self)
        self[self.key] = True