#!/usr/bin/env python
"""
_Registry_

Plugin Registry Module for PilotManager plugins

A Manager implementation must be registered with a Unique name
with this registry by doing:


"""
import types
import logging

class Registry:
    """
    _Registry_

    Static Class that is used to contain the map of Creator object to
    creator name. Class level object provides singleton like behaviour
    
    """
    ManagerRegistry = {}
   
    def __init__(self):
        msg = "ResourceMananger.Registry should not be initialised"
        raise RuntimeError, msg
    

def registerManager(objectRef, name):
    """
    _registerManager_

    Register a new Manager with the name provided

    """
    logging.info("registerManager: %s"%name)
    logging.info(Registry.ManagerRegistry)
    if name in Registry.ManagerRegistry.keys():
        msg = "Duplicate Name used to registerManager object:\n"
        msg += "%s already exists\n" % name
        logging.error(msg)
        raise RuntimeError, msg
    if not type(objectRef) == types.ClassType:
        msg = "Error: Manager Plugin named: %s\n" % name
        logging.error(msg)
        raise RuntimeError, msg

    Registry.ManagerRegistry[name] = objectRef

    return

def retrieveManager(name):
    """
    _retrieveManager_

    Get the Manager object mapped to the name provided

    """
    if name not in Registry.ManagerRegistry.keys():
        msg = "Name: %s not a registered Manager\n" % name
        msg += "No object registered with that name in PilotManager Registry"
        logging.error(msg)
        raise RuntimeError, msg
    registeredObject = Registry.ManagerRegistry[name]
    return registeredObject()



