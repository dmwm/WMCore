#!/usr/bin/env python
# encoding: utf-8
"""
Decorators.py

Decorator Utils for GroupUser modules

Created by Dave Evans on 2010-07-20.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

def requireConnection(funcRef):
    """
    _requireConnection_

    Decorator method to connect the function's class object to couch
    """
    def wrapper(self, *args, **opts):
        if not self.connected:
            self.connect()
        return funcRef(self, *args, **opts)
    return wrapper

def requireGroup(funcRef):
    """
    _requireGroup_

    Decorator method to enforce setting a group attribute

    """
    def wrapper(self, *args, **opts):
        if getattr(self, "group", None) == None:
            msg = "Group Attribute not present/set"
            raise RuntimeError(msg)
        return funcRef(self, *args, **opts)
    return wrapper


def requireUser(funcRef):
    """
    _requireUser_

    Decorator method to enforce setting a user attribute for another
    object
    """
    def wrapper(self, *args, **opts):
        if getattr(self, "owner", None) == None:
            msg = "Group Attribute not present/set"
            raise RuntimeError(msg)
        return funcRef(self, *args, **opts)
    return wrapper
