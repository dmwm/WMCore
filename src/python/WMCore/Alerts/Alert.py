#!/usr/bin/env python
# encoding: utf-8
"""
Alert.py

Created by Dave Evans on 2011-02-24.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os



class Alert(dict):
    """
    _Alert_
    
    Alert structure
    
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
    

        