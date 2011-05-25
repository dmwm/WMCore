#!/usr/bin/env python
# encoding: utf-8
"""
WMBase.py

Created by Dave Evans on 2011-05-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os


def getWMBASE():
    """ returns the root of WMCore install """
    if __file__.find("src/python") != -1:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    else:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))        

