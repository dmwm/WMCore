#!/usr/bin/env python
# encoding: utf-8
"""
WMBase.py

Created by Dave Evans on 2011-05-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import os.path
import inspect

def getWMBASE():
    """ returns the root of WMCore install """
    if __file__.find("src/python") != -1:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    else:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))

def getTestBase(importFlag = True):
    """
    _getTestBase_

    Returns a base that can be used for testing.  Defaults to
    getWMBase if no environment variables WMCORE_TEST_ROOT is defined
    """
    basePath = os.path.normpath(os.path.join(getWMBASE(), 'test/python'))
    envPath  = os.environ.get("WMCORE_TEST_ROOT", None)

    # First, try getting things from the environment
    if envPath != None:
        try:
            if os.path.isdir(envPath):
                return envPath
        except:
            pass

    if importFlag:
        # Then try importing things from WMCore_t and see if we can
        # find the directory
        try:
            import WMCore_t.__init__ as testImport
            testPath = os.path.dirname(inspect.getsourcefile(testImport))
            return os.path.normpath(os.path.join(testPath, '../'))
        except ImportError:
            pass

    return basePath
