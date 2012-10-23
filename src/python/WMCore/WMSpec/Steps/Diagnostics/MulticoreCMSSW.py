#!/usr/bin/env python
# encoding: utf-8
"""
MulticoreCMSSW.py

Created by Dave Evans on 2010-12-13.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os


from WMCore.WMSpec.Steps.Diagnostics.CMSSW import CMSSW


class MulticoreCMSSW(CMSSW):
    """
    _MulticoreCMSSW_

    Diagnostic for Multicore CMSSW jobs
    """
    def __init__(self):
        CMSSW.__init__(self)
