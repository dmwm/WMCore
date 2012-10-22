#!/usr/bin/env python
# encoding: utf-8
"""
MulticoreCMSSW.py

Implement Multicore CMSSW Support in parallel to main CMSSW support while its still new and exciting

Created by Dave Evans on 2010-11-11.
Copyright (c) 2010 Fermilab. All rights reserved.
"""



from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSW, CMSSWStepHelper


class MulticoreCMSSWHelper(CMSSWStepHelper):
    """
    _MulticoreCMSSWHelper_

    Same as the normal helper but with utils for handling multicore settings

    """

    def setMulticoreCores(self, ncores):
        """
        _setMulticoreCores_

        Preset the number of cores for CMSSW to run on, expect this to dribble away
        as batch systems get better at dynamic discovery etc, or be used as an override for
        testing
        """
        self.data.application.multicore.numberOfCores = ncores

    def numberOfCores(self):
        """
        _numberOfCores_

        Get number of cores
        """
        return self.data.application.multicore.numberOfCores

class MulticoreCMSSW(CMSSW):
    """
    _MulticoreCMSSW_

    Add fields for multicore support to the basic CMSSW template
    """


    def install(self, step):
        """
        _install_

        Install normal CMSSW fields plus the multicore information
        """
        CMSSW.install(self, step)
        step.stepType = "MulticoreCMSSW"
        step.application.section_("multicore")
        step.application.multicore.numberOfCores = 1
        step.application.multicore.inputfilelist = "input.filelist"
        step.application.multicore.inputmanifest = "manifest.json"
        step.application.multicore.edmFileUtil = "edmFileUtil --JSON -F input.filelist > manifest.json"


    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return MulticoreCMSSWHelper(step)
