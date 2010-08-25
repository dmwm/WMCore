#!/usr/bin/env python
"""
_InstallScenario_

Runtime script that installs the scenario based configuration PSet into the job

"""


from WMCore.WMRuntime.ScriptInterface import ScriptInterface

class InstallScenario(ScriptInterface):


    def __call__(self):

        print "InstallScenario"
        return 0
