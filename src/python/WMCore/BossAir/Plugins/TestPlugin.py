#!/usr/bin/env python
"""
_BossAirPlugin_

Base class for BossAir plugins
"""


from WMCore.BossAir.Plugins.BasePlugin import BasePlugin



class TestPlugin(BasePlugin):
    """
    Test implementation of BasePlugin

    Does nothing
    """
    @staticmethod
    def stateMap():
        """
        For a given name, return a global state


        """

        stateDict = {'New': 'Pending',
                     'Gone': 'Error',
                     'Dead': 'Error'}

        # This call is optional but needs to for testing
        BasePlugin.verifyState(stateDict)
        return stateDict

    def __init__(self, config):

        BasePlugin.__init__(self, config)

        self.states = ['New', 'Dead', 'Gone']


    def submit(self, jobs, info = None):
        """
        Check and see if we have jobs

        """

        for job in jobs:
            #print job
            pass


        return jobs, []



    def track(self, jobs, info = None):
        """
        Label the jobs as done

        """

        return [], [], jobs
