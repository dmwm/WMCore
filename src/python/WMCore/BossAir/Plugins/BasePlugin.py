#!/usr/bin/env python
"""
_BasePlugin_

Base class for BossAir plugins
"""

from WMCore.WMException import WMException



class BossAirPluginException(WMException):
    """

    Generalized exception class for BossAir Plugins


    """




class BasePlugin:
    """
    Base class for BossAir Plugins

    Does nothing useful, just to be overridden later
    """


    # the common states which needs to be mapped from each plugIn states
    globalState = ['Pending', 'Running', 'Complete','Error']

    @staticmethod
    def verifyState(map):
        for state in map.values():
            if state not in BasePlugin.globalState:
                raise BossAirPluginException, "not valid state %s" % state
        return map

    @staticmethod
    def stateMap():
        raise NotImplementedError, "stateMap is not implemented"

    def __init__(self, config):

        self.config = config

        # NOTE: Don't overwrite this.
        # However stateMap should be implemented in child class.
        self.states = self.stateMap().keys()



    def submit(self, jobs, info = None):
        """
        _submit_

        Submits jobs
        """

        return


    def track(self, jobs):
        """
        _track_

        Tracks jobs
        Returns three lists:
        1) the running jobs
        2) the jobs that need to be updated in the DB
        3) the complete jobs
        """

        return jobs, jobs, []


    def complete(self, jobs):
        """
        _complete_

        Run any complete code
        """


        return


    def kill(self, jobs):
        """
        _kill_

        Kill any and all jobs
        """


        return
