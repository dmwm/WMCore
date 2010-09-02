#!/usr/bin/env python
"""
_BasePlugin_

Base class for BossAir plugins
"""

__version__ = "$Id: BossLiteAPI.py,v 1.14 2010/06/28 19:05:14 spigafi Exp $"
__revision__ = "$Revision: 1.14 $"







class BasePlugin:
    """
    Base class for BossAir Plugins

    Does nothing useful, just to be overridden later
    """



    def __init__(self, config):

        self.config = config

        # NOTE: This MUST be overridden in the plugin
        # MUST MUST MUST
        self.states = []



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
        """

        return


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
