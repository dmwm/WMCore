#!/usr/bin/env python
"""
_BossAirPlugin_

Base class for BossAir plugins
"""

__version__ = "$Id: BossLiteAPI.py,v 1.14 2010/06/28 19:05:14 spigafi Exp $"
__revision__ = "$Revision: 1.14 $"



from WMCore.BossAir.Plugins.BasePlugin import BasePlugin



class TestPlugin(BasePlugin):
    """
    Test implementation of BasePlugin
    
    Does nothing
    """

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
