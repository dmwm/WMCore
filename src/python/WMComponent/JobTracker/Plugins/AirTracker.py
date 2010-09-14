#!/bin/env python

# Prototype for BossAir


__revision__ = "$Id: CondorTracker.py,v 1.2 2010/06/03 21:32:04 mnorman Exp $"
__version__ = "$Revision: 1.2 $"


import logging
import os

import subprocess
import re
import time


from WMCore.BossAir.BossAirAPI import BossAirAPI

from WMComponent.JobTracker.Plugins.TrackerPlugin  import TrackerPlugin

class AirTracker(TrackerPlugin):
    """
    _AirTracker_

    Plugin layer for bossAir (to be replaced by changes
     to JobTrackerPoller)
    """



    def __init__(self, config):

        TrackerPlugin.__init__(self, config)
        self.bossAir = BossAirAPI(config = config)

        return


    def __call__(self):
        """
        __call__
        
        Actually run the BossAir API and see what's ready
        for the tracker to handle
        """


        return self.bossAir.getComplete()


