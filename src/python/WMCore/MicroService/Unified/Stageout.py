"""
File       : UnifiedStageoutManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedStageoutManager class provides full functionality of the UnifiedStageout service.
"""

# futures
from __future__ import print_function, division


class UnifiedStageoutManager(object):
    """
    Initialize UnifiedStageoutManager class
    """
    def __init__(self, config=None):
        self.config = config

    def status(self):
        "Return current status about UnifiedStageout"
        sdict = {}
        return sdict

    def request(self, **kwargs):
        "Process request given to UnifiedStageout"
        return {}
