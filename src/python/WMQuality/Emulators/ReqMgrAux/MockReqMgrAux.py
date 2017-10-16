from __future__ import (division, print_function)
from WMCore.Agent.DefaultConfig import DEFAULT_AGENT_CONFIG

class MockReqMgrAux(object):
    """
    Version of Services/PhEDEx intended to be used with mock or unittest.mock
    """

    def __init__(self, *args, **kwargs):
        print("Using MockReqMgrAux")

    def getWMAgentConfig(self, agentName):
        """
        macking getWMAgentConfig returns default config.
        """
        return DEFAULT_AGENT_CONFIG
