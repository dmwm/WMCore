import unittest
import os

from WMCore.Configuration import loadConfigurationFile

class ConfigurationTest(unittest.TestCase):
    """
        primitive test for all example configuration in WMCore/etc
        Currently it is just testing syntax errors on configurations
        by importing the configuration.
        To do: extend to test semantic errors if necessary.

    """
    def setUp(self):
        self.configBase = "%s/etc" % os.environ["WMCORE_ROOT"]

    def testGlobalWorkQueueConfig(self):
        config = "%s/GlobalWorkQueueConfig.py" % self.configBase
        configObj = loadConfigurationFile(config)

    def testEmulaterConfig(self):
        config = "%s/EmulatorConfig.py" % self.configBase
        configObj = loadConfigurationFile(config)

    def testWMAgentConfig(self):
        config = "%s/WMAgentConfig.py" % self.configBase
        configObj = loadConfigurationFile(config)

    def testWMAgentConfigAgentOnly(self):
        config = "%s/WMAgentConfigAgentOnly.py" % self.configBase
        configObj = loadConfigurationFile(config)

if __name__ == "__main__":
    unittest.main()
