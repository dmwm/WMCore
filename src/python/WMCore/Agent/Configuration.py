#!/usr/bin/env python
"""
_Configuration_

Module dealing with Agent Configuration file in python format

This code has been moded to WMCore.Configuration


"""

from WMCore.Configuration import ConfigSection as BaseConfigSection
from WMCore.Configuration import Configuration as BaseConfiguration
from WMCore.Configuration import loadConfigurationFile as baseLoadConfigurationFile
from WMCore.Configuration import saveConfigurationFile as baseSaveConfigurationFile


class ConfigSection(BaseConfigSection):
    """
    _ConfigSection_

    Chunk of configuration information

    """
    def __init__(self, name = None):
        BaseConfigSection.__init__(self, name)


class Configuration(BaseConfiguration):
    """
    _Configuration_

    Top level configuration object

    """
    def __init__(self):
        BaseConfiguration.__init__(self)
        self._internal_sections.append("Agent")
        self.Agent = ConfigSection("Agent")
        self.Agent.agentName = None
        self.Agent.teamName = None
        self.Agent.hostName = None
        self.Agent.contact = None

def loadConfigurationFile(filename):
    """
    _loadConfigurationFile_

    Load a Configuration File

    """
    assert filename != ""
    return baseLoadConfigurationFile(filename)

def saveConfigurationFile(configInstance, filename, **options):
    """
    _saveConfigurationFile_

    Save the configuration as a python module
    Options controls the format of documentation

    comment = True means save docs as comments
    document = True means save docs as document_ calls


    """
    baseSaveConfigurationFile(configInstance, filename, **options)
