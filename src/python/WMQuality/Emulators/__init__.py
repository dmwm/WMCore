"""
"""


import logging
import os
from WMCore.Configuration import loadConfigurationFile

def _getConfiguration():
    configFile = os.environ.get("EMULATOR_CONFIG")
    if configFile:
        return loadConfigurationFile(configFile)
    return None

def emulatorSwitch(name):
    try:
        config = _getConfiguration()
        if config:
            if getattr(config.Emulator, name, False):
                logging.info("Using %s Emulator ..." %  name)
                return True
        return False

    except Exception as e:
        logging.warning("Emulator Config is not set:  %s"  % str(e))
        return False
