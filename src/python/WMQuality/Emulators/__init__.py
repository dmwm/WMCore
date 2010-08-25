"""
"""
__revision__ = "$"
__version__ = "$"
import logging
import os
from WMCore.Configuration import loadConfigurationFile

def _getConfiguration():
    configFile = os.environ["EMULATOR_CONFIG"]
    config = loadConfigurationFile(configFile)
    return config
    
def emulatorSwitch(name):
    try:
        config = _getConfiguration()
         
        if getattr(config.Emulator, name, False):
            logging.info("Using %s Emulator" %  name)
            return True
        else:
            return False    
    
    except Exception, e:
        logging.warning("Emulator Config is not set:  %s"  % str(e))
        return False
        
        
    
    