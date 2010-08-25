#!/usr/bin/env python

from ProdAgentCore.Configuration import loadProdAgentConfiguration

def loadLSFConfig():	
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex :
        msg = "Error reading configuration: \n"
        msg += str(ex) 
        logging.error(msg)
        raise RuntimeError, msg
	    
    if config.has_key("LSF"):
       try:
           lsfConfig = config.getConfig('LSF')
           return lsfConfig 
       except StandardError, ex:
           msg = "Error reading ocnfiguration for LSF: \n"
           msg += str(ex)
           logging.error(msg)
           raise RuntimeError, msg
	       
    return None       
	
