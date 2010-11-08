#!/usr/bin/env python

"""
Defines default config values for HarvestingScheduler specific
parameters.
"""
__all__ = []



import os
import os.path

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_('HarvestingScheduler')
config.HarvestingScheduler.namespace = "WMComponent.HarvestingScheduler.HarvestingScheduler"
config.HarvestingScheduler.componentDir = os.getcwd() + "/HarvestingScheduler"
config.HarvestingScheduler.workloadCache = os.getcwd(), + "/HarvestingScheduler/workloadCache"
config.HarvestingScheduler.targetSite = 'srm-cms.cern.ch'
config.HarvestingScheduler.scramArch = "slc5_ia32_gcc434"
config.HarvestingScheduler.cmsPath = "/afs/cern.ch/cms/sw"
config.HarvestingScheduler.proxy = os.getenv("X509_USER_PROXY", "bad_proxy")
config.HarvestingScheduler.dqmGuiUrl = "https://cmsweb.cern.ch/dqm/dev"
config.HarvestingScheduler.couchurl = "http://127.0.0.1:5984"
config.HarvestingScheduler.couchDBName = "datasets_to_harvest"
config.HarvestingScheduler.doStageOut = True
config.HarvestingScheduler.doDqmUpload = True
config.HarvestingScheduler.phedexURL = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/'
config.HarvestingScheduler.dbsUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
config.HarvestingScheduler.expiryTime = 3600 * 24 * 30 # One month 
config.HarvestingScheduler.cooloffTime = 3600 * 24 # One day
config.HarvestingScheduler.pollInterval = 60

