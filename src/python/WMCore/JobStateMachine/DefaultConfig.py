from WMCore.Configuration import Configuration
from os import environ

config = Configuration()

jsm = config.component_('JobStateMachine')
jsm.couchurl = 'http://localhost:5984'
jsm.default_retries = 1