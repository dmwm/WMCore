from WMCore.Configuration import Configuration
import os

config = Configuration()

jsm = config.component_('JobStateMachine')

if (os.getenv('COUCHURL') != None):
    couchurl = os.getenv('COUCHURL')
else:
    couchurl = 'localhost:5984'

jsm.couchurl = couchurl
jsm.default_retries = 1