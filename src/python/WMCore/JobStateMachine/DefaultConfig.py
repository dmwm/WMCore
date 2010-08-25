from WMCore.Configuration import Configuration
import os

config = Configuration()

jsm = config.component_('JobStateMachine')

if (os.getenv('COUCHURL') != None):
    jsm.couchurl = os.getenv('COUCHURL')
else:
    jsm.couchurl = 'http://localhost:5984'
jsm.default_retries = 1