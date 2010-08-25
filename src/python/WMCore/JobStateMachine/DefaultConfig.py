from WMCore.Configuration import Configuration
from os import environ

config = Configuration()

config.component_('JobStateMachine')
config.JobStateMachine.couchurl = 'http://localhost:5984'