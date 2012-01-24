"""
OpsClipboard auxiliary development script.

Create a database in CouchDB, loads OpsClipboard couchapp and 
loads some sample requests.

TODO:
this is under development, #3113 for more details - add all automation and
    parametrization to this helper development script to be finished

"""

import os
import sys

from WMCore.Database.CMSCouch import CouchServer, Database
import WMCore.RequestManager.OpsClipboard.Inject as OpsClipboard
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore_t.RequestManager_t.OpsClipboard_t import getTestRequests

# TODO
# should be all parametrized ...  done like the alerts thing ... 
# and not dependent on testinit
# should check presence of db and also load couchapp

#couchUrl = "https://localhost:2000/couchdb/"
couchUrl = "https://maxadmwm.cern.ch/couchdb/"
#couchUrl = os.environ.get("COUCHURL")

print "couchUrl:", couchUrl

#dbName = "opsclipboard_t"
#dbName = "opsclipboard"
dbName = "ops_clipboard"

# need to implement the functionality of dropping the database and reloading
# and couchapp when testing with remote instance of couchdb
# including remove couchapp reloading?

# doesn't work for dropping a couchdb database via frontend, locally it does
# doens't even work for creating the database, likely nor for pushing the OpsClipboard couchapp
testInit = TestInitCouchApp(__file__, dropExistingDb=True)
testInit.setupCouch(dbName, "OpsClipboard") # also loads the couchapp


# if needing to create a request in the couch under a name that
# already exists in the ReqMgr (ticket 3113 - having to fiddle this way) - should all
# automatic and parametrized (applies to all above - couchurl, db name)
requestName = "testinguser_120123_173037_2711"
requests = [{u"RequestName": requestName, u"CampaignName": "campaign_2"}]
#requests, campaignIds, requestIdsrequests = getTestRequests(4)
numRequests = len(requests)

OpsClipboard.inject(couchUrl, dbName, *requests)

# test
couch = Database(dbName, couchUrl)
results = couch.loadView("OpsClipboard", "all")
if len(results[u"rows"]) != numRequests:
    print "Error, something went wrong, incorrect number of requests."
else:
    print "OK"