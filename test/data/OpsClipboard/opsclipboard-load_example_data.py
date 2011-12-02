"""
OpsClipboard auxiliary development script.

Create a database in CouchDB, loads OpsClipboard couchapp and 
loads some sample requests.

"""

from WMCore.Database.CMSCouch import CouchServer, Database
import WMCore.RequestManager.OpsClipboard.Inject as OpsClipboard
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore_t.RequestManager_t.OpsClipboard_t import getTestRequests

#dbName = "opsclipboard_t"
dbName = "opsclipboard"
testInit = TestInitCouchApp(__file__, dropExistingDb=True)
testInit.setupCouch(dbName, "OpsClipboard") # also load the couchapp

requests, campaignIds, requestIds = getTestRequests(10)
OpsClipboard.inject(testInit.couchUrl, testInit.couchDbName, *requests)

# test
couch = Database(dbName, testInit.couchUrl)
results = couch.loadView("OpsClipboard", "all")
if len(results[u"rows"]) != 10:
    print "Error, something went wrong, incorrect number of requests."
else:
    print "OK"
