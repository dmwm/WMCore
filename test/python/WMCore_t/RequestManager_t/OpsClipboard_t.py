"""

More involved test for OpsClipboard application.

Testing views defined in OpsClipboard. Creating some documents in the
couch database that represent requests in the "ops-hold" state in
the ReqMgr and loading the views.


"""


import sys
import os
import traceback
from httplib2 import Http
from urllib import urlencode
import urlparse
import unittest

import WMCore.RequestManager.OpsClipboard.Inject as OpsClipboard
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Database



def changeState(couchUrl, couchDbName, docId, newState):
    """
    Posts the expected form to the changestate.js update function.
    For use in this test to mimic operators clicking on stuff (i.e. using
    httplib2 rather than CMSCouth direct REST interface).

    """
    fragments = urlparse.urlparse(couchUrl)
    userpassw = fragments[1].split("@",1)[0]
    user,passw = userpassw.split(":",1)
    newUrl = couchUrl.replace(userpassw, "")
    newUrl = newUrl.replace("@", "")
    h = Http()
    h.add_credentials(user, passw)
    data = {"newState": newState}
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    url = "%s/%s/_design/OpsClipboard/_update/changestate/%s" % (newUrl, couchDbName, docId)
    resp, content = h.request(url, "POST", urlencode(data), headers=headers)
    if content != "OK":
        raise Exception("Exception at CouchDB server: response: '%s', content: '%s'" % (resp, content))



def addDescription(couchUrl, couchDbName, docId, description):
    """
    Test the adddescription.js update function.
    For use in this test to mimic operators clicking on stuff (i.e. using
    httplib2 rather than CMSCouth direct REST interface).

    """
    fragments = urlparse.urlparse(couchUrl)
    userpassw = fragments[1].split("@",1)[0]
    user,passw = userpassw.split(":",1)
    newUrl = couchUrl.replace(userpassw, "")
    newUrl = newUrl.replace("@", "")
    h = Http()
    h.add_credentials(user, passw)
    data = {"newDescription" : description}
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    url = "%s/%s/_design/OpsClipboard/_update/adddescription/%s" % (newUrl, couchDbName, docId)
    resp, content = h.request(url, "POST", urlencode(data),headers= headers)
    if content != "OK":
        raise Exception("Exception at CouchDB server: response: '%s', content: '%s'" % (resp, content))



def getTestRequests(numRequests):
    """
    Generate a list of sample requests (i.e. dictionaries).

    """
    requests = []
    campaignIds = ["campaign_1", "campaign_2"]
    requestIds = []
    c = 0
    for i in range(numRequests):
        requestId = "request_id_%s" % i
        requestIds.append(requestId)
        campaignId = campaignIds[0] if c % 2 == 0 else campaignIds[1]
        c += 1
        requests.append({u"RequestName": requestId, u"Campaign" : campaignId})
    return requests, campaignIds, requestIds



class OpsClipboardTest(unittest.TestCase):
    def setUp(self):
        # For experiments with CouchDB content it's useful when the docs
        # remain the the database by commenting out tearDownCouch statement.
        # If the database exists at this point, tearDownCouch was probably
        # commented out, so do not drop the database
        #self.testInit = TestInitCouchApp(__file__, dropExistingDb=False)
        self.testInit = TestInitCouchApp(__file__, dropExistingDb=True)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        dbName = "opsclipboard_t"
        self.testInit.setupCouch(dbName, "OpsClipboard")
        # the tests uses httplib2 for accessing the OpsClipboard couchapp to
        # emulate web browser access rather than direct REST access
        # couch attribute is only used for back verification of written/modified data
        #couchServer = CouchServer(os.environ["COUCHURL"])
        #self.configDatabase = couchServer.connectDatabase(dbName)
        # used to verify written / modified data in CouchDB
        self.couch = Database(dbName, self.testInit.couchUrl)


    def tearDown(self):
        # comment out to see stuff remaining in the database
        self.testInit.tearDownCouch() # self.testInit.couch gets None-ed here
        #pass


    def _inject(self, numRequests):
        # This creates 10 documents using the test data above representing
        # 10 requests belonging to two campaigns that have just been placed
        # into the "ops-hold" into the ReqMgr.
        # Whenever a request enters the "ops-hold" state, the dict containing the
        # request params should be added to the OpsClipboard using the
        # inject API call (see Assign.py):
        requests, campaignIds, requestIds = getTestRequests(numRequests)
        OpsClipboard.inject(self.testInit.couchUrl, self.testInit.couchDbName, *requests)
        return requests, campaignIds, requestIds


    def _getViewResults(self, viewName, options = {}):
        """
        Query CouchDB viewName, return rows.

        """
        try:
            result = self.couch.loadView("OpsClipboard", viewName, options)
        except Exception as ex:
            msg = "Error loading OpsClipboard view: '%s', reason:%s\n" % (viewName, ex)
            self.fail(msg)
        return result[u"rows"]


    def testA_view_all(self):
        """
        Testing the 'all' view.

        """
        self._inject(10) # creates 10 documents
        # Now read back data for the test requests and verify
        # that we have 10 requests in the OpsClipboard
        # all view returns all requests in the OpsClipboard
        allRequests = self._getViewResults("all")
        self.assertEqual(len(allRequests), 10) # expected 10 requests
        for request in allRequests:
            self.failUnless(request[u"key"] == u"NewlyHeld")


    def testB_view_campaign(self):
        """
        Testing the 'campaign' view.
        Returns requests with campaign_id as keys.

        """
        _, campaignIds, requestIds = self._inject(7) # creates x docs/requests
        campView = self._getViewResults("campaign")
        self.assertEqual(len(campView), 7)
        for c in campView:
            self.failUnless(c[u"key"] in campaignIds)
            self.failUnless(c[u"value"][u"request_id"] in requestIds)
            # check that result ('value') dictionary has all these keys
            map(self.failUnless, [c[u"value"].has_key(key) for key in ("doc_id", "state", "updated")])


    def testC_view_campaign_ids(self):
        """
        Testing the 'campaign_ids' view.
        Returns a list of campaign names (campaign_ids) with duplicates removed.

        """
        _, campaignIds, _ = self._inject(8) # creates x docs/requests
        campList = self._getViewResults("campaign_ids", options = {"group": True})
        expected = [campList[0]["key"], campList[1]["key"]]
        self.assertEqual(expected, campaignIds)


    def testD_view_reject_update_changestate(self):
        """
        Testing the 'reject' view.
        Calls changeState function which also tests 'changestate'
            update (CouchDB) function.
        Returns a list of requests in the 'ReadyToReject' state.

        """
        numRequests = 8
        self._inject(numRequests) # creates x docs/requests
        # all currently injected requests are in the
        # "NewlyHeld" state, none in the "ReadyToReject" state
        rejectList = self._getViewResults("reject")
        self.assertEqual(len(rejectList), 0)
        # change state, need to get docIds from CouchDB first
        allList = self._getViewResults("all")
        for allItem in allList:
            docId = allItem[u"id"]
            try:
                changeState(self.testInit.couchUrl, self.testInit.couchDbName, docId, "ReadyToReject")
            except Exception as ex:
                self.fail(ex)
        rejectList = self._getViewResults("reject")
        self.assertEqual(len(rejectList), numRequests)


    def testE_view_release_update_changestate(self):
        """
        Testing the 'release' view.
        Calls changeState function which also tests 'changestate'
            update (CouchDB) function.
        Returns a list of requests in the 'ReadyToRelease' state.

        """
        numRequests = 18
        self._inject(numRequests) # creates x docs/requests
        # all currently injected requests are in the
        # "NewlyHeld" state, none in the "ReadyToRelease" state
        rejectList = self._getViewResults("release")
        self.assertEqual(len(rejectList), 0)
        # change state, need to get docIds from CouchDB first
        allList = self._getViewResults("all")
        for allItem in allList:
            docId = allItem[u"id"]
            try:
                changeState(self.testInit.couchUrl, self.testInit.couchDbName, docId, "ReadyToRelease")
            except Exception as ex:
                self.fail(ex)
        rejectList = self._getViewResults("release")
        self.assertEqual(len(rejectList), numRequests)


    def testF_view_request(self):
        """
        Testing the 'request' view.
        This view allows for look up of some request details by id.

        """
        _, _, requestIds = self._inject(15) # creates x docs/requests
        requestView = self._getViewResults("request")
        self.assertEqual(len(requestView), 15)
        for reqView in requestView:
            self.failUnless(reqView[u"key"] in requestIds)
            self.failUnless(reqView[u"value"][u"state"] == u"NewlyHeld")


    def testG_view_request_id(self):
        """
        Testing the 'request_ids' view.
        'request_ids' maps couch docs to request ids.

        """
        self._inject(11) # creates x docs/requests
        viewResult = self._getViewResults("request_ids")
        requestIds  = [ x[u"key"] for x in viewResult ]
        self.assertEqual(len(requestIds), 11)


    def testH_view_expunge(self):
        """
        Testing the 'expunge' view.

        """
        self._inject(4) # creates x docs/requests
        requestView = self._getViewResults("all")
        # no "ReadyToReject" or "ReadyToReject" request, everything is in "NewlyHeld"
        self.assertEqual(len(requestView), 4)
        c = 0
        for req in requestView:
            docId = req[u"value"]
            try:
                state = "ReadyToReject" if c % 2 == 0 else "ReadyToReject"
                changeState(self.testInit.couchUrl, self.testInit.couchDbName, docId, state)
            except Exception as ex:
                self.fail(ex)
            c += 1
        expungeView = self._getViewResults("expunge")
        self.assertEqual(len(expungeView), 4)
        for req in expungeView:
            self.assertTrue(req[u"key"] in ("ReadyToReject", "ReadyToReject"))


    def testI_requestStructure(self):
        """
        Pull documents for each request and check structure.

        """
        _, campaignIds, requestIds = self._inject(20) # creates x documents / requests
        allRequests = self._getViewResults("all")
        for req in allRequests:
            docId = req[u"id"]
            state = req[u"key"]
            # all requests should be NewlyHeld state
            self.assertEqual(state, "NewlyHeld")
            # check that the doc is well formed and matches the data we inserted
            doc = self.couch.document(docId)
            self.failUnless(doc[u"state"] == "NewlyHeld")
            self.failUnless(doc.has_key(u"created"))
            self.failUnless(doc.has_key(u"timestamp"))
            # description is a list of dictionaries, the first one is the initial message
            self.failUnless("Initial injection by the RequestManager" in doc[u"description"][0].values())
            self.failUnless(doc[u"request"][u"campaign_id"] in campaignIds)
            self.failUnless(doc[u'request'][u'request_id'] in requestIds)


    def testJ_update_adddescription(self):
        """
        Create a document and update function 'adddescription' handler
        to add descriptions (Ops notes) to request documents.

        """
        request = {"RequestName" : "testB_request", "CampaignName" : "testB_campaign"}
        OpsClipboard.inject(self.testInit.couchUrl, self.testInit.couchDbName, *[request])
        allRequests = self._getViewResults("all")
        self.assertEqual(len(allRequests), 1) # check only one request
        docId = allRequests[0][u"id"]
        # update the doc descriptions
        addDescription(self.testInit.couchUrl, self.testInit.couchDbName, docId, "NewDescription")
        doc = self.couch.document(docId)
        descriptions = doc["description"]
        # description entry is a list of dictionaries, each newly created request
        # has first initial description, just above added was the second one, index 1
        self.failUnless("NewDescription" in doc[u"description"][1].values())



if __name__ == "__main__":
    unittest.main()
