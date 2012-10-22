"""
Inject.py

Injects requests of certain type from ReqMgr RMDBS into
CouchDB, for use by the OpsClipboard application.

Application comment in the __init__ file.

Created by Dave Evans on 2011-06-14.
Copyright (c) 2011 Fermilab. All rights reserved.

"""


import os
import httplib2
import json
import time
from WMCore.Database.CMSCouch import Document, Database



# TODO
# this is currently not used anywhere and also not tested
def getRequestsInState(rmUrl, state):
    """
    Grab requests in the assigned state from the reqmgr URL provided using
    the requestnames and request GET APIs.

    Expect this method to be mostly used for testing.

    """
    reqListUrl = "%s/reqmgr/rest/requestnames" % rmUrl
    reqDetailUrl = "%s/reqmgr/rest/request?requestName=" % rmUrl
    result = []
    h = httplib2.Http(".cache")
    resp, content = h.request(reqListUrl, "GET")
    contentJson = json.loads(content)
    for req in contentJson:
        # filter by state
        if req[u'status'] != state:
            continue
        reqId = req[u'request_name']

        # get the details
        detailsUrl = reqDetailUrl
        detailsUrl += str(reqId)

        resp, content = h.request(detailsUrl, "GET")
        realcontent = json.loads(content)
        # reformat/cleanout a bit
        realcontent = realcontent.get(u"WMCore.RequestManager.DataStructs.Request.Request", {})
        if realcontent.has_key(u'RequestUpdates'):
            del realcontent[u'RequestUpdates']
        result.append(realcontent)
    return result



def _makeClipboardDoc(req):
    """
    Build a couch Document for the clipboard entry for the request provided.

    """
    req['request_id'] = req[u'RequestName']
    req['campaign_id'] = req.get(u'Campaign', None)
    data = {
        "request": req,
        # this is internal, in-OpsClipboard request state
        "state": "NewlyHeld",
        # all future timestamp updates will happen in Javascript where Data.now()
        # returns time in milliseconds, not in seconds like Python, be in sync here
        "timestamp": time.time() * 1000,
        "created": time.time() * 1000,
        "description": [{"timestamp": time.time() * 1000,
                        "info": "Initial injection by the RequestManager"}]
    }
    doc = Document(inputDict=data)
    return doc



def inject(clipboardUrl, clipboardDb, *requests):
    """
    Query CouchDB to check for overlap of therein already stored requests
    and requests in the input argument requests. Store in CouchDB the
    outstanding ones.

    """
    couch = Database(clipboardDb, clipboardUrl)
    knownDocs = couch.loadView("OpsClipboard", "request_ids")
    knownReqs = [ x[u'key'] for x in knownDocs['rows']]
    print "There already are %s requests in OpsClipboard." % len(knownReqs)
    for req in requests:
        if req[u'RequestName'] in knownReqs:
            continue
        doc = _makeClipboardDoc(req)
        print "Injecting request into OpsClipboard: '%s'" % doc["request"][u"RequestName"]
        couch.queue(doc)
    couch.commit()
