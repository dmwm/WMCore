#!/usr/bin/env python
# encoding: utf-8
"""
Insert.py

Created by Dave Evans on 2011-06-14.
Copyright (c) 2011 Fermilab. All rights reserved.
"""
import os
import httplib2
import json
import time
from WMCore.Database.CMSCouch import Document, Database

def getRequestsInState(rmUrl, state):
    """
    _getRequestsInState_
    
    Grab requests in the assigned state from the reqmgr URL provided using
    the requestnames and request GET APIs
    
    Expect this method to be mostly used for testing
    
    """
    reqListUrl = "%s/reqmgr/reqMgr/requestnames" % rmUrl
    reqDetailUrl = "%s/reqmgr/reqMgr/request?requestName=" % rmUrl
    results = []
    h = httplib2.Http(".cache")
    resp, content = h.request(reqListUrl, "GET")
    contentJson = json.loads(content)
    for req in contentJson:
        # filter by state
        if req[u'status'] != state: continue
        reqId = req[u'request_name']
        
        #get the details
        detailsUrl = reqDetailUrl 
        detailsUrl += str(reqId)
        
        resp, content = h.request(detailsUrl, "GET")
        realcontent = json.loads(content)
        #reformat/cleanout a bit
        realcontent = realcontent.get(u"WMCore.RequestManager.DataStructs.Request.Request", {})
        if realcontent.has_key(u'RequestUpdates'):
            del realcontent[u'RequestUpdates']


        results.append(realcontent)
    return results
    
def makeClipboardDoc(req):
    """
    _makeClipboardDoc_
    
    Build a couch Document for the clipboard entry for the request provided
    """
    req['request_id'] = req[u'RequestName']
    # need to check what the CampaignName will actually be.
    req['campaign_id'] = req.get(u'CampaignName', None)
    data = {
        "state" : "NewlyHeld",
        "timestamp" : time.time(),
        "created" : time.time(),
        "request" : req,
        "description" : { time.time(): "Initial injection by the RequestManager"}
    }
    
    doc = Document(dict = data)
    return doc
    
def inject(clipboardUrl, clipboardDb, *requests):
    """
    _inject_
    
    
    """
    couch = Database(clipboardDb, clipboardUrl)
    knownDocs = couch.loadView("OpsClipboard", "request_ids")
    knownReqs = [ x[u'key'] for x in knownDocs['rows']]
    for req in requests:
        if req[u'RequestName'] in knownReqs: continue
        doc = makeClipboardDoc(req)
        couch.queue(doc)
    couch.commit()

