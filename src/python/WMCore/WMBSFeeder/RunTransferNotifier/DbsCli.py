#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2008 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2008

"""
DBS data discovery command line interface
"""



import httplib, urllib, urllib2, types, string, os, sys

def sendMessage(host, port, dbsInst, userInput, page, limit, xml=0, case='on',
                iface='dbsapi', details=0, cff=0, debug=0):
    if xml: xml=1
    else:   xml=0
    if cff: cff=1
    else:   cff=0
    input=urllib.quote(userInput)
    if debug:
        httplib.HTTPConnection.debuglevel = 1
        print "Contact",host,port
    _port=443
    if host.find("http://")!=-1:
        _port=80
    if host.find("https://")!=-1:
        _port=443
    host=host.replace("http://","").replace("https://","")
    if host.find(":")==-1:
        port=_port
    prefix_path=""
    if host.find("/")!=-1:
        hs=host.split("/")
        host=hs[0]
        prefix_path='/'.join(hs[1:])
    if host.find(":")!=-1:
        host,port=host.split(":")
    port=int(port)
    path="/aSearch"
    if prefix_path:
        path="/"+prefix_path+path[1:]
    if port==443:
        url = "https://"+host+":%s"%port+path
    else:
        url = "http://"+host+":%s"%port+path
    if details: details=1
    else:       details=0
    params  = {'dbsInst':dbsInst, 'html':0, 'caseSensitive':case, '_idx':page,
               'pagerStep':limit, 'userInput':input, 'xml':xml,
               'details':details, 'cff':cff, 'method':iface}
    agent   = "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)"
    ctypes  = "text/plain"
    headers = { 'User-Agent':agent, 'Accept':ctypes}
    data    = urllib.urlencode(params,doseq=True)
    if  debug:
        print url,data,headers
    req     = urllib2.Request(url, data, headers)
    data    = ""
    try:
        response = urllib2.urlopen(req)
        data = response.read()
    except urllib2.HTTPError, e:
        if e.code==201:
            print e.headers       
            print e.msg
            pass
        else:
            raise e
    return data
