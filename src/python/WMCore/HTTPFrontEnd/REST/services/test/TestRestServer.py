#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
# Author:  Valentin Kuznetsov, 2007
"""
This code include simple class ParamServer to
send GET/POST messages to the server and print
out results.
"""

# system modules
import os
import httplib
import urllib
import urllib2

class ParamServer(object): 
    """Class to send GET/POST messages to the server"""
    def __init__(self, server, verbose=1):
        self.verbose = verbose
        self.secure  = 0
        self.user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
        if  server.find("https://") != -1:
            server = server.replace("https://","")
            self.secure = 1
        elif server.find("http://") != -1:
            server = server.replace("http://","")
        self.path = ""
        self.server  = server
        if  server.find("/") != -1:
            url, path = server.split("/", 1)
            self.server = url
            self.path = path

    def message(self, url="index.html", method="GET",
                      params=None, ctype='text/html'):
        """
           send request to the server via provided URL and method.
        """
        if  method == "POST":
            return self.sendpostmessage(url, params, ctype)
        return self.sendmessage(url, method, params, ctype)

    def sendmessage(self, ifile="index.html", method="GET",
                          params=None, ctype='text/html'):
        """
           send request to the server via provided method,
           suitable for GET, HEAD, DELETE
        """
        if  self.verbose:
            httplib.HTTPConnection.debuglevel = 1
        print self.server
        if  self.secure:
            http_conn = httplib.HTTPSConnection(self.server)
        else:
            http_conn = httplib.HTTPConnection(self.server)
        if  self.path:
            ifile = "/%s/%s" % (self.path, ifile)
        print "sendmessage %s with params %s" % (ifile, params)
        url = "/%s?" % ifile
        for key, val in params.iteritems():
            url += "%s=%s" % (key, val)
        headers = { 'User-Agent' : self.user_agent, 'Accept':ctype}
        body = "test"
        http_conn.request(method, url, body, headers)
        response = http_conn.getresponse()
        data = ""
        if  response.reason != "OK":
            data = response.status, response.reason
            print data
        else:
            data = response.read()
        http_conn.close()
        return data

    def sendpostmessage(self, method, iparams, ctype='text/html'):
        """
           send request to the server via POST message
        """
        if  self.verbose:
            httplib.HTTPConnection.debuglevel = 1
        url = self.server+"/"+self.path+"/"+method
        if  self.secure:
            url = "https://"+os.path.normpath(url)
        else:
            url = "http://"+os.path.normpath(url)
        if  self.verbose:
            print "sendpostmessage %s %s" % (url, iparams)
        headers = { 'User-Agent' : self.user_agent, 'Accept':ctype}
        data = urllib.urlencode(iparams, doseq=True)
        req  = urllib2.Request(url, data, headers)
        data = ""
        try:
            response = urllib2.urlopen(req)
            data = response.read()
        except urllib2.HTTPError, err:
            if  err.code == 201:
                print err.headers       
                print err.msg
            else:
                raise err
        return data

#
# main
#
if  __name__ == "__main__":

    URL = "services/rest/word"
    SERVER = ParamServer(server="http://localhost:8080", verbose=1)
    PARAMS = {'test':1}
    for CTYPE in ['application/xml', 'text/json', 'text/html']:
        for METHOD in ['HEAD', 'GET', 'POST', 'DELETE']:
            print "-------- %s REQUEST ---------" % METHOD
            print CTYPE, PARAMS
            page = SERVER.message(URL, METHOD, PARAMS, CTYPE)
            print page
