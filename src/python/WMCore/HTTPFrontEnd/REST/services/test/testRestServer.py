#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2007 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2007

# system modules
import sys, os, string, logging, types, time, socket
import httplib, urllib, inspect, urllib2

class ParamServer(object): 
    def __init__(self,server,verbose=1):
        self.verbose = verbose
        self.secure  = 0
        self.user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
        print "got",server
        if server.find("https://")!=-1:
           server=server.replace("https://","")
           self.secure=1
        elif server.find("http://")!=-1:
           server=server.replace("http://","")
        self.path=""
        self.server  = server
        if server.find("/")!=-1:
           url,path=server.split("/",1)
           self.server=url
           self.path=path

    def sendMessage(self,method="GET",file="index.html",ctype='text/html',params={},debug=0):
        if debug:
           httplib.HTTPConnection.debuglevel = 1
        print self.server
        if self.secure:
           http_conn = httplib.HTTPSConnection(self.server)
        else:
           http_conn = httplib.HTTPConnection(self.server)
        if self.path:
           file="/%s/%s"%(self.path,file)
        print "sendMessage",file
        headers = { 'User-Agent' : self.user_agent, 'Accept':ctype}
        data = urllib.urlencode(params,doseq=True)
        http_conn.request(method, "/%s"%file, data, headers)
        response = http_conn.getresponse()
        data = ""
        if response.reason!="OK":
           data=response.status,response.reason
           print data
        else:
           data = response.read()
        http_conn.close()
        return data

    def sendPostMessage(self,method,iParams,ctype='text/html',debug=0):
        if debug:
           httplib.HTTPConnection.debuglevel = 1
        url=self.server+"/"+self.path+"/"+method
        if self.secure:
           url="https://"+os.path.normpath(url)
        else:
           url="http://"+os.path.normpath(url)
        if  debug:
            print "sendPostMessage",url,iParams
        headers = { 'User-Agent' : self.user_agent, 'Accept':ctype}
        data = urllib.urlencode(iParams,doseq=True)
        req = urllib2.Request(url, data, headers)
        data = ""
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
#        data=urllib2.urlopen(url,urllib.urlencode(iParams,doseq=True)).read()
        return data

#
# main
#
if __name__ == "__main__":

    url="services/rest/word"
    server = ParamServer(server="http://localhost:8080",verbose=1)
    params={}
    for ctype in ['application/xml','text/json','text/html']:
        print "\n### GET message via",ctype
        page = server.sendMessage("GET",url,ctype,params,debug=1)
        print page
