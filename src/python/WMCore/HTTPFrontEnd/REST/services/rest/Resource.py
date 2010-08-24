#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2008 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2008
#
# This work based on example from CherryPy Essentials book, by Sylvain Hellegouarch
"""
REST resource class which handle all requests.

-----------------------------------------------------------------------------------------
HTTP method     Idempotent      Operation
-----------------------------------------------------------------------------------------
HEAD            YES             Retrieves the resource metadata. The response is the same 
                                as  the one to a GET minus the body.
GET             YES             Retrieves resource metadata and content
POST            NO              Requests the server to create a new resource
                                using the data enclosed in the request body
PUT             YES             Requests the server to replace an existing
                                resource with the one enclosed in the request
                                body. The server cannot apply the enclosed
                                resource to a resource not identified by that URL
DELETE          YES             Requests the server to remove the resource
                                identified by that URL
OPTIONS         YES             Requests the server to return details about capabilities
                                either globally or specifically towards a resource.
-----------------------------------------------------------------------------------------
"""

import cherrypy
import traceback
import types
from cherrypy.lib.cptools import accept

__all__ = ['Resource']

class Resource(object):
    def __init__(self):
        self.xmlReturnType= 'application/xml'
        self.supportTypes =['application/xml', 'application/atom+xml',
                            'text/json', 'text/x-json', 'application/json',
                            'text/html','text/plain']
        self.defaultType  = 'text/html'
        self.qlKeys       = []
        print "+++ Init Resource",self._url
        
    def printResponse(self):
        headers = cherrypy.response.headers
        print "REST Resource response  :"
        print "-------------------------"
        for key in headers:
            print "%s: %s"%(key,headers[key])
        print "CherryPy response body  :"
        print "-------------------------"
        print cherrypy.response.body
        print "CherryPy response status:"
        print "-------------------------"
        print cherrypy.response.status

    def getData(self,params,kwargs):
        return self._model.getData(params,**kwargs)

    def createData(self,params,kwargs):
        return self._model.createData(params,**kwargs)

    def updateData(self,params,kwargs):
        return self._model.updateData(params,**kwargs)

    def deleteData(self,params,kwargs):
        return self._model.deleteData(params,**kwargs)

    def setHeader(self,ctype,length=None):
       cherrypy.response.headers['Content-Type'] = ctype
       if length: cherrypy.response.headers['Content-Length'] = length

    def response(self,iData,method):
        # inspect Accept header for matching type
        dataType = accept(self.supportTypes)
        # look-up data in appropriate format for our object
        data = None
        rType= None
        if dataType in ['application/xml', 'application/atom+xml']:
           data = self._formatter.to_xml(iData)
           rType= self.xmlReturnType
        elif dataType in ['text/json', 'text/x-json', 'application/json']:
           data = self._formatter.to_json(iData)
           rType= dataType
        elif dataType in ['text/plain']:
           data = self._formatter.to_txt(iData)
           rType= dataType
        elif dataType in ['text/html']:
           data = self._formatter.to_html(self._host,self._url,self._mUrl,self._fUrl,iData)
           rType= dataType
        else:
           data = iData
           rType= self.defaultType

        if  method=="head":
            self.setHeader(rType,len(data))
            return
        else:
            self.setHeader(rType)

        if  self._verbose:
            self.printResponse()
        if  data:
            return data
        raise cherrypy.HTTPError(400, 'Bad Request')

    def checkAccept(self):
        reqAccept = cherrypy.request.headers['Accept']
        error = None
        if reqAccept.find(",")!=-1:
           if self._verbose:
              print "\n+++ Reqested multiple types",reqAccept,
           aList = reqAccept.split(",")[0]
           type  = None
           if reqAccept.find(self.defaultType)!=-1:
              type=self.defaultType
           else:
              for t in aList:
                  if t in self.supportTypes:
                     type=t
                     break
           if not type: 
              return
           cherrypy.request.headers['Accept']=type
           if self._verbose:
              print "will use",type
        elif reqAccept not in self.supportTypes:
           error = 1
        else:
           error = None

        if  not cherrypy.request.headers.elements('Accept'):
            error = 1
        if  error:
            msg ="Not Acceptable, missing Accept attr in a header:"+str(cherrypy.request.headers)
            msg+="REST services accept the following types"
            msg+=str(self.supportTypes)
            raise cherrypy.HTTPError(406, msg)

    def updateInputDict(self,args,kwargs):
        for item in args:
            if type(item) is types.DictType:
                for key in item: kwargs[key]=item[key]
        return kwargs

    def handle_HEAD(self, *args, **kwargs):
        if  self._verbose:
            print "Calling handle_HEAD",args,kwargs
        self.checkAccept()
        kwargs= self.updateInputDict(args,kwargs)
        data  = self.getQuery(args[0],kwargs) # we just ask about what query correspond to request
        # generate response
        self.response(data,"head")
    
    def handle_GET(self, *args, **kwargs):
        if  self._verbose:
            print "Calling handle_GET",args,kwargs
        self.checkAccept()
        kwargs= self.updateInputDict(args,kwargs)
        data  = self.getData(args[0],kwargs)
        # generate response
        return self.response(data,"get")

    def handle_POST(self, *args, **kwargs):
        if  self._verbose:
            print "Calling handle_POST",args,kwargs
        self.checkAccept()
        kwargs= self.updateInputDict(args,kwargs)
        params = args[0]
        data   = self.createData(params,kwargs)

        # set appropriate headers for POST method
        cherrypy.response.status = '201 Created'
        location = '%s/%s'%(self._url,'/'.join(params))
        cherrypy.response.headers['Location'] = location

        # generate response
        return self.response(data,"post")

    def handle_PUT(self, *args, **kwargs):
        if  self._verbose:
            print "Calling handle_PUT",args,kwargs
        self.checkAccept()
        kwargs= self.updateInputDict(args,kwargs)
        data  = self.updateData(args[0],**kwargs)
        # generate response
        return self.response(data,"put")

    def handle_DELETE(self, *args, **kwargs):
        if  self._verbose:
            print "Calling handle_DELETE",args,kwargs
        self.checkAccept()
        kwargs= self.updateInputDict(args,kwargs)
        data  = self.deleteData(args[0],kwargs)
        # generate response
        return self.response(data,"delete")

