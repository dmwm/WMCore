#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# This work based on example from 
# CherryPy Essentials book, by Sylvain Hellegouarch
"""
REST resource class which handle all requests.

-----------------------------------------------------------------------------
HTTP method     Idempotent      Operation
-----------------------------------------------------------------------------
HEAD            YES             Retrieves the resource metadata. 
                                The response is the same 
                                as  the one to a GET minus the body.
GET             YES             Retrieves resource metadata and content
POST            NO              Requests the server to create a new resource
                                using the data enclosed in the request body
PUT             YES             Requests the server to replace an existing
                                resource with the one enclosed in the request
                                body. The server cannot apply the enclosed
                                resource to a resource not identified by that
                                URL
DELETE          YES             Requests the server to remove the resource
                                identified by that URL
OPTIONS         YES             Requests the server to return details about 
                                capabilities either globally or specifically 
                                towards a resource.
-----------------------------------------------------------------------------
"""





import cherrypy
import types
import traceback
from cherrypy.lib.cptools import accept

__all__ = ['Resource']

def printresponse():
    """print HTTP responses"""
    headers = cherrypy.response.headers
    print "REST Resource response  :"
    print "-------------------------"
    for key in headers:
        print "%s: %s" % (key, headers[key])
    print "CherryPy response body  :"
    print "-------------------------"
    print cherrypy.response.body
    print "CherryPy response status:"
    print "-------------------------"
    print cherrypy.response.status

def updateinputdict(args, kwargs):
    """
       helper function to update a given value in a dict.
    """
    for item in args:
        if type(item) is types.DictType:
            for key in item:
                kwargs[key] = item[key]
    return kwargs

def setheader(ctype, length=None):
    """Set header of the server response"""
    cherrypy.response.headers['Content-Type'] = ctype
    if  length: 
        cherrypy.response.headers['Content-Length'] = length

def makemethod(args):
    try:
        method = '/'.join(args)
    except:
        traceback.print_exc()
        method = ''
        pass
    return method

class Resource(object):
    """
       Base class for REST server. It defines a list of resources, e.g.
       how to create/update/delete data. It uses provided data model
       and formetter.
    """
    def __init__(self):
        self._model = None
        self._url   = None
        self._verbose = None
        self._formatter = None
        self.xmlreturntype = 'application/xml'
        self.supporttypes  = ['application/xml', 'application/atom+xml',
                             'text/json', 'text/x-json', 'application/json',
                             'text/html','text/plain']
        self.defaulttype   = 'text/html'
#        print "+++ Init Resource %s" % self._url
        
    def response(self, idata, method):
        """Create server response"""
#        print "rest.Resource.response"
#        print cherrypy.response.headers
#        print "#### accept", cherrypy.request.headers['Accept']
        # inspect Accept header for matching type
        datatype = accept(self.supporttypes)
        # look-up data in appropriate format for our object
        data  = None
        rtype = None
        if  datatype in ['application/xml', 'application/atom+xml']:
            data  = self._formatter.to_xml(idata)
            rtype = self.xmlreturntype
        elif datatype in ['text/json', 'text/x-json', 'application/json']:
            data  = self._formatter.to_json(idata)
            rtype = datatype
        elif datatype in ['text/plain']:
            data  = self._formatter.to_txt(idata)
            rtype = datatype
        elif datatype in ['text/html']:
            data  = self._formatter.to_html(self._url, idata)
            rtype = datatype
        else:
            data  = idata
            rtype = datatype
#            rtype = self.defaulttype

        if  method == "head":
            setheader(rtype, len(data))
            return
        else:
            setheader(rtype)

        if  self._verbose:
            printresponse()
        if  data:
            return data
        raise cherrypy.HTTPError(400, 'Bad Request')

    def checkaccept(self):
        """helper function to check accept type of the incoming request"""
        reqaccept = cherrypy.request.headers['Accept']
        error = None
        if  reqaccept.find(",") != -1:
            if  self._verbose:
                print "\n+++ Reqested multiple types %s" % reqaccept
            alist = reqaccept.split(",")[0]
            mimetype  = None
            if  reqaccept.find(self.defaulttype) != -1:
                mimetype = self.defaulttype
            else:
                for item in alist:
                    if  item in self.supporttypes:
                        mimetype = item
                        break
            if  not mimetype: 
                return
            cherrypy.request.headers['Accept'] = mimetype
            if  self._verbose:
                print "will use %s" % mimetype
        elif reqaccept not in self.supporttypes:
            error = 1
        else:
            error = None

        if  not cherrypy.request.headers.elements('Accept'):
            error = 1
        if  error:
            msg  = "Not Acceptable, missing Accept attr in a header:"
            msg += str(cherrypy.request.headers)
            msg += "REST services accept the following types"
            msg += str(self.supporttypes)
            raise cherrypy.HTTPError(406, msg)

    def handle_head(self, *args, **kwargs):
        """
           handle HEAD requests
        """
        if  self._verbose:
            print "Calling handle_head %s %s" % (args, kwargs)
        self.checkaccept()
        kwargs = updateinputdict(args, kwargs)
        method = makemethod(args[0])
        data = self._model.getdata(method, kwargs)
        # generate response
        self.response(data, "head")
    
    def handle_get(self, *args, **kwargs):
        """
           handle GET requests
        """
        if  self._verbose:
            print "Calling handle_get %s %s" % (args, kwargs)
        self.checkaccept()
        kwargs = updateinputdict(args, kwargs)
        method = makemethod(args[0])
        data = self._model.getdata(method, kwargs)
        # generate response
        return self.response(data, "get")

    def handle_post(self, *args, **kwargs):
        """
           handle POST requests
        """
        if  self._verbose:
            print "Calling handle_post %s %s" % (args, kwargs)
        self.checkaccept()
        kwargs = updateinputdict(args, kwargs)
        method = makemethod(args[0])
        data = self._model.createdata(method, kwargs)

        # set appropriate headers for POST method
        cherrypy.response.status = '201 Created'
        location = '%s/%s' % (self._url, method)
        cherrypy.response.headers['Location'] = location

        # generate response
        return self.response(data, "post")

    def handle_put(self, *args, **kwargs):
        """
           handle PUT requests
        """
        if  self._verbose:
            print "Calling handle_put %s %s" % (args, kwargs)
        self.checkaccept()
        kwargs = updateinputdict(args, kwargs)
        method = makemethod(args[0])
        data = self._model.updatedata(method, kwargs)
        # generate response
        return self.response(data, "put")

    def handle_delete(self, *args, **kwargs):
        """
           handle DELETE requests
        """
        if  self._verbose:
            print "Calling handle_delete %s %s" % (args, kwargs)
        self.checkaccept()
        kwargs = updateinputdict(args, kwargs)
        method = makemethod(args[0])
        data = self._model.deletedata(method, kwargs)
        # generate response
        return self.response(data, "delete")

