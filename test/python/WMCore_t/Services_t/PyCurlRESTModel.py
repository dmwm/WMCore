from WMCore.WebTools.RESTModel import RESTModel
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
import WMCore

import logging
import unittest
import threading
import cherrypy
import os
import uuid
import tempfile
from cgi import FieldStorage

def noBodyProcess():
    """Sets cherrypy.request.process_request_body = False, giving
    us direct control of the file upload destination. By default
    cherrypy loads it to memory, we are directing it to disk."""
    cherrypy.request.process_request_body = False
cherrypy.tools.noBodyProcess = cherrypy.Tool('before_request_body', noBodyProcess)

class PyCurlRESTModel(RESTModel):
    """
    Check if the file upload work with the rest model
    """
    def __init__(self, config={}):
        RESTModel.__init__(self, config)

        self._addMethod('POST', 'file', self.uploadFile,
                        args=['file1'])

        cherrypy.engine.subscribe('start_thread', self.initThread)

    def initThread(self, thread_index):
        """
        The ReqMgr expects the DBI to be contained in the Thread
        """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    @cherrypy.expose
    @cherrypy.tools.noBodyProcess()
    def uploadFile(self, file1):
        """
        Saves the file passed by the client in the current directory
        """
        try:
            with open('UploadedFile.txt', 'wb') as f:
                f.write( file1.file.read() )
        except Exception as e:
            logging.exception(e)

        return {'result':'Success'}
