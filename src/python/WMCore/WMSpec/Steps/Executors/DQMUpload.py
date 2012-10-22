#!/usr/bin/env python
"""
_Step.Executor.DQMUpload_

Implementation of an Executor for a DQMUpload step

"""
import os
import sys
import httplib
import urllib2
import logging
import traceback

from cStringIO import StringIO
from mimetypes import guess_type
from gzip import GzipFile

# Compatibility with python2.3 or earlier
HTTPS = httplib.HTTPS
if sys.version_info[:3] >= (2, 4, 0):
    HTTPS = httplib.HTTPSConnection

# Compatibility with python2.4 or earlier
try:
    from hashlib import md5
except ImportError:
    from md5 import md5

from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.FwkJobReport.Report import Report

from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure


class DQMUpload(Executor):
    """
    _DQMUpload_

    Execute a DQMUpload Step

    """

    def pre(self, emulator = None):
        """
        _pre_

        Pre execution checks

        """
        #Are we using an emulator?
        if emulator != None:
            return emulator.emulatePre(self.step)

        print "Steps.Executors.DQMUpload.pre called"
        return None

    def execute(self, emulator = None):
        """
        _execute_

        """
        #Are we using emulators again?
        if emulator != None:
            return emulator.emulate(self.step, self.job)

        if self.step.upload.proxy:
            try:
                self.stepSpace.getFromSandbox(self.step.upload.proxy)
            except Exception, ex:
                #Let it go, it wasn't in the sandbox. Then it must be
                #somewhere else
                pass

        # Search through steps for analysis files
        for step in self.stepSpace.taskSpace.stepSpaces():
            if step == self.stepName:
                #Don't try to parse your own report; it's not there yet
                continue
            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s" % step)
            reportLocation = os.path.join(stepLocation, 'Report.pkl')
            if not os.path.isfile(reportLocation):
                logging.error("Cannot find report for step %s in space %s" \
                              % (step, stepLocation))
                continue

            # First, get everything from a file and 'unpersist' it
            stepReport = Report()
            stepReport.unpersist(reportLocation, step)

            # Don't upload nor stage out files from bad steps.
            if not stepReport.stepSuccessful(step):
                continue

            # Pulling out the analysis files from each step
            analysisFiles = stepReport.getAnalysisFilesFromStep(step)

            # Working on analysis files
            for analysisFile in analysisFiles:
                # only deal with DQM files
                if analysisFile.FileClass == "DQM":
                    # uploading file to the server
                    self.httpPost(os.path.join(stepLocation,
                                               os.path.basename(analysisFile.fileName)))

            # Am DONE with report
            # Persist it
            stepReport.persist(reportLocation)

        return

    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        # Another emulator check
        if emulator != None:
            return emulator.emulatePost(self.step)

        print "Steps.Executors.DQMUpload.post called"
        return None


    #
    # for the latest DQM upload code see https://github.com/rovere/dqmgui/blob/master/bin/visDQMUpload
    #

    def httpPost(self, filename):
        """
        _httpPost_

        perform an HTTP POST operation to a webserver

        """
        args = {}

        # Preparing a checksum
        blockSize = 0x10000
        def upd(m, data):
            m.update(data)
            return m
        fd = open(filename, 'rb')
        try:
            contents = iter(lambda: fd.read(blockSize), '')
            m = reduce(upd, contents, md5())
        finally:
            fd.close()

        args['checksum'] = 'md5:%s' % m.hexdigest()
        #args['checksum'] = 'md5:%s' % md5.new(filename).read()).hexdigest()
        args['size'] = os.path.getsize(filename)

        msg = "HTTP Upload is about to start:\n"
        msg += " => URL: %s\n" % self.step.upload.URL
        msg += " => Filename: %s\n" % filename
        logging.info(msg)

        try:
            (headers, data) = self.upload(self.step.upload.URL, args, filename)
            msg = 'HTTP upload finished succesfully with response:\n'
            msg += 'Status code: %s\n' % headers.get("Dqm-Status-Code", None)
            msg += 'Message: %s\n' % headers.get("Dqm-Status-Message", None)
            msg += 'Detail: %s\n' % headers.get("Dqm-Status-Detail", None)
            msg += 'Data: %s\n' % str(data)
            logging.info(msg)
        except urllib2.HTTPError, ex:
            msg = 'HTTP upload failed with response:\n'
            msg += 'Status code: %s\n' % ex.hdrs.get("Dqm-Status-Code", None)
            msg += 'Message: %s\n' % ex.hdrs.get("Dqm-Status-Message", None)
            msg += 'Detail: %s\n' % ex.hdrs.get("Dqm-Status-Detail", None)
            msg += 'Error: %s\n' % str(ex)
            logging.error(msg)
            raise WMExecutionFailure(60318, "DQMUploadFailure", msg)
        except Exception, ex:
            msg = 'HTTP upload failed with response:\n'
            msg += 'Problem unknown.\n'
            msg += 'Error: %s\n' % str(ex)
            msg += 'Traceback: %s\n' % traceback.format_exc()
            logging.error(msg)
            raise WMExecutionFailure(60318, "DQMUploadFailure", msg)

        return

    def filetype(self, filename):
        return guess_type(filename)[0] or 'application/octet-stream'

    def encode(self, args, files):
        """
        Encode form (name, value) and (name, filename, type) elements into
        multi-part/form-data. We don't actually need to know what we are
        uploading here, so just claim it's all text/plain.
        """
        boundary = '----------=_DQM_FILE_BOUNDARY_=-----------'
        (body, crlf) = ('', '\r\n')
        for (key, value) in args.items():
            payload = str(value)
            body += '--' + boundary + crlf
            body += ('Content-Disposition: form-data; name="%s"' % key) + crlf
            body += crlf + payload + crlf
        for (key, filename) in files.items():
            body += '--' + boundary + crlf
            body += ('Content-Disposition: form-data; name="%s"; filename="%s"'
                     % (key, os.path.basename(filename))) + crlf
            body += ('Content-Type: %s' % self.filetype(filename)) + crlf
            body += ('Content-Length: %d' % os.path.getsize(filename)) + crlf
            body += crlf + open(filename, "r").read() + crlf
            body += '--' + boundary + '--' + crlf + crlf
        return ('multipart/form-data; boundary=' + boundary, body)

    def marshall(self, args, files, request):
        """
        Marshalls the arguments to the CGI script as multi-part/form-data,
        not the default application/x-www-form-url-encoded. This improves
        the transfer of the large inputs and eases command line invocation
        of the CGI script.
        """
        (type, body) = self.encode(args, files)
        request.add_header('Content-Type', type)
        request.add_header('Content-Length', str(len(body)))
        request.add_data(body)
        return

    def upload(self, url, args, filename):
        """
        _upload_

        Perform a file upload to the dqm server using HTTPS auth with the
        service proxy provided

        """
        uploadProxy = self.step.upload.proxy or os.environ.get('X509_USER_PROXY', None)

        class HTTPSCertAuth(HTTPS):
            def __init__(self, host, *args, **kwargs):
                HTTPS.__init__(self, host,
                               key_file = uploadProxy,
                               cert_file = uploadProxy,
                               **kwargs)

        class HTTPSCertAuthenticate(urllib2.AbstractHTTPHandler):
            def default_open(self, req):
                return self.do_open(HTTPSCertAuth, req)

        ident = "WMAgent python/%d.%d.%d" % sys.version_info[:3]

        msg = "HTTP POST upload arguments:\n"
        for arg in args:
            msg += "  ==> %s: %s\n" % (arg, args[arg])
        logging.info(msg)

        datareq = urllib2.Request(url + '/data/put')
        datareq.add_header('Accept-encoding', 'gzip')
        datareq.add_header('User-agent', ident)
        self.marshall(args, {'file' : filename}, datareq)
        if 'https://' in url:
            result = urllib2.build_opener(HTTPSCertAuthenticate()).open(datareq)
        else:
            result = urllib2.build_opener(urllib2.ProxyHandler({})).open(datareq)

        data = result.read()
        if result.headers.get('Content-encoding', '') == 'gzip':
            data = GzipFile(fileobj=StringIO(data)).read ()
        return (result.headers, data)
