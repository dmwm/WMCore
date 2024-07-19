#!/usr/bin/env python
"""
_Step.Executor.DQMUpload_

Implementation of an Executor for a DQMUpload step

"""
from __future__ import print_function

from future.utils import viewitems

import logging
import os
import sys
from io import BytesIO
from functools import reduce
from gzip import GzipFile
from hashlib import md5
from mimetypes import guess_type

try:
    # python2
    import urllib2
    HTTPError = urllib2.HTTPError
    OpenerDirector = urllib2.OpenerDirector
    Request = urllib2.Request
    ProxyHandler = urllib2.ProxyHandler
except:
    # python3
    import urllib.request
    HTTPError = urllib.error.HTTPError
    OpenerDirector = urllib.request.OpenerDirector
    Request = urllib.request.Request
    ProxyHandler = urllib.request.ProxyHandler

from Utils.Utilities import decodeBytesToUnicode, encodeUnicodeToBytesConditional, encodeUnicodeToBytes
from Utils.PythonVersion import PY3, PY2

from WMCore.FwkJobReport.Report import Report
from WMCore.Services.HTTPS.HTTPSAuthHandler import HTTPSAuthHandler
from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure


class DQMUpload(Executor):
    """
    _DQMUpload_

    Execute a DQMUpload Step

    """

    def pre(self, emulator=None):
        """
        _pre_

        Pre execution checks

        """
        # Are we using an emulator?
        if emulator is not None:
            return emulator.emulatePre(self.step)

        logging.info("Steps.Executors.%s.pre called", self.__class__.__name__)
        return None

    def execute(self, emulator=None):
        """
        _execute_

        """
        # Are we using emulators again?
        if emulator is not None:
            return emulator.emulate(self.step, self.job)

        logging.info("Steps.Executors.%s.execute called", self.__class__.__name__)

        if self.step.upload.proxy:
            try:
                self.stepSpace.getFromSandbox(self.step.upload.proxy)
            except Exception as ex:
                # Let it go, it wasn't in the sandbox. Then it must be
                # somewhere else
                del ex

        # Search through steps for analysis files
        for step in self.stepSpace.taskSpace.stepSpaces():
            if step == self.stepName:
                # Don't try to parse your own report; it's not there yet
                continue
            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s", step)
            reportLocation = os.path.join(stepLocation, 'Report.pkl')
            if not os.path.isfile(reportLocation):
                logging.error("Cannot find report for step %s in space %s", step, stepLocation)
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

    def post(self, emulator=None):
        """
        _post_

        Post execution checkpointing

        """
        # Another emulator check
        if emulator is not None:
            return emulator.emulatePost(self.step)

        logging.info("Steps.Executors.%s.post called", self.__class__.__name__)
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

        with open(filename, 'rb') as fd:
            contents = iter(lambda: fd.read(blockSize), b'')
            m = reduce(upd, contents, md5())

        args['checksum'] = 'md5:%s' % m.hexdigest()
        # args['checksum'] = 'md5:%s' % md5.new(filename).read()).hexdigest()
        args['size'] = os.path.getsize(filename)

        msg = "HTTP Upload is about to start:\n"
        msg += " => URL: %s\n" % self.step.upload.URL
        msg += " => Filename: %s\n" % filename
        logging.info(msg)

        try:
            logException = True
            for uploadURL in self.step.upload.URL.split(';'):
                (headers, data) = self.upload(uploadURL, args, filename)
                msg = '  Status code: %s\n' % headers.get("Dqm-Status-Code", None)
                msg += '  Message: %s\n' % headers.get("Dqm-Status-Message", None)
                msg += '  Detail: %s\n' % headers.get("Dqm-Status-Detail", None)
                msg += '  Data: %s\n' % str(data)
                if int(headers.get("Dqm-Status-Code", 400)) >= 400:
                    logException = False
                    raise Exception(msg)
                else:
                    msg = 'HTTP upload finished successfully with response:\n' + msg
                    logging.info(msg)
        except HTTPError as ex:
            msg = 'HTTP upload failed with response:\n'
            msg += '  Status code: %s\n' % ex.hdrs.get("Dqm-Status-Code", None)
            msg += '  Message: %s\n' % ex.hdrs.get("Dqm-Status-Message", None)
            msg += '  Detail: %s\n' % ex.hdrs.get("Dqm-Status-Detail", None)
            msg += '  Error: %s\n' % str(ex)
            logging.exception(msg)
            raise WMExecutionFailure(70318, "DQMUploadFailure", msg)
        except Exception as ex:
            msg = 'HTTP upload failed! Error:\n%s' % str(ex)
            if logException:
                logging.exception(msg)
            else:
                logging.error(msg)
            raise WMExecutionFailure(70318, "DQMUploadFailure", msg)

        return

    def filetype(self, filename):
        return guess_type(filename)[0] or 'application/octet-stream'

    def encode(self, args, files):
        """
        Encode form (name, value) and (name, filename, type) elements into
        multi-part/form-data. We don't actually need to know what we are
        uploading here, so just claim it's all text/plain.
        """
        boundary = b'----------=_DQM_FILE_BOUNDARY_=-----------'
        (body, crlf) = (b'', b'\r\n')
        for (key, value) in viewitems(args):
            logging.debug("encode value - %s, %s", type(value), value)
            if PY2:
                payload = str(value)
            elif PY3:
                payload = value
                if not isinstance(payload, bytes):
                    payload = str(payload)
                payload = encodeUnicodeToBytes(payload)
                key = encodeUnicodeToBytes(key)
            logging.debug("encode payload - %s, %s", type(payload), payload)
            body += b'--' + boundary + crlf
            body += (b'Content-Disposition: form-data; name="%s"' % key) + crlf
            body += crlf + payload + crlf
        for (key, filename) in viewitems(files):
            body += b'--' + boundary + crlf
            key = encodeUnicodeToBytesConditional(key, condition=PY3)
            filepath = encodeUnicodeToBytesConditional(os.path.basename(filename), condition=PY3)
            body += (b'Content-Disposition: form-data; name="%s"; filename="%s"'
                     % (key, filepath)) + crlf
            body += (b'Content-Type: %s' % encodeUnicodeToBytes(self.filetype(filename))) + crlf
            body += (b'Content-Length: %d' % os.path.getsize(filename)) + crlf
            logging.debug("encode body (without binary file) -%s, %s", type(body), body)
            with open(filename, "rb") as fd:
                body += crlf + fd.read() + crlf
            body += b'--' + boundary + b'--' + crlf + crlf
        return (b'multipart/form-data; boundary=' + boundary, body)

    def marshall(self, args, files, request):
        """
        Marshalls the arguments to the CGI script as multi-part/form-data,
        not the default application/x-www-form-url-encoded. This improves
        the transfer of the large inputs and eases command line invocation
        of the CGI script.
        """
        (contentType, body) = self.encode(args, files)
        request.add_header(b'Content-Type', contentType)
        if PY2:
            request.add_header('Content-Length', str(len(body)))
            request.add_data(body)
        elif PY3:
            request.data = body
        return

    def upload(self, url, args, filename):
        """
        _upload_

        Perform a file upload to the dqm server using HTTPS auth with the
        service proxy provided
        """
        ident = "WMAgent python/%d.%d.%d" % sys.version_info[:3]
        uploadProxy = self.step.upload.proxy or os.environ.get('X509_USER_PROXY', None)
        logging.info("Using proxy file: %s", uploadProxy)
        logging.info("Using CA certificate path: %s", os.environ.get('X509_CERT_DIR'))

        msg = "HTTP POST upload arguments:\n"
        for arg in args:
            msg += "  ==> %s: %s\n" % (arg, args[arg])
        logging.info(msg)

        handler = HTTPSAuthHandler(key=uploadProxy, cert=uploadProxy)
        opener = OpenerDirector()
        opener.add_handler(handler)

        # setup the request object
        url = decodeBytesToUnicode(url) if PY3 else encodeUnicodeToBytes(url)
        datareq = Request(url + '/data/put')
        datareq.add_header('Accept-encoding', 'gzip')
        datareq.add_header('User-agent', ident)
        self.marshall(args, {'file': filename}, datareq)

        if 'https://' in url:
            result = opener.open(datareq)
        else:
            opener.add_handler(ProxyHandler({}))
            result = opener.open(datareq)

        data = result.read()
        if result.headers.get('Content-encoding', '') == 'gzip':
            data = GzipFile(fileobj=BytesIO(data)).read()

        return (result.headers, data)
