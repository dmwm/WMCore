#!/usr/bin/env python
"""
_Step.Executor.DQMUpload_

Implementation of an Executor for a StageOut step

"""

__revision__ = "$Id: StageOut.py,v 1.26 2010/07/05 00:54:18 meloam Exp $"
__version__ = "$Revision: 1.26 $"

import os
import sys
import logging
import signal
import traceback
import httplib
import urllib2
import re
from mimetypes import guess_type
from gzip import GzipFile
from cStringIO import StringIO

# Compatibility with python2.4 or earlier
try:
    from hashlib import md5
except ImportError:
    from md5 import md5

from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.FwkJobReport.Report import Report

import WMCore.Storage.StageOutMgr as StageOutMgr
import WMCore.Storage.FileManager

from WMCore.WMSpec.ConfigSectionTree import nodeParent, nodeName
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure        

# Compatibility with python2.3 or earlier
HTTPS = httplib.HTTPS
if sys.version_info[:3] >= (2, 4, 0):
    HTTPS = httplib.HTTPSConnection


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
        if emulator is not None:
            return emulator.emulatePre(self.step)

        print "Steps.Executors.DQMUpload.pre called"
        return None

    def execute(self, emulator = None):
        """
        _execute_

        """
        #Are we using emulators again?
        if (emulator != None):
            return emulator.emulate(self.step, self.job)

        # Initial Setup

        # Pulling override parameters
        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()

        # Do we want to stage out the analysis files
        if self.step.stageOut.active:
            logging.info("Staging out is ACTIVE.")
            if overrides.has_key('newStageOut') and \
                                        overrides.get('newStageOut'):
                # new style
                logging.info("LOGARCHIVE IS USING NEW STAGEOUT CODE")
                manager = WMCore.Storage.FileManager.StageOutMgr(
                                retryPauseTime  = self.step.stageOut.retryDelay,
                                numberOfRetries = self.step.stageOut.retryCount,
                                **overrides)
            else:
                # old style
                manager = StageOutMgr.StageOutMgr(**overrides)
                manager.numberOfRetries = self.step.stageOut.retryCount
                manager.retryPauseTime  = self.step.stageOut.retryDelay

            # Set wait to 15 minutes
            waitTime = overrides.get('waitTime', 900)

        # Do we have to upload analysis file to the DQM server
        if self.step.upload.active:
            logging.info("DQM Upload is ACTIVE.")

            if self.step.upload.proxy:
                try:
                   self.stepSpace.getFromSandbox(self.step.upload.proxy)
                except Exception, ex:
                    #Let it go, it wasn't in the sandbox. Then it must be
                    #somewhere else
                    pass

        # Now, let's work...

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
                if not hasattr(analysisFile, 'fileName'):
                    msg = "Not an analysis file: %s" % file
                    logging.error(msg)
                    continue

                pfn = os.path.join(stepLocation,
                                   os.path.basename(analysisFile.fileName))

                # staging out file
                if self.step.stageOut.active:
                    try:
                        lfn = self.buildLFN(analysisFile.fileName)
                    except Exception, ex:
                        msg = "Unable to stage out DQM File: " \
                              "Could not create LFN from analysis file.\n"
                        msg += str(ex) + "\n"
                        msg += traceback.format_exc()
                        logging.error(msg)
                        raise WMExecutionFailure(60318, "DQMUploadFailure", msg)

                    aFileForTransfer = {'LFN': lfn,
                                        'PFN': pfn,
                                        'SEName': None,
                                        'StageOutCommand': None} 

                    self.stageOut(manager, aFileForTransfer, waitTime)

                # uploading file to the server
                if self.step.upload.active:
                    self.httpPost(pfn)

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
        if emulator is not None:
            return emulator.emulatePost(self.step)
        
        print "Steps.Executors.DQMUpload.post called"
        return None


    # Auxiliary Methods
    # Here, I'm putting the DQM upload methods and the special stage out
    # methods. These methods may be moved to some other location. For the
    # momemt just let them have a shelter before the winter comes.

    # Here comes the hotstepper...

    def httpPost(self, filename):
        """
        _httpPost_

        perform an HTTP POST operation to a webserver

        """
        args = {}
        args['url'] = self.step.upload.URL

        msg = "HTTP Upload of the following file is about to start:\n" \
              " => Filename: %s\n" % filename
        logging.info(msg)

        try:
            (headers, data) = self.upload(args, filename)
            msg = 'HTTP upload finished succesfully with response:\n'
            msg += 'Status code: %s\n' % headers.get("Dqm-Status-Code", None)
            msg += 'Message:     %s\n' % headers.get("Dqm-Status-Message", None)
            msg += 'Detail:      %s\n' % headers.get("Dqm-Status-Detail", None)
            msg += 'Data:        %s' % str(data)
            logging.info(msg)
        except urllib2.HTTPError, ex:
            msg = 'Automated upload of %s failed:\n' % filename
            msg += 'Status code: %s\n' % ex.hdrs.get("Dqm-Status-Code", None)
            msg += 'Message:     %s\n' % ex.hdrs.get("Dqm-Status-Message", None)
            msg += 'Detail:      %s\n' % ex.hdrs.get("Dqm-Status-Detail", None)
            msg += 'Error:       %s\n' % str(ex)
            logging.error(msg)
            raise WMExecutionFailure(60318, "DQMUploadFailure", msg)
        except Exception, ex:
            msg = 'Automated upload of %s failed:\n' % filename
            msg += 'Problem unknown.\n'
            msg += 'Traceback:       %s\n' % str(ex)
            msg += traceback.format_exc()
            logging.error(msg)
            raise WMExecutionFailure(60318, "DQMUploadFailure", msg)

    def encode(self, args, files):
        """
        Encode form (name, value) and (name, filename, type) elements into
        multi-part/form-data. We don't actually need to know what we are
        uploading here, so just claim it's all text/plain.
        """
        boundary = '----------=_DQM_FILE_BOUNDARY_=-----------'
        (body, crlf) = ('', '\r\n')
        for (key, value) in args.items():
            body += '--' + boundary + crlf
            body += ('Content-disposition: form-data; name="%s"' % key) + crlf
            body += crlf + str(value) + crlf
        for (key, filename) in files.items():
            filetype = guess_type(filename)[0] or 'application/octet-stream'
            body += '--' + boundary + crlf
            body += ('Content-Disposition: form-data; name="%s"; filename="%s"'
                     % (key, os.path.basename(filename))) + crlf
            body += ('Content-Type: %s' % filetype) + crlf
            body += crlf + open(filename, "r").read() + crlf
        body += '--' + boundary + '--' + crlf + crlf
        return ('multipart/form-data; boundary=' + boundary, body)

    def upload(self, args, file):
        """
        _upload_

        Perform a file upload to the dqm server using HTTPS auth with the
        service proxy provided

        """
        # Preparing a checksum
        blockSize = 0x10000
        def upd(m, data):
            m.update(data)
            return m
        fd = open(file, 'rb')
        try:
            contents = iter(lambda: fd.read(blockSize), '')
            m = reduce(upd, contents, md5())
        finally:
            fd.close()

        args['checksum'] = 'md5:' + m.hexdigest()
        args['size'] = str(os.stat(file)[6])
        proxyLoc = self.step.upload.proxy or os.environ.get('X509_USER_PROXY', None)

        class HTTPSCertAuth(HTTPS):
            def __init__(self, host, timeout):
                HTTPS.__init__(self, host,
                               key_file=proxyLoc,
                               cert_file=proxyLoc,
                               timeout=timeout)

        class HTTPSCertAuthenticate(urllib2.AbstractHTTPHandler):
            def default_open(self, req):
                return self.do_open(HTTPSCertAuth, req)

        #
        # HTTPS see : http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/VisMonitoring/DQMServer/scripts/visDQMUpload?r1=1.1&r2=1.2
        #
        # Add user identification
        # Eg: ProdAgent python version, this modules __version__ attr
        ident = "WMAgent Python %s.%s.%s %s" % (sys.version_info[0] ,
                                                  sys.version_info[1] ,
                                                  sys.version_info[2] ,
                                                  __version__)

        msg = "HTTP POST upload arguments:\n"
        for arg in args:
            msg += "  ==> %s: %s\n" % (arg, args[arg])
        logging.info(msg)

        try:
            authreq = urllib2.Request(args['url'] + '/digest')
            authreq.add_header('User-agent', ident)
            result = urllib2.build_opener(HTTPSCertAuthenticate()).open(authreq)
            cookie = result.headers.get('Set-Cookie')
            if not cookie:
                raise RuntimeError
        except Exception, ex:
            msg = "Unable to authenticate to DQM Server:\n"
            msg += "%s\n" % args['url']
            msg += "With Proxy from:\n"
            msg += "%s\n" % proxyLoc
            msg += "Exception: %s\n" % str(ex)
            msg += traceback.format_exc()
            logging.error(msg)
            raise WMExecutionFailure(60318, "DQMUploadFailure", msg)

        cookie = cookie.split(";")[0]

        # open a connection and upload the file
        url = args.pop('url') + "/data/put"
        request = urllib2.Request(url)
        (type, body) = self.encode(args, {'file': file})
        request.add_header('Accept-encoding', 'gzip')
        request.add_header('User-agent', ident)
        request.add_header('Cookie', cookie)
        request.add_header('Content-type',    type)
        request.add_header('Content-length',  str(len(body)))
        request.add_data(body)
        result = urllib2.build_opener().open(request)
        data   = result.read()
        if result.headers.get('Content-encoding', '') == 'gzip':
            data = GzipFile(fileobj=StringIO(data)).read()
        return (result.headers, data)

    # Auxiliary methods for staging out DQM files
    def stageOut(self, manager, file, waitTime):
        """
        _stageOut_

        Stages out a generic file.
        """
        # Small exception
        class Alarm(Exception):
            pass

        # Small signal handler
        def alarmHandler(signum, frame):
            raise Alarm

        signal.signal(signal.SIGALRM, alarmHandler)
        signal.alarm(waitTime)
        try:
            manager(file)
        except Alarm:
            msg = "Indefinite hang during stageOut."
            logging.error(msg)
            raise WMExecutionFailure(60311, "StageOutFailure", msg)
        except Exception, ex:
            # Turning off alarm
            signal.alarm(0)
            msg = "Unable to stage out DQM File:\n"
            msg += str(ex) + "\n"
            msg += traceback.format_exc()
            logging.error(msg)
            raise WMExecutionFailure(60311, "StageOutFailure", msg)

        signal.alarm(0)
        return

    def buildLFN(self, fileName):
        """
        _buildLFN_

        This method creates the LFN which the DQM files will be stage out with
        The LFN will have the ofllowing structure:

        [lfn_prefix]/[acq_era]/[sample_name]/[TIER]/[processing_string-processing_version]/[run_padding]/[analysis_file].root

        The different parts of the LFN will be figure out from the analysis
        file name which should match the following re:

        ^(DQM)_V\d+(_[A-Za-z]+)?_R(\d+)(__.*)?\.root

        where thelast group corresponds to the dataset name.
        """

        if hasattr(self.task.data, 'dqmBaseLFN'):
            baseLFN = self.task.data.dqmBaseLFN
        else:
            baseLFN = '/store/temp/WMAgent/dqm/'

        filebasename = os.path.basename(fileName)

        m = re.match(r'^(DQM)_V\d+(_[A-Za-z]+)?_R(\d+)(__.*)?\.root', 
                     filebasename)

        if not m:
            msg = "Unable to stage out DQM file %s: " \
                  "It's name does not match the expected " \
                  "convention." % filebasename
            logging.error(msg)
            raise RuntimeError, msg

        run_number = int(m.group(3))
        run_padding1 = str(run_number // 1000).zfill(4)

        dataset_name = m.group(4).replace("__", "/")

        if re.match(r'^(/[-A-Za-z0-9_]+){3}$', dataset_name) is None:
            msg = "Unable to stage out DQM file %s: " \
                  "Dataset %s It does not match the expected " \
                  "convention." % (filebasename, dataset_name)
            logging.error(msg)
            raise RuntimeError, msg

        m1 = re.match(r'^/([-A-Za-z0-9_]+)/?([A-Za-z0-9_]+)-([-A-Za-z0-9_]+)/([-A-Za-z0-9_]+)',
                      dataset_name)

        acq_era = m1.group(2)
        primary_ds = m1.group(1)
        tier = m1.group(4)
        proc_string = m1.group(3)

        lfn = os.path.join(baseLFN, acq_era, primary_ds, tier, proc_string,
                           run_padding1, filebasename)
        return lfn

