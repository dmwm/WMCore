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
import pickle
import json
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
from WMCore.Algorithms.Alarm import Alarm, alarmHandler
from WMCore.Storage.DeleteMgr import DeleteMgr, DeleteMgrError
from WMCore.Storage.FileManager import DeleteMgr as NewDeleteMgr
import WMCore.Storage.StageOutMgr as StageOutMgr
import time

class DQMUpload(Executor):
    """
    _DQMUpload_

    Execute a DQMUpload Step

    """

    def __init__(self):
        super(DQMUpload, self).__init__()
        self.retryDelay = 300
        self.retryCount = 3
        self.registerLFNBase = '/store/unmerged/DQMGUI'
        self.registerEOSPrefix = '/eos/cms'
        self.registerURL = 'https://cmsweb-testbed.cern.ch/dqm/offline-test-new/api/v1/register'

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
                    # uploading file to the server (old visDQMUpload method)
                    self.httpPost(os.path.join(stepLocation,
                                               os.path.basename(analysisFile.fileName)))
                    # Upload to EOS and register (new method)
                    self.uploadToEOSAndRegister(step, stepLocation, analysisFile)

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
                    msg = 'HTTP upload finished succesfully with response:\n' + msg
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

    def _uploadToEOS(self, stepLocation, analysisFile):
        """
        Upload  an analysis file to EOS

        :param stepLocation: Path location for step
        :param analysisFile: analysisFile from WMCore step report
        :return: bool, with upload status
        """
        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()
        eosStageOutParams = {}
        eosStageOutParams['command'] = overrides.get('command', "xrdcp")
        eosStageOutParams['option'] = overrides.get('option', "--wma-disablewriterecovery")
        eosStageOutParams['phedex-node'] = overrides.get('eos-phedex-node', "T2_CH_CERN")
        eosStageOutParams['lfn-prefix'] = overrides.get('eos-lfn-prefix',
                                                        "root://eoscms.cern.ch/%s" % self.registerEOSPrefix)

        # Switch between old and newstageOut manager.
        # Use old by default
        useNewStageOutCode = False
        if 'newStageOut' in overrides and overrides.get('newStageOut'):
            useNewStageOutCode = True

        if not useNewStageOutCode:
            # old style
            eosmanager = StageOutMgr.StageOutMgr(**eosStageOutParams)
            eosmanager.numberOfRetries = self.retryCount
            eosmanager.retryPauseTime = self.retryDelay
        else:
            # new style
            logging.info("DQMUpload is using new StageOut code for EOS Copy")
            eosmanager = WMCore.Storage.FileManager.StageOutMgr(
                retryPauseTime=self.retryDelay,
                numberOfRetries=self.retryCount,
                **eosStageOutParams)

        eosFileInfo = {'LFN': self.getAnalysisFileLFN(analysisFile),
                       'PFN': os.path.join(stepLocation,
                                           os.path.basename(analysisFile.fileName)),
                       'PNN': None,  # Assigned in StageOutMgr
                       'GUID': None  # Assigned in StageOutMgr
                       }

        msg = "Writing DQM root files to CERN EOS with retries: %s and retry pause: %s"
        logging.info(msg, eosmanager.numberOfRetries, eosmanager.retryPauseTime)
        try:
            eosmanager(eosFileInfo)
        except Alarm:
            msg = "Indefinite hangout while staging out to EOS"
            logging.error(msg)
            raise WMExecutionFailure(70317, "DQMUploadStageOutTimeout", msg)
        except Exception as ex:
            msg = "EOS copy failed, lfn: %s. Error: %s" % (eosFileInfo['LFN'], str(ex))
            logging.exception(msg)
            raise WMExecutionFailure(70318, "DQMUploadFailure", msg)

        return eosFileInfo['LFN']

    def getAnalysisFileLFN(self, analysisFile):
        """
        Construct an LFN for an analysisFile

        :param analysisFile: analysisFile from WMCore step report
        :return: str, analysis file LFN
        """
        # If lfn start with '/', make it relative to it
        lfnFile = os.path.relpath(analysisFile.lfn, start='/')
        lfn = os.path.join(self.registerLFNBase, lfnFile)

        return lfn

    def _register(self, registerURL, args):
        """
        POST request to register URL

        :param registerURL: str, url for registration
        :param args: dict, POST arguments
        :return: result object from opening  the request
        """

        ident = "WMAgent python/%d.%d.%d" % sys.version_info[:3]
        uploadProxy = self.step.upload.proxy or os.environ.get('X509_USER_PROXY', None)
        logging.info("Using proxy file: %s", uploadProxy)
        logging.info("Using CA certificate path: %s", os.environ.get('X509_CERT_DIR'))

        msg = "HTTP Register POST arguments: %s\n" % args
        logging.info(msg)

        handler = HTTPSAuthHandler(key=uploadProxy, cert=uploadProxy)
        opener = OpenerDirector()
        opener.add_handler(handler)

        # setup the request object
        url = decodeBytesToUnicode(registerURL)
        datareq = Request(url)
        datareq.add_header('Accept-encoding', 'gzip')
        datareq.add_header('User-agent', ident)
        datareq.data = encodeUnicodeToBytes(json.dumps(args))

        if 'https://' in url:
            result = opener.open(datareq)
        else:
            opener.add_handler(ProxyHandler({}))
            result = opener.open(datareq)

        return result

    def _registerAnalysisFile(self, stepName, analysisFile):
        """
        Register a file to new DQM GUi
        - https://github.com/cms-DQM/dqmgui
        HTTP request body:
            [{"dataset": "/a/b/c", "run": "123456", "lumi": "0", "file": "/eos/cms/store/group/comm_dqm/DQMGUI_data/location/file.root", "fileformat": 1}]
        Set lumis to 0, as this is reproducing current per Run based root files
        - https://github.com/dmwm/WMCore/issues/10287#issuecomment-1052558061
        DQM root files produced by Harvesting processing are type 1 (plain ROOT)
        - https://github.com/cms-DQM/dqmgui#file-formats

        :param stepName: str, Name of the step we are running through
        :param analysisFile: WMCore.Configuration.ConfigSection, Analysis file in this step from WMCore step report
        :return:
        """
        # Get task description
        jobBag = self.job.getBaggage()
        datasetName = self.job.get('inputDataset', None)

        # Get runNumber, depending on the harvesting job mode (byRun vs multiRun)
        multiRun = getattr(jobBag, "multiRun", False)
        forceRunNumber = getattr(jobBag, "forceRunNumber", 999999)
        if multiRun:
            runNumber = forceRunNumber
        else:
            try:
                runNumber = list(self.job['mask']['runAndLumis'].keys())[0]
            except Exception as ex:
                msg = "Error while retrieving run number from job definition:\n %s" % str(ex)
                raise WMExecutionFailure(70320, "DQMUploadFailure", msg)

        args = {}
        args['file'] = self.registerEOSPrefix + \
                       self.getAnalysisFileLFN(analysisFile)
        args['dataset'] = datasetName
        args['run'] = runNumber
        args['lumi'] = 0
        args['fileformat'] = 1

        msg = "HTTP Upload is about to start:\n"
        msg += " => URL: %s\n" % self.registerURL
        msg += " => Filename: %s\n" % args['file']
        logging.info(msg)

        for numRetry in range(self.retryCount + 1):
            try:
                logException = True
                result = self._register(self.registerURL, [args])
                msg = '  Status code: %s\n' % result.getcode()
                if result.getcode() >= 400 or result.getcode() is None:
                    logException = False
                    raise Exception(msg)
                else:
                    msg = 'HTTP POST to register url finished succesfully with response:\n' + msg
                    logging.info(msg)
                    break
            except Exception as ex:
                if numRetry == self.retryCount:
                    msg = 'HTTP POST to register url failed! Error:\n%s' % str(ex)
                    if logException:
                        logging.exception(msg)
                    else:
                        logging.error(msg)
                    raise WMExecutionFailure(70319, "DQMUploadFailure", msg)
                time.sleep(self.retryDelay)

        return

    def _deleteFromEOS(self, eosFileLFN):
        """
        Delete a file uploaded to EOS.
        I.e.: If we upload a DQM root file but registration failed for some reason

        :param eosFileLFN: str, LFN file
        :return:
        """
        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()

        # Note "xrdcp" here will basically map to the XRDCPImpl storage backend
        # The actual delete command is set in the backend
        eosStageOutParams = {}
        eosStageOutParams['command'] = overrides.get('command', "xrdcp")
        eosStageOutParams['phedex-node'] = overrides.get('eos-phedex-node', "T2_CH_CERN")
        eosStageOutParams['lfn-prefix'] = overrides.get('eos-lfn-prefix',
                                                        "root://eoscms.cern.ch/%s" % self.registerEOSPrefix)

        # Switch between old and new stageOut manager.
        # Use old by default
        useNewStageOutCode = False
        if 'newStageOut' in overrides and overrides.get('newStageOut'):
            useNewStageOutCode = True

        if not useNewStageOutCode:
            # old style
            manager = DeleteMgr(**eosStageOutParams)
            manager.numberOfRetries = self.retryCount
            manager.retryPauseTime = self.retryDelay
        else:
            # new style
            logging.info("DQMUpload is using new StageOut code to delete files from EOS")
            manager = NewDeleteMgr(retryPauseTime=self.retryDelay,
                                   numberOfRetries=self.retryCount,
                                   **eosStageOutParams)

        logging.info("Deleting LFN: %s", eosFileLFN)
        eosFileInfo = {'LFN': eosFileLFN,
                       'PFN': None,  # PFNs are assigned in the Delete Manager
                       'PNN': None,  # PNNs are assigned in the Delete Manager
                       'StageOutCommand': None}
        try:
            manager(fileToDelete=eosFileInfo)
        except Alarm:
            msg = "Indefinite hang while deleting file from EOS"
            logging.error(msg)
            raise WMExecutionFailure(70321, "DQMUploadStageOutTimeout", msg) from None
        except Exception as ex:
            msg = "General failure while deleting file. Error: %s" % str(ex)
            logging.error(msg)
            raise WMExecutionFailure(70322, "DQMUploadFailure", msg) from None

        return

    def uploadToEOSAndRegister(self, stepName, stepLocation, analysisFile):
        """
        Copy DQM Root files to EOS and register files to new DQMGUI

        :param stepName: str, Name of the step we are running through
        :param stepLocation: str, Path location of the step
        :param analysisFile: WMCore.Configuration.ConfigSection, Analysis file in this step from WMCore step report
        :return:
        """
        eosFileLFN = self._uploadToEOS(stepLocation, analysisFile)
        try:
            self._registerAnalysisFile(stepName, analysisFile)
        except:
            # If we fail to register, delete uploaded file
            self._deleteFromEOS(eosFileLFN)
            raise
        return
