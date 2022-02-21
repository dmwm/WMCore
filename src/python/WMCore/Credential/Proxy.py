#!/usr/bin/env python
"""
_Proxy_
Wrap gLite proxy commands.
"""

from __future__ import division
from builtins import filter, str, range

import contextlib
import copy
import os
import re
import subprocess
import time
from datetime import datetime
from hashlib import sha1

from WMCore.Credential.Credential import Credential
from WMCore.WMException import WMException
from Utils.PythonVersion import PY3
from Utils.Utilities import decodeBytesToUnicode, encodeUnicodeToBytes


def execute_command(command, logger, timeout, redirect=True):
    """
    _execute_command_
    Function to manage commands.
    """

    stdout, stderr, rc = None, None, 99999
    if redirect:
        proc = subprocess.Popen(
            command, shell=True, cwd=os.environ['PWD'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
    else:
        proc = subprocess.Popen(command, shell=True, cwd=os.environ['PWD'])

    t_beginning = time.time()
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command)
            return stdout, rc

        time.sleep(0.1)

    stdout, stderr = proc.communicate()
    stdout = decodeBytesToUnicode(stdout) if PY3 else stdout
    stderr = decodeBytesToUnicode(stderr) if PY3 else stderr
    rc = proc.returncode

    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))

    return stdout, stderr, rc


def destroyListCred(credNameList=None, credTimeleftList=None, logger=None, timeout=0):
    """
    _destroyListCred_
    Get list of credential name and their timelefts to destroy the one
    with timeleft = 0 from myproxy.
    """
    credNameList = credNameList or []
    credTimeleftList = credTimeleftList or {}
    cleanCredCmdList = []

    for credIdx in range(len(credNameList)):
        hours, minutes, seconds = credTimeleftList[credIdx]
        timeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        if timeleft == 0:
            cleanupCmd = "myproxy-destroy -d -k %s" % (credNameList[credIdx])
            cleanCredCmdList.append(cleanupCmd)

    cleanCredCmd = " && ".join(cleanCredCmdList)
    if len(cleanCredCmd) > 0:
        execute_command(cleanCredCmd, logger, timeout)

    return


# TODO not used anymore. #3810 deletes lasts unused dependencies in the client
class CredentialException(WMException):
    """
    Credential exceptions should be defined in this class.
    """
    pass


@contextlib.contextmanager
def myProxyEnvironment(userDN, serverCert, serverKey, myproxySrv, proxyDir, logger):
    """
    Allows us to user a context manager within which a MyProxy delegated proxy
    is set to X509_USER_PROXY and things are restored on exit
    """

    originalEnvironment = copy.deepcopy(os.environ)
    try:
        args = {}
        args['server_cert'] = serverCert
        args['server_key'] = serverKey
        args['myProxySvr'] = myproxySrv
        args['credServerPath'] = proxyDir
        args['logger'] = logger
        proxy = Proxy(args=args)

        proxy.userDN = userDN
        filename = proxy.logonRenewMyProxy()
        os.environ['X509_USER_PROXY'] = filename

        # host certs can be taken first, get rid of them
        deleteKeys = ['X509_USER_CERT', 'X509_USER_KEY', 'X509_HOST_CERT', 'X509_HOST_KEY']
        for key in deleteKeys:
            if key in os.environ:
                del os.environ[key]
        yield filename
    finally:
        os.environ = originalEnvironment


class Proxy(Credential):
    """
    Basic class to handle user Proxy
    """

    def __init__(self, args):
        """
        __init__
        Initialize proxy object.
        """
        Credential.__init__(self, args)

        # Set the default commands execution timeout to 20 mn
        self.commandTimeout = args.get("ServiceContactTimeout", 1200)
        self.myproxyServer = args.get("myProxySvr", 'myproxy.cern.ch')
        self.serverDN = args.get("serverDN", '')
        self.userDN = args.get("userDN", '')
        self.proxyValidity = args.get("proxyValidity", '')  # lenght of the proxy
        self.myproxyValidity = args.get("myproxyValidity", '168:00')  # lenght of the myproxy
        self.myproxyMinTime = args.get("myproxyMinTime", 4)  # threshold used in checkProxy
        self.myproxyAccount = args.get("myproxyAccount", "")  # to be used when computing myproxy account (-l option)
        self.rfcCompliant = args.get("rfcCompliant", True)  # to be used when computing myproxy account (-l option)
        self.trustedRetrievers = None

        # User vo paramaters
        self.vo = 'cms'
        self.group = args.get("group", '')
        self.role = args.get("role", '')

        self.logger = args.get("logger", '')

        ## adding ui script to source
        self.uisource = args.get("uisource", '')
        self.cleanEnvironment = args.get("cleanEnvironment", False)

        ## adding credential path
        self.credServerPath = args.get("credServerPath", '/tmp')
        if not self.cmd_exists('voms-proxy-info'):
            raise CredentialException('voms-proxy-info command not found')

    def setEnv(self, cmd):
        """
        Return the source command to be pre added to each command to be executed.
        """
        ret = cmd
        if self.uisource is not None and len(self.uisource) > 0:
            ret = 'source ' + self.uisource + ' && ' + ret
        if self.cleanEnvironment:
            # Need to escape ' in cmd
            ret = ret.replace("'", r"'\''")
            ret = "env -i sh -c '%s'" % ret

        return ret

    def cmd_exists(self, cmd):
        return subprocess.call(self.setEnv("type " + cmd), shell=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE) == 0
    def getUserCertFilename(self):
        if 'X509_USER_CERT' not in os.environ:
            ucFilename = '~/.globus/usercert.pem'
        else:
            ucFilename = os.environ['X509_USER_CERT']
        return ucFilename

    def getUserCertTimeLeft(self, openSSL=True):
        """
        Return the number of seconds until the expiration of the user cert
        in .globus/usercert.pem or $X509_USER_CERT if set
        Uses openssl by default and fallback to voms-proxy-info in case of problems
        """
        certLocation = self.getUserCertFilename()
        if openSSL:
            out, _, retcode = execute_command('openssl x509 -noout -in %s -dates'
                                              % certLocation, self.logger,
                                              self.commandTimeout)
            if retcode == 0:
                out = out.split('notAfter=')[1]
                if out[-1] == '\n':
                    out = out[:-1]

                possibleFormats = ['%b  %d  %H:%M:%S %Y %Z',
                                   '%b %d %H:%M:%S %Y %Z']
                exptime = None
                for frmt in possibleFormats:
                    try:
                        exptime = datetime.strptime(out, frmt)
                    except ValueError:
                        pass  # try next format
                if not exptime:
                    # If we cannot decode the output in any way print
                    # a message and fallback to voms-proxy-info command
                    self.logger.warning(
                        'Cannot decode "openssl x509 -noout -in %s -dates" date format. Falling back to voms-proxy-info' % certLocation)
                else:
                    # if everything is fine then we are ready to return!!
                    timeleft = (exptime - datetime.utcnow()).total_seconds()
                    return int(timeleft)

        # uses this as a fallback
        timeleft = self.getTimeLeft(proxy=certLocation, checkVomsLife=False)
        if self.retcode:
            raise CredentialException('Cannot get user certificate remaining time with "voms-proxy-info"')

        return timeleft

    def getUserCertEnddate(self, openSSL=True):
        """
        Return the number of days until the expiration of the user cert
        in .globus/usercert.pem or $X509_USER_CERT if set
        Uses openssl by default and fallback to voms-proxy-info in case of problems
        """
        timeleft = self.getUserCertTimeLeft(openSSL)
        daystoexp = int(timeleft // (60. * 60 * 24))

        return daystoexp

    def getProxyDetails(self):
        """
        Return the vo details that should be in the user proxy.
        """
        proxyDetails = "/%s" % self.vo
        if self.group:
            proxyDetails += "/%s" % self.group
        if self.role and self.role != 'NULL':
            proxyDetails += "/Role=%s" % self.role

        return proxyDetails

    def getProxyFilename(self, serverRenewer=False):
        """
        Try to get the proxy file path from:

        1.  the delegated server
        2.  from an ui
        """
        if serverRenewer:
            uniqName = self.userDN + self.vo + self.group + self.role
            proxyFilename = os.path.join(self.credServerPath,
                                         sha1(encodeUnicodeToBytes(uniqName)).hexdigest())
        elif 'X509_USER_PROXY' in os.environ:
            proxyFilename = os.environ['X509_USER_PROXY']
        else:
            proxyFilename = '/tmp/x509up_u' + str(os.getuid())

        return proxyFilename

    def getSubject(self, proxy=None):
        """
        Get proxy subject from a proxy file.
        """
        subject = None

        if proxy is None:
            proxy = self.getProxyFilename()
        getSubjectCmd = "voms-proxy-info -file " + proxy + " -identity"
        subject, _, retcode = execute_command(self.setEnv(getSubjectCmd), self.logger, self.commandTimeout)

        if retcode == 0:
            subject = subject.strip()

        return subject

    def getSubjectFromCert(self, certFile=None):
        """
        Get the subject from cert file.
        """
        subject = ''

        if not certFile:
            certFile = self.getProxyFilename()

        subjFromCertCmd = 'openssl x509 -in ' + certFile + ' -subject -noout'
        subjectResult, _, retcode = execute_command(self.setEnv(subjFromCertCmd), self.logger, self.commandTimeout)

        subject = None
        if retcode == 0:
            subject = subjectResult.split('subject=')[1].strip()

        return subject

    def getUserName(self, proxy=None):
        """
        Get the user name from a proxy file.
        """
        subject = self.getSubject(proxy)
        uName = ''

        for cname in subject.split('/'):
            if cname[:3] == "CN=" and cname[3:].find('proxy') == -1:
                uName = cname[3:]

        return uName

    def getMyproxyUsernameForCRAB(self):
        """
        :return: username (a string) to be used when myproxy-* command is called with the option
               -l | --username        <username> Username for the delegated proxy

        CRAB (and only CRAB as of March 2020) needs to obtain from myproxy a proxy from a credential
        uploaded by another user. So that it can submit jobs and data transfers authenticated
        with the user proxy. For this a special credential is uploaded in myproxy server by
        CRAB Client which is connected to a username associated to the user and has a
        list of authorized retrievers as a list of DN's of CRAB TaskWorkers maintained in
        central CRAB configuration.
        The username is passed to myproxy-* command via the "-l" option
        Three different algorithms for defining this username have been used in CRAB:
        1. the hash of the user DN + the fqdn of the CRAB REST host
        2. the user CERN primary account username + the _CRAB string
        3. the hash of the user DN
        During spring 2020 CRAB migrates from 1. to 2. For a smooth migration the
        new client needs to upload both credentials and the TW will try 2. and fall back to 1.
        Only after all tasks submited with old client are gone from the system, can we change
        to support only 2.
        The reasons to change from 1. to 2. are to make the username readable (helps support) and
        to allow using different REST hosts (helps in K8s world). The reasons for the complicated
        recipe in 1. are unknown, aside some security by obscurity attempt.
        The caller decides if the call to myproxy-* done by this module will use 1. or 2. via
        the userName key in the dictionary passed as argument to Proxy() at __init__ time
           - if the dictionary contains the key 'userName', algorithm 2 is used
           - if the dictionary does not have it, algorithm 1. is used
        But at times user change their DN and can't act on credentials stored in myproxy
        with old DN. They need to switch to a new credential name, for this 3. neede3d to be put back
        in Summer 2020.
        """
        if self.userName:
            self.logger.debug("using %s as credential login name", self.userName)
            username = self.userName
        else:
            if self.myproxyAccount:
                self.logger.debug(
                    "Calculating hash of %s for credential name" % (self.userDN + "_" + self.myproxyAccount))
                username = sha1(encodeUnicodeToBytes(self.userDN + "_" + self.myproxyAccount)).hexdigest()
            else:
                self.logger.debug(
                    "Calculating hash of %s for credential name" % (self.userDN))
                username = sha1(encodeUnicodeToBytes(self.userDN)).hexdigest()
        return username

    def checkAttribute(self, proxy=None):
        """
        Check attributes from a proxy file.
        """
        valid = True

        if proxy is None:
            proxy = self.getProxyFilename()

        checkAttCmd = 'voms-proxy-info -fqan -file ' + proxy
        proxyDetails = self.getProxyDetails()

        attribute = execute_command(self.setEnv(checkAttCmd), self.logger, self.commandTimeout)

        if not re.compile(r"^" + proxyDetails).search(attribute[0]):
            valid = False

        return valid

    def create(self):
        """
        Proxy creation.
        """
        createCmd = 'voms-proxy-init -voms %s:%s -valid %s %s' % (
            self.vo, self.getProxyDetails(), self.proxyValidity, '-rfc' if self.rfcCompliant else '')
        execute_command(self.setEnv(createCmd), self.logger, self.commandTimeout, redirect=False)

        return

    def renew(self):
        """
        Proxy renew.
        """
        self.create()
        return

    def destroy(self, credential=None):
        """
        Proxy destruction.
        """
        if not credential:
            credential = self.getProxyFilename()

        destroyCmd = 'rm -f %s' % credential
        execute_command(destroyCmd, self.logger, self.commandTimeout)

        return

    def delegate(self, credential=None, serverRenewer=False, nokey=False):
        """
        Delegate the user proxy to myproxy.
        It is possible also to delegate a server
        (specifying serverRenewer = True) to
        manage your proxy in myproxy server.
        """
        if not credential:
            credential = self.getProxyFilename(serverRenewer)

        if self.myproxyServer:
            myproxyDelegCmd = 'export GT_PROXY_MODE=%s ; myproxy-init -d -n -s %s' % (
                'rfc' if self.rfcCompliant else 'old', self.myproxyServer)

            if nokey is True:
                myproxyUsername = self.getMyproxyUsernameForCRAB()
                myproxyDelegCmd = 'export GT_PROXY_MODE=%s ; myproxy-init -d -n -s %s -x -R \'%s\' -x -Z \'%s\' -l \'%s\' -t 168:00 -c %s' \
                                  % ('rfc' if self.rfcCompliant else 'old', self.myproxyServer, self.serverDN, \
                                     self.serverDN, myproxyUsername, self.myproxyValidity)
            elif serverRenewer and len(self.serverDN.strip()) > 0:
                serverCredName = sha1(encodeUnicodeToBytes(self.serverDN)).hexdigest()
                myproxyDelegCmd += ' -x -R \'%s\' -Z \'%s\' -k %s -t 168:00 -c %s ' \
                                   % (self.serverDN, self.serverDN, serverCredName, self.myproxyValidity)
            _, stderr, _ = execute_command(self.setEnv(myproxyDelegCmd), self.logger, self.commandTimeout)
            if stderr.find('proxy will expire') > -1:
                raise CredentialException('Your certificate is shorter than %s ' % self.myproxyValidity)
        else:
            self.logger.error("myproxy server not set for the proxy %s" % credential)

        return

    def getMyProxyTimeLeft(self, proxy=None, serverRenewer=False, nokey=False):
        """
        Get myproxy timeleft. Speciying serverRenewer=True means
        that your are delegating your proxy management in myproxy
        to a server.
        """
        proxyTimeleft = -1
        if self.myproxyServer:
            if nokey is True and serverRenewer is True:
                myproxyUsername = self.getMyproxyUsernameForCRAB()
                checkMyProxyCmd = 'myproxy-info -l %s -s %s' % (myproxyUsername, self.myproxyServer)
                output, _, retcode = execute_command(self.setEnv(checkMyProxyCmd), self.logger, self.commandTimeout)
                if retcode > 0 or not output:
                    return proxyTimeleft

                trustedRetrList = re.compile('trusted retrieval policy: (.*)').findall(output)
                if len(trustedRetrList) > 1 or len(trustedRetrList) == 0:
                    raise CredentialException(
                        "Unexpected result while decoding trusted retrievers list: " + str(trustedRetrList))
                else:
                    self.trustedRetrievers = trustedRetrList[0]

                timeleftList = re.compile(
                    "timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)").findall(output)
                if len(timeleftList) > 1 or len(timeleftList) == 0:
                    raise CredentialException(str(timeleftList))
                else:
                    hours, minutes, seconds = timeleftList[0]
                    proxyTimeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                    return proxyTimeleft

            if not proxy:
                proxy = self.getProxyFilename(serverRenewer)
            checkMyProxyCmd = 'myproxy-info -d -s ' + self.myproxyServer
            output, _, retcode = execute_command(self.setEnv(checkMyProxyCmd), self.logger, self.commandTimeout)

            if retcode > 0 or not output:
                return proxyTimeleft

            timeleftList = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)").findall(
                output)
            hours, minutes, seconds = 0, 0, 0

            if not serverRenewer:

                try:
                    hours, minutes, seconds = timeleftList[0]
                    proxyTimeleft = int(hours) * 3600 + int( \
                        minutes) * 60 + int(seconds)
                except Exception as e:
                    self.logger.error('Error extracting timeleft from proxy %s' % str(e))

            elif len(self.serverDN.strip()) > 0:
                serverCredName = sha1(encodeUnicodeToBytes(self.serverDN)).hexdigest()
                credNameList = re.compile(" name: (?P<CN>.*)").findall(output)

                if len(timeleftList) == len(credNameList):
                    credTimeleftList = timeleftList
                else:
                    credTimeleftList = timeleftList[1:]

                if serverCredName not in credNameList:
                    self.logger.error('Your proxy needs retrieval and renewal policies for the requested server.')
                    proxyTimeleft = 0
                else:
                    try:
                        hours, minutes, seconds = credTimeleftList[credNameList.index(serverCredName)]
                        proxyTimeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                    except Exception as e:
                        self.logger.error('Error extracting timeleft from credential name %s' % str(e))

            else:
                self.logger.error('Configuration Error')

        else:
            self.logger.error("myproxy server not set")

        return proxyTimeleft

    def checkMyProxy(self, proxy=None, checkRenewer=False):
        """
        Return True if myproxy validity is bigger than minTime.
        """
        if self.myproxyServer:
            valid = True

            if not proxy:
                proxy = self.getProxyFilename(checkRenewer)

            checkMyProxyCmd = 'myproxy-info -d -s ' + self.myproxyServer
            output, _, retcode = execute_command(self.setEnv(checkMyProxyCmd), self.logger, self.commandTimeout)

            if retcode > 0 and not output:
                valid = False
                return valid

            minTime = self.myproxyMinTime * 24 * 3600
            # regex to extract the right information
            timeleftList = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)").findall(
                output)
            timeleft, hours, minutes, seconds = 0, 0, 0, 0

            # the first time refers to the flat user proxy,
            # the other ones are related to the server credential name
            if not checkRenewer:

                try:
                    hours, minutes, seconds = timeleftList[0]
                    timeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                except Exception as e:
                    self.logger.error('Error extracting timeleft from proxy %s' % str(e))
                    return False

                if timeleft < minTime:
                    self.logger.error('Your proxy will expire in:\n\t%s hours %s minutes %s seconds\n the minTime : %s'
                                      % (hours, minutes, seconds, minTime))
                    valid = False

            # check the timeleft for the required server
            elif len(self.serverDN.strip()) > 0:

                serverCredName = sha1(encodeUnicodeToBytes(self.serverDN)).hexdigest()
                credNameList = re.compile(" name: (?P<CN>.*)").findall(output)

                # check if the server credential exists
                if serverCredName not in credNameList:
                    self.logger.error('Your proxy needs retrieval and renewal policies for the requested server.')
                    return False

                if len(timeleftList) == len(credNameList):
                    credTimeleftList = timeleftList
                else:
                    credTimeleftList = timeleftList[1:]

                # clean up expired credentials for other servers anyway
                destroyListCred(credNameList, credTimeleftList, self.logger, self.commandTimeout)

                try:
                    hours, minutes, seconds = credTimeleftList[credNameList.index(serverCredName)]
                    timeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                except Exception as e:
                    self.logger.error('Error extracting timeleft from credential name %s' % str(e))
                    return False

                if timeleft < minTime:
                    logMsg = 'Your credential for the required server will expire in:\n\t%s hours %s minutes %s seconds\n' \
                             % (hours, minutes, seconds)
                    self.logger.error(logMsg)
                    valid = False

            else:
                self.logger.error('Configuration Error')
                valid = False

        else:
            self.logger.error('Error delegating credentials : myproxyserver is not specified.')
            valid = False

        return valid

    def logonRenewMyProxy(self, proxyFilename=None, credServerName=None):
        """
        Refresh/retrieve proxyFilename in/from myproxy.
        """
        if not proxyFilename:
            proxyFilename = self.getProxyFilename(serverRenewer=True)

        attribute = self.getAttributeFromProxy(proxyFilename)
        if not attribute:
            attribute = self.getProxyDetails()
        voAttribute = self.prepareAttForVomsRenewal(attribute)

        # compose the delegation or renewal commands
        # with the regeneration of Voms extensions
        cmdList = []
        cmdList.append('unset X509_USER_CERT X509_USER_KEY')
        cmdList.append('&& env')
        cmdList.append('X509_USER_CERT=%s' % self.serverCert)
        cmdList.append('X509_USER_KEY=%s' % self.serverKey)

        ## get a new delegated proxy
        uniqName = self.userDN + self.vo + self.group + self.role
        proxyFilename = os.path.join(self.credServerPath,
                                     sha1(encodeUnicodeToBytes(uniqName)).hexdigest())
        # Note that this is saved in a temporary file with the pid appended to the filename. This way we will avoid adding many
        # signatures later on with vomsExtensionRenewal in case of multiple processing running at the same time
        tmpProxyFilename = proxyFilename + '.' + str(os.getpid())

        myproxyUsername = self.getMyproxyUsernameForCRAB()

        cmdList.append('myproxy-logon -d -n -s %s -o %s -l \"%s\" -t 168:00'
                       % (self.myproxyServer, tmpProxyFilename, myproxyUsername) )
        logonCmd = ' '.join(cmdList)
        msg, _, retcode = execute_command(self.setEnv(logonCmd), self.logger, self.commandTimeout)

        if retcode > 0:
            self.logger.error("Unable to retrieve delegated proxy using login %s for user DN %s! Exit code:%s output:%s",
                              myproxyUsername, self.userDN, retcode, msg)
            return proxyFilename

        self.vomsExtensionRenewal(tmpProxyFilename, voAttribute)
        os.rename(tmpProxyFilename, proxyFilename)

        return proxyFilename

    def prepareAttForVomsRenewal(self, attribute='/cms'):
        """
        Prepare attribute for the voms renewal.
        """
        # prepare the attributes for voms extension
        voAttribute = self.vo + ':' + attribute

        # Clean attribute to extend voms
        voAttribute = voAttribute.replace('/Role=NULL', '')
        voAttribute = voAttribute.replace('/Capability=NULL', '')

        return voAttribute

    def vomsExtensionRenewal(self, proxy, voAttribute='cms'):
        """
        Renew voms extension of the proxy
        """
        # get RFC/noRFC type for retrieved flat proxy
        msg, _, retcode = execute_command(self.setEnv('voms-proxy-info -type -file %s' % proxy), self.logger,
                                          self.commandTimeout)
        if retcode > 0:
            self.logger.error('Cannot get proxy type %s' % msg)
            return
        isRFC = msg.startswith('RFC')  # can be 'RFC3820 compliant impersonation proxy' or 'RFC compliant proxy'

        # get validity time for retrieved flat proxy
        cmd = 'grid-proxy-info -file ' + proxy + ' -timeleft'
        timeLeft, _, retcode = execute_command(self.setEnv(cmd), self.logger, self.commandTimeout)

        if retcode != 0:
            self.logger.error("Error while checking retrieved proxy timeleft for %s" % proxy)
            return


        # timeLeft indicates how many seconds the proxy is still valid for.
        # We will add a VOMS extension via voms-proxy-init -noregen but if we
        # ask for an exactly matching validity time, we often end with
        #  Warning: your certificate and proxy will expire Tue Mar 31 23:46:06 2020
        #  which is within the requested lifetime of the proxy
        # which causes a non zero exit status and hence a false error logging.
        # Therefore let's take 10 minutes off
        vomsTime = int(timeLeft.strip()) - 600

        vomsValid = '00:00'
        if vomsTime > 0:
            vomsValid = "%d:%02d" % (int(vomsTime // 3600), int((vomsTime % 3600) // 60))
        self.logger.debug('Requested voms validity: %s' % vomsValid)

        ## set environ and add voms extensions
        cmdList = []
        cmdList.append('env')
        cmdList.append('X509_USER_PROXY=%s' % proxy)
        cmdList.append('voms-proxy-init -noregen -voms %s -out %s -bits 2048 -valid %s %s'
                       % (voAttribute, proxy, vomsValid, '-rfc' if isRFC  else ''))
        cmd = ' '.join(cmdList)
        msg, _, retcode = execute_command(self.setEnv(cmd), self.logger, self.commandTimeout)

        if retcode > 0:
            self.logger.error('Unable to renew proxy voms extension: %s' % msg)

        return

    def renewMyProxy(self, proxy=None, serverRenewer=False):
        """
        Renew MyProxy
        """
        if not proxy:
            proxy = self.getProxyFilename(serverRenewer)
        self.delegate(proxy, serverRenewer)

        return

    ##################### Check timeleft
    def getTimeLeft(self, proxy=None, checkVomsLife=True):
        """
        Get proxy timeleft. Validate the proxy timeleft
        with the voms life.
        """
        timeLeft = 0
        if not proxy:
            proxy = self.getProxyFilename()

        timeLeftCmd = 'voms-proxy-info -file ' + proxy + ' -timeleft'
        timeLeftLocal, _, self.retcode = execute_command(self.setEnv(timeLeftCmd), self.logger, self.commandTimeout)

        if self.retcode != 0:
            self.logger.error("Error while checking proxy timeleft for %s" % proxy)
            return timeLeft
        try:
            timeLeft = int(timeLeftLocal.strip())
        except ValueError:
            timeLeft = sum(int(x) * 60 ** i for i, x in enumerate(reversed(timeLeftLocal.strip().split(":"))))

        if checkVomsLife and timeLeft > 0:
            ACTimeLeftLocal = self.getVomsLife(proxy)
            if timeLeft != ACTimeLeftLocal:
                msg = "Proxy lifetime %s secs is different from " % timeLeft
                msg += "voms extension lifetime %s secs for proxy: %s" % (ACTimeLeftLocal, proxy)
                self.logger.debug(msg)
            timeLeft = min(timeLeft, ACTimeLeftLocal)

        return timeLeft

    def getVomsLife(self, proxy):
        """
        Get proxy voms life.
        """
        result = 0
        cmd = 'voms-proxy-info -file ' + proxy + ' -actimeleft'
        ACtimeLeftLocal, _, retcode = execute_command(self.setEnv(cmd), self.logger, self.commandTimeout)

        if retcode != 0:
            return result
        try:

            result = int(ACtimeLeftLocal)
        except ValueError:
            result = sum(int(x) * 60 ** i for i, x in enumerate(reversed(ACtimeLeftLocal.split(":"))))

        return result

    def getAttributeFromProxy(self, proxy, allAttributes=False):
        """
        Get proxy attribute.
        Build the proxy attribute from existing and not from parameters as
        done by getProxyDetails.
        """
        roleCapCmd = 'env X509_USER_PROXY=%s voms-proxy-info -fqan' % proxy
        attribute, _, retcode = execute_command(self.setEnv(roleCapCmd),
                                                self.logger,
                                                self.commandTimeout)
        if retcode == 0:
            if allAttributes:
                return list(filter(bool, attribute.split('\n')))
            else:
                return attribute.split('\n')[0]
        else:
            return ''

    def getUserGroupAndRoleFromProxy(self, proxy):
        """
        Get user group and role from the proxy attribute.
        """
        group, role = '', ''
        attribute = self.getAttributeFromProxy(proxy)
        if attribute:
            attributeToList = attribute.split('/')
            if len(attributeToList) > 4:
                group = attributeToList[2]
                role = attributeToList[3].split('=')[1]
            else:
                role = attributeToList[2].split('=')[1]

        return group, role

    def getAllUserGroups(self, proxy):
        """
        Get all the attributes for the user using getAttributeFromProxy
        and strip the ROLE and CAPABILITIES part.

        Return a generator of things like '/cms/integration', '/cms'
        """
        attributes = self.getAttributeFromProxy(proxy, allAttributes=True)
        for attribute in attributes:
            splAttr = attribute.split('/')  # splitted attribut
            filtAttr = [part for part in splAttr if
                        not (part.startswith('Role=') or part.startswith('Capability='))]  # filtered attribute
            yield '/'.join(filtAttr)
