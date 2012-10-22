#!/usr/bin/env python
#pylint: disable=C0103,W0613,C0321
"""
_Proxy_
Wrap gLite proxy commands.
"""

import contextlib
import copy
import os, subprocess
import re
from WMCore.Credential.Credential import Credential
from WMCore.WMException import WMException
import time
from hashlib import sha1

def execute_command( command, logger, timeout ):
    """
    _execute_command_
    Funtion to manage commands.
    """

    stdout, stderr, rc = None, None, 99999
    proc = subprocess.Popen(
            command, shell=True, cwd=os.environ['PWD'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
    )

    t_beginning = time.time()
    seconds_passed = 0
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command )
            return stdout, rc

        time.sleep(0.1)

    stdout, stderr = proc.communicate()
    rc = proc.returncode

    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))

    return stdout, rc

def destroyListCred( credNameList = [], credTimeleftList = { }, logger = None, timeout = 0 ):
    """
    _destroyListCred_
    Get list of credential name and their timelefts to destroy the one
    with timeleft = 0 from myproxy.
    """
    cleanCredCmdList = []

    for credIdx in xrange(len(credNameList)):
        hours, minutes, seconds = credTimeleftList[ credIdx ]
        timeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        if timeleft == 0:
            cleanupCmd = "myproxy-destroy -d -k %s" % (credNameList[credIdx])
            cleanCredCmdList.append( cleanupCmd )

    cleanCredCmd = " && ".join(cleanCredCmdList)
    if len(cleanCredCmd)>0:
        execute_command( cleanCredCmd, logger, timeout )

    return

#TODO not used anymore. #3810 deletes lasts unused dependencies in the client
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
        args['server_key']  = serverKey
        args['myProxySvr']  = myproxySrv
        args['credServerPath'] = proxyDir
        args['logger'] = logger
        proxy = Proxy(args=args)

        proxy.userDN = userDN
        filename = proxy.logonRenewMyProxy()
        os.environ['X509_USER_PROXY'] = filename

        # host certs can be taken first, get rid of them
        deleteKeys = ['X509_USER_CERT', 'X509_USER_KEY', 'X509_HOST_CERT', 'X509_HOST_KEY']
        for key in deleteKeys:
            if os.environ.has_key(key):
                del os.environ[key]
        yield filename
    finally:
        os.environ = originalEnvironment


class Proxy(Credential):
    """
    Basic class to handle user Proxy
    """
    def __init__( self, args ):
        """
        __init__
        Initialize proxy object.
        """
        Credential.__init__( self, args )

        # Set the default commands execution timeout to 20 mn
        self.commandTimeout = args.get( "ServiceContactTimeout", 1200 )
        self.myproxyServer = args.get( "myProxySvr", 'myproxy.cern.ch')
        self.serverDN = args.get( "serverDN", '')
        self.userDN = args.get( "userDN", '')
        self.proxyValidity = args.get( "proxyValidity", '') #lenght of the proxy
        self.myproxyValidity = args.get( "myproxyValidity", '168:00') #lenght of the myproxy
        self.myproxyMinTime = args.get( "myproxyMinTime", 4) #threshold used in checkProxy

        # User vo paramaters
        self.vo = 'cms'
        self.group = args.get( "group",'')
        self.role = args.get( "role",'')

        self.logger = args.get( "logger", '')

        ## adding ui script to source
        self.uisource = args.get("uisource", '')
        self.cleanEnvironment = args.get("cleanEnvironment", False)

        ## adding credential path
        self.credServerPath = args.get("credServerPath", '/tmp')

    def setUI(self):
        """
        Return the source command to be pre added to each command to be executed.
        """
        ui = ''
        if self.cleanEnvironment:
            ui += 'unset LD_LIBRARY_PATH; '
        if self.uisource is not None and len(self.uisource) > 0:
            ui += 'source ' + self.uisource + ' && '

        return ui

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

    def getProxyFilename( self, serverRenewer=False ):
        """
        Try to get the proxy file path from:

        1.  the delegated server
        2.  from an ui
        """
        if serverRenewer:
            proxyFilename = os.path.join( self.credServerPath, sha1(
            self.userDN + self.vo + self.group + self.role ).hexdigest() )
        elif os.environ.has_key('X509_USER_PROXY'):
            proxyFilename = os.environ['X509_USER_PROXY']
        else:
            proxyFilename = '/tmp/x509up_u'+str(os.getuid())

        return proxyFilename

    def getSubject( self, proxy = None ):
        """
        Get proxy subject from a proxy file.
        """
        subject = None

        if proxy == None: proxy = self.getProxyFilename()
        getSubjectCmd = "voms-proxy-info -file "+proxy+" -identity"
        subject, retcode = execute_command(self.setUI() + getSubjectCmd, self.logger, self.commandTimeout)

        if retcode == 0:
            subject = subject.strip()

        return subject

    def getSubjectFromCert(self, certFile = None):
        """
        Get the subject from cert file.
        """
        subject = ''

        if not certFile:
            certFile = self.getProxyFilename()

        subjFromCertCmd = 'openssl x509 -in '+certFile+' -subject -noout'
        subjectResult, retcode = execute_command(self.setUI() + subjFromCertCmd, self.logger, self.commandTimeout)

        subject = None
        if retcode == 0:
            subject = subjectResult.split('subject=')[1].strip()

        return subject

    def getUserName(self, proxy = None ):
        """
        Get the user name from a proxy file.
        """
        subject = self.getSubject( proxy )
        uName = ''

        for cname in subject.split('/'):
            if cname[:3] == "CN=" and cname[3:].find('proxy') == -1:
                uName = cname[3:]

        return uName

    def checkAttribute( self, proxy = None ):
        """
        Check attributes from a proxy file.
        """
        valid = True

        if proxy == None:
            proxy = self.getProxyFilename()

        checkAttCmd = 'voms-proxy-info -fqan -file ' + proxy
        proxyDetails = self.getProxyDetails( )

        attribute = execute_command(self.setUI() +  checkAttCmd, self.logger, self.commandTimeout )

        if not re.compile(r"^"+proxyDetails).search(attribute[0]):
            valid = False

        return valid

    def create( self ):
        """
        Proxy creation.
        """
        createCmd = 'voms-proxy-init -voms %s:%s -valid %s' % (self.vo, self.getProxyDetails( ), self.proxyValidity )
        execute_command(self.setUI() +  createCmd, self.logger, self.commandTimeout )

        return

    def renew( self ):
        """
        Proxy renew.
        """
        self.create( )
        return

    def destroy(self, credential = None):
        """
        Proxy destruction.
        """
        if not credential:
            credential = self.getProxyFilename()

        destroyCmd = 'rm -f %s' % credential
        execute_command( destroyCmd, self.logger, self.commandTimeout )

        return

    def delegate(self, credential = None, serverRenewer = False ):
        """
        Delegate the user proxy to myproxy.
        It is possible also to delegate a server
        (specifying serverRenewer = True) to
        manage your proxy in myproxy server.
        """
        if not credential:
            credential = self.getProxyFilename( serverRenewer )

        if self.myproxyServer:
            myproxyDelegCmd = 'X509_USER_PROXY=%s ; myproxy-init -d -n -s %s' % (credential, self.myproxyServer)

            if serverRenewer and len( self.serverDN.strip() ) > 0:
                serverCredName = sha1(self.serverDN).hexdigest()
                myproxyDelegCmd += ' -x -R \'%s\' -Z \'%s\' -k %s -t 168:00 -c %s ' \
                                   % (self.serverDN, self.serverDN, serverCredName, self.myproxyValidity )
            execute_command( self.setUI() +  myproxyDelegCmd, self.logger, self.commandTimeout )

        else:
            self.logger.error( "myproxy server not set for the proxy %s" % credential )

        return

    def getMyProxyTimeLeft( self , proxy = None, serverRenewer = False ):
        """
        Get myproxy timeleft. Speciying serverRenewer=True means
        that your are delegating your proxy management in myproxy
        to a server.
        """
        proxyTimeleft = -1

        if self.myproxyServer:

            if not proxy:
                proxy = self.getProxyFilename( serverRenewer )
            checkMyProxyCmd = 'myproxy-info -d -s ' + self.myproxyServer
            output, retcode = execute_command(self.setUI() +  checkMyProxyCmd, self.logger, self.commandTimeout )

            if retcode > 0 or not output:
                return proxyTimeleft

            timeleftList = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)").findall(output)
            hours, minutes, seconds = 0, 0, 0

            if not serverRenewer:

                try:
                    hours, minutes, seconds = timeleftList[0]
                    proxyTimeleft = int(hours)*3600 + int(\
                      minutes)*60 + int(seconds)
                except Exception, e:
                    self.logger.error('Error extracting timeleft from proxy %s' % str(e))

            elif len(self.serverDN.strip()) > 0:
                serverCredName = sha1(self.serverDN).hexdigest()
                credNameList = re.compile(" name: (?P<CN>.*)").findall(output)

                if len(timeleftList) == len(credNameList):
                    credTimeleftList = timeleftList
                else:
                    credTimeleftList = timeleftList[1:]

                if serverCredName not in credNameList :
                    self.logger.error('Your proxy needs retrieval and renewal policies for the requested server.')
                    proxyTimeleft =  0
                else:
                    try:
                        hours, minutes, seconds = credTimeleftList[ credNameList.index(serverCredName) ]
                        proxyTimeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                    except Exception, e:
                        self.logger.error('Error extracting timeleft from credential name %s' % str(e))

            else:
                self.logger.error('Configuration Error')

        else:
            self.logger.error("myproxy server not set")

        return proxyTimeleft


    def checkMyProxy( self , proxy = None, checkRenewer = False):
        """
        Return True if myproxy validity is bigger than minTime.
        """
        if self.myproxyServer:
            valid = True

            if not proxy:
                proxy = self.getProxyFilename( checkRenewer )

            checkMyProxyCmd = 'myproxy-info -d -s ' + self.myproxyServer
            output, retcode = execute_command( self.setUI() +  checkMyProxyCmd, self.logger, self.commandTimeout )

            if retcode > 0 and not output:
                valid = False
                return valid

            minTime = self.myproxyMinTime * 24 * 3600
            # regex to extract the right information
            timeleftList = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)").findall(output)
            timeleft, hours, minutes, seconds = 0, 0, 0, 0

            # the first time refers to the flat user proxy,
            # the other ones are related to the server credential name
            if not checkRenewer:

                try:
                    hours, minutes, seconds = timeleftList[0]
                    timeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                except Exception, e:
                    self.logger.error('Error extracting timeleft from proxy %s' % str(e))
                    return False

                if timeleft < minTime:
                    self.logger.error('Your proxy will expire in:\n\t%s hours %s minutes %s seconds\n the minTime : %s'
                                      % (hours, minutes, seconds, minTime) )
                    valid = False

            # check the timeleft for the required server
            elif len(self.serverDN.strip()) > 0:

                serverCredName = sha1(self.serverDN).hexdigest()
                credNameList = re.compile(" name: (?P<CN>.*)").findall(output)

                # check if the server credential exists
                if serverCredName not in credNameList :
                    self.logger.error('Your proxy needs retrieval and renewal policies for the requested server.')
                    return False

                if len(timeleftList) == len(credNameList):
                    credTimeleftList = timeleftList
                else:
                    credTimeleftList = timeleftList[1:]

                # clean up expired credentials for other servers anyway
                destroyListCred( credNameList, credTimeleftList, self.logger, self.commandTimeout )

                try:
                    hours, minutes, seconds = credTimeleftList[ credNameList.index(serverCredName) ]
                    timeleft = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                except Exception, e:
                    self.logger.error('Error extracting timeleft from credential name %s' % str(e))
                    return False

                if timeleft < minTime:
                    logMsg  = 'Your credential for the required server will expire in:\n\t%s hours %s minutes %s seconds\n' \
                              % ( hours, minutes, seconds )
                    self.logger.error(logMsg)
                    valid = False

            else:
                self.logger.error('Configuration Error')
                valid = False

        else:
            self.logger.error('Error delegating credentials : myproxyserver is not specified.')
            valid = False

        return valid

    def logonRenewMyProxy( self, proxyFilename = None, credServerName = None ):
        """
        Refresh/retrieve proxyFilename in/from myproxy.
        """
        if not proxyFilename:
            proxyFilename = self.getProxyFilename( serverRenewer = True )

        attribute = self.getAttributeFromProxy( proxyFilename )
        if not attribute:
            attribute = self.getProxyDetails( )
        voAttribute = self.prepareAttForVomsRenewal( attribute )

        # get the credential name for this retriever
        if not credServerName:
            subject = self.getSubjectFromCert( self.serverCert )
            if subject:
                credServerName = sha1(subject).hexdigest()
            else:
                self.logger.error("Unable to to get the subject from the cert for user %s" % (self.userDN))
                return proxyFilename

        # compose the delegation or renewal commands
        # with the regeneration of Voms extensions
        cmdList = []
        cmdList.append('unset X509_USER_CERT X509_USER_KEY')
        cmdList.append('&& env')
        cmdList.append('X509_USER_CERT=%s' % self.serverCert)
        cmdList.append('X509_USER_KEY=%s' % self.serverKey)

        ## get a new delegated proxy
        proxyFilename = os.path.join( self.credServerPath, sha1( self.userDN + self.vo + self.group + self.role ).hexdigest() )
        cmdList.append('myproxy-logon -d -n -s %s -o %s -l \"%s\" -k %s -t 168:00'
                       % (self.myproxyServer, proxyFilename, self.userDN, credServerName) )
        logonCmd = ' '.join(cmdList)
        msg, retcode = execute_command(self.setUI() + logonCmd, self.logger, self.commandTimeout)

        if retcode > 0 :
            self.logger.error("Unable to retrieve delegated proxy for user DN %s! Exit code:%s output:%s" \
                              % (self.userDN, retcode, msg) )
            return proxyFilename

        self.vomsExtensionRenewal(proxyFilename, voAttribute)

        return proxyFilename

    def prepareAttForVomsRenewal(self, attribute = '/cms'):
        """
        Prepare attribute for the voms renewal.
        """
        # prepare the attributes for voms extension
        voAttribute = self.vo + ':' + attribute

        # Clean attribute to extend voms
        voAttribute = voAttribute.replace('/Role=NULL','')
        voAttribute = voAttribute.replace('/Capability=NULL','')

        return voAttribute

    def vomsExtensionRenewal(self, proxy, voAttribute = 'cms'):
        """
        Renew voms extension of the proxy
        """
        ## get validity time for retrieved flat proxy
        cmd = 'grid-proxy-info -file ' + proxy + ' -timeleft'
        timeLeft, retcode = execute_command(self.setUI() + cmd, self.logger, self.commandTimeout)

        if retcode != 0:
            self.logger.error("Error while checking retrieved proxy timeleft for %s" % proxy )
            return

        vomsValid = '00:00'
        timeLeft = int( timeLeft.strip() )

        if timeLeft > 0:
            vomsValid = "%d:%02d" % ( timeLeft / 3600, ( timeLeft - ( timeLeft / 3600 ) *3600 ) / 60 )

        self.logger.debug( 'Requested voms validity: %s' % vomsValid )

        ## set environ and add voms extensions
        cmdList = []
        cmdList.append('env')
        cmdList.append('X509_USER_CERT=%s' %proxy)
        cmdList.append('X509_USER_KEY=%s' %proxy)
        cmdList.append('voms-proxy-init -noregen -voms %s -cert %s -key %s -out %s -bits 1024 -valid %s'
                       % (voAttribute, proxy, proxy, proxy, vomsValid) )
        cmd = ' '.join(cmdList)
        msg, retcode = execute_command(self.setUI() + cmd, self.logger, self.commandTimeout)

        if retcode > 0:
            self.logger.error('Unable to renew proxy voms extension: %s' % msg )

        return

    def renewMyProxy( self, proxy = None, serverRenewer = False ):
        """
        Renew MyProxy
        """
        if not proxy:
            proxy = self.getProxyFilename( serverRenewer )
        self.delegate( proxy, serverRenewer )

        return

##################### Check timeleft
    def getTimeLeft( self, proxy = None ):
        """
        Get proxy timeleft. Validate the proxy timeleft
        with the voms life.
        """
        timeLeft = 0
        if not proxy:
            proxy = self.getProxyFilename()

        timeLeftCmd = 'voms-proxy-info -file '+proxy+' -timeleft'
        timeLeftLocal, retcode = execute_command(self.setUI() + timeLeftCmd, self.logger, self.commandTimeout)

        if retcode != 0:
            self.logger.error( "Error while checking proxy timeleft for %s" % proxy )
            return timeLeft

        timeLeft = int( timeLeftLocal.strip() )

        if timeLeft > 0:
            ACTimeLeftLocal = self.getVomsLife(proxy)
            if ACTimeLeftLocal > 0:
                timeLeft = self.checkLifeTimes(timeLeft, ACTimeLeftLocal, proxy)
            else:
                timeLeft = 0

        return timeLeft

    def checkLifeTimes(self, ProxyLife, VomsLife, proxy):
        """
        Evaluate the proxy validity comparing it with voms
        validity.
        """
        # TODO: make the minimum value between proxyLife and vomsLife configurable
        if abs(ProxyLife - VomsLife) > 900 :
            hours = int(ProxyLife) / 3600
            minutes = (int(ProxyLife) - hours * 3600) / 60
            proxyLife = "%d:%02d" % (hours, minutes)
            hours = int(VomsLife) / 3600
            minutes = (int(VomsLife) - hours * 3600) / 60
            vomsLife = "%d:%02d" % (hours, minutes)
            msg =  "Proxy lifetime %s is different from \
                   voms extension lifetime %s for proxy %s" \
                   % (proxyLife, vomsLife, proxy)
            self.logger.debug(msg)
            result = 0
        else:
            result = ProxyLife

        return result

    def getVomsLife(self, proxy):
        """
        Get proxy voms life.
        """
        result = 0
        cmd = 'voms-proxy-info -file ' + proxy + ' -actimeleft'
        ACtimeLeftLocal, retcode = execute_command(self.setUI() + cmd, self.logger, self.commandTimeout)

        if retcode != 0:
            return result

        result = int( ACtimeLeftLocal )

        return result

    def getAttributeFromProxy(self, proxy):
        """
        Get proxy attribute.
        Build the proxy attribute from existing and not from parameters as
        done by getProxyDetails.
        """
        roleCapCmd = 'env X509_USER_PROXY=%s voms-proxy-info -fqan' % proxy
        attribute, retcode = execute_command(self.setUI() + roleCapCmd,
                                             self.logger,
                                             self.commandTimeout)
        if retcode == 0:
            return attribute.split('\n')[0]
        else:
            return ''

    def getUserGroupAndRoleFromProxy(self, proxy):
        """
        Get user group and role from the proxy attribute.
        """
        group , role = '', ''
        attribute = self.getAttributeFromProxy(proxy)
        if attribute:
            attributeToList = attribute.split('/')
            if len(attributeToList) > 4:
                group = attributeToList[2]
                role = attributeToList[3].split('=')[1]
            else:
                role = attributeToList[2].split('=')[1]

        return group , role
