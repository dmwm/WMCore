#!/usr/bin/env python
#pylint: disable=C0103,W0613,C0321
import os, subprocess
import traceback
import re
from Credential import Credential

try:
    from hashlib import sha1
except:
    from sha import sha as sha1

def execute_command( command, logger, timeout ):
    """
    _execute_command_
    Funtion to manage commands.
    """
    import time

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
            logger.error('\
    Timeout in %s execution.' %command )
            return stdout, stderr, rc

        time.sleep(0.1)

    stdout, stderr = proc.communicate()
    rc = proc.returncode

    return stdout, stderr, rc

def destroyListCred( credNameList = [], credTimeleftList = { }, logger = None, timeout = 0 ):
    """
    _destroyListCred_
    Get list of credential name and their timelefts to destroy the one
    with timeleft = 0 from myproxy.
    """
    cleanCredCmdList = []
    for credIdx in xrange(len(credNameList)):
        hours, minutes, seconds = credTimeleftList[ credIdx ]
        timeleft = int(hours)*3600 + int(minutes)*60 + int(seconds)
        if timeleft == 0:
            cleanupCmd = "myproxy-destroy -d -k %s" % (credNameList[credIdx])
            cleanCredCmdList.append( cleanupCmd )

    cleanCredCmd = " && ".join(cleanCredCmdList)
    if len(cleanCredCmd)>0:
        logger.debug('Removing expired credentials: %s'%cleanCredCmd)
        try:
            execute_command( cleanCredCmd, logger, timeout )
        except:
            logger.debug('\
Error in cleaning expired credentials. Ignore and go ahead.')

    return

from WMCore.WMException import WMException
class CredentialException(WMException):
    """
    Credential exceptions should be defined in this class.
    """
    pass

class Proxy(Credential):
    """
    Basic class to handle user Proxy
    """
    def __init__( self, args ):
        """
        __init__
        Build proxy object.
        """
        Credential.__init__( self, args )

        self.commandTimeout = args.get( "ServiceContactTimeout", 1200 ) #The default is 20 mn.
        self.myproxyServer = args.get( "myProxySvr", '')
        self.serverDN = args.get( "serverDN", '')
        self.userDN = args.get( "userDN", '')
        self.proxyValidity = args.get( "proxyValidity", '')
        self.myproxyValidity = args.get( "myproxyValidity", 4)
        self.group = args.get( "group",'')
        self.role = args.get( "role",'')

        self.logger = args.get( "logger", '')

        ## adding ui script to source
        self.uisource = args.get("uisource", '')

        ## adding credential path
        self.credServerPath = args.get("credServerPath", '/tmp')
        self.vo = 'cms'

        self.args = args

    def setUI(self):
        """
        Return the source command to be pre added to each command to be executed
        """
        if self.uisource is not None and len(self.uisource) > 0:
            return 'source ' + self.uisource + ' && '
        return ''

#################### Proxy specific stuff

    def getProxyFilename( self, serverRenewer=False ):
        """
        Get the proxy file path in UI or in a delegated server.
        """

        if serverRenewer:
            proxyFilename = os.path.join(\
self.credServerPath, sha1(self.userDN + self.vo).hexdigest() )
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

        subject, error, retcode = execute_command(self.setUI() + getSubjectCmd, self.logger, self.commandTimeout)

        if retcode != 0 :
            msg = "Error while checking proxy subject for %s since %s"\
                   % (proxy, error)
            raise CredentialException(msg)

        self.logger.debug(\
'Getting subject : \n command : %s\n subject : %s retcode : %s' %(\
    getSubjectCmd, subject, retcode) )
        return subject.strip()

    def getSubjectFromCert(self, certFile = None):
        """
        Get the subject from cert file.
        """
        subject = None
        if certFile == None: certFile = self.getProxyFilename()

        subjFromCertCmd = 'openssl x509 -in '+certFile+' -subject -noout'

        subjectResult, error, retcode = execute_command(self.setUI() + subjFromCertCmd, self.logger, self.commandTimeout)

        if retcode != 0 :
            msg = "Error while checking proxy subject for %s since %s"\
               % (certFile, error)
            raise CredentialException(msg)

        try:
            subject = subjectResult.split('subject=')[1].strip()
        except:
            subject = None

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

    def checkAttribute( self, proxy=None ):
        """
        Check attributes from a proxy file.
        """
        valid = True

        if proxy == None: proxy = self.getProxyFilename()

        checkAttCmd = 'voms-proxy-info -fqan -file ' + proxy

        claimed = "/%s/" % self.vo
        if self.group:
            claimed += self.group
        if self.role and self.role != 'NULL': claimed += "/Role=%s" % self.role

        attribute, error, retcode = execute_command(self.setUI() +  checkAttCmd, self.logger, self.commandTimeout )

        if retcode != 0 :
            msg = "Error while checking attribute for %s since %s"\
                 % (proxy, error)
            raise CredentialException(msg)

        if not re.compile(r"^"+claimed).search(attribute):
            self.logger.error("Wrong VO group/role.")
            valid = False

        return valid

############### Credential base method

    def create( self ):
        """
        Proxy creation.
        """
        createCmd = 'voms-proxy-init -voms %s' % self.vo

        if self.group:
            createCmd += ':/'+self.vo+'/'+self.group
            if self.role and self.role != 'NULL': createCmd += '/Role='+self.role
        else:
            if self.role and self.role != 'NULL': createCmd += ':/'+self.vo+'/Role='+self.role

        createCmd += ' -valid ' + self.proxyValidity

        self.logger.debug(createCmd)

        output, error, retcode = execute_command(self.setUI() +  createCmd, self.logger, self.commandTimeout )

        if retcode != 0 :

            raise CredentialException(\
"Unable to create a valid proxy using command \
   %s\n the output %s\n retcode %s\n since %s" \
        %(createCmd, output, retcode, error))

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
        if credential == None: credential = self.getProxyFilename()

        if credential == None:
            msg = "Error no valid proxy to remove "
            raise CredentialException(msg)

        destroyCmd = 'rm -f %s' % credential

        output, error, retcode = execute_command( destroyCmd, self.logger, self.commandTimeout )

        if retcode != 0 :
            msg = " Error while removing proxy %s using command %s \
             and output is % s since % s" % ( credential, \
                    destroyCmd, output, error )
            raise CredentialException( msg )

        return

############Myproxy stuff. To remove in another class

    def delegate(self, credential = None, serverRenewer = False ):
        """
        Delegate the user proxy to myproxy.
        It is possible also to delegate a server
        (specifying serverRenewer = True) to
        manage your proxy in myproxy server.
        """
        if credential == None: credential = self.getProxyFilename\
                      ( serverRenewer )

        if self.myproxyServer:

            myproxyDelegCmd = 'X509_USER_PROXY = % s ; \
           myproxy-init -d -n -s % s' % (credential, self.myproxyServer)

            if serverRenewer and len( self.serverDN.strip() ) > 0:

                serverCredName = sha1(self.serverDN).hexdigest()
                myproxyDelegCmd += ' -x -R \'%s\' -Z \'%s\' -k %s -t 168:00 ' \
                    % (self.serverDN, self.serverDN, serverCredName )

            output, error, retcode = execute_command(self.setUI() +  myproxyDelegCmd, self.logger, self.commandTimeout )

            self.logger.debug('MyProxy delegation :\n command: %s\n output:\
                     %s\n ret: %s'%( myproxyDelegCmd, output, retcode ) )

            if retcode > 0 :
                raise CredentialException(\
"Unable to delegate the proxy to myproxyserver %s !\n since %s"\
             % (self.myproxyServer, error) )

        else: raise CredentialException("myproxy server not set for the proxy %s\
                          " %credential )

        return


    def getMyProxyTimeLeft( self , proxy=None, serverRenewer=False ):
        """
        Get myproxy timeleft. Speciying serverRenewer=True means
        that your are delegating your proxy management in myproxy
        to a server.
        """
        proxyTimeleft = -1
        if self.myproxyServer:

            if proxy == None: proxy = self.getProxyFilename( serverRenewer )

            checkMyProxyCmd = 'myproxy-info -d -s ' + self.myproxyServer

            output, error, retcode = execute_command(self.setUI() +  checkMyProxyCmd, self.logger, self.commandTimeout )

            self.logger.debug( \
 'Checking myproxy for %s...command : %s\n output : %s\n retcode : %s\n'\
         %(proxy, checkMyProxyCmd, output, retcode) )

            if retcode > 0 :

                msg = "Error while checking myproxy timeleft \
                           for %s from %s: %s since %s" \
                   % ( proxy, self.myproxyServer, output, error )
                raise CredentialException(msg)

            if not output:

                self.logger.error(\
     'No credential delegated to myproxy server %s since %s.'\
               %(self.myproxyServer, error) )

            else:

                timeleftList = re.compile(\
"timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)").\
                      findall(output)
                hours, minutes, seconds = 0, 0, 0

                if not serverRenewer:

                    try:

                        hours, minutes, seconds = timeleftList[0]
                        proxyTimeleft = int(hours)*3600 + int(\
                          minutes)*60 + int(seconds)

                    except Exception, e:

                        self.logger.error('Error extracting timeleft from proxy')
                        self.logger.debug( str(e) )

                elif len(self.serverDN.strip()) > 0:

                    serverCredName = sha1(\
                      self.serverDN).hexdigest()
                    credNameList = re.compile(\
                   " name: (?P<CN>.*)").findall(output)
                    if len(timeleftList) == len(credNameList):
                        credTimeleftList = timeleftList
                    else:
                        credTimeleftList = timeleftList[1:]


                    if serverCredName not in credNameList :

                        self.logger.error(\
'Your proxy needs retrieval and renewal policies for \
the requested server.')
                        proxyTimeleft =  0

                    else:

                        try:

                            hours, minutes, seconds = \
            credTimeleftList[ credNameList.index(serverCredName) ]
                            proxyTimeleft = int(hours)*3600\
                         + int(minutes)*60 + int(seconds)

                        except Exception, e:

                            self.logger.error(\
          'Error extracting timeleft from credential name')
                            self.logger.debug( str(e) )

                else:
                    self.logger.error(\
                  'Configuration Error')

        else:

            raise CredentialException("myproxy server not set")

        return proxyTimeleft


    def checkMyProxy( self , proxy=None, checkRenewer=False):
        """
        Return True if myproxy validity is bigger than minTime.
        """
        if self.myproxyServer:

            if proxy == None: proxy = self.getProxyFilename( checkRenewer )
            valid = True

            checkMyProxyCmd = 'myproxy-info -d -s ' + self.myproxyServer

            output, error, retcode = execute_command(self.setUI() +  checkMyProxyCmd, self.logger, self.commandTimeout )
            self.logger.debug( 'Checking myproxy...command \
: %s\n output : %s\n retcode : %s\n' %(checkMyProxyCmd, output, retcode) )

            if retcode > 0 :
                msg = "Error while checking myproxy timeleft for \
            % s from % s: % s since % s" % (proxy, self.myproxyServer, \
                                 output, error)
                raise CredentialException(msg)

            if not output:

                self.logger.error(\
'No credential delegated to myproxy server %s .'%self.myproxyServer)
                valid = False

            else:

                minTime = self.myproxyValidity * 24 * 3600

                # regex to extract the right information
                timeleftList = re.compile(\
"timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)"\
                ).findall(output)
                timeleft, hours, minutes, seconds = 0, 0, 0, 0

                # the first time refers to the flat user proxy,
                # the other ones are related to the server credential name
                if not checkRenewer:
                    try:
                        hours, minutes, seconds = timeleftList[0]
                        timeleft = int(hours)*3600 + int(minutes)*60 + int(seconds)
                    except Exception, e:
                        self.logger.info('Error extracting timeleft from proxy')
                        self.logger.debug( str(e) )
                        valid = False
                    if timeleft < minTime:
                        self.logger.info(\
    'Your proxy will expire in:\n\t%s hours %s minutes %s seconds\n the minTime\
              : %s'%(hours,minutes,seconds,minTime))
                        valid = False

                # check the timeleft for the required server
                elif len(self.serverDN.strip()) > 0:

                    serverCredName = sha1(self.serverDN).hexdigest()
                    credNameList = re.compile(" name: (?P<CN>.*)").\
                                findall(output)

                    if len(timeleftList) == len(credNameList):
                        credTimeleftList = timeleftList
                    else:
                        credTimeleftList = timeleftList[1:]


                    # check if the server credential exists
                    if serverCredName not in credNameList :
                        self.logger.error(\
'Your proxy needs retrieval and renewal policies for the requested server.')
                        valid = False
                        return valid

                    try:
                        hours, minutes, seconds = credTimeleftList[ \
                           credNameList.index(serverCredName) ]
                        timeleft = int(hours)*3600 + int(minutes)*60\
                                 + int(seconds)
                    except Exception, e:
                        self.logger.error(\
                  'Error extracting timeleft from credential name')
                        self.logger.debug( str(e) )
                        valid = False
                    if timeleft < minTime:
                        logMsg  = 'Your credential for the required\
                              server will expire in:\n\t'
                        logMsg += '%s hours %s minutes %s seconds\n'% \
                             (hours,minutes,seconds)
                        self.logger.debug(logMsg)
                        valid = False
                    # clean up expired credentials for other servers
                    destroyListCred( credNameList, credTimeleftList, self.logger, self.commandTimeout )
                else:
                    self.logger.error(\
                  'Configuration Error')
                    valid = False


            return valid

        else:
            self.logger.error(\
'Error delegating credentials : myproxyserver is not specified.')
            return False

    def logonRenewMyProxy( self, proxyFilename = None, credServerName = None ):
        """
        Refresh/retrieve proxyFilename in/from myproxy.
        """
        #if not proxyFilename:
            # compose the VO attriutes
        #    voAttr = self.vo
        #    if self.group:
        #        voAttr += ':/'+self.vo+'/'+self.group
        #        if self.role: voAttr += '/Role='+self.role
        #    else:
        #        if self.role: voAttr += ':/'+self.vo+'/Role='+self.role
        if not proxyFilename: proxyFilename = self.getProxyFilename( serverRenewer = True )

        else:

            # get vo, group and role from the current certificate
            getVoCmd = 'env X509_USER_PROXY=%s voms-proxy-info -vo' \
                            % proxyFilename
            attribute, error, retcode = execute_command(self.setUI() + getVoCmd,
                                                        self.logger,
                                                        self.commandTimeout)
            if retcode != 0:
                raise CredentialException("Unable to get VO for proxy \
                  %s! Exit code:%s"%(proxyFilename, retcode) )
            vo = attribute.replace('\n','')

            # at least /cms/Role=NULL/Capability=NULL
            roleCapCmd = 'env X509_USER_PROXY=%s voms-proxy-info -fqan' \
                        % proxyFilename
            attribute, error, retcode = execute_command(self.setUI() + roleCapCmd,
                                                        self.logger,
                                                        self.commandTimeout)
            if retcode != 0:
                raise CredentialException(\
  "Unable to get FQAN for proxy %s! Exit code:%s since %s"\
               %(proxyFilename, retcode, error) )

            # prepare the attributes
            attribute = attribute.split('\n')[0]
            attribute = attribute.replace('/Role=NULL','')
            attribute = attribute.replace('/Capability=NULL','')

            #voAttribute = vo + ':' + attribute
            voAttribute = self.vo

        #FIXME: Build correctly voAttribute
        voAttribute = self.vo

        # get the credential name for this retriever
        if not credServerName:
            credServerName = sha1(self.getSubjectFromCert( self.serverCert )).hexdigest()

        # compose the delegation or renewal commands
        # with the regeneration of Voms extensions
        cmdList = []
        cmdList.append('unset X509_USER_CERT X509_USER_KEY')
        cmdList.append('&& env')
        cmdList.append('X509_USER_CERT=%s' % self.serverCert)
        cmdList.append('X509_USER_KEY=%s' % self.serverKey)

        ## get a new delegated proxy
        proxyFilename = os.path.join(\
self.credServerPath, sha1(self.userDN + voAttribute).hexdigest() )

        cmdList.append(\
    'myproxy-logon -d -n -s %s -o %s -l \"%s\" -k %s -t 168:00'%\
  (self.myproxyServer, proxyFilename, self.userDN, credServerName) )

        logonCmd = ' '.join(cmdList)
        msg, error, retcode = execute_command(self.setUI() + logonCmd, self.logger, self.commandTimeout)
        self.logger.debug('MyProxy logon - retrieval:\n%s'%logonCmd)

        if retcode > 0 :
            self.logger.debug('MyProxy result - retrieval :\n%s'%error)
            self.logger.debug(\
"Unable to retrieve delegated proxy for user DN %s! Exit code:%s since %s"\
         %(self.userDN, retcode, error) )
            return proxyFilename

        self.vomsExtensionRenewal(proxyFilename, voAttribute)

        return proxyFilename

    def vomsExtensionRenewal(self, proxy, voAttribute = 'cms'):
        """
        Renew voms extension of the proxy
        """
        ## get validity time for retrieved flat proxy
        cmd = 'grid-proxy-info -file '\
       + proxy + ' -timeleft'

        timeLeft, error, retcode = execute_command(self.setUI() + cmd, self.logger, self.commandTimeout)
        if retcode != 0 and retcode != 1:
            raise CredentialException(\
"Error while checking retrieved proxy timeleft for %s since %s"\
                 %(proxy, error) )

        try:
            timeLeft = int(timeLeft) - 60
        except:
            timeLeft = 0

        self.logger.debug( \
'Timeleft for retrieved proxy: (exit code %s) %s'\
      %(retcode, timeLeft) )

        vomsValid = ''

        if timeLeft > 0:

            vomsValid = "%d:%02d" % (\
 timeLeft/3600, (timeLeft-(timeLeft/3600)*3600)/60 )

        self.logger.debug( 'Requested voms validity: %s'%vomsValid )

        ## set environ and add voms extensions
        cmdList = []
        cmdList.append('env')
        cmdList.append('X509_USER_CERT=%s'%proxy)
        cmdList.append('X509_USER_KEY=%s'%proxy)
        cmdList.append(\
'voms-proxy-init -noregen -voms %s -cert %s -key\
       %s -out %s -bits 1024 -valid %s'%\
        (voAttribute, proxy, proxy, proxy, vomsValid) )

        cmd = ' '.join(cmdList)
        msg, error, retcode = execute_command(self.setUI() + cmd, self.logger, self.commandTimeout)
        self.logger.debug('Voms extension:\n%s'%cmd)

        if retcode > 0:
            self.logger.debug(\
    'Voms extension result:\n%s'%msg)
            raise CredentialException(\
"Unable to renew proxy voms extension: %s! Exit code:%s" \
             %(proxy, retcode) )

        return

    def renewMyProxy( self, proxy = None, serverRenewer = False ):
        """
        Renew MyProxy
        """
        if proxy == None: proxy = self.getProxyFilename( serverRenewer )
        self.delegate( proxy, serverRenewer )

        return

##################### Check timeleft
    def getTimeLeft(self, proxy = None ):
        """
        Get proxy timeleft. Validate the proxy timeleft
        with the voms life.
        """
        if proxy == None: proxy = self.getProxyFilename()
        if not os.path.exists(proxy):
            return 0

        timeLeftCmd = 'voms-proxy-info -file '+proxy+' -timeleft'
        timeLeftLocal, error, retcode = execute_command(self.setUI() + timeLeftCmd, self.logger, self.commandTimeout)

        if retcode != 0 and retcode != 1:
            msg = "Error while checking proxy timeleft for %s since %s"\
                % (proxy, error)
            raise CredentialException(msg)

        try:
            result = int(timeLeftLocal)
        except Exception:
            result = 0
        if result > 0:
            ACTimeLeftLocal = self.getVomsLife(proxy)
            if ACTimeLeftLocal > 0:
                result = self.checkLifeTimes(\
         int(timeLeftLocal), ACTimeLeftLocal, proxy)
            else:
                result = 0
        self.logger.debug("Time left %s" %result)

        return result

    def checkLifeTimes(self, ProxyLife, VomsLife, proxy):
        """
        Evaluate the proxy validity comparing it with voms
        validity.
        """
        # TODO: get the minimum value between proxyLife and vomsLife
        # configurable

        if abs(ProxyLife - VomsLife) > 900 :

            hours = int(ProxyLife)/3600
            minutes = (int(ProxyLife)-hours*3600)/60
            proxyLife = "%d:%02d" % (hours, minutes)
            hours = int(VomsLife)/3600
            minutes = (int(VomsLife)-hours*3600)/60
            vomsLife = "%d:%02d" % (hours, minutes)
            msg =  "proxy lifetime %s is different from \
            voms extension lifetime %s for proxy %s \n" \
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
        cmd = 'voms-proxy-info -file '+proxy+' -actimeleft'

        ACtimeLeftLocal, error, retcode = execute_command(self.setUI() + cmd, self.logger, self.commandTimeout)

        if retcode != 0 and retcode != 1:
            msg = "Error while checking proxy actimeleft for %s since %s"\
                     % (proxy, error)
            raise CredentialException(msg)

        try:

            result = int( ACtimeLeftLocal )

        except Exception:
            msg  =  "voms extension lifetime for proxy %s is 0 \n" % proxy
            self.logger.error(msg)
            result = 0

        return result
