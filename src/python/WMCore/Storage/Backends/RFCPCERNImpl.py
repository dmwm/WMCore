#!/usr/bin/env python
"""
_RFCPCERNImpl_

Implementation of StageOutImpl interface for RFIO in Castor2
with specific code to set the RAW tape families for CERN

"""
import os
import re

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutError

from WMCore.Storage.Execute import execute
from WMCore.Storage.Execute import runCommandWithOutput

class RFCPCERNImpl(StageOutImpl):
    """
    _RFCPCERNImpl_
    
    """

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        self.numRetries = 5
        self.retryPause = 300


    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

         uses pfn

        """
        return "%s" % pfn


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        create dir with group permission

        """
        # EOS stageout auto-creates directories
        if self.isEOS(targetPFN):
            return

        targetDir = os.path.dirname(self.parseCastorPath(targetPFN))

        # targetDir does not exist => create it
        if not self.checkDirExists(targetDir):

            # only use the fileclass code path if we run on t0export
            serviceClass = os.environ.get('STAGE_SVCCLASS', None)
            if serviceClass == 't0export':

                # determine file class from PFN
                fileclass = None

                # check for correct naming convention in PFN
                regExpParser = re.compile('/castor/cern.ch/cms/store/([^/]*data)/([^/]+)/([^/]+)/([^/]+)/')
                match = regExpParser.match(targetDir)
                if ( match != None ):

                    # RAW data files use cms_raw, all others cms_production
                    if match.group(4) == 'RAW':
                        fileclass = 'cms_raw'
                    else:
                        fileclass = 'cms_production'

                    fileclassDir = '/castor/cern.ch/cms/store/%s/%s/%s/%s' % match.group(1,2,3,4)

                    # fileclassDir does not exist => create it
                    if not self.checkDirExists(fileclassDir):
                        self.createDir(fileclassDir)
                        if ( fileclass != None ):
                            self.setFileClass(fileclassDir,fileclass)

            # now create targetDir
            self.createDir(targetDir)

        return


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None, checksums = None):
        """
        _createStageOutCommand_

        Build the stageout command: rfcp for castor and xrdcp for eos

        If adler32 checksum is provided, use it for the transfer

        """
        isTargetEOS = self.isEOS(targetPFN)

        result = ""

        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
            result += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            result += "echo \"Local File Size is: $LOCAL_SIZE\"\n"

        useChecksum = ( checksums != None and checksums.has_key('adler32') and not self.stageIn )

        if isTargetEOS:

            result += "source /afs/cern.ch/project/eos/installation/pro/etc/setup.sh\n"
            result += "xrdcp -f -s "

            if useChecksum:

                checksums['adler32'] = "%08x" % int(checksums['adler32'], 16)

                result += "-ODeos.targetsize=$LOCAL_SIZE\&eos.checksum=%s " % checksums['adler32']

        else:

            if useChecksum:

                targetFile = self.parseCastorPath(targetPFN)

                result += "nstouch %s\n" % targetFile
                result += "nssetchecksum -n adler32 -k %s %s\n" % (checksums['adler32'], targetFile)

            result += "rfcp "
            if options != None:
                result += " %s " % options

        result += " \"%s\" " % sourcePFN
        result += " \"%s\" \n" % targetPFN

        if self.stageIn:
            result += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            result += "echo \"Local File Size is: $LOCAL_SIZE\"\n"

        if isTargetEOS:

            remotePFN = remotePFN.replace("root://eoscms//eos/cms/", "/eos/cms/", 1)

            result += "REMOTE_FILEINFO=`eos fileinfo '%s' -m`\n" % remotePFN
            result += "REMOTE_SIZE=`echo \"$REMOTE_FILEINFO\" | sed -r 's/.* size=([0-9]+) .*/\\1/'`\n"
            result += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"

            if useChecksum:

                result += "echo \"Local File Checksum is: %s\"\n" % checksums['adler32']
                result += "REMOTE_XS=`echo \"$REMOTE_FILEINFO\" | sed -r 's/.* xstype=adler xs=([0-9a-fA-F]{8})[0]+ .*/\\1/'`\n"
                result += "echo \"Remote File Checksum is: $REMOTE_XS\"\n"

                result += "if [ $REMOTE_SIZE ] && [ $REMOTE_XS ] && [ $LOCAL_SIZE == $REMOTE_SIZE ] && [ '%s' == $REMOTE_XS ]; then exit 0; " % checksums['adler32']
                result += "else echo \"Error: Size or Checksum Mismatch between local and SE\"; eos rm '%s'; exit 60311 ; fi" % remotePFN

            else:

                result += "if [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; "
                result += "else echo \"Error: Size Mismatch between local and SE\"; eos rm '%s'; exit 60311 ; fi" % remotePFN

        else:

            result += "REMOTE_SIZE=`rfstat '%s' | grep Size | cut -f2 -d: | tr -d ' '`\n" % remotePFN
            result += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"

            result += "if [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi"

        return result


    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        """
        if self.isEOS(pfnToRemove):

            command = "xrd eoscms rm %s" % pfnToRemove.replace("root://eoscms//eos/cms/", "/eos/cms/", 1)

        else:

            simplePFN = self.parseCastorPath(pfnToRemove)

            command = "stager_rm -a -M %s ; nsrm %s" % (simplePFN, simplePFN)

        execute(command)
        return


    def checkDirExists(self, directory):
        """
        _checkDirExists_

        Check if directory exists (will throw if it exists as a file)

        Only used for castor and local file

        """
        command = "rfstat %s 2> /dev/null | grep Protection" % directory
        print "Check dir existence : %s" % command
        try:
            exitCode, output = runCommandWithOutput(command)
        except Exception, ex:
            msg = "Error: Exception while invoking command:\n"
            msg += "%s\n" % command
            msg += "Exception: %s\n" % str(ex)
            msg += "Fatal error, abort stageout..."
            raise StageOutError(msg)

        if exitCode != 0:
            return False
        else:
            regExpParser = re.compile('^Protection[ ]+: d')
            if ( regExpParser.match(output) == None):
                raise StageOutError("Output path is not a directory !")
            else:
                return True


    def createDir(self, directory):
        """
        _createDir_

        Creates directory with correct permissions

        Only used for castor and local files

        """
        command = "nsmkdir -p \"%s\"" % directory

        execute(command)
        return


    def setFileClass(self, directory, fileclass):
        """
        _setFileClass_

        Sets fileclass for specified directory

        Only used for castor

        """
        cmd = "nschclass %s %s" % (fileclass, directory)
        execute(cmd)
        return


    def parseCastorPath(self, complexCastorPath):
        """
        _parseCastorPath_

        Castor filenames can be full URLs with control
        statements for the rfcp

        Some other castor command line tools do not understand
        that syntax, so we need to retrieve the path and other
        parameters from the URL
        
        """
        simpleCastorPath = None

        # full castor PFNs
        if simpleCastorPath == None:
            regExpParser = re.compile('/+castor/cern.ch/(.*)')
            match = regExpParser.match(complexCastorPath)
            if ( match != None ):
                simpleCastorPath = '/castor/cern.ch/' + match.group(1)

        # rfio style URLs
        if simpleCastorPath == None:
            regExpParser = re.compile('rfio:.*/+castor/cern.ch/([^?]+).*')
            match = regExpParser.match(complexCastorPath)
            if ( match != None ):
                simpleCastorPath = '/castor/cern.ch/' + match.group(1)

        # xrootd/castor style URLs
        if simpleCastorPath == None:
            regExpParser = re.compile('root:.*/+castor/cern.ch/([^?]+).*')
            match = regExpParser.match(complexCastorPath)
            if ( match != None ):
                simpleCastorPath = '/castor/cern.ch/' + match.group(1)

        # if that does not work raise an error
        if simpleCastorPath == None:
            raise StageOutError("Can't parse castor path out of URL !")

        # remove multi-slashes from path
        while ( simpleCastorPath.find('//') > -1 ):
            simpleCastorPath = simpleCastorPath.replace('//','/')

        return simpleCastorPath


    def isEOS(self, pfn):
        """
        _isEOS_

        Check if the PFN is for EOS

        """
        return pfn.startswith("root://eoscms//")


registerStageOutImpl("rfcp-CERN", RFCPCERNImpl)
