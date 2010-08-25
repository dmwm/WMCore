#!/usr/bin/env python
"""
_RFCPCERNImpl_

Implementation of StageOutImpl interface for RFIO in Castor2
with specific code to set the RAW tape families for CERN

"""
import os
import re
import logging
from subprocess import Popen, PIPE


from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure
from WMCore.Storage.Execute import execute
from WMCore.Storage.Execute import runCommandWithOutput

class RFCPCERNImpl(StageOutImplV2):
    """
    _RFCPCERNImpl_
    
    """

    def __init__(self, stagein=False):
        StageOutImplV2.__init__(self, stagein)
        self.numRetries = 5
        self.retryPause = 300

        # permissions for target directory
        self.permissions = '775'

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        create dir with group permission
        """

        # check how the targetPFN looks like and parse out the target dir
        targetdir = None

        if targetdir == None:
            regExpParser = re.compile('/+castor/(.*)')
            match = regExpParser.match(targetPFN)
            if ( match != None ):
                targetdir = os.path.dirname(targetPFN)

        if targetdir == None:
            regExpParser = re.compile('rfio:/+castor/(.*)')
            match = regExpParser.match(targetPFN)
            if ( match != None ):
                targetdir = os.path.dirname('/castor/' + match.group(1))

        if targetdir == None:
            regExpParser = re.compile('rfio:.*path=/+castor/(.*)')
            match = regExpParser.match(targetPFN)
            if ( match != None ):
                targetdir = os.path.dirname('/castor/' + match.group(1))

        # raise exception if we have no rule that can parse the target dir
        if targetdir == None:
            raise StageOutError("Cannot parse directory out of targetPFN")

        # remove multi-slashes from path
        while ( targetdir.find('//') > -1 ):
            targetdir = targetdir.replace('//','/')

        #
        # determine file class from LFN
        #
        fileclass = None

        # temp or unmerged files use cms_no_tape
        if ( fileclass == None ):
            regExpParser = re.compile('.*/castor/cern.ch/cms/store/temp/')
            if ( regExpParser.match(targetdir) != None ):
                fileclass = 'cms_no_tape'
        if ( fileclass == None ):
            regExpParser = re.compile('.*/castor/cern.ch/cms/store/unmerged/')
            if ( regExpParser.match(targetdir) != None ):
                fileclass = 'cms_no_tape'

        # RAW data files use cms_raw
        if ( fileclass == None ):
            regExpParser = re.compile('.*/castor/cern.ch/cms/store/data/[^/]+/[^/]+/RAW/')
            if ( regExpParser.match(targetdir) != None ):
                fileclass = 'cms_raw'

        # otherwise we assume another type of production file
        if ( fileclass == None ):
            fileclass = 'cms_production'

        print "Setting fileclass to : %s" % fileclass

        # check if directory exists
        rfstatCmd = "rfstat %s 2> /dev/null | grep Protection" % targetdir
        print "Check dir existence : %s" % rfstatCmd
        try:
            rfstatExitCode, rfstatOutput = runCommandWithOutput(rfstatCmd)
        except Exception, ex:
            msg = "Error: Exception while invoking command:\n"
            msg += "%s\n" % rfstatCmd
            msg += "Exception: %s\n" % str(ex)
            msg += "Fatal error, abort stageout..."
            raise StageOutError(msg)

        # does not exist => create it
        if rfstatExitCode:
            if ( fileclass != None ):
                self.createDir(targetdir, '000')
                self.setFileClass(targetdir,fileclass)
                self.changeDirMode(targetdir, self.permissions)
            else:
                self.createDir(targetdir, self.permissions)
        else:
            # check if this is a directory
            regExpParser = re.compile('Protection.*: d')
            if ( regExpParser.match(rfstatOutput) == None):
                raise StageOutError("Output path is not a directory !")
            else:
                # check if directory is writable
                regExpParser = re.compile('Protection.*: d---------')
                if ( regExpParser.match(rfstatOutput) != None and fileclass != None ):
                    self.setFileClass(targetdir,fileclass)
                    self.makeDirWritable(targetdir)

        return

    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, protocol  ):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
            
            if stageOut is true:
                The fromPfn is the LOCAL FILE NAME on the node, without file://
                the toPfn is the target PFN, mapped from the LFN using the TFC or overrrides
            if stageOut is false:
                The toPfn is the LOCAL FILE NAME on the node, without file://
                the fromPfn is the target PFN, mapped from the LFN using the TFC or overrrides
            
            this behavior is because most transfer commands will switch their direction
            simply by swapping the order of the arguments. the stageOut flag is provided
            however, because sometimes you want to pass different command line args
                
        """
        
        ourCommand = \
            self.generateCommandFromPreAndPostParts(\
                        ["rfcp"],
                        [fromPfn, toPfn],
                        options)
        self.runCommandFailOnNonZero(ourCommand)
        
        # keeping this logic though I don't believe in it
        # AMM -7/13/2010
        if not stageOut:
            remotePFN, localPFN = fromPfn, toPfn
        else:
            remotePFN, localPFN = toPfn, fromPfn

        localSize  = os.path.getsize( localPFN )                
        p1 = Popen(["rfstat", remotePFN], stdout=PIPE)
        p2 = Popen(["grep", "Size"], stdin=p1.stdout, stdout=PIPE)
        p3 = Popen(['cut','-f2','-d'], stdin=p2.stdout, stdout=PIPE)
        remoteSize = p3.communicate()[0]
        logging.info("Localsize: %s Remotesize: %s" % (localSize, remoteSize))
        if int(localSize) != int(remoteSize):
            try:
                self.doDelete(toPfn,None,None,None,None)
            except:
                pass
            raise StageOutFailure, "File sizes don't match"
        
        return toPfn
    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided: specific for Castor-1

        """
        self.runCommandWarnOnNonZero(['stager_rm','-M',pfnToRemove])
        self.runCommandWarnOnNonZero(['nsrm',pfnToRemove])

    def createDir(self, directory, mode):
        """
        _createDir_

        Creates directory with no permissions

        """
        self.runCommandWarnOnNonZero(['nsmkdir','-m',mode,'-p',directory])

    def setFileClass(self, directory, fileclass):
        """
        _createDir_

        Sets fileclass for specified directory

        """
        self.runCommandWarnOnNonZero(['nschclass', fileclass, directory])

    def changeDirMode(self, directory, mode):
        """
        _createDir_

        Sets mode for directory

        """
        self.runCommandWarnOnNonZero(['nschmod', mode, directory])


