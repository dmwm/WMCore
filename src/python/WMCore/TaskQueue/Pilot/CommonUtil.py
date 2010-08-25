#!/usr/bin/env python

import cPickle
import sys
import os

import popen2
import fcntl, select


CMSSW_INFO = None
CMS_ARCH = None

#isVariableSet
def isVariableSet( envName ):
    """
    __isVariableSet__

    Check if the given evnName is set as an Environement Variable
    Return:
        True if non-none value found, false if None is found
    """
    print ('looking for %s' %envName)
    if ( os.environ.get ( envName ) != None ):
        print '%s:%s' % (envName, os.environ.get ( envName ))
        return True
    print ('sending false for %s' % envName)
    return False

#############################
#
############################
def getCMSSWInfo():
    #source vocms_sw_dir
    #first check if vo_cms_sw_dir exists
    if ( isVariableSet ('VO_CMS_SW_DIR') == True ):
        fileName = None
        if ( os.path.exists('%s/cmsset_default.sh' %  os.getenv('VO_CMS_SW_DIR')) ):
            fileName = '$VO_CMS_SW_DIR/cmsset_default.sh'
        elif ( os.path.exists ('%s/cmsset_default.csh' %  os.getenv('VO_CMS_SW_DIR')) ):
            fileName = '$VO_CMS_SW_DIR/cmsset_default.csh'
        if ( fileName is not None):
            cmd = 'source %s ' % fileName
            os.system(cmd)
            #now get the cmssw versions
            cmd = "echo `scram list -c | grep CMSSW | awk '{print $2;}'`"
            try:
                output = executeCommand(cmd)
                print '################################'
                #remove \n
                output = output.replace('\n','')
                cmssw_list = output.split(" ")
                #print 'CMSSW_LIST %s' % cmssw_list
                #pickled_list = cPickle.dumps(cmssw_list)
                
                return {'CMSSW':cmssw_list}
            except:
                print 'errorrrrrrrrrrr'
                return {'ERROR':'COMMAND ERROR'}
        else:
            print 'VO_CMS_SW_DIR/cmsset_default.(c)sh file does not exists'
            return {'ERROR':'File Not exists'}
    else:
        print 'VO_CMS_SW_DIR does not exists'
        return {'ERROR': 'VO_CMS_SW_DIR not exists'}

#########################
#
#########################
def getScramInfo():
    if ( isVariableSet ('VO_CMS_SW_DIR') == True ):
        fileName = None
        if ( os.path.exists('%s/cmsset_default.sh' %  os.getenv('VO_CMS_SW_DIR')) ):
            fileName = '$VO_CMS_SW_DIR/cmsset_default.sh'
        elif ( os.path.exists ('%s/cmsset_default.csh' %  os.getenv('VO_CMS_SW_DIR')) ):
            fileName = '$VO_CMS_SW_DIR/cmsset_default.csh'
        if ( fileName is not None):
            cmd = 'source %s ' % fileName
            #os.system(cmd)
            cmd = "scram arch"
            try:
                output = executeCommand(cmd)
                output = output.replace('\n','') 
                #print 'SCRAM OUTPUT %s' % output
                return {'SCRAM_ARCH':output}
            except:
                print 'ERROR: %s' % sys.exc_info()[0]
                return {'ERROR':'Command Error'}
        else:
            print 'VO_CMS_SW_DIR/cmsset_default.(c)sh not found'
            return {'ERROR':'File not exists'}
    else:
        print 'VO_CMS_SW_DIR not exists'
        return {'ERROR':'VO_CMS_SW_DIR not exists'}

#############################
#
#############################
def getOutputName(srchDir):
    try:
        files = os.listdir(srchDir)
        for file in files:
            if ( file.endswith('.root') ):
                return file
        return None
    except:
        print 'getOutputName: %s' % sys.exc_info()[0]
        print 'getOutputName: %s' % sys.exc_info()[1]
        return None


def removeJobDir( jobdir):
    """ __removeJobDir__

    cleanup job directory when it is finished
    """
    try:
        import shutil
        shutil.rmtree(jobdir)
    except:
         print 'Problem removeJobDir: %s' % sys.exc_info()[0]
         remove(jobdir)

def remove(jobdir):
    files = os.listdir(jobdir)
    for file in files:
        if file == '.' or file == '..': continue
        path = jobdir + os.sep + file
        if os.path.isdir(path):
            remove(path)
        else:
            os.unlink(path)
        os.rmdir(jobdir)

def makeNonBlocking(fd):
    """
    __makeNonBlocking__
    """
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)

##############################
#execute the script
##############################
def executeCommand(command):
    """
    _executeCommand_

    Util it execute the command provided in a popen object

    """
    print 'executeCommand: %s' % command
    # capture stdout and stderr from command
    child = popen2.Popen3(command, 1)
    # don't need to talk to child
    child.tochild.close()
    outfile = child.fromchild
    outfd = outfile.fileno()
    errfile = child.childerr
    errfd = errfile.fileno()
    makeNonBlocking(outfd)
    makeNonBlocking(errfd)
    outdata = errdata = ''
    outeof = erreof = 0
    stdoutBuffer = ""
    stderrBuffer = ""
    while 1:
        ready = select.select([outfd, errfd], [], []) # wait for input
        if outfd in ready[0]:
            outchunk = outfile.read()
            if outchunk == '':
                outeof = 1
            stdoutBuffer += outchunk
            sys.stdout.write(outchunk)
        if errfd in ready[0]:
            errchunk = errfile.read()
            if errchunk == '':
                erreof = 1
            stderrBuffer += errchunk
            sys.stderr.write(errchunk)
        if outeof and erreof:
            break
        select.select([], [], [], .1) # give a little time for buffers to fill

    try:
        exitCode = child.poll()
    except Exception, ex:
        msg = "Error retrieving child exit code: %s\n" % ex
        msg += "while executing command:\n"
        msg += command
        msg += "\n"
        print("PilotJob:Failed to Execute Command")
        print(msg)
        raise RuntimeError, msg

    if exitCode:
        msg = "Error executing command:\n"
        msg += command
        msg += "\n"
        msg += "Exited with code: %s\n" % exitCode
        msg += "Returned stderr: %s\n" % stderrBuffer
        print("PilotJob:Failed to Execute Command")
        print(msg)
        raise RuntimeError, msg

    return  stdoutBuffer


