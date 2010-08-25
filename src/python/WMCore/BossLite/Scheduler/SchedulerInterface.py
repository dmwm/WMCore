#!/usr/bin/env python
"""
_SchedulerInterface_

"""

import os
from WMCore.BossLite.Common.System import executeCommand
from WMCore.BossLite.Common.Exceptions import SchedulerError

__version__ = "$Id: SchedulerInterface.py,v 1.1 2010/04/23 21:00:11 mnorman Exp $"
__revision__ = "$Revision: 1.1 $"



class SchedulerInterface(object):
    """
    Upper layer for scheduler interaction

    """

    def __init__(self, **args):
        """
        initialization
        """

        self.cert = args.get( "user_proxy", '')
        self.timeout = args.get( "timeout", None )
        self.invalidList = args.get( "skipList", ['A', 'K', 'E'] )
        self.validProxy = args.get( "skipProxyCheck", None )
        self.logging = args.get( "logger", None )
        if self.logging is None:
            import logging
            self.logging = logging

    ##########################################################################
    def valid( self, runningJob ) :
        """
        evaluate if the runningJob is valid for scheduler interaction

        """

        if runningJob is not None \
               and runningJob.active == True \
               and runningJob['schedulerId'] is not None \
               and runningJob['closed'] == "N" \
               and runningJob['status'] not in self.invalidList :
            return True
        else :
            return False

    ##########################################################################

    def ExecuteCommand( self, command ):
        """
        _ExecuteCommand_

        Util it execute the command provided in a popen object with a timeout
        """

        return executeCommand( command, self.timeout )

    ##########################################################################

    def setTimeout( self, timeout ):
        """
        _setTimeout_

        Redefine timeout value
        """

        if timeout is not None:
            timeout = int ( timeout )
            
        self.timeout = timeout

    ##########################################################################

    def getUserProxy(self):
        """ retrieve PROXY Default Certificate File name"""

        if self.cert is not None and self.cert != '':
            return self.cert
        try:
            return os.environ['X509_USER_PROXY']
        except:
            return '/tmp/x509up_u'+ repr(os.getuid())

    ##########################################################################

    def checkUserProxy( self ):
        """
        Retrieve the user proxy for the task
        If the proxy is valid pass, otherwise raise an axception
        """

        if self.validProxy is not None :
            return self.validProxy

        command = 'voms-proxy-info'

        if self.cert != '' :
            command += ' --file ' + self.cert

        output, ret = self.ExecuteCommand( command )

        try:
            output = output.split("timeleft  :")[1].strip()
        except IndexError:
            self.validProxy = False
            raise SchedulerError("Missing Proxy", output.strip())

        if output == "0:00:00":
            self.validProxy = False
            raise SchedulerError("Proxy Expired", output.strip())

        self.validProxy = True
        return self.validProxy

    ##########################################################################

    def jobDescription ( self, obj, requirements='', config='', service = '' ):
        """
        retrieve scheduler specific job description
        return it as a string
        """
        raise NotImplementedError

    ##########################################################################

    def decode ( self, obj, requirements='' ) :
        """
        prepare scheduler specific job description

        used by self.submit(), return everithing is needed
        for the actual submission

        """
        raise NotImplementedError

    ##########################################################################

    def submit ( self, obj, requirements='', config='', service = '' ) :
        """
        set up submission parameters and submit
        uses self.decode()

        return jobAttributes, bulkId, service

        - jobAttributs is a map of the format
              jobAttributes[ 'name' : 'schedulerId' ]
        - bulkId is an eventual bulk submission identifier
        - service is a endpoit to connect withs (such as the WMS)
        """
        raise NotImplementedError

    ##########################################################################

    def query(self, obj, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        It may use single 'node' scheduler id or bulk id for association
        The strategy must be defined in the specific implementation.

        """
        raise NotImplementedError

    ##########################################################################

    def getOutput( self, obj, outdir='' ):
        """
        retrieve output or just put it in the destination directory

        does not return
        """
        raise NotImplementedError

    ##########################################################################

    def kill( self, obj ):
        """
        kill the job instance

        does not return
        """
        raise NotImplementedError

    ##########################################################################

    def postMortem ( self, schedIdList, outfile, service ) :
        """
        execute any post mortem command such as logging-info
        and write it in outfile
        """
        raise NotImplementedError

    ##########################################################################

    def purgeService( self, obj ):
        """
        purge the service used by the scheduler from job files
        not available for every scheduler

        does not return
        """
        return

    ##########################################################################

    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        perform a resources discovery
        returns a list of resulting sites
        """
        raise NotImplementedError

    ##########################################################################


