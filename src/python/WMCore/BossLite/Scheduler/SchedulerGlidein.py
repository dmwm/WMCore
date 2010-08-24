#!/usr/bin/env python
"""
_SchedulerGlidein_
"""




from WMCore.BossLite.Scheduler.SchedulerCondorCommon import SchedulerCondorCommon
import os

class SchedulerGlidein(SchedulerCondorCommon) :
    """
    basic class to handle glite jobs through wmproxy API
    """
    def __init__( self, **args ):

        # call super class init method
        super(SchedulerGlidein, self).__init__(**args)

    def specificBulkJdl(self, job, requirements=''):
        """
        build a job jdl easy to be handled by the wmproxy API interface
        and gives back the list of input files for a better handling
        """

        jdl  = 'Universe  = vanilla\n'

        #x509 = self.x509Proxy()
        #if x509:
            #jdl += 'x509userproxy = $ENV(X509_USER_PROXY) \n'

        # Glidein parameters
        jdl += 'Environment = ' \
               'JobRunCount=$$([ ifThenElse(' \
                'isUndefined(JobRunCount),0,JobRunCount) ]);' \
               'GlideinMemory=$$([ ifThenElse(' \
                'isUndefined(ImageSize_RAW),0,ImageSize_RAW) ]);' \
               'Glidein_MonitorID=$$([ ifThenElse(' \
                'isUndefined(Glidein_MonitorID),0,Glidein_MonitorID) ]) \n'
        jdl += '+JOB_Site = "$$(GLIDEIN_Site:Unknown)" \n'
        jdl += '+JOB_VM = "$$(Name:Unknown)" \n'
        jdl += 'x509userproxy = $ENV(X509_USER_PROXY) \n'
        #jdl += '+JOB_Machine_KFlops = \"\$\$(KFlops:Unknown)\" \n");
        #jdl += '+JOB_Machine_Mips = \"\$\$(Mips:Unknown)\" \n");
        jdl += '+JOB_GLIDEIN_Schedd = "$$(GLIDEIN_Schedd:Unknown)" \n'
        jdl += '+JOB_GLIDEIN_ClusterId = "$$(GLIDEIN_ClusterId:Unknown)" \n'
        jdl += '+JOB_GLIDEIN_ProcId = "$$(GLIDEIN_ProcId:Unknown)" \n'
        jdl += '+JOB_GLIDEIN_Factory = "$$(GLIDEIN_Factory:Unknown)" \n'
        jdl += '+JOB_GLIDEIN_Name = "$$(GLIDEIN_Name:Unknown)" \n'
        jdl += '+JOB_GLIDEIN_Frontend = "$$(GLIDEIN_Client:Unknown)" \n'
        jdl += '+JOB_Gatekeeper = "$$(GLIDEIN_Gatekeeper:Unknown)" \n'
        jdl += '+JOB_GridType = "$$(GLIDEIN_GridType:Unknown)" \n'
        jdl += '+JOB_GlobusRSL = "$$(GLIDEIN_GlobusRSL:Unknown)" \n'
        jdl += 'since=(CurrentTime-EnteredCurrentStatus)\n'
        jdl += 'Periodic_Remove = (((JobStatus == 2) && ' \
               '((CurrentTime - JobStartDate) > ' \
                '(MaxWallTimeMins*60))) =?= True) || '
        jdl += '(JobStatus==5 && $(since)>691200) || ' \
               '(JobStatus==1 && $(since)>691200)\n'

        return jdl

    def delegateProxy(self):
        """
        fake
        """
        return

