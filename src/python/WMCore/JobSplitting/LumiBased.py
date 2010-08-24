#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_LumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections

"""
__revision__ = "$Id: LumiBased.py,v 1.1 2008/09/25 14:48:28 evansde Exp $"
__version__  = "$Revision: 1.1 $"



from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory




class LumiBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, job_instance = None, *args, **kwargs):
        """
        _algorithm_

        Implement event based splitting algorithm

        """
        jobs = Set()

        #  //
        # // get the fileset
        #//
        fileset = self.subscription.availableFiles()

        lumisPerJob = kwargs['lumis_per_job']

        for f in fileset:
            lumisInFile = sum([ len(run) for run in f['runs']])
            accumLumis = []
            for lumi in range(0, lumisInFile):
                accumLumis.append(lumi)
                if len(accumLumis) == lumisPerJob:
                    job = job_instance(self.subscription)
                    job.addFile(f)
                    job.mask.setMaxAndSkipLumis(lumisPerJob, lumi)
                    jobs.add(job)
                    accumLumis = []
            if len(accumLumis) != 0:
                job = job_instance(self.subscription)
                job.addFile(f)
                job.mask.setMaxAndSkipLumis(lumisPerJob, lumi)
                jobs.add(job)





        return jobs



