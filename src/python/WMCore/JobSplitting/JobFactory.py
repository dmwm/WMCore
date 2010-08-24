import logging
from sets import Set
from WMCore.DataStructs.WMObject import WMObject

class JobFactory(WMObject):
    """
    A JobFactory is created with a subscription (which has a fileset). It is a
    base class of an object instance of an object representing some job 
    splitting algorithm. It is called with a job type (at least) to return a 
    JobGroup object. The JobFactory should be subclassed by real splitting 
    algorithm implementations.
    """
    def __init__(self, package='WMCore.DataStructs', subscription=None):
        self.package = package
        self.subscription = subscription

    def __call__(self, jobtype='Job', grouptype='JobGroup', *args, **kwargs):
        """
        The default behaviour of JobFactory.__call__ is to return a single
        Job associated with all the files in the subscription's fileset
        """
        module = "%s.%s" % (self.package, jobtype)
        module = __import__(module, globals(), locals(), [jobtype])#, -1)
        job_instance = getattr(module, jobtype.split('.')[-1])
        
        module = "%s.%s" % (self.package, grouptype)
        module = __import__(module, globals(), locals(), [grouptype])
        group_instance = getattr(module, grouptype.split('.')[-1])

        group = group_instance(subscription = self.subscription)
        
        basename = "%s-%s" % (self.subscription.name(), group.id)

        jobs = self.algorithm(job_instance=job_instance, jobname=basename,
                                  *args, **kwargs)

        group.add(jobs)
        group.commit()

        # Acquire the files used in the job group, job groups should run on 
        # complete files.
        group.recordAcquire(list(jobs))
        return group
    
    def algorithm(self, job_instance=None, jobname=None, *args, **kwargs):
        """
        a splitting algorithm to be overridden by a proper algorithm that does 
        something, anything!
        """
        return Set([job_instance(name=jobname, 
                                 files=self.subscription.availableFiles())])
