"""
Rest Model for WMBS Monitoring.
"""

__revision__ = "$Id"
__version__ = "$Revision: 1.8 $"


from WMCore.Wrappers import JsonWrapper
from WMCore.HTTPFrontEnd.WorkQueue.Services.ServiceInterface import ServiceInterface
from cherrypy import HTTPError

#TODO: needs to point to the global workqueue if it can make it for the both
from WMCore.WorkQueue.WorkQueue import WorkQueue

from functools import partial
from os import path

# change to a url here so unit tests aren't affected
def wrapGetWork(workqueue, *args, **kwargs):
    """Change url's to be web-accessible"""
    result = workqueue.getWork(*args, **kwargs)
    for item in result:
        item['url'] = "%s/wf/%s" % (workqueue.params['QueueURL'],
                                    path.basename(item['url']))
    return result

def serveWorkflow(workqueue, name):
    """Return a workflow from the cache"""
    name = path.normpath(path.join(workqueue.params['CacheDir'], name))
    if path.commonprefix([name, workqueue.params['CacheDir']]) != workqueue.params['CacheDir']:
        raise HTTPError(403)
    if not path.exists(name):
        print "datadata:"
        import os
        os.system("ls -la1  /home/xmax/tmp/caltech/dmwm/WMCORE/wf_cache/")
        raise HTTPError(404, "%s not found" % name)
    data = ''
    with open(name, 'rb') as infile:
        data = infile.read()
    return data


class WorkQueueService(ServiceInterface):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def register(self):
        # we don't populate wmbs - WorkQueueManager does (in the LocalQueue)
        
        self.model.config.queueParams['PopulateFilesets'] = False
        
        self.wq = WorkQueue(logger = self.model, dbi = self.model.dbi, **self.model.config.queueParams)


        self.model.addMethod('POST', 'getwork', partial(wrapGetWork, self.wq),
                             args = ["siteJobs", "pullingQueueUrl"])
        self.model.addMethod('POST', 'status', self.wq.status,
                             args = ["status", "before", "after", "elementIDs", "subs", "dictKey"],
                             validation = [self.validateIds])
        self.model.addMethod('POST', 'wf', partial(serveWorkflow, self.wq), args = ['name'])
        self.model.addMethod('PUT', 'synchronize', self.wq.synchronize,
                             args = ["child_url", "child_report"])
        self.model.addMethod('PUT', 'gotwork', self.wq.gotWork, args = ["elementIDs"],
                             validation = [self.validateIds])
        self.model.addMethod('PUT', 'failwork', self.wq.failWork, args = ["elementIDs"],
                             validation = [self.validateIds])
        self.model.addMethod('PUT', 'donework', self.wq.doneWork, args = ["elementIDs"],
                             validation = [self.validateIds])
        self.model.addMethod('PUT', 'cancelwork', self.wq.cancelWork, args = ["elementIDs"],
                             validation = [self.validateIds])
        
        #TODO: this needs to be more clearly defined (current deleteWork doesn't do anything) 
        #self.model.addMethod('DELETE', 'deletework', self.wq.deleteWork, args=["elementIDs"])

        
    
    def validateIds(self, inpt):
        """Validate inpt argument - element id: elementIDs
           only list of positive integers allowed.
        """
        key = "elementIDs"
        try:
            for id in inpt[key]:
                if int(id) < 0:
                    raise ValueError
        except (TypeError, ValueError, KeyError):
            m = ("Incorrect input - list of positive integers under '%s' key, "
                 "got: '%s'" % (key, inpt))        
            raise AssertionError, m
        else:
            return inpt