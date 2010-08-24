#!/usr/bin/env python
#pylint: disable-msg=W0231
"""
_Subscription_

A simple object representing a Subscription in WMBS.

A subscription is just a way to link many sets of jobs to a 
fileset and track the process of the associated jobs. It is 
associated to a single fileset and a single workflow.

workflow + fileset = subscription

subscription + application logic = jobs

TABLE wmbs_subscription
    id      INT(11) NOT NULL AUTO_INCREMENT,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    type    ENUM("Merge", "Frocessing")
"""

__revision__ = "$Id: Subscription.py,v 1.25 2009/01/02 19:25:18 sfoulkes Exp $"
__version__ = "$Revision: 1.25 $"

import threading

from sets import Set
from sqlalchemy.exceptions import IntegrityError
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Actions.Subscriptions.ChangeState import ChangeStateAction
from WMCore.DataStructs.Subscription import Subscription as WMSubscription
from WMCore.DAOFactory import DAOFactory

class Subscription(WMSubscription):
    def __init__(self, fileset = None, workflow = None, id = -1,
                 whitelist = None, blacklist = None, type = "Processing",
                 split_algo = "FileBased"): 

        if whitelist == None:
            whitelist = Set()
        if blacklist == None:
            blacklist = Set()

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.dialect = myThread.dialect
        self.dbi = myThread.dbi
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
        
        self.setdefault('fileset', fileset)
        self.setdefault('workflow', workflow)
        self.setdefault('type', type)
        self.setdefault('split_algo', split_algo)
        self.setdefault('id', id)
        self.setdefault('whitelist', whitelist)
        self.setdefault('blacklist', blacklist)
        
    def create(self):
        """
        Add the subscription to the database
        """
        try:
            action = self.daofactory(classname="Subscriptions.New")
            action.execute(fileset = self["fileset"].id, 
                           type = self["type"], split = self["split_algo"],
                           workflow = self["workflow"].id)
            
        except IntegrityError:
            self.logger.exception('Subcription %s:%s exists' % (self['fileset'], 
                                                                self['workflow']))
        
        action = self.daofactory(classname="Subscriptions.Exists")
        self['id'] = action.execute(fileset = self['fileset'].id, 
                                    type = self['type'],
                                    workflow = self['workflow'].id)
        return
    
    def exists(self):
        """
        See if the subscription is in the database
        """
        action = self.daofactory(classname="Subscriptions.Exists")
        value = action.execute(fileset = self["fileset"].id, 
                               type = self["type"],
                               workflow = self["workflow"].id)
        return value
    
    def load(self):
        """
        _load_

        """
        if self["fileset"] != None:
            fileset = self["fileset"].id
        else:
            fileset = None

        if self["workflow"] != None:
            workflow = self["workflow"].id
        else:
            workflow = None

        action = self.daofactory(classname='Subscriptions.Load')
        result = action.execute(fileset = fileset,
                                workflow = workflow,
                                id = self["id"], 
                                type = self['type'])
        
        if not result:
            raise RuntimeError, "Subscription for %s:%s unknown" % \
                                    (self['fileset'].name, self['workflow'].spec)
        self['fileset'] = Fileset(id = result['fileset']).load('Fileset.LoadFromID')
        self['workflow'] = Workflow(id = result['workflow'])
        self["workflow"].load(method = 'Workflow.LoadFromID')

        self['type'] = result['type']
        self['id'] = result['id']
        self.split_algo = result['split_algo']

        # load available files
        # load acquired files
        # load completed files
        # load failed files
    
    def markLocation(self, location, whitelist = True):
        """
        Add a location to the subscriptions white or black list
        """
        # Check the location exists, add it if not
        try:
            self.daofactory(classname='Locations.New').execute(location)
        except IntegrityError:
            # location exists, do nothing
            pass
        
        # Mark the location as appropriate
        action = self.daofactory(classname='Subscriptions.MarkLocation')
        action.execute(self['id'], location, whitelist)
          
    def filesOfStatus(self, status=None):
        """
        fids will be a set of id's, we'll then load the corresponding file 
        objects.
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.Get%s' % status)
        for f in action.execute(self):
            fl = File(id=f[0])
            fl.load()
            files.add(fl)
        return files 
    
    def acquireFiles(self, files = None, size = 0):
        """
        Acquire size files, activating them for the subscription. If size = 0 
        acquire all files (default behaviour). Return a list of files objects 
        for those acquired.
        """
        action = self.daofactory(classname='Subscriptions.AcquireFiles')
        if files:
            files = self.makelist(files)
            action.execute(self['id'], [x['id'] for x in files])
            return files
        else:
            acq = self.acquiredFiles()
            files = self.availableFiles()
            l = Set()
            if len(files) < size or size == 0:
                size = len(files)
            i = 0
            while i < size:
                l.add(files.pop()['id'])
                i = i + 1
            action.execute(self['id'], [x for x in l])
            ret = self.acquiredFiles() - acq
            
            return ret
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        if files and not isinstance(files, list) and not isinstance(files, set):
            files = [files]
        statechanger = ChangeStateAction(self.logger)
        statechanger.execute(subscription = self['id'], 
                                  file = [x['id'] for x in files], 
                                  daofactory = self.daofactory)
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        if files and not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        statechanger = ChangeStateAction(self.logger)
        statechanger.execute(subscription = self['id'], 
                                  file = [x['id'] for x in files], 
                                  state = "FailFiles",
                                  daofactory = self.daofactory)

    def getJobs(self):
        """
        Return a list of all the jobs associated with a subscription
        """
        return self.daofactory(classname='Subscriptions.Jobs')
        
    def delete(self):
        """
        _delete_

        """
        action = self.daofactory(classname = "Subscriptions.Delete")
        action.execute(id = self["id"])
