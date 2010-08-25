#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.13 2009/07/21 19:31:18 meloam Exp $"
__version__ = "$Revision: 1.13 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from WMCore.JobStateMachine.Transitions import Transitions
from sets import Set
import threading


class ChangeState(WMObject):
    """
    Propagate the state of a job through the JSM.
    """
    def __init__(self, config={}, couchDbName = 'jsm_job_history'):
        WMObject.__init__(self, config)
        self.myThread = threading.currentThread()
        self.logger = self.myThread.logger
        self.dialect = self.myThread.dialect
        self.dbi = self.myThread.dbi
        self.dbname = couchDbName
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)

        self.couchdb = CouchServer(self.config.JobStateMachine.couchurl)
        if self.dbname not in self.couchdb.listDatabases():
            self.createDatabase()
        self.database = self.couchdb.connectDatabase(couchDbName)

    def propagate(self, jobs, newstate, oldstate):
        """
        Move the job from a state to another. Book keep the change to CouchDB.
        Take a list of job objects (dicts) and the desired state change.
        Return nothing, throw assertion error if the state change is not allowed
        and other exceptions as appropriate
        """
        # 1. Is the state transition allowed?
        self.check(newstate, oldstate)
        # 2. Are the jobs actually in the old state we're claiming?
        # FIXME race conditions?
#        for job in jobs:
#            realstate = job.getState()
#            assert job.getState() == oldstate ,\
#                    "Job id %s in state %s, not %s" %\
#                    (job['id'], realstate, oldstate)
                    
        # 3. Document the state transition
        jobs = self.recordInCouch(jobs, newstate, oldstate)
        # 4. Make the state transition
        self.persist(jobs, newstate, oldstate)
        # TODO: decide if I should update the doc created in step 2 after
        # completing step 3.

    def getCouchByParentID(self, id):
        return self.database.loadView('jobs','get_by_parent_couch_id',{},[id])
    def getCouchByJobID(self, id):
        return self.database.loadView('jobs','get_by_job_id',{},[id])

    def check(self, newstate, oldstate):
        """
        check that the transition is allowed. return a tuple of the transition
        if it is allowed, throw up an exception if not.
        """
        # Check for wrong transitions
        transitions = Transitions()
        assert newstate in transitions[oldstate], \
                            "Illegal state transition requested"


    def recordInCouch(self, jobs, newstate, oldstate):
        """
        Write a document for the job to CouchDB for each state transition for
        each job. Do this as a bulk operation.
        TODO: handle attachments
        """
        for job in jobs:
            doc = {'type': 'state change'}
            doc['old_state'] = oldstate
            doc['new_state'] = newstate
            if 'couch_record' in job:
                doc['parent'] = job['couch_record']
            doc['job'] = job
            self.database.queue(doc, timestamp=True)
        goodresult = self.database.commit()
        
        assert len(jobs) == len(goodresult), \
                    "Got less than I was expecting from CouchDB: \n %s" %\
                        (goodresult,)
        if oldstate == 'none':
            def function(item1, item2):
                item1['couch_record'] = item2['id']
                return item1
            jobs = map(function, jobs, goodresult)
        return jobs

    def createDatabase(self):
        ''' initializes a non-existant database'''
        database = self.couchdb.createDatabase(self.dbname)
        hashViewDoc = database.createDesignDoc('jobs')
        hashViewDoc['views'] = {'get_by_parent_couch_id': {"map": \
                              """function(doc) {
                                    if (doc.parent) {
                                      emit(doc.parent, doc);
                                    } else {
                                      emit(doc._id, doc);
                                    }
                                 } 
                     """ },
                             'get_by_job_id': {"map": \
                              """function(doc) {
                                    if (doc.job) {
                                      if (doc.job.id) {
                                        emit(doc.job.id, doc);
                                      }
                                    }
                                  }""" }}
     
        database.queue( hashViewDoc )
        database.commit()
        return database

    def persist(self, jobs, newstate, oldstate):
        """
        Write the state change to WMBS, via DAO
        """
        for job in jobs:
            job['state'] = newstate
            job['oldstate'] = oldstate
        dao = self.daofactory(classname = "Jobs.ChangeState")
        dao.execute(jobs)