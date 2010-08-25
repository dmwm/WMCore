#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.2 2009/05/08 17:11:44 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer, Database
from WMCore.DataStructs.WMObject import WMObject

class ChangeState(WMObject):
    def __init__(self, config={}):
        WMObject.__init__(self, config)
        server = CouchServer()
        self.db = server.connectDatabase(self.config.JobStateMachine.couchurl)

    def propagate(self, jobs, newstate, oldstate):
        """
        Move the job from a state to another. Book keep the change to CouchDB.
        Take a list of job objects (dicts) and the desired state change.
        Return nothing, throw assertion error if the state change is not allowed
        and other exceptions as appropriate
        """
        # 1. Is the state transition allowed?
        self.check(newstate, oldstate)
        # 2. Document the state transition
        jobs = self.recordInCouch(jobs, newstate, oldstate)
        # 3. Make the state transition
        self.persist(jobs, newstate, oldstate)
        # TODO: decide if I should update the doc created in step 2.

    def check(self, newstate, oldstate):
        """
        check that the transition is allowed. return a tuple of the transition
        if it is allowed, throw up an exception if not.
        """
        transitions = {}
        transitions['none'] = ['new']
        transitions['new'] = ['created', 'createfailed']
        transitions['created'] = ['executing', 'submitfailed']
        transitions['executing'] = ['complete']
        transitions['complete'] = ['jobfailed', 'success']
        transitions['createfailed'] = ['createcooloff', 'exhausted']
        transitions['submitfailed'] = ['submitcooloff', 'exhausted']
        transitions['jobfailed'] = ['jobcooloff', 'exhausted']
        transitions['createcooloff'] = ['new']
        transitions['submitcooloff'] = ['created']
        transitions['jobcooloff'] = ['created']
        transitions['success'] = ['closeout']
        transitions['exhausted'] = ['closeout']
        transitions['closeout'] = ['cleanout']

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
            doc['oldstate'] = oldstate
            doc['newstate'] = newstate
            doc['job'] = job
            self.db.queue(doc, True)
        result = self.db.commit()
        assert len(jobs) == len(result), \
                    "Got less than I was expecting from CouchDB"
        if oldstate == 'none':
            def function(item1, item2):
                item1['couch_record'] = item2['id']
                return item1
            jobs = map(function, jobs, result)
        return jobs

    def persist(self, jobs, newstate, oldstate):
        """
        Write the state change to WMBS, via DAO
        """
        pass