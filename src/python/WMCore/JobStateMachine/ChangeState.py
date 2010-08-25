#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.6 2009/07/02 19:21:26 meloam Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from sets import Set
import threading

class Transitions(dict):
    """
    All allowed state transitions in the JSM.
    """
    def __init__(self):
        self.setdefault('none', ['new'])
        self.setdefault('new', ['created', 'createfailed'])
        self.setdefault('created', ['executing', 'submitfailed'])
        self.setdefault('executing', ['complete'])
        self.setdefault('complete', ['jobfailed', 'success'])
        self.setdefault('createfailed', ['createcooloff', 'exhausted'])
        self.setdefault('submitfailed', ['submitcooloff', 'exhausted'])
        self.setdefault('jobfailed', ['jobcooloff', 'exhausted'])
        self.setdefault('createcooloff', ['new'])
        self.setdefault('submitcooloff', ['created'])
        self.setdefault('jobcooloff', ['created'])
        self.setdefault('success', ['closeout'])
        self.setdefault('exhausted', ['closeout'])
        self.setdefault('closeout', ['cleanout'])

    def states(self):
        """
        Return a list of all known states, derive it in case we add new final
        states other than cleanout.
        """
        knownstates = Set(self.keys())
        for possiblestates in self.values():
            for i in possiblestates:
                knownstates.add(i)
        return list(knownstates)



class ChangeState(WMObject):
    """
    Propagate the state of a job through the JSM.
    """
    def __init__(self, config={}):
        WMObject.__init__(self, config)
        self.myThread = threading.currentThread()
        self.logger = self.myThread.logger
        self.dialect = self.myThread.dialect
        self.dbi = self.myThread.dbi
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)

        server = CouchServer(self.config.JobStateMachine.couchurl)
        self.couchdb = server.connectDatabase('JSM/JobHistory')

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
        self.persist(jobs, newstate)
        # TODO: decide if I should update the doc created in step 2 after
        # completing step 3.

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
            doc['oldstate'] = oldstate
            doc['newstate'] = newstate
            if job['couch_record']:
                doc['parent'] = job['couch_record']
            doc['job'] = job
            self.couchdb.queue(doc, True)
        result = self.couchdb.commit()
        assert len(jobs) == len(result), \
                    "Got less than I was expecting from CouchDB"
        if oldstate == 'none':
            def function(item1, item2):
                item1['couch_record'] = item2['id']
                return item1
            jobs = map(function, jobs, result)
        return jobs


    def persist(self, jobs, newstate):
        """
        Write the state change to WMBS, via DAO
        """
        for job in jobs:
            job['state'] = newstate
        dao = self.daofactory(classname = "Jobs.ChangeState")
        dao.execute(jobs)