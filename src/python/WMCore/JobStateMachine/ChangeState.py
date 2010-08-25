#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.1 2009/05/08 14:15:31 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer, Database

class ChangeState:
    def propagate(self, job, newstate, oldstate, attachments):
        """
        Move the job from a state to another. Book keep the change to CouchDB.
        """
        pass

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
                            "Illegal statetransition requested"

    def recordInCouch(self, job, transition, attachments):
        """
        Write a document for the job to CouchDB for each state transition
        """
        pass

    def persist(self, job, transition):
        """
        Write the state change to WMBS, via DAO
        """
        pass