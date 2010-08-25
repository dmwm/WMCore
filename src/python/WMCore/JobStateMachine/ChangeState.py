#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.20 2009/08/03 17:02:43 meloam Exp $"
__version__ = "$Revision: 1.20 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.Services.UUID import makeUUID
import base64
import urllib
from sets import Set
import threading


class ChangeState(WMObject):
    """
    Propagate the state of a job through the JSM.
    """
    def __init__(self, config={}, couchDbName = 'jsm_job_history'):
        WMObject.__init__(self, config)
        self.myThread = threading.currentThread()
        self.attachmentList = {}
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
        Return the jobs back, throw assertion error if the state change is not allowed
        and other exceptions as appropriate
        """
        # 1. Is the state transition allowed?
        self.check(newstate, oldstate)
        # 2. Document the state transition
        jobs = self.recordInCouch(jobs, newstate, oldstate)
        # 3. Make the state transition
        self.persist(jobs, newstate, oldstate)

        return jobs
    
    def addAttachment(self,name,jobid,url):
        """
            addAttachment(name, jobid, url)
        enqueues an attachment to be stuck onto the couchrecord at the
        next recordInCouch() call
        """
        if (not jobid in self.attachmentList):
            self.attachmentList[jobid] = {}
        self.attachmentList[jobid][name] = url
        return
    
    
    def getCouchByHeadID(self, id):
        if (type(id) == type([])):
            return self.database.loadView('jobs','get_by_head_couch_id',{},id)
        else:            
            return self.database.loadView('jobs','get_by_head_couch_id',{},[id])
        
    def getCouchByJobID(self, id):
        if (type(id) == type([])):
            return self.database.loadView('jobs','get_by_job_id',{},id)
        else:            
            return self.database.loadView('jobs','get_by_job_id',{},[id])
        
    def getCouchByJobIDAndState(self, ids, state):
        keylist = []
        for oneid in ids:
            keylist.append([oneid, state])

        possibleMatches = self.database.loadView('jobs','get_by_job_id_and_state',{},keylist)
        retval = {}
        for onematch in possibleMatches['rows']:
            onematch = onematch['value']
            if onematch['job']['id'] not in retval:
                retval[onematch['job']['id']] = onematch
            else:
                if retval[onematch['job']['id']]['timestamp'] < onematch['timestamp']:
                    retval[onematch['job']['id']] = onematch
        return retval.values()
    
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
        couch_head - the first state transition
        couch_parent - the state transition before thisone
        couch_record - the document describing this state transition        
        """
        
        for job in jobs:
            newCouchID = makeUUID()
            doc = {'type': 'state change'}
            doc['_id'] = newCouchID
            doc['old_state'] = oldstate
            doc['new_state'] = newstate
            if not 'couch_head' in job:
                job['couch_head'] = newCouchID
            doc['couch_head'] = job['couch_head']
            
            if 'couch_record' in job:
                doc['couch_parent'] = job['couch_record']
                job['couch_parent'] = job['couch_record']
            
            job['couch_record'] = newCouchID
            doc['job'] = job
            
            # it's gross, but we have to base64 encode the attachments to use the bulk api
            if job['id'] in self.attachmentList:
                if not '_attachments' in doc:
                    doc['_attachments'] = {}
                for attachmentName in self.attachmentList[job['id']]:
                    url = self.attachmentList[job['id']][attachmentName]
                    attachmentString = urllib.urlopen( url ).read(-1)
                    base64Attachment = base64.b64encode(attachmentString)
                    doc['_attachments'][attachmentName] = {
                                        "content_type":"test\/plain",
                                        "data": base64Attachment}
                                                           
                
            
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
        hashViewDoc['views'] = {'get_by_head_couch_id': {"map": \
                              """function(doc) {
                                    if (doc.couch_head) {
                                      emit(doc.couch_head, doc);
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
                                  }""" },
                              'get_by_job_id_and_state': {"map":\
                        """function(doc) {
  if ( (doc.job)  && (doc.type) && (doc.new_state) && (doc.job.id)) {
    emit([doc.job.id,doc.new_state],doc);
  }
}"""}}
     
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