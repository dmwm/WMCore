#!/usr/bin/env python
"""
_WMBSMySQL_

MySQL Compatibility layer for WMBS


"""

__revision__ = "$Id: MySQL.py,v 1.8 2008/05/12 11:58:06 swakef Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBCore import DBInterface
from sqlalchemy.exceptions import IntegrityError

class MySQLDialect(DBInterface):   
    """
    initial implementation of WMBS API for MySQL. Should be used as 
    the base class for other dialects.
    
    Contains SQL and API methods for WMBS; creates, inserts, selects,
    deletes, updates
    
    Change the appropriate dictionary key for dialect specific SQL If
    necessary over ride the relevant methods for dialect specific
    operations e.g. creating sequences, indexes, timestamps.
    """  
    
    logger = ""
    engine = ""
    
    create = {}
    insert = {}
    select = {}
    update = {}
    delete = {}
    
    sqlIgnoreError = '' #'IGNORE'
    
    def timestamp(self):
        """
        generate a timestamp
        """
        return "NOW()"
    
    def __init__(self, logger, engine):
        DBInterface.__init__(self, logger, engine)
        self.create['wmbs_fileset'] = """CREATE TABLE wmbs_fileset (
                id int(11) NOT NULL AUTO_INCREMENT,
                name varchar(255) NOT NULL,
                open boolean NOT NULL DEFAULT FALSE,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,  
                PRIMARY KEY (id), UNIQUE (name))"""
        self.create['wmbs_fileset_parent'] = """CREATE TABLE wmbs_fileset_parent (
                child INT(11) NOT NULL,
                parent INT(11) NOT NULL,
                FOREIGN KEY (child) references wmbs_fileset(id)
                    ON DELETE CASCADE,
                FOREIGN KEY (parent) references wmbs_fileset(id),
                UNIQUE(child, parent))""" 
        self.create['wmbs_fileset_files'] = """CREATE TABLE wmbs_fileset_files (
                file    int(11)      NOT NULL,
                fileset int(11) NOT NULL,
                status ENUM ("active", "inactive", "invalid"),
                insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY(fileset) references wmbs_fileset(id)
                    ON DELETE CASCADE,
                FOREIGN KEY(file) REFERENCES wmbs_file_details(id)
                    ON DELETE CASCADE    
                    )"""
        self.create['wmbs_file_parent'] = """CREATE TABLE wmbs_file_parent (
                child INT(11) NOT NULL,
                parent INT(11) NOT NULL,
                FOREIGN KEY (child) references wmbs_file(id)
                    ON DELETE CASCADE,
                FOREIGN KEY (parent) references wmbs_file(id),
                UNIQUE(child, parent))"""  
        self.create['wmbs_file_details'] = """CREATE TABLE wmbs_file_details (
                id int(11) NOT NULL AUTO_INCREMENT,
                lfn     VARCHAR(255) NOT NULL,
                size    int(11),
                events  int(11),
                run     int(11),
                lumi    int(11),
                UNIQUE(lfn),
                PRIMARY KEY(id),
                INDEX (lfn))"""
        self.create['wmbs_location'] = """CREATE TABLE wmbs_location (
                id int(11) NOT NULL AUTO_INCREMENT,
                se_name VARCHAR(255) NOT NULL,
                UNIQUE(se_name),
                PRIMARY KEY(id))"""
        self.create['wmbs_file_location'] = """CREATE TABLE wmbs_file_location (
                file     int(11),
                location int(11),
                UNIQUE(file, location),
                FOREIGN KEY(file)     REFERENCES wmbs_file(id)
                    ON DELETE CASCADE,
                FOREIGN KEY(location) REFERENCES wmbs_location(id)
                    ON DELETE CASCADE)"""
        self.create['wmbs_workflow'] = """CREATE TABLE wmbs_workflow (
                id           INT(11) NOT NULL AUTO_INCREMENT,
                spec         VARCHAR(255) NOT NULL,
                owner        VARCHAR(255),
                PRIMARY KEY (id))"""
        self.create['wmbs_subscription'] = """CREATE TABLE wmbs_subscription (
                id      INT(11) NOT NULL AUTO_INCREMENT,
                fileset INT(11) NOT NULL,
                workflow INT(11) NOT NULL,
                type    ENUM("Merge", "Processing", "Job"),
                parentage INT(11) NOT NULL DEFAULT 0,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY(id),
                UNIQUE(fileset, workflow, type),
                FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
                    ON DELETE CASCADE,
                FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
                    ON DELETE CASCADE)"""
        self.create['wmbs_sub_files_acquired'] = """
CREATE TABLE wmbs_sub_files_acquired (
    subscription INT(11) NOT NULL,
    file         INT(11) NOT NULL,
    FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
        ON DELETE CASCADE,
    FOREIGN KEY (file) REFERENCES wmbs_file(id))
"""
        self.create['wmbs_sub_files_failed'] = """
CREATE TABLE wmbs_sub_files_failed (
    subscription INT(11) NOT NULL,
    file         INT(11) NOT NULL,
    FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
        ON DELETE CASCADE,
    FOREIGN KEY (file) REFERENCES wmbs_file(id))"""
        self.create['wmbs_sub_files_complete'] = """
CREATE TABLE wmbs_sub_files_complete (
    subscription INT(11) NOT NULL,
    file         INT(11) NOT NULL,
    FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
        ON DELETE CASCADE,
    FOREIGN KEY (file) REFERENCES wmbs_file(id))"""
        self.create['wmbs_job'] = """CREATE TABLE wmbs_job (
                id           INT(11) NOT NULL AUTO_INCREMENT,
                subscription INT(11) NOT NULL,
                job_spec_id VARCHAR(255) NOT NULL,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                    ON DELETE CASCADE)"""        
        self.create['wmbs_job_assoc'] = """CREATE TABLE wmbs_job_assoc (
                job    INT(11) NOT NULL,
                file   INT(11) NOT NULL,
                FOREIGN KEY (job) REFERENCES wmbs_job(id)
                    ON DELETE CASCADE,
                FOREIGN KEY (file) REFERENCES wmbs_file(id))"""
        
        # What history tables do we need?
        # What statistics/monitoring is needed?
        self.insert['fileset'] = """
            insert %s into wmbs_fileset (name, open, last_update) values (:fileset, :is_open, :timestamp)""" % self.sqlIgnoreError
        self.insert['fileset_parent'] = """
            insert into wmbs_fileset_parent (child, parent) 
                values ((select id from wmbs_fileset where name = :child),
                (select id from wmbs_fileset where name = :parent))
        """
        self.insert['newfile'] = """
            insert %s into wmbs_file_details (lfn, size, events, run, lumi) 
                values (:lfn, :size, :events, :run, :lumi)""" % self.sqlIgnoreError
        self.insert['fileforfileset'] = """
            insert into wmbs_fileset_files (file, fileset) 
                values ((select id from wmbs_file_details where lfn = :file),
                (select id from wmbs_fileset where name = :fileset))
        """
        self.insert['newworkflow'] = """
            insert into wmbs_workflow (spec, owner)
                values (:spec, :owner)
        """
        self.insert['newsubscription'] = """
            insert into wmbs_subscription (fileset, workflow, type, parentage, last_update) 
                values ((select id from wmbs_fileset where name =:fileset),
                (select id from wmbs_workflow where spec = :spec and owner = :owner), :type, 
                :parentage, :timestamp)
        """
        self.insert['newlocation'] = """
            insert %s into wmbs_location (se_name) values (:location) % self.sqlIgnore
        """
        self.insert['putfileatlocation'] = """
            insert into wmbs_file_location (file, location) 
                values ((select id from wmbs_file_details where lfn = :file),
                (select id from wmbs_location where se_name = :location))"""
        #TODO: The following should be bulk inserts
        self.insert['acquirefiles'] = """insert into wmbs_sub_files_acquired 
                (subscription, file) values (:subscription, :file)"""
        self.insert['failfiles'] = """insert into wmbs_sub_files_failed 
                (subscription, file) values (:subscription, :file)"""
        self.insert['completefiles'] = """insert into wmbs_sub_files_complete 
                (subscription, file) values (:subscription, :file)"""
        self.insert['parentage'] = """insert into wmbs_file_parent (child, parent) values
        ( (select id from wmbs_file_details where lfn = :child), 
          (select id from wmbs_file_details where lfn = :parent)
        )"""
                        
        self.select['allfileset'] = """
            select * from wmbs_fileset order by last_update, name
            """
        self.select['fileset'] = """
            select id, open, last_update from wmbs_fileset 
            where name = :fileset
            """
        self.select['filesetparentage'] = """
            select name, open, last_update from wmbs_fileset where id in 
            (select parent from wmbs_fileset_parent where child = (
            select id from wmbs_fileset where name = :fileset            
            ))
            """
        self.select['filesinfileset'] = """
            select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_fileset_files where 
                fileset = (select id from wmbs_fileset where name = :fileset))
            """
        self.select['filesetexists'] = """select count(*) from wmbs_fileset 
            where name = :name"""
        self.select['subscriptions'] = """
            select id, fileset, workflow, type, parentage from wmbs_subscription
        """
        self.select['subscriptionsoftype'] = """
            select id, fileset, workflow from wmbs_subscription where type=:type
        """
        self.select['subscriptionsforfileset'] = """
            select id, workflow, type from wmbs_subscription 
                where fileset=(select id from wmbs_fileset where name =:fileset)
        """
        self.select['subscriptionsforfilesetoftype'] = """
            select id, workflow from wmbs_subscription where type=:type 
                and fileset=(select id from wmbs_fileset where name =:fileset)
        """
        self.select['idofsubscription'] = """
            select id from wmbs_subscription where type=:type 
                and workflow = (select id from wmbs_workflow where spec = :spec and owner = :owner)
                and fileset=(select id from wmbs_fileset where name =:fileset)
        """
        self.select['idofsubscription'] = """
            select id from wmbs_subscription where type=:type 
                and workflow = (select id from wmbs_workflow where spec = :spec and owner = :owner)
                and fileset=(select id from wmbs_fileset where name =:fileset)
        """
        self.select['subscriptionsforworkflowoftype'] = """
            select id from wmbs_subscription where type=:type and workflow=:name
        """
        self.select['filesetforsubscription'] = """
            select name from wmbs_fileset 
                where id = (
                    select fileset from wmbs_subscription where id = :subscription
                    )
        """ 
        self.select['filesets'] = """select name, open, last_update 
                                    from wmbs_fileset"""
        self.select['alllocations'] = "select id, se_name from wmbs_location"
        self.select['filesatlocation'] = """
            select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_file_location where location =
                    (select id from wmbs_location where se_name = :location))
        """
        self.select['whereisfile'] = """select se_name from wmbs_location 
                where id in (select location from wmbs_file_location 
                    where file in (select id from wmbs_file_details where lfn=:file))
        """
#        self.select['nextfiles'] = """select id, lfn, size, events, run, lumi from wmbs_file_details where id IN (
#        select file from wmbs_fileset_files where
#            fileset = (select fileset from wmbs_subscription where id=:subscription)
#            and file not in 
#                (select file from wmbs_sub_files_acquired where subscription=:subscription)
#            and file not in 
#                (select file from wmbs_sub_files_failed where subscription=:subscription)
#            and file not in 
#                (select file from wmbs_sub_files_complete where subscription=:subscription)
#                )
#        """
        self.select['nextfiles'] = """select lfn from wmbs_file_details
                where id in (select file from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and file not in 
                (select file from wmbs_sub_files_acquired where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_failed where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_complete where subscription=:subscription)
                )
        """
        self.select['filedetails'] = """select id, lfn, size, events, run, lumi
                 from wmbs_file_details where lfn = :file"""
        self.select['filedetailsfromid'] = """select id, lfn, size, events, run, lumi
                 from wmbs_file_details where id = :file"""
        self.select['fileparentage'] = """select lfn from wmbs_file_details where id IN (
        select parent from wmbs_file_parent where child = :file)"""
        self.select['activefiles'] = """
        select file from wmbs_sub_files_acquired 
            where subscription=:subscription
            and file not in 
                (select file from wmbs_sub_files_failed where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_complete where subscription=:subscription)
        """
        self.select['failedfiles'] = """
            select file from wmbs_sub_files_failed where subscription=:subscription
        """
        self.select['completedfiles'] = """
            select file from wmbs_sub_files_complete 
            where subscription=:subscription"""
        self.select['newfilessincedateforset'] = """
            select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_fileset_files where 
                fileset = (select id from wmbs_fileset where name = :fileset)
                and insert_time > :timestamp)"""
        self.select['filesindaterangeforset'] = """
            select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_fileset_files where 
                fileset = (select id from wmbs_fileset where name = :fileset)
                and insert_time > :oldstamp
                and insert_time < :newstamp)"""
        self.select['workflowexists'] = """select count(*) from wmbs_workflow
            where spec = :spec and owner = :owner"""
        self.select['workflows'] = """select spec, owner from wmbs_workflow"""
        self.select['workflowid'] = """select id from wmbs_workflow
            where spec = :spec and owner = :owner"""
        
        self.delete['fileset'] = "delete from wmbs_fileset where name = :fileset"
        self.delete['workflow'] = "delete from wmbs_workflow where spec = :spec and owner = :owner"    
               
    def createFilesetTable(self):
        """
        Fileset.
        Group a set of files
        Map DBS dataset path/block path to an index for fast
        referencing.
        This is all we need to know about a dataset. Ever.

        name - DBS Dataset Path/Block Path
        id   - auto index that links a set of files to the fileset
        """
        self.processData(self.create['wmbs_fileset'])
    
    def createFilesetParentTable(self):
        """ 
        Express parent/child relations ship between files
        constrain parent != child? 
        """
        self.processData(self.create['wmbs_fileset_parent'])
    
    def createFileTable(self):
        """
        File.
        Create a unique index representing a file, give it a status
        and partition the DB on that status so that only active
        files are in use
        
        id       - auto index used to link the file to other details
        fileset  - index of fileset to which file belongs
        status   - active or inactive
        """
        self.processData(self.create['wmbs_fileset_files'])
        
    def createFileParentTable(self):
        """ 
        Express parent/child relations ship between files
        constrain parent != child? 
        """
        self.processData(self.create['wmbs_file_parent'])
        
    def createFileDetailsTable(self): 
        """ 
        wmbs_file_details.
        All the slow stuff that isnt needed for cross referencing
         
        file      - index of file entry in wmbs_file
        lfn       - The LFN of the file
        size      - Size of file in bytes
        events    - events in file 
        run       - run number of file
        lumi      - lumi section of file
        """
        self.processData(self.create['wmbs_file_details'])
        
    def createLocationTable(self): 
        """     
        wmbs_location.
        Entry representing a unique SE name and an index
        to cross reference it
        
        id      - Auto index of the SE
        se_name - DBS/PhEDEx SE Name
        """
        self.processData(self.create['wmbs_location'])

    def createFileLocationsTable(self): 
        """
        wmbs_file_location
        Track replicas of the file at storage elements
        Note that this isnt a global replica tracker, just for
        the WM tool that is processing the file within its cloud
        of sites.
        
        file      - index of file entity
        location  - index of location entity where file exists
        """
        self.processData(self.create['wmbs_file_location'])
    
    def createSubscriptionsTable(self):
        """ 
        Subscription
        Entity representing a processing subscription to a set of files
         
        A subscription consists of a fileset, tracks the files
        associated to that fileset through several state tables
        and groups files into logical job definitions that
        get turned into physical processing jobs for a PA/CS.
        """
        self.processData(self.create['wmbs_subscription'])
               
    def createSubscriptionAcquiredFilesTable(self):
        """
        Status of a file having been acquired by a subscription
        If the file is in a fileset but does not appear in this 
        table, then it is new and can be picked up
        """
        self.processData(self.create['wmbs_sub_files_acquired'])
        
    def createSubscriptionFailedFilesTable(self): 
        """
        A file that has failed as part of a subscription 
        processing task.
        """
        self.processData(self.create['wmbs_sub_files_failed'])
        
    def createSubscriptionCompletedFilesTable(self):
        """
        A table of files that have been successfully 
        processed by the subscription
        """
        self.processData(self.create['wmbs_sub_files_complete'])
   
    def createJobTable(self):       
        """
        Job Entity.
        Logical subset of files belonging to a subscription
        that will be used to define some physical processing
        job. 
        
        May want to add some job status information, either in this
        table or associated tables.
        """
        self.processData(self.create['wmbs_job'])
        
    def createJobAssociationTable(self):     
        """
        Create the table to link a file to a job instance
        """
        self.processData(self.create['wmbs_job_assoc'])
    
    def createWorkflowTable(self):     
        """
        Create the table to define workflows
        """
        self.processData(self.create['wmbs_workflow'])
        
    def createWMBS(self):
        self.createFilesetTable()
        self.createFilesetParentTable()
        self.createFileTable()
        self.createFileParentTable()
        self.createFileDetailsTable()
        self.createLocationTable()
        self.createFileLocationsTable()
        self.createWorkflowTable()
        self.createSubscriptionsTable()
        self.createSubscriptionAcquiredFilesTable()
        self.createSubscriptionFailedFilesTable()
        self.createSubscriptionCompletedFilesTable()
        self.createJobTable()
        self.createJobAssociationTable()

    def insertFileset(self, fileset = None, is_open=True, parents = None, conn = None, transaction = False):
        """
        insert a fileset to WMBS
        
        TODO: Currently both child and parent set to same open status
        """
        #binds = None
        #if type(fileset) == type('string'):
        if not parents:
            parents = []
        
        binds = {'fileset':fileset, 'timestamp':self.timestamp(), 'is_open':is_open}
        # multiple datasets not currently supported
#        elif isinstance(fileset, list):
#            #we have a list!
#            binds = []
#            for f in fileset:
#                binds.append({'fileset':f})
        self.processData(self.insert['fileset'], binds, 
                         conn = conn, transaction = transaction)
        
        for parent in parents:
#            binds = {'fileset':parent, 'timestamp':self.timestamp(), 'is_open':parents_open}
#            self.processData(self.insert['fileset'], binds, 
#                         conn = conn, transaction = transaction)
            binds = {'child' : fileset, 'parent' : parent}
            self.processData(self.insert['fileset_parent'], binds, 
                         conn = conn, transaction = transaction)
    
    
    
    def showAllFilesets(self, conn = None, transaction = False):
        """
        List all the filesets in WMBS
        """
        return self.processData(self.select['allfileset'], 
                                conn = conn, transaction = transaction)
        
    def getFileset(self, fileset, conn = None, transaction = False):
        """
        list fileset parents of given sub
        """
        return self.processData(self.select['fileset'], 
                                {'fileset' : fileset},
                                conn = conn, transaction = transaction)[0]
        
    def getFilesetParents(self, fileset, conn = None, transaction = False):
        """
        list fileset parents of given sub
        """
        return self.processData(self.select['filesetparentage'], 
                                {'fileset' : fileset},
                                conn = conn, transaction = transaction)

    def filesetExists(self, name = None, conn = None, transaction = False):
        binds = {'name': name}
        return self.processData(self.select['filesetexists'], binds, 
                                conn = conn, transaction = transaction)   
     
    def deleteFileset(self, fileset = None, conn = None, transaction = False):
        """
        delete a fileset from WMBS
        """
        binds = {'fileset':fileset}
        self.processData(self.delete['fileset'], binds, 
                         conn = conn, transaction = transaction)                               

    def insertFiles(self, files=None, size=0, events=0, run=0, lumi=0, 
                    conn = None, transaction = False):
        """
        Add a new file to WMBS
        """ 
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'size': size, 
                     'events': events, 
                     'run': run, 
                     'lumi':lumi,
                     'timestamp':self.timestamp()}  
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, run and lumi
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'size': f[1], 
                              'events': f[2], 
                              'run': f[3], 
                              'lumi':f[4],
                              'timestamp':self.timestamp()})
        self.processData(self.insert['newfile'], binds, 
                            conn = conn, transaction = transaction)


    def insertFilesForFileset(self, files=None, size=0, events=0, run=0, lumi=0,
                              fileset=None, conn = None, transaction = False):
        """
        Add a (set of) file(s) to a fileset
        """ 
        self.logger.debug ("inserting %s for fileset %s" % (files, fileset))
        self.insertFiles(files, size, events, run, lumi, conn, transaction)
        binds = {}
        #Now add the files into the mapping table
        if type(files) == type('string'):            
            binds = {'file': files, 'fileset':fileset, 'timestamp' : self.timestamp()}
        elif type(files) == type([]):
            binds = []
            for f in files:
                binds.append({'file': f[0], 'fileset':fileset, 'timestamp' : self.timestamp()})
        # Replace with some bulk operation
        self.processData(self.insert['fileforfileset'], binds, 
                        conn = conn, transaction = transaction)

            
    def showFilesInFileset(self, fileset = None, 
                           conn = None, transaction = False):
        """
        List the files in a fileset
        """
        binds = {'fileset' : fileset}
        return self.processData(self.select['filesinfileset'], 
                                binds, conn = conn, transaction = transaction)
                                
    def newWorkflow(self, spec=None, owner=None, 
                           conn = None, transaction = False):
        """
        Create a workflow ready for subscriptions
        """
        binds = {'spec':spec, 'owner':owner}
        return self.processData(self.insert['newworkflow'], binds, 
                             conn = conn, transaction = transaction)
        
    def workflowExists(self, spec=None, owner=None, 
                           conn = None, transaction = False):
        """
        Check if a workflow exists
        """
        binds = {'spec':spec, 'owner':owner}
        return self.processData(self.select['workflowexists'], binds, 
                             conn = conn, transaction = transaction)
        
    def showAllWorkflows(self, conn = None, transaction = False):
        """
        list all workflows
        """
        return self.processData(self.select['workflows'], {}, 
                             conn = conn, transaction = transaction)
        
    def workflowId(self, spec, owner, conn = None, transaction = False):
        """
        get a workflow id
        """
        binds = {'spec':spec, 'owner':owner}
        return self.processData(self.select['workflowid'], binds, 
                             conn = conn, transaction = transaction)
        
        
    def deleteWorkflow(self, spec=None, owner=None, 
                           conn = None, transaction = False):
        """
        Delete a workflow
        """
        binds = {'spec':spec, 'owner':owner}
        return self.processData(self.delete['workflow'], binds, 
                             conn = conn, transaction = transaction)  
                                               
            
    def newSubscription(self, fileset, spec = None, owner = None, subtype='Processing',
                        parentage = 0, conn = None, transaction = False):
        """
        Create a new subscription on a fileset
        """
        if type(fileset) == type('string'):
            binds = {'fileset': fileset, 'spec': spec, 'owner': owner, 'type': subtype, 'parentage' : parentage,
                     'timestamp':self.timestamp()}
            self.processData(self.insert['newsubscription'], binds, 
                             conn = conn, transaction = transaction)
        elif type(fileset) == type([]):
            binds = []
            for f in fileset:
                binds.append({'fileset': f, 'spec': spec, 'owner': owner, 'type': subtype, 'parentage' : parentage,
                               'timestamp':self.timestamp()})
            self.processData(self.insert['newsubscription'], binds, 
                             conn = conn, transaction = transaction) 
            
    def subscriptionID(self, fileset = None, spec = None, owner = None,
                        subtype='Processing', 
                        conn = None, transaction = False):
        binds = {'fileset': fileset, 'spec': spec, 'owner': owner, 'type': subtype}
        return self.processData(self.select['idofsubscription'], binds, 
                             conn = conn, transaction = transaction)
            
    def subscriptionsForFileset(self, fileset = None, subtype=None, 
                                conn = None, transaction = False):
        """
        List all subscriptions for a fileset
        """
        binds = {'fileset' : fileset}
        
        if subtype:
            binds['type'] = subtype
            subs = self.processData(self.select['subscriptionsforfilesetoftype'],
                                 binds, conn = conn, transaction = transaction)
            return self.processData(self.select['subscriptionsforfilesetoftype'],
                                 binds, conn = conn, transaction = transaction)
        
        return self.processData(self.select['subscriptionsforfileset'], 
                                binds, conn = conn, transaction = transaction)

	def subscriptionsForWorkflow(self):
        #TODO: return all subscriptions using the workflow
            pass 
            
    def listSubscriptionsOfType(self, subtype=None, 
                                conn = None, transaction = False):
        """
        List all subscriptions of a certain type
        """
        if subtype:
            binds = {'type':subtype}
            return self.processData(self.select['subscriptionsoftype'], 
                               binds, conn = conn, transaction = transaction)


    def getSubscription(self, workflow, type, conn = None, 
                        transaction = False):
        """
        Get sub for given type and workflow
        """
        binds = {'name' : workflow, 'type' : type}
        # assume only 1 result possible here
        return self.processData(self.select['subscriptionsforworkflowoftype'],
                                binds, conn = conn, transaction = transaction)[0][0]


    def filesForSubscription(self, subscription=0, 
                               conn = None, transaction = False):
        """
        List all files in a subscription
        """
        if subscription > 0:
            binds = {'subscription':subscription}
            fileset = self.processData(self.select['filesetforsubscription'], 
                               binds, conn = conn, transaction = transaction)
            fs = fileset[0][0] #''
#            for f in fileset:
#                #fs = str(f.fetchone()[0])
                
            return self.showFilesInFileset(fileset=fs)
        
        
    def listFileSets(self, only_open=False,
                                conn=None, transaction=False):
        """
        return active fileSets and their properties
        """
        results = []
        filesets = self.processData(self.select['filesets'], 
                                conn = conn, transaction = transaction)
        for fileset in filesets:
            is_open = bool(fileset[1])
            if only_open and not is_open:
                continue
            results.append({'fileset':fileset[0], 'is_open':is_open, 'last_updated':fileset[2]}) 
        return results
    
            
    def addNewLocation(self, sename = None, conn = None, transaction = False):
        """
        Tell WMBS about a new SE
        """
        if not isinstance(sename, list): 
            sename = [sename]
        binds = []
        for s in sename:
            binds.append({'location':s})
        return self.processData(self.insert['newlocation'], 
                                binds, conn = conn, transaction = transaction)
    
    def listAllLocations(self, conn = None, transaction = False):
        """
        list all locations known to a WMBS
        """
        return self.processData(self.select['alllocations'], 
                                conn = conn, transaction = transaction)
    
    def listFilesAtLocation(self, sename, conn = None, transaction = False):
        """
        list all the files at a location
        """
        if not isinstance(sename, list): 
            sename = [sename]
        binds = []
        for s in sename:
            binds.append({'location':s})
        return self.processData(self.select['filesatlocation'], 
                                binds, conn = conn, transaction = transaction)
                                
    def putFileAtLocation(self, file = None, sename=None, 
                          conn = None, transaction = False):
        """
        Associate a (list of) file with a (set of) SE
        """
        if not isinstance(file, list): 
            file = [file]
        if not isinstance(sename, list): 
            sename = [sename]
        binds = []
        for f in file:
            for s in sename:
                binds.append({'file':f, 'location':s})
        return self.processData(self.insert['putfileatlocation'], 
                                binds, conn = conn, transaction = transaction)
    
    def fileDetails(self, lfns, conn=None, transaction=None):
        """
        get deftails for file
        """
        if not isinstance(lfns, list): 
            lfns = [lfns]
        binds = []
#        for f in file:
#            binds.append({'file':lfn})
#        
#        results = self.processData(self.select['filedetails'], 
#                                binds, conn = conn, transaction = transaction)
        results = []
        for lfn in lfns:
            try:
                lfn = int(lfn)
                results.extend(self.processData(self.select['filedetailsfromid'], 
                                    {'file':lfn}, conn = conn, transaction = transaction))
            except ValueError:
                results.extend(self.processData(self.select['filedetails'], 
                                    {'file':lfn}, conn = conn, transaction = transaction))
        if not results:
            raise RuntimeError, "File not found"
        
        for result in results:
            if not result:
                raise RuntimeError, "File not found"
            locations = self.locateFile(result[1], conn = conn, transaction = transaction)
            #result.append(self.locateFile(result[1], conn = conn, transaction = transaction))
            result.append([x[0] for x in locations])
    
        return results
    
    def locateFile(self, file, conn = None, transaction = False):
        """
        Where is a file located?
        """
        if not isinstance(file, list): 
            file = [file]
        binds = []
        for f in file:
            binds.append({'file':f})
        return self.processData(self.select['whereisfile'], 
                                binds, conn = conn, transaction = transaction)
    
    def listAvailableFiles(self, subscription=None, 
                           conn = None, transaction = False):
        """
        List the available files for a subscription
        """
        
        results = []
        
        if not isinstance(subscription, int):
            self.logger.exception("listAvailableFiles requires a single id number for subscription")
            raise "non-integer subscription id given"

        files = self.processData(self.select['nextfiles'], 
                                    {'subscription':subscription},
                                    conn = None, transaction = False)
        
#        for file in files:
#            f = {'id' : file[0],
#                 'lfn' : file[1],
#                 'size': file[2], 
#                 'events' : file[3], 
#                 'run' : file[4],
#                 'lumi':  file[5],
#                 'parents' : []}
#            f['locations'] = self.locateFile(f['lfn'],
#                                conn = None, transaction = False)
#            results.append(f)
#            
#        # get parentage levels 
#        num_parents = 1
#        if num_parents:
#            for file in results:
#                file = self.parentsForFile(file, num_parents,
#                                          conn = None, transaction = False)
        

        return files    
        #return results
        
    def parentsForFile(self, file, conn = None, transaction = False):
        """ 
        get parents for given file
        """
        
        
        lfns = self.processData(self.select['fileparentage'], 
                                {'file':file},
                                conn = None, transaction = False)
        return [x[0] for x in lfns]
        
        
    def acquireNewFiles(self, subscription=0, files=None, 
                        conn = None, transaction = False):
        """
        Acquire new files for a subscription
        """
        if 0 < subscription:
            binds = []
            if not isinstance(files, list): 
                files = [files]
            for f in files:
                binds.append({'subscription':subscription, 
                              'file':f})                
            return self.processData(self.insert['acquirefiles'], 
                                        binds)
            
        else:
            self.logger.exception("acquireNewFiles requires a single id number for subscription")
            raise "non-integer subscription id given"

    def failFiles(self, subscription=0, files=None, 
                  conn = None, transaction = False):
        """
        Fail files for a subscription
        """
        if 0 < subscription:
            binds = []
            if not isinstance(files, list): 
                files = [files]
            for f in files:
                binds.append({'subscription':subscription, 'file':f})                
            return self.processData(self.insert['failfiles'], binds)
            
        else:
            self.logger.exception("failFiles requires a single id number for subscription")
            raise "non-integer subscription id given"
        
    def completeFiles(self, subscription=0, files=None, 
                      conn = None, transaction = False):
        """
        Complete files for a subscription
        """
        if 0 < subscription:
            binds = []
            if not isinstance(files, list): 
                files = [files]
            for f in files:
                binds.append({'subscription':subscription, 'file':f})                
            return self.processData(self.insert['completefiles'], binds)
            
        else:
            self.logger.exception("completeFiles requires a single id number for subscription")
            raise "non-integer subscription id given"        

    def newFilesSinceDate(self, fileset = None, timestamp=0, 
                          conn = None, transaction = False):
        """
        List new files availabel for a subscription since a timestamp
        """        
        binds = {'fileset':fileset, 'timestamp':timestamp}
        return self.processData(self.select['newfilessincedateforset'], binds)

    def filesInDateRange(self, fileset = None, oldstamp=0, newstamp=0, 
                         conn = None, transaction = False):
        """
        List files added for a subscription in a date range
        """                
        binds = {'fileset':fileset, 'oldstamp':oldstamp, 'newstamp':newstamp}
        return self.processData(self.select['filesindaterangeforset'], binds)

    def listAcquiredFiles(self, subscription=None, 
                          conn = None, transaction = False):
        """
        List all acquired files for a subscription
        """
        if 0 < subscription:
            return self.processData(self.select['activefiles'], 
                                    {'subscription':subscription})
        else:
            self.logger.exception("listActiveFiles requires a single id number for subscription")
            raise "non-integer subscription id given"    
    
    def listFailedFiles(self, subscription=None, 
                        conn = None, transaction = False):
        """
        List all failed files for a subscription
        """
        if 0 < subscription:
            return self.processData(self.select['failedfiles'], 
                                    {'subscription':subscription})
        else:
            self.logger.exception("listFailedFiles requires a single id number for subscription")
            raise "non-integer subscription id given"
    
    def listCompletedFiles(self, subscription=None, 
                           conn = None, transaction = False):
        """
        List all completed files for a subscription
        """
        if 0 < subscription:
            return self.processData(self.select['completedfiles'], 
                                    {'subscription':subscription})
        else:
            self.logger.exception("listFailedFiles requires a single id number for subscription")
            raise "non-integer subscription id given"

###
#
#    Compound API calls - run in one transaction
#
###

    def addNewFilesetToLocation(self, file=None, fileset=None, sename=None, 
                                conn = None, transaction = False):
        """
        Create a fileset, put some files in it, locate the files
        """
        if not conn: 
            conn = self.engine.connect()
        if not transaction: 
            trans = conn.begin()
        try:
            self.insertFileset(fileset=fileset, 
                                    conn = conn, transaction=trans)
            self.insertFilesForFileset(files=file, fileset=fileset, 
                                    conn = conn, transaction=trans)
            self.putFileAtLocation(file, sename, 
                                    conn = conn, transaction=trans)
            if not transaction: 
                trans.commit()
        except Exception, e:
            if not transaction: 
                trans.rollback()
            raise e
        
    def addNewFileToLocation(self, file=None, fileset=None, sename=None, 
                             conn = None, transaction = False):
        """
        Add some new files, put them at a location
        """
        if not conn: 
            conn = self.engine.connect()
        if not transaction: 
            trans = conn.begin()
        try:
            self.insertFilesForFileset(files=file, fileset=fileset)
            self.putFileAtLocation(file, sename)
            if not transaction: 
                trans.commit()
        except Exception, e:
            if not transaction: 
                trans.rollback()
            raise e


    def addNewFileToNewLocation(self, fileDetails=None, fileset=None,
                                conn = None, transaction = False):
        """
        Note that the use of New actually means that the file or 
        location may be new to the db, but not necessarilly.
        """
        if not conn: 
            conn = self.engine.connect()
        if not transaction: 
            trans = conn.begin()
        
        lfn = fileDetails[0]
        id = fileDetails[1]
        size = fileDetails[2]
        events = fileDetails[3]
        run = fileDetails[4]
        lumi = fileDetails[5]
        locations = fileDetails[6]
        parents = fileDetails[7]
            
        try:
            self.insertFiles(lfn, size, events, run, lumi,
                             conn = conn, transaction = transaction)
            self.insertFilesForFileset(files=lfn, fileset=fileset, conn=conn, transaction=trans)
            self.setFileParentage(lfn, parents, conn=conn, transaction=trans)
            try:
                self.addNewLocation(locations, conn=conn, transaction=trans)
            except IntegrityError:
                pass
            self.putFileAtLocation(lfn, locations, conn=conn, transaction=trans)
            if not transaction: 
                trans.commit()
        except Exception:
            if not transaction: 
                trans.rollback()
            raise


    def setFileParentage(self, file, parents, conn = None, transaction = False):
        """
        set parentage for the given file
        """
        if not conn: 
            conn = self.engine.connect()
        if not transaction: 
            trans = conn.begin()   
        binds = []
        for p in parents:
            binds.append({'child':file, 'parent':p})
        self.processData(self.insert['parentage'], binds)



    









