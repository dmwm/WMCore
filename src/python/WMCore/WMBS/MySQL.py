#!/usr/bin/env python
"""
_WMBSMySQL_

MySQL Compatibility layer for WMBS


"""

__revision__ = "$Id: MySQL.py,v 1.6 2008/05/02 13:49:43 metson Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBCore import DBInterface


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
    
    def __init__(self, logger, engine):
        DBInterface.__init__(self, logger, engine)
        self.create['wmbs_fileset'] = """CREATE TABLE wmbs_fileset (
                id int(11) NOT NULL AUTO_INCREMENT,
                name varchar(255) NOT NULL,  
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,  
                PRIMARY KEY (id), UNIQUE (name))"""
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
                type    ENUM("merge", "processing"),
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY(id),
                FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
                    ON DELETE CASCADE),
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
            insert into wmbs_fileset (name) values (:fileset)"""
        self.insert['newfile'] = """
            insert into wmbs_file_details (lfn, size, events, run, lumi) 
                values (:lfn, :size, :events, :run, :lumi)"""
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
            insert into wmbs_subscription (fileset, workflow, type) 
                values ((select id from wmbs_fileset where name =:fileset), 
                (select id from wmbs_workflow where spec = :spec and owner = :owner), :type)
        """
        self.insert['newlocation'] = """
            insert into wmbs_location (se_name) values (:location)
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
                        
        self.select['allfileset'] = """
            select * from wmbs_fileset order by last_update, name
            """
        self.select['filesinfileset'] = """
            select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_fileset_files where 
                fileset = (select id from wmbs_fileset where name = :fileset))
            """
        self.select['filesetexists'] = """select count (*) from wmbs_fileset 
            where name = :name"""
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
        self.select['filesetforsubscription'] = """
            select name from wmbs_fileset 
                where id = (
                    select fileset from wmbs_subscription where id = :subscription
                    )
        """ 
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
        self.select['nextfiles'] = """select file from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and file not in 
                (select file from wmbs_sub_files_acquired where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_failed where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_complete where subscription=:subscription)
        """
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
        self.select['workflowexists'] = """select count (*) from wmbs_workflow
            where spec = :spec and owner = :owner"""
            
               
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
        
    def insertFileset(self, fileset = None, conn = None, transaction = False):
        """
        insert a (list of) fileset(s) to WMBS
        """
        binds = None
        if type(fileset) == type('string'):
            binds = {'fileset':fileset}
        elif isinstance(fileset, list):
            #we have a list!
            binds = []
            for f in fileset:
                binds.append({'fileset':f})
        self.processData(self.insert['fileset'], binds, 
                         conn = conn, transaction = transaction)
    
    def showAllFilesets(self, conn = None, transaction = False):
        """
        List all the filesets in WMBS
        """
        return self.processData(self.select['allfileset'], 
                                conn = conn, transaction = transaction)
            
    def filesetExists(self, name = None, conn = None, transaction = False):
        binds = {'name': name}
        return self.processData(self.select['filesetexists'], binds, 
                                conn = conn, transaction = transaction)
    
    def insertFiles(self, files=None, size=0, events=0, run=0, lumi=0, 
                    conn = None, transaction = False):
        """
        Add a new file to WMBS
        """ 
        self.logger.debug ("inserting %s " % (files))
        self.logger.debug (type(files))
        binds = {}
        if type(files) == type('string'):
            binds = {'lfn': files, 
                     'size': size, 
                     'events': events, 
                     'run': run, 
                     'lumi':lumi}  
        elif type(files) == type([]):
        # files is a list of tuples containing lfn, size, events, run and lumi
            binds = []
            for f in files:
                binds.append({'lfn': f[0], 
                              'size': f[1], 
                              'events': f[2], 
                              'run': f[3], 
                              'lumi':f[4]})
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
            binds = {'file': files, 'fileset':fileset}
        elif type(files) == type([]):
            binds = []
            for f in files:
                binds.append({'file': f[0], 'fileset':fileset})
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
                  
    def newSubscription(self, fileset = None, spec = None, owner = None,
                        subtype='processing', 
                        conn = None, transaction = False):
        """
        Create a new subscription on a fileset
        """
        if type(fileset) == type('string'):
            binds = {'fileset': fileset, 'spec': spec, 'owner': owner, 'type': subtype}
            self.processData(self.insert['newsubscription'], binds, 
                             conn = conn, transaction = transaction)
        elif type(fileset) == type([]):
            binds = []
            for f in fileset:
                binds.append({'fileset': f, 'spec': spec, 'owner': owner, 'type': subtype})
            self.processData(self.insert['newsubscription'], binds, 
                             conn = conn, transaction = transaction) 
            
    def subscriptionID(self, fileset = None, spec = None, owner = None,
                        subtype='processing', 
                        conn = None, transaction = False):
        binds = {'fileset': fileset, 'spec': spec, 'owner': owner, 'type': subtype}
        return self.processData(self.select['idofsubscription'], binds, 
                             conn = conn, transaction = transaction)
        
            
    def subscriptionsForFileset(self, fileset = None, subtype=None, 
                                conn = None, transaction = False):
        """
        List all subscriptions for a fileset
        """
        if fileset:
            if subtype:
                binds = {'fileset' : fileset, 'type':subtype}
                return self.processData(
                                self.select['subscriptionsforfilesetoftype'], 
                                binds, conn = conn, transaction = transaction)
            else:
                binds = {'fileset' : fileset}
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

    def filesForSubscription(self, subscription=0, 
                               conn = None, transaction = False):
        """
        List all files in a subscription
        """
        if subscription > 0:
            binds = {'subscription':subscription}
            fileset = self.processData(self.select['filesetforsubscription'], 
                               binds, conn = conn, transaction = transaction)
            fs = ''
            for f in fileset:
                fs = str(f.fetchone()[0])
            return self.showFilesInFileset(fileset=fs)
            
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
        if isinstance(subscription, int):
            return self.processData(self.select['nextfiles'], 
                                    {'subscription':subscription})
        else:
            self.logger.exception("listAvailableFiles requires a single id number for subscription")
            raise "non-integer subscription id given"
        
    def acquireNewFiles(self, subscription=0, files=None, 
                        conn = None, transaction = False):
        """
        Acquire new files for a subscription
        """
        if isinstance(subscription, int):
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
        if isinstance(subscription, int):
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
        if isinstance(subscription, int):
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
        if isinstance(subscription, int):
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
        if isinstance(subscription, int):
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
        if isinstance(subscription, int):
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








    









