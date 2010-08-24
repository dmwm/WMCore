#!/usr/bin/env python
"""
_WMBSMySQL_

MySQL Compatibility layer for WMBS


"""

__revision__ = "$Id: MySQL.py,v 1.10 2008/06/16 16:06:11 metson Exp $"
__version__ = "$Revision: 1.10 $"

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
        print "THIS CLASS IS DEPRECATED!!"
        
        DBInterface.__init__(self, logger, engine)
        
        # What history tables do we need?
        # What statistics/monitoring is needed?

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
        self.select['filesetparentage'] = """
            select name, open, last_update from wmbs_fileset where id in 
            (select parent from wmbs_fileset_parent where child = (
            select id from wmbs_fileset where name = :fileset            
            ))
            """
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
        self.select['workflows'] = """select spec, owner from wmbs_workflow"""
        self.select['workflowid'] = """select id from wmbs_workflow
            where spec = :spec and owner = :owner"""
        
               
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
                          
    def workflowId(self, spec, owner, conn = None, transaction = False):
        """
        get a workflow id
        """
        binds = {'spec':spec, 'owner':owner}
        return self.processData(self.select['workflowid'], binds, 
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
            fs = fileset[0][0]                 
            return self.showFilesInFileset(fileset=fs)
        
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
        return files    
        
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



    









