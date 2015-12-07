#!/usr/bin/env python

"""
DBS3 Migration Help Class
This migrates a dataset from local to global
Input: Dictionary from RequestQuery
"""
import os
import traceback
from dbs.apis.dbsClient import DbsApi
from threading import Timer


class MigrationToGlobal:
    
    def __init__(self):
        
        # Initialize dbs API
        dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSMigrate/'
        #dbsUrl = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSMigrate/'
        self.dbsApi = DbsApi(url = dbsUrl)
        
        # Timing variable
        self.isOver = False
        
        # Timeout of the script
        self.time_out = 600 #10 min
        
        # Migration Requests
        self.migrationRequests = {}
        
    def updateRequest(self, over):
        """
        This updates the migration requests status
        First calls DBS3 for the migration status. If it is the last loop (over = True),
        remove submitted requests and handle the uncomplete requests
        """
        for task in self.migrationRequests.keys():
            # Loop over all the submitted migration requests
            if self.migrationRequests[task]['status'] == 'submitted':
                request_status = self.dbsApi.statusMigration(migration_rqst_id = self.migrationRequests[task]['id'])
                status = request_status[0]['migration_status']
                if status == 2: # Migration completed
                    self.migrationRequests[task]['status'] = 'successful'
                    print 'Migration to global succeed: '+ self.migrationRequests[task]['dataset']
                elif status == 9: # Migration failed
                    self.migrationRequests[task]['status'] = 'migration failed'
                    print 'Migration to global fail: '+ self.migrationRequests[task]['dataset']
                elif status == 3 and over == True: # Migration failed, no more retries due to script timeout
                    self.removeRequest(self.migrationRequests[task])
                    self.migrationRequests[task]['status'] = 'migration failed'
                    print 'Migration to global fail: '+ self.migrationRequests[task]['dataset']
                elif status == 0 and over == True: # Migration is still pending, remove it
                    self.removeRequest(self.migrationRequests[task])
                    self.migrationRequests[task]['status'] = 'migration not processed'
                    print 'Migration to global not proccesed: '+ self.migrationRequests[task]['dataset']
                elif status == 1 and over == True: # Migration in progress...
                    self.migrationRequests[task]['status'] = 'still processing'
                    print 'DBS3 is still processing Migration of %s' % self.migrationRequests[task]['id']
            
    def removeRequest(self, migration):
        """
        Remove a migration request
        This only works if the status is from DBS is 0 (pending) or 3 (fail)
        """
        try:
            toDelete = {'migration_rqst_id': migration['id']}
            self.dbsApi.removeMigration(toDelete)
        except Exception as ex:
            print 'There was something wrong when migrating %s' % migration['dataset']
            print 'Exception: '+str(ex)+'/n'
            print 'Traceback: '+str(traceback.format_exc())
    
    def migrationPending(self):
        """
        Loop over migration requests and returns True if there is pending work
        """
        pending = False
        for task in self.migrationRequests.keys():
            if self.migrationRequests[task]['status'] == 'submitted':
                pending = True
        return pending
    
    def createReport(self):
        """
        Create a report of the migrations attempted
        """
        report = {}
        print "%20s %15s %25s %150s" %('Savannah Ticket','Migration id','Migration Status','Dataset') 
        print "%20s %15s %25s %150s" %('-'*20,'-'*15,'-'*25,'-'*150)
        for task in self.migrationRequests.keys():
            #report[task] = [self.migrationRequests[task]['status'],self.migrationRequests[task]['dataset']]
            print "%20s %15s %25s %150s" %(task, self.migrationRequests[task]['id'],
                                       self.migrationRequests[task]['status'],
                                       self.migrationRequests[task]['dataset'])
    
    """
    Timing helping 
    """
    def setOver(self):
        print 'Timer is over'
        self.isOver = True
    def over(self):
        return self.isOver
    
    def Migrates(self, reportSet):
        """
        This handle the entire migration process
        Reads from the report made by RequestQuery
        Retorn the summary of the migration requests
        """        
        
        # Create Migration objects from RequestQuery results
        for request in reportSet:
            migration = {}
            migration['dataset'] = request['InputDataset']
            migration['status'] = 'pending'
            migrationObj = dict(migration_url=request['localUrl'],
                                migration_input=request['InputDataset'])
            migration['object'] = migrationObj
            self.migrationRequests[request['task']]=migration
            
        # Submit Migration request
        # Creates a Dictionary with the migration request and its status 
        for task in self.migrationRequests.keys():
            
            try:
                request = self.migrationRequests[task]
                # Submitting migration request 
                #print 'Submitting: '+ request['dataset']
                print 'Migrate: from url '+request['object']['migration_url']+' dataset: '+request['object']['migration_input']
                migration_request = self.dbsApi.submitMigration(request['object'])
                migration_request_id = migration_request['migration_details']['migration_request_id']
                # Update internal info
                self.migrationRequests[task]['status'] = 'submitted'
                self.migrationRequests[task]['id'] = migration_request_id
                # Report submitting
                print "Migration submitted: Request %s" % migration_request_id
            
            except Exception as ex:
                print 'Exception: '+str(ex)+'/n'
                print 'Traceback: '+str(traceback.format_exc())
                
                self.migrationRequests[task]['status'] = 'submitting fail'
                print "Migration request was not submitted" 
                continue
        
        # Check if the migration was successful, 
        # Start Timer
        t = Timer(self.time_out, self.setOver)
        print "Timer started, timeout = %s seconds" % self.time_out
        t.start()
        
        workPending = True
        runs = 100000000
        print "Querying migrations status..."
        while runs > 0 and not self.over() and workPending:
            self.updateRequest(False)
            workPending = self.migrationPending()
        
        # Stop Timer if not already done
        t.cancel()
            
        # If the work is done before timeout
        if not workPending:
            print "All migration requests are done"
            self.createReport()
            return
        
        # If the timeout is reached before all the work is done
        self.updateRequest(True)
        self.createReport()
        return
