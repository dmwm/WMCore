#!/usr/bin/env python

"""
Compare Oracle ReqMgr tables to data stored in CouchDB database.

Needs to have credentials for accessing CMS web ready in
$X509_USER_CERT $X509_USER_KEY, or proxy stored in /tmp/x509up_u<ID>

Assumes ReqMgr's CouchDB database consistent with Oracle reqmgr_request
    table, so that both of these contains the same and mutually
    corresponding requests.
    
This tools checks consistency on the request level, that is data
fields in CouchDB request documents with data stored in Oracle.

This script should also correct any inconsistencies (e.g. removing
certain fields). It will have to be run repeatedly like Oracle/CouchDB
database level consistency check (which is done here at the beginning too).
Not finished - CouchDB ReqMgr CouchApp doesn't have yet update function
deployed on CMS web. Continue at TODO.


"""

couch_url = "https://cmsweb.cern.ch/couchdb"
couchdb_name = "reqmgr_workload_cache"
 

import sys
import cx_Oracle

from oracle_tables import reqmgr_oracle_tables_defition
from WMCore.Database.CMSCouch import CouchServer, Database, Document



def oracle_query(oradb, sql_cmd):
    print "Retrieving data from Oracle ..."
    ora_cursor = cx_Oracle.Cursor(oradb)
    print "# SQL %s" % sql_cmd
    ora_cursor.prepare(sql_cmd)
    ora_cursor.execute(sql_cmd)
    return ora_cursor


def get_oracle_row_count(oradb, table_name, table_def):
    cmd = "select %s from %s" % (", ".join(table_def), table_name)
    ora_cursor = oracle_query(oradb, cmd)
    # accessing just .rowcount integer was returning 0 on non-empty result
    num_requests = len(ora_cursor.fetchall())
    ora_cursor.close()
    return num_requests


def get_oracle_data(oradb, table_name, table_def):
    cmd = "select %s from %s" % (", ".join(table_def), table_name)
    ora_cursor = oracle_query(oradb, cmd)
    # result is a list of tuples, make a dict from it
    request = {}
    for row in ora_cursor.fetchall():
        for k, v in zip(table_def, row):
             request[k] = v
        yield request
    ora_cursor.close()
    

def get_oracle_data_all(oradb, table_name, table_def):
    """
    For small tables.
    
    """
    cmd = "select %s from %s" % (", ".join(table_def), table_name)
    ora_cursor = oracle_query(oradb, cmd)
    # result is a list of tuples, make a dict from it
    result = []
    for row in ora_cursor.fetchall():
        r = {}
        for k, v in zip(table_def, row):
             r[k] = v
        result.append(r)
    ora_cursor.close()
    return result
    

def get_couchdb_row_count(db):
    """
    Returns number of request documents excluding design documents.
    
    """
    print "Retrieving data from CouchDB ..."
    doc_count = 0 
    for row in db.allDocs()["rows"]:
        if row["id"].startswith("_design"): continue
        doc_count += 1
    return doc_count
    

def main():
    if len(sys.argv) < 2:
        print "Missing the connect Oracle TNS argument (user/password@server)."
        sys.exit(1)
    tns = sys.argv[1]
    
    # 1) reqmgr_request table
    reqmgr_request_table_def = reqmgr_oracle_tables_defition["reqmgr_request"]

    print "Creating CouchDB database connection ..."
    couchdb = Database(couchdb_name, couch_url)
    print "Creating Oracle database connection ..."
    oradb = cx_Oracle.Connection(tns)
    
    num_couch_requests = get_couchdb_row_count(couchdb)
    print "CouchDB request documents: %s" % num_couch_requests
    num_oracle_requests = get_oracle_row_count(oradb, "reqmgr_request",
                                               reqmgr_request_table_def) 
    print "Oracle requests entries: %s" % num_oracle_requests
        
    print "Database cross-check (Oracle request names vs CouchDB) ..."
    if num_couch_requests != num_oracle_requests:
        print "Number of requests in Oracle, CouchDB don't agree, fix that first."
        sys.exit(1)
    
    # in order to process data in reqmgr_request table, need to load
    # data from reqmgr_request_type table
    oracle_request_type_list = \
        get_oracle_data_all(oradb, "reqmgr_request_type",
                            reqmgr_oracle_tables_defition["reqmgr_request_type"])
    oracle_request_type = {}
    for req_type in oracle_request_type_list:
        oracle_request_type[req_type["TYPE_ID"]] = req_type["TYPE_NAME"]
    # and data from reqmgr_request_status table
    oracle_request_status_list = \
        get_oracle_data_all(oradb, "reqmgr_request_status",
                            reqmgr_oracle_tables_defition["reqmgr_request_status"])
    oracle_request_status = {}
    for req_status in oracle_request_status_list:
         oracle_request_status[req_status["STATUS_ID"]] = req_status["STATUS_NAME"]
         
    map_names = [{"oracle": "REQUEST_NAME", "couch": "RequestName"},
                 {"oracle": "REQUEST_TYPE", "couch": "RequestType"},
                 {"oracle": "REQUEST_STATUS", "couch": "RequestStatus"},
                 {"oracle": "REQUEST_PRIORITY", "couch": "RequestPriority"}]
    # fields exist in newer couchdb documents but may still be None/Null
    # RequestSizeEvents
    # RequestSizeFiles
    # PrepID
    # RequestNumEvents
    counter = 0
    for oracle_req in get_oracle_data(oradb, "reqmgr_request",
                                      reqmgr_request_table_def):
        req_name = oracle_req["REQUEST_NAME"]
        couch_req = couchdb.document(req_name)
        for mapping in map_names:
            if mapping["oracle"] == "REQUEST_TYPE":
                # it's just an index now, get oracle value
                o = oracle_request_type[oracle_req[mapping["oracle"]]]
            elif mapping["oracle"] == "REQUEST_STATUS":
                # it's just an index now, get oracle value
                o = oracle_request_status[oracle_req[mapping["oracle"]]]
            else:
                o = oracle_req[mapping["oracle"]]
            c = couch_req[mapping["couch"]]
            
            o = str(o)
            c = str(c)

            # compare oracle and couch values
            if o != c:
                print "%s %s %s != %s" % (req_name, mapping, o, c)
                # correct couch
                if mapping["oracle"] == "REQUEST_STATUS":
                    #print "Correcting in Couch: %s ..." % mapping["oracle"]     
                    fields = {mapping["couch"]: o} # oracle value
                    # TODO
                    # CONTINUE
                    
                    # not yet deployed on cms web ...
                    # couchdb.updateDocument(req_name, "ReqMgr", "updaterequest", fields=fields)
        
        # fields that should be removed from couch
        to_remove = ["ReqMgrGroupID", "RequestWorkflow", "ReqMgrRequestID",
                     "Workflowspec", "ReqMgrRequestBasePriority", "ReqMgrRequestorID"]
        print "Couch fields to remove, values: ..."
        for removable in to_remove:
            try:
                val = couch_req[removable]
            except KeyError:
                continue
            print "%s: %s: %s" % (req_name, removable, val)
                
        counter += 1
        if counter > 10:
            break
    # // for for oracle_req in ...
        
        

if __name__ == "__main__":
    main()