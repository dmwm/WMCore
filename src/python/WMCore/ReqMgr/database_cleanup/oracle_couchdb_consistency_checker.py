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

This script should also correct any inconsistencies (correcting values
in CouchDB and later removing dispensable CouchDB request fields).
It will have to be run repeatedly like Oracle/CouchDB database level
consistency check (which is done here at the beginning too).

Run with -c (commit) to perform updates in CouchDB.

All checks and updates correspond to progress recoreded on
https://github.com/dmwm/WMCore/issues/4388

"""

couch_url = "https://cmsweb.cern.ch/couchdb"
couchdb_name = "reqmgr_workload_cache"
 

import sys
import re

import cx_Oracle

from WMCore.Database.CMSCouch import CouchServer, Database, Document


# do not consider consistency of these, to be removed from Couch later
COUCH_TO_IGNORE = (
                   "ReqMgrGroupID",
                   "ReqMgrRequestID",
                   "ReqMgrRequestBasePriority",
                   "ReqMgrRequestorID",
                   "Workflowspec",
                   "RequestSizeEvents",
                   "RequestEventSize",
                   )

# columns in the Oracle SQL statement,
# the order and number of items here have to agree with select columns ...
MAPPING=(
         {"oracle": "REQUEST_NAME", "couch": "RequestName"},
         {"oracle": "TYPE_NAME", "couch": "RequestType"},
         {"oracle": "STATUS_NAME", "couch": "RequestStatus"},
         {"oracle": "REQUEST_PRIORITY", "couch": "RequestPriority"},
         {"oracle": "REQUESTOR_GROUP_ID", "couch": "ReqMgrGroupID"},
         {"oracle": "WORKFLOW", "couch": "RequestWorkflow"},
         {"oracle": "REQUEST_EVENT_SIZE", "couch": "RequestEventSize"},
         {"oracle": "REQUEST_SIZE_FILES", "couch": "RequestSizeFiles"},
         {"oracle": "PREP_ID", "couch": "PrepID"},
         {"oracle": "REQUEST_NUM_EVENTS", "couch": "RequestNumEvents"},
         {"oracle": "GROUP_NAME", "couch": "Group"},
         {"oracle": "REQUESTOR_HN_NAME", "couch": "Requestor"},
         {"oracle": "REQUESTOR_DN_NAME", "couch": "RequestorDN"},
         
         # team
         # applies only to requests which reached assignment status
         # WARNING:
         # by including teams, not all requests will be returned in
         # the query to compare/update the above data fields
         # {"oracle": "TEAM_NAME", "couch": "Team"},
         
         # reqmgr_input_dataset
         # WARNING:
         # only some requests have InputDataset assigned, so not
         # all requests are returned
         #{"oracle": "DATASET_NAME", "couch": "InputDataset"},
         #{"oracle": "DATASET_TYPE", "couch": "InputDatasetTypes"},
         
         # reqmgr_output_dataset
         #{"oracle": "DATASET_NAME", "couch": "OutputDatasets"},
         #{"oracle": "SIZE_PER_EVENT", "couch": "SizePerEvent"},
         
         # reqmgr_software reqmgr_software_dependency
         {"oracle": "SOFTWARE_NAME", "couch": "CMSSWVersion"},
         {"oracle": "SCRAM_ARCH", "couch": "ScramArch"},         
        )

ORACLE_FIELDS = [item["oracle"] for item in MAPPING]
COUCH_FIELDS = [item["couch"] for item in MAPPING]


SQL=("select "
     "reqmgr_request.REQUEST_NAME, "
     "reqmgr_request_type.TYPE_NAME, "
     "reqmgr_request_status.STATUS_NAME, "
     "reqmgr_request.REQUEST_PRIORITY, "
     "reqmgr_request.REQUESTOR_GROUP_ID, "
     "reqmgr_request.WORKFLOW, " 
     "reqmgr_request.REQUEST_EVENT_SIZE, "
     "reqmgr_request.REQUEST_SIZE_FILES, "
     "reqmgr_request.PREP_ID, "
     "reqmgr_request.REQUEST_NUM_EVENTS, "
     "reqmgr_group.GROUP_NAME, "
     "reqmgr_requestor.REQUESTOR_HN_NAME, "
     "reqmgr_requestor.REQUESTOR_DN_NAME, "
     
     # team
     #"reqmgr_teams.TEAM_NAME "
     
     # reqmgr_input_dataset
     #"reqmgr_input_dataset.DATASET_NAME, "
     #"reqmgr_input_dataset.DATASET_TYPE "
     
     # reqmgr_output_dataset
     #"reqmgr_output_dataset.DATASET_NAME, "
     #"reqmgr_output_dataset.SIZE_PER_EVENT "
     
     # reqmgr_software reqmgr_software_dependency
     "reqmgr_software.SOFTWARE_NAME, "
     "reqmgr_software.SCRAM_ARCH "
     
     "from reqmgr_request, reqmgr_request_type, reqmgr_request_status, "
     "reqmgr_group, reqmgr_group_association, reqmgr_requestor, "
     
     # team
     #"reqmgr_teams, reqmgr_assignment "
     
     # reqmgr_input_dataset
     # "reqmgr_input_dataset "
     
     # reqmgr_output_dataset
     # "reqmgr_output_dataset "
     
     # reqmgr_software reqmgr_software_dependency
     "reqmgr_software, reqmgr_software_dependency "
     
     "where reqmgr_request_type.TYPE_ID=reqmgr_request.REQUEST_TYPE "
     "and reqmgr_request_status.STATUS_ID=reqmgr_request.REQUEST_STATUS "
     "and reqmgr_group.GROUP_ID=reqmgr_group_association.GROUP_ID "
     "and reqmgr_group_association.ASSOCIATION_ID=reqmgr_request.REQUESTOR_GROUP_ID "
     "and reqmgr_request.REQUESTOR_GROUP_ID=reqmgr_group_association.ASSOCIATION_ID "
     "and reqmgr_group_association.REQUESTOR_ID=reqmgr_requestor.REQUESTOR_ID "
     # team
     #"and reqmgr_request.REQUEST_ID=reqmgr_assignment.REQUEST_ID "
     #"and reqmgr_assignment.TEAM_ID=reqmgr_teams.TEAM_ID "
     
     # reqmgr_input_dataset
     #"and reqmgr_request.REQUEST_ID=reqmgr_input_dataset.REQUEST_ID"
     
     # reqmgr_output_dataset
     #"and reqmgr_request.REQUEST_ID=reqmgr_output_dataset.REQUEST_ID"
     
     # reqmgr_software reqmgr_software_dependency
     "and reqmgr_request.REQUEST_ID=reqmgr_software_dependency.REQUEST_ID "
     "and reqmgr_software_dependency.SOFTWARE_ID=reqmgr_software.SOFTWARE_ID"
     
# limit the number of rows returned by oracle
#     "and rownum < 5"
    )
# END OF SQL

# ; semi-colon at the end nicely yields
# cx_Oracle.DatabaseError: ORA-00911: invalid character
# without any other description



def oracle_query(oradb, sql_cmd):
    print "Retrieving data from Oracle ..."
    ora_cursor = cx_Oracle.Cursor(oradb)
    print "# SQL: '%s'" % sql_cmd
    ora_cursor.prepare(sql_cmd)
    ora_cursor.execute(sql_cmd)
    return ora_cursor


def get_oracle_row_count(oradb, table_name):
    cmd = "select * from %s" % table_name
    ora_cursor = oracle_query(oradb, cmd)
    # accessing just .rowcount integer was returning 0 on non-empty result
    num_requests = len(ora_cursor.fetchall())
    ora_cursor.close()
    return num_requests


def get_oracle_data(oradb):
    ora_cursor = oracle_query(oradb, SQL)
    # result is a list of tuples, make a dict from it
    request = {}
    for row in ora_cursor.fetchall():
        for k, v in zip(ORACLE_FIELDS, row):
             request[k] = v
        yield request
    ora_cursor.close()
    

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
    
    print "Creating CouchDB database connection ..."
    couchdb = Database(couchdb_name, couch_url)
    print "Creating Oracle database connection ..."
    oradb = cx_Oracle.Connection(tns)
    
    num_couch_requests = get_couchdb_row_count(couchdb)
    print "Total CouchDB request documents in ReqMgr: %s" % num_couch_requests
    num_oracle_requests = get_oracle_row_count(oradb, "reqmgr_request")                                                
    print "Total Oracle requests entries in ReqMgr: %s" % num_oracle_requests
        
    if num_couch_requests != num_oracle_requests:
        print "Number of requests in Oracle, CouchDB don't agree, fix that first."
        sys.exit(1)
    else:
        print "Database cross-check (Oracle request names vs CouchDB): DONE, THE SAME."
        
    
    def get_couch_value(couch_req, mapping):
        try:
            c = couch_req[mapping["couch"]]
            couch_missing = False
        except KeyError:            
            # comparison will not happen due to missing flag, the value
            # will be stored in couch
            c = "N/A"
            couch_missing = False
        return str(c), couch_missing
    
    
    def check_oracle_worflow_value(oracle_value, mapping, req_name):
        # check Oracle WORKFLOW value
        if mapping["oracle"] == "WORKFLOW":
            # https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/linacre_2011A_442p2_DataReprocessingMuOnia_111119_005717/spec
            from_wf_url_req_name = oracle_value.rsplit('/', 2)[-2]
            if req_name != from_wf_url_req_name:
                print "Workflow URL mismatch: %s" % o
                sys.exit(1) 


    counter = 0
    for oracle_req in get_oracle_data(oradb):
        req_name = oracle_req["REQUEST_NAME"]

        # FILTER
        # check only requests injected approx. after last deployment (a lot of
        # stuff should have already been fixed in ReqMgr)
        # _13041._*$ (ending of request name with date/time)
        #if not re.match(".*_1304[0-3][0-9]_.*$", req_name): # all April 2013
        #    continue
        
        counter += 1
        print "\n\n%s (%s)" % (req_name, counter)        
                
        couch_req = couchdb.document(req_name)
        couch_fields_to_correct = {}
        for mapping in MAPPING:
            if mapping["couch"] in COUCH_TO_IGNORE:
                continue
            o = str(oracle_req[mapping["oracle"]])
            c, couch_missing = get_couch_value(couch_req, mapping)
            check_oracle_worflow_value(o, mapping, req_name)
            
            # compare oracle and couch values
            # don't update value in couch if it exists and is non-empty
            if (couch_missing or o != c) and c in ('None', '0', '', "N/A"):
                print "%s %s != %s" % (mapping, o, c)
                # correct couch request by oracle value
                couch_fields_to_correct[mapping["couch"]] = o
        
        if couch_fields_to_correct:
            print "Couch corrected fields:"
            print couch_fields_to_correct
            if sys.argv[-1] == "-c":
                couchdb.updateDocument(req_name, "ReqMgr", "updaterequest",
                                       fields=couch_fields_to_correct, useBody=True)
                print "Couch updated"
        else:
            print "OK"
        
        # fields that should be removed from couch
        """
        print "Couch fields to remove, values: ..."
        for removable in COUCH_TO_IGNORE:
            try:
                val = couch_req[removable]
            except KeyError:
                continue
            print "%s: %s: %s" % (req_name, removable, val)
        """
                
    # // for for oracle_req in ...
        

if __name__ == "__main__":
    main()
