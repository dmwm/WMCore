#!/usr/bin/env python
"""
_ReqMgr.RequestDB.MySQL_

MySQL Compatibility layer for Request Manager DB
"""




import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.RequestManager.RequestDB.Settings.RequestTypes import TypesList
from WMCore.RequestManager.RequestDB.Settings.RequestStatus import StatusList

class Create(DBCreator):
    """

    Implementation of ReqMgr DB for MySQL

    """

    def __init__(self, logger=None, dbi=None, param=None):
        if dbi == None:
            myThread = threading.currentThread()
            dbi = myThread.dbi
            logger = myThread.logger
        DBCreator.__init__(self, logger, dbi)

        self.create = {}
        self.constraints = {}
        #  //
        # // Define create statements for each table
        #//
        #  //
        # // State/Type tables: enumerated type lists
        #//  Type will be the type of workflow: MC, CmsGen, Reco, Skim etc
        self.create['a_reqmgr_request_type'] = """
        CREATE TABLE reqmgr_request_type(
        type_id NUMBER(11) NOT NULL,
        type_name VARCHAR(255) NOT NULL,
        PRIMARY KEY(type_id), UNIQUE(type_name)
        )
        """
        self.create['a_reqmgr_request_type_seq'] = """
        CREATE SEQUENCE reqmgr_request_type_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['a_reqmgr_request_type_trg'] =  """
        CREATE TRIGGER reqmgr_request_type_trg
        BEFORE INSERT ON reqmgr_request_type
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_request_type_seq.nextval INTO :new.type_id FROM dual;
        END;"""
        #  //
        # // states for a request to be in
        #//  new, untested, tested, approved, failed, aborted, junk etc
        self.create['b_reqmgr_request_status'] = """
        CREATE TABLE reqmgr_request_status(
        status_id NUMBER(11) NOT NULL,
        status_name VARCHAR(255) NOT NULL,
        PRIMARY KEY(status_id), UNIQUE(status_name)
        )
        """
        self.create['b_reqmgr_request_status_seq'] = """
        CREATE SEQUENCE reqmgr_request_status_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['b_reqmgr_request_status_trg'] =  """
        CREATE TRIGGER reqmgr_request_status_trg
        BEFORE INSERT ON reqmgr_request_status
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_request_status_seq.nextval INTO :new.status_id FROM dual;
        END;"""
        #  //
        # // Group definitions
        #//  Which group to bill, also allows a base priority modifier
        #  //which will allow certain groups to be raised/lowered
        # // with respect to each other.
        #//
        self.create['c_reqmgr_group'] = """
        CREATE TABLE reqmgr_group(

        group_id NUMBER(11) NOT NULL,
        group_name VARCHAR(255) NOT NULL,
        group_base_priority NUMBER(11) DEFAULT 0,

        UNIQUE(group_name),
        PRIMARY KEY(group_id)

        )
        """
        self.create['c_reqmgr_group_seq'] = """
        CREATE SEQUENCE reqmgr_group_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['c_reqmgr_group_trg'] =  """
        CREATE TRIGGER reqmgr_group_trg
        BEFORE INSERT ON reqmgr_group
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_group_seq.nextval INTO :new.group_id FROM dual;
        END;"""
        #  //
        # // Request Owner details
        #//  Actual person who makes a request.
        #  //
        # //
        #//
        #  //
        # //
        #//  Can also be used to prioritise users if need be. (We may want
        #  //to use this to track users who make a lot of bad requests
        # // and lower their priorities...)
        #//
        self.create['d_reqmgr_requestor'] = """

        CREATE TABLE reqmgr_requestor (

        requestor_id NUMBER(11) NOT NULL,
        requestor_hn_name VARCHAR(255) NOT NULL,
        contact_email VARCHAR(255) NOT NULL,

        requestor_dn_name VARCHAR(255),
        requestor_base_priority NUMBER(11) DEFAULT 0,

        PRIMARY KEY(requestor_id),
        UNIQUE (requestor_hn_name)


        )
        """
        self.create['d_reqmgr_requestor_seq'] = """
        CREATE SEQUENCE reqmgr_requestor_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['d_reqmgr_requestor_trg'] =  """
        CREATE TRIGGER reqmgr_requestor_trg
        BEFORE INSERT ON reqmgr_requestor
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_requestor_seq.nextval INTO :new.requestor_id FROM dual;
        END;"""

        #  //
        # //  requestor/group association, allows users to belong
        #//   to multiple groups
        #  //
        # //
        #//
        self.create['e_reqmgr_group_association'] = """
        CREATE TABLE reqmgr_group_association (

        association_id NUMBER(11) NOT NULL,
        requestor_id NUMBER(11) NOT NULL,
        group_id NUMBER(11) NOT NULL,

        PRIMARY KEY(association_id),
        UNIQUE(requestor_id, group_id),
        FOREIGN KEY(requestor_id) references reqmgr_requestor(requestor_id)
          ON DELETE CASCADE,
        FOREIGN KEY(group_id) references reqmgr_group(group_id)

        )
        """
        self.create['e_reqmgr_group_association_seq'] = """
        CREATE SEQUENCE reqmgr_group_association_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['e_reqmgr_group_association_trg'] =  """
        CREATE TRIGGER reqmgr_group_association_trg
        BEFORE INSERT ON reqmgr_group_association
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_group_association_seq.nextval INTO :new.association_id FROM dual;
        END;"""
        #  //
        # // Production/Processing teams
        #//  Simple team name for now, could be expanded to include
        #  //contact details, ProdAgent URLs etc
        # //
        #//
        self.create['f_reqmgr_teams'] = """
        CREATE TABLE reqmgr_teams (

        team_id NUMBER(11) NOT NULL,
        team_name VARCHAR(255),

        UNIQUE(team_name),
        PRIMARY KEY(team_id)

        )
        """
        self.create['f_reqmgr_teams_seq'] = """
        CREATE SEQUENCE reqmgr_teams_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['f_reqmgr_teams_trg'] =  """
        CREATE TRIGGER reqmgr_teams_trg
        BEFORE INSERT ON reqmgr_teams
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_teams_seq.nextval INTO :new.team_id FROM dual;
        END;"""
        #  //
        # // Main request table

        #//
        #  // just basics for now, anything else we want here?
        # //  Could generate GUIDs as request name.
        #//
        #  // workflow will be an LFN like name for the workflow spec file
        # //
        #//  basic estimates of request size in events or files can be added
        self.create['g_reqmgr_request'] = """
        CREATE TABLE  reqmgr_request(

        request_id NUMBER(11) NOT NULL,
        request_name VARCHAR(255) NOT NULL,
        request_type NUMBER(11) NOT NULL,
        request_status NUMBER(11) NOT NULL,
        request_priority NUMBER(11) NOT NULL,
        requestor_group_id NUMBER(11) NOT NULL,
        workflow VARCHAR(255) NOT NULL,

        request_size_events NUMBER(11) DEFAULT 0,
        request_size_files NUMBER(11)  DEFAULT 0,

        UNIQUE(request_name),
        FOREIGN KEY(request_type) REFERENCES reqmgr_request_type(type_id),
        FOREIGN KEY(request_status) references
                    reqmgr_request_status(status_id),
        FOREIGN KEY(requestor_group_id) references
                    reqmgr_group_association(association_id),
        PRIMARY KEY(request_id)


        )
        """
        self.create['g_reqmgr_request_seq'] = """
        CREATE SEQUENCE reqmgr_request_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['g_reqmgr_request_trg'] =  """
        CREATE TRIGGER reqmgr_request_trg
        BEFORE INSERT ON reqmgr_request
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_request_seq.nextval INTO :new.request_id FROM dual;
        END;"""

        #  //
        # // Assignment of request to team(s)
        #//  Also includes priority modifier to
        #  //allow assignments to teams at different priorities
        # //
        #//
        self.create['h_reqmgr_assignment'] = """
        CREATE TABLE reqmgr_assignment (

        request_id NUMBER(11) NOT NULL,
        team_id NUMBER(11) NOT NULL,
        priority_modifier NUMBER(11) DEFAULT 0,

        UNIQUE( request_id, team_id),
        FOREIGN KEY (request_id) references
           reqmgr_request(request_id) ON DELETE CASCADE,
        FOREIGN KEY (team_id) references
           reqmgr_teams(team_id)

        )
        """
        #  //
        # // Request Attributes useful for scheduling and
        #//  prioritisation

        #  //
        # // Input datasets are needed for scheduling, need to track
        #//  which one is main input, any secondary input or pileup

        self.create['i_reqmgr_input_dataset'] = """
        CREATE TABLE reqmgr_input_dataset (

        dataset_id NUMBER(11) NOT NULL,
        request_id NUMBER(11) NOT NULL,
        dataset_name VARCHAR(255) NOT NULL,
        dataset_type VARCHAR(11),
        CONSTRAINT dataset_type_cons 
                CHECK (dataset_type IN ('source', 'secondary', 'pileup')),
        FOREIGN KEY(request_id) references
                reqmgr_request(request_id)
                  ON DELETE CASCADE,
        PRIMARY KEY(dataset_id)

        )
        """
        self.create['i_reqmgr_input_dataset_seq'] = """
        CREATE SEQUENCE reqmgr_input_dataset_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['i_reqmgr_input_dataset_trg'] =  """
        CREATE TRIGGER reqmgr_input_dataset_trg
        BEFORE INSERT ON reqmgr_input_dataset
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_input_dataset_seq.nextval INTO :new.dataset_id FROM dual;
        END;"""

        #  //
        # // Output datasets arent necessarily needed for scheduling
        #//  but we need some idea of what comes from each job
        #  // and tests will give us size per event estimates which will
        # //  be needed for storage planning
        #//
        self.create['j_reqmgr_output_dataset'] = """
        CREATE TABLE reqmgr_output_dataset (

        dataset_id NUMBER(11) NOT NULL,
        request_id NUMBER(11) NOT NULL,
        dataset_name VARCHAR(255) NOT NULL,
        size_per_event NUMBER(11),
        custodial_site VARCHAR(255),
        FOREIGN KEY(request_id) references
                reqmgr_request(request_id)
                  ON DELETE CASCADE,
        PRIMARY KEY(dataset_id)
        )
        """
        self.create['j_reqmgr_output_dataset_seq'] = """
        CREATE SEQUENCE reqmgr_output_dataset_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['j_reqmgr_output_dataset_trg'] =  """
        CREATE TRIGGER reqmgr_output_dataset_trg
        BEFORE INSERT ON reqmgr_output_dataset
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_output_dataset_seq.nextval INTO :new.dataset_id FROM dual;
        END;"""
        #  //
        # //  CMSSW version requirements are needed for planning
        #//   associate a set of SW versions with a request
        #  //
        # //  Since ReqMgr will have to create workflows, using
        #//   a release, this may be something we can use to show what
        #  // releases are available for production as well.
        # //
        #//
        self.create['k_reqmgr_software'] = """
        CREATE TABLE reqmgr_software (

        software_id NUMBER(11) NOT NULL,
        software_name VARCHAR(255) NOT NULL,

        UNIQUE(software_name),
        PRIMARY KEY(software_id)
        )
        """
        self.create['k_reqmgr_software_seq'] = """
        CREATE SEQUENCE reqmgr_software_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['k_reqmgr_software_trg'] =  """
        CREATE TRIGGER reqmgr_software_trg
        BEFORE INSERT ON reqmgr_software
        FOR EACH ROW
        BEGIN
        SELECT reqmgr_software_seq.nextval INTO :new.software_id FROM dual;
        END;"""
        #  //
        # // Association between SW versions and a request
        #//  (chained processing could be multiple versions)
        self.create['l_reqmgr_software_dependency'] = """
        CREATE TABLE reqmgr_software_dependency (

           request_id NUMBER(11) NOT NULL,
           software_id NUMBER(11) NOT NULL,

           UNIQUE(request_id, software_id),
           FOREIGN KEY(software_id) references
                reqmgr_software(software_id),

           FOREIGN KEY(request_id) references
                reqmgr_request(request_id)
                  ON DELETE CASCADE
        )
        """

        #  //
        # // Request status and assignment tables used for tracking
        #//  and monitoring progress of a request
        #  // Should be coarse grained wide interval update (Eg per alloc
        # //  in PM) enough to give a big picture. Finer details can come
        #//   from drilling down to the PM/PA if needed
        self.create['m_reqmgr_progress_update'] = """

        CREATE TABLE reqmgr_progress_update (

        request_id NUMBER(11) NOT NULL,
        update_time TIMESTAMP,

        events_written NUMBER(11) DEFAULT 0,
        events_merged NUMBER(11) DEFAULT 0,
        files_written NUMBER(11) DEFAULT 0,
        files_merged NUMBER(11) DEFAULT 0,
        associated_dataset VARCHAR(255) DEFAULT NULL,
        time_per_event NUMBER(11) DEFAULT 0,
        size_per_event NUMBER(11) DEFAULT 0,
        percent_success FLOAT(11) DEFAULT 0,
        percent_complete FLOAT(11) DEFAULT 0,

        FOREIGN KEY(request_id) references
            reqmgr_request(request_id)
               ON DELETE CASCADE
        )
        """
        #  //
        # //  Attach messages to a request, eg:
        #//   "Request is now 50% done"
        #  // "Request was aborted due to failures"
        # //  "Which utter truncheon gave us a broken cfg?"
        #//   etc. Some kind of verbose summary
        self.create['n_reqmgr_message'] = """
        CREATE TABLE reqmgr_message(

        request_id NUMBER(11) NOT NULL,
        update_time TIMESTAMP,
        message VARCHAR2(1000) NOT NULL,

        FOREIGN KEY(request_id) references
            reqmgr_request(request_id)
               ON DELETE CASCADE
        )
        """
        #  //
        # // track the assigned ProdMgr
        #//
        self.create['o_reqmgr_assigned_prodmgr'] = """
        CREATE TABLE reqmgr_assigned_prodmgr (

        request_id NUMBER(11) NOT NULL,
        prodmgr_id VARCHAR(255) NOT NULL,

        UNIQUE(request_id, prodmgr_id),
        FOREIGN KEY (request_id) references
                reqmgr_request(request_id)
                  ON DELETE CASCADE
        )
        """
        #  //
        # // can also track assigned prodagents
        #//  expect this to flesh out more later, when we add
        #  //details of all PA instances and resources to enable
        # // better scheduling decisions, For now will probably just
        #// serve as a link to the HTTPFrontend to provide detailed
        #  //monitoring.
        # //
        #//
        self.create['p_reqmgr_assigned_prodagent'] = """
        CREATE TABLE reqmgr_assigned_prodagent (

        request_id NUMBER(11) NOT NULL,
        prodagent_id VARCHAR(255) NOT NULL,

        UNIQUE(request_id, prodagent_id),
        FOREIGN KEY (request_id) references
                reqmgr_request(request_id)
                  ON DELETE CASCADE

        )
        """

        for typeName in TypesList:
            sql = "INSERT INTO reqmgr_request_type (type_name) VALUES ('%s')" % typeName
            self.inserts["reqtype" + typeName] = sql

        for status in StatusList:
            sql = "INSERT INTO reqmgr_request_status (status_name) VALUES('%s')" % status
            print sql
            self.inserts["reqstatus" + status] = sql
