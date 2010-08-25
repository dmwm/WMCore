#!/bin/env python


from WMCore.BossLite.MySQL.Create import Create as MySQLCreate
from WMCore.Database.DBCreator import DBCreator

import threading
import logging



class Create(MySQLCreate):
    """
    Create implementation for creating BossLite files in SQLite

    """

    def __init__(self, logger = None, dbi = None):
        """
        Create all tables

        """


        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi

        DBCreator.__init__(self, myThread.logger, myThread.dbi)

        self.requiredTables = ["01bl_task",
                               "02bl_job",
                               "03bl_runningjob",
                               "04jt_group"]

        self.create["01bl_task"] = \
"""CREATE TABLE bl_task
  (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 VARCHAR(255),
    dataset              VARCHAR(255),
    start_dir            TEXT,
    output_dir           TEXT,
    global_sandbox       TEXT,
    cfg_name             TEXT,
    server_name          TEXT,
    job_type             TEXT,
    user_proxy           TEXT,
    outfile_basename     TEXT,
    common_requirements  TEXT,
    UNIQUE               (name)
  );
"""

        self.create["02bl_job"] = \
"""CREATE TABLE bl_job
  (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id           INT(11) NOT NULL,
    job_id            INT(11) NOT NULL,
    name              VARCHAR(255),
    executable        TEXT,
    events            TEXT,
    arguments         TEXT,
    stdin             TEXT,
    stdout            TEXT,
    stderr            TEXT,
    input_files       TEXT,
    output_files      TEXT,
    dls_destination   TEXT,
    submission_number INT(11)    default 0,
    closed            CHAR   default 'N',
    UNIQUE            (job_id, task_id),
    FOREIGN KEY       (task_id) REFERENCES bl_task(id) ON DELETE CASCADE
  );
"""
                                
        self.create["03bl_runningjob"] = \
"""CREATE TABLE bl_runningjob
(
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id                  INT(11) NOT NULL,
    task_id                 INT(11) NOT NULL, 
    submission              INT(11) NOT NULL,
    state                   VARCHAR(255),
    scheduler               TEXT,
    service                 TEXT,
    sched_attr              TEXT,
    scheduler_id            VARCHAR(255),
    scheduler_parent_id     VARCHAR(255),
    status_scheduler        VARCHAR(255),
    status                  VARCHAR(255),
    status_reason           TEXT,
    destination             TEXT, 
    creation_timestamp      TIMESTAMP,
    lb_timestamp            TIMESTAMP,
    submission_time         TIMESTAMP,
    scheduled_at_site       TIMESTAMP,
    start_time              TIMESTAMP,
    stop_time               TIMESTAMP,
    stageout_time           TIMESTAMP,
    getoutput_time          TIMESTAMP,
    output_request_time     TIMESTAMP,
    output_enqueue_time     TIMESTAMP,
    getoutput_retry         INT(11),
    output_dir              TEXT,
    storage                 TEXT,
    lfn                     TEXT,
    application_return_code INT(11),
    wrapper_return_code     INT(11),
    process_status          TEXT         DEFAULT 'created',
    closed                  CHAR default 'N',
    UNIQUE                  (submission, job_id, task_id),
    FOREIGN KEY             (job_id)  REFERENCES bl_job(job_id) ON DELETE CASCADE,
    FOREIGN KEY             (task_id) REFERENCES bl_task(id)    ON DELETE CASCADE
  );
"""

        self.create["04jt_group"] = \
"""CREATE TABLE jt_group 
  (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id INT(11)   DEFAULT NULL,
  task_id  INT(11) NOT NULL,
  job_id   INT(11) NOT NULL,
  UNIQUE   (task_id,job_id),
  FOREIGN KEY(job_id)  REFERENCES bl_job(job_id) ON DELETE CASCADE,
  FOREIGN KEY(task_id) REFERENCES bl_task(id)    ON DELETE CASCADE
  );
"""

        return
