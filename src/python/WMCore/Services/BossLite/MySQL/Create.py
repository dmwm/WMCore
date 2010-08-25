"""
_Create_

Base class for creating the BossLite database.
"""

__revision__ = "$Id: Create.py,v 1.1 2009/07/09 18:47:44 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from WMCore.JobStateMachine.ChangeState import Transitions

class Create(DBCreator):
    """
    This should create the BossLite schema; since they don't do it.

    """

    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi

        DBCreator.__init__(self, logger, dbi)

        self.requiredTables = ["01bl_task",
                               "02bl_job",
                               "03bl_runningjob",
                               "04jt_group"]

        self.create["01bl_task"] = \
"""CREATE TABLE bl_task
  (
    id INT auto_increment,
    name VARCHAR(255),
    dataset VARCHAR(255),
    start_dir TEXT,
    output_dir TEXT,
    global_sanbox TEXT,
    cfg_name TEXT,
    server_name TEXT,
    job_type TEXT,
    user_proxy TEXT,
    outfile_basename TEXT,
    common_requirements TEXT,
    primary key(id),
    unique(name)
  )
  ENGINE = InnoDB DEFAULT CHARSET=latin1;
"""

        self.create["02bl_job"] = \
                                """CREATE TABLE bl_job
                                  (
                                    id INT auto_increment,
                                    task_id INT NOT NULL,
                                    job_id INT NOT NULL,
                                    name VARCHAR(255),
                                    executable TEXT,
                                    arguments TEXT,
                                    stdin TEXT,
                                    stdout TEXT,
                                    stderr TEXT,
                                    input_files TEXT,
                                    output_files TEXT,
                                    dls_destination TEXT,
                                    submission_number INT default 0,
                                    closed CHAR default 'N',
                                    PRIMARY KEY(id),
                                    INDEX sub_n (submission_number),
                                    UNIQUE(job_id, task_id),
                                    FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
                                  )
                                ENGINE = InnoDB DEFAULT CHARSET=latin1;
                                """
                                
        self.create["03bl_runningjob"] = \
"""CREATE TABLE bl_runningjob
(
    id INT auto_increment,
    job_id INT NOT NULL,
    task_id INT NOT NULL, 
    submission INT NOT NULL,
    state VARCHAR(255),
    scheduler TEXT,
    service TEXT,
    sched_attr TEXT,
    scheduler_id VARCHAR(255),
    scheduler_parent_id VARCHAR(255),
    status_scheduler VARCHAR(255),
    status VARCHAR(255),
    status_reason TEXT,
    destination TEXT, 
    creation_timestamp TIMESTAMP,
    lb_timestamp TIMESTAMP,
    submission_time TIMESTAMP,
    scheduled_at_site TIMESTAMP,
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    stageout_time TIMESTAMP,
    getoutput_time TIMESTAMP,
    output_request_time TIMESTAMP,
    output_enqueue_time TIMESTAMP,
    getoutput_retry INT,
    output_dir TEXT,
    storage TEXT,
    lfn TEXT,
    application_return_code INT,
    wrapper_return_code INT,
    process_status enum('created', 'not_handled', 'handled', 'failed',
                        'output_requested','in_progress','output_retrieved',
			'processed') default 'created',
    closed CHAR default 'N',
    PRIMARY KEY(id),
    INDEX closed_ind (closed),
    INDEX procs_st (process_status),
    INDEX sts (status),
    UNIQUE(submission, job_id, task_id),
    FOREIGN KEY(job_id) references bl_job(job_id) ON DELETE CASCADE,
    FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
  )
  ENGINE = InnoDB DEFAULT CHARSET=latin1;
"""

        self.create["04jt_group"] = \
"""CREATE TABLE jt_group 
  (
  id int(11) NOT NULL auto_increment,
  group_id int(11) default NULL,
  task_id int(11) NOT NULL,
  job_id int(11) NOT NULL,
  PRIMARY KEY  (id),
  INDEX gr (group_id),
  UNIQUE KEY task_id (task_id,job_id),
  FOREIGN KEY(job_id) references bl_job(job_id) ON DELETE CASCADE,
  FOREIGN KEY(task_id) references bl_task(id) ON DELETE CASCADE
  )
  ENGINE = InnoDB DEFAULT CHARSET=latin1;
"""

        return

    def execute(self, conn = None, transaction = None):
        """
        _execute_

        Check to make sure that all required tables have been defined.  If
        everything is in place have the DBCreator make everything.
        """
        for requiredTable in self.requiredTables:
            if requiredTable not in self.create.keys():
                raise WMException("The table '%s' is not defined." % \
                                  requiredTable, "WMCORE-2")

        try:
            DBCreator.execute(self, conn, transaction)
            return True
        except Exception, e:
            print "ERROR: %s" % e
            return False
