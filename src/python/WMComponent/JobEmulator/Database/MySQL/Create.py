#!/usr/bin/python

"""
_Create_

Class for creating MySQL specific schema for job emulation.

"""

__revision__ = "$Id: Create.py,v 1.1 2009/02/27 22:30:02 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import logging
import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for job emulation.
    
    """

    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        msg = """
create jem_node table which contains information about worker nodes
"""
        logging.debug(msg)
        self.create['a'] = """
CREATE TABLE jem_node(
    host_id INT NOT NULL AUTO_INCREMENT,
    host_name VARCHAR(255) NOT NULL,
    number_jobs SMALLINT UNSIGNED,
    UNIQUE (host_name),
    PRIMARY KEY (host_id)
)
"""
        msg = """
create jem_job table which contains the jobs submitted to JobEmulator
"""
        logging.debug(msg)
        self.create['b'] = """
CREATE TABLE jem_job(
    job_id  VARCHAR(255) NOT NULL,
    job_type ENUM('Processing', 'Merge', 'CleanUp'),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status ENUM('new', 'finished', 'failed'),
    worker_node_id INT,
    job_spec VARCHAR(255),
    FOREIGN KEY (worker_node_id) REFERENCES jobEM_node_info (host_id),
    PRIMARY KEY (job_id)
)
"""
        msg = """
a tracker table to simulate tracker info when we oprate in-situ.
"""
        logging.debug(msg)
        self.create['c'] = """
CREATE TABLE jem_placebo_tracker_db(
    job_id  VARCHAR(255) NOT NULL,
    status ENUM('submitted', 'running', 'completed', 'failed')
) 
"""
