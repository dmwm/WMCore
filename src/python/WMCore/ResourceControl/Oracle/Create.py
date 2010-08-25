#!/usr/bin/python

"""
_Create_

Class for creating Oracle specific schema for resource control.

"""

__revision__ = "$Id: Create.py,v 1.1 2009/10/05 20:03:01 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating Oracle specific schema for resource control.
    """
    
    
    
    def __init__(self, **params):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}

        tablespaceTable = ""
        tablespaceIndex = ""

        if params:
            if params.has_key("tablespace_table"):
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if params.has_key("tablespace_index"):
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        self.create['rc1_site'] = \
        """CREATE TABLE rc_site (
            site_index  INTEGER       NOT NULL,
            site_name   VARCHAR(255)  NOT NULL,
            se_name     VARCHAR(255)  NOT NULL,
            ce_name     VARCHAR(255),
            is_active   VARCHAR(5)   DEFAULT 'true'
            ) %s""" % tablespaceTable

        self.create["rc1_site_seq"] = \
          """CREATE SEQUENCE rc_site_seq
               START WITH 1
               INCREMENT BY 1
               NOMAXVALUE"""

        self.indexes["rc1_site_pk"] = \
          """ALTER TABLE rc_site ADD
               (CONSTRAINT rc_site_pk PRIMARY KEY (site_index) %s)""" % tablespaceIndex

        self.indexes["rc1_site_un"] = \
          """ALTER TABLE rc_site ADD
               (CONSTRAINT rc_site_un UNIQUE (site_name) %s)""" % tablespaceIndex

        self.constraints["rc1_site_ck"] = \
          """ALTER TABLE rc_site ADD
               (CONSTRAINT rc_site_ck CHECK(is_active IN ('true', 'false')))"""

        self.create["rc1_site_trg"] = \
          """CREATE TRIGGER rc_site_trg
               BEFORE INSERT ON rc_site
               FOR EACH ROW
                 BEGIN
                   SELECT rc_site_seq.nextval INTO :new.site_index FROM dual;
                 END;"""



        self.create['rc2_site_threshold'] = """
        CREATE TABLE rc_site_threshold(
            site_index      INTEGER      NOT NULL,
            threshold_name  VARCHAR(255) NOT NULL,
            threshold_value INTEGER      DEFAULT 0
            ) %s""" % tablespaceTable

        self.indexes["rc2_site_threshold_un"] = \
          """ALTER TABLE rc_site_threshold ADD
               (CONSTRAINT rc_site_threshold_un UNIQUE (threshold_name, site_index) %s)""" % tablespaceIndex

        self.constraints["rc2_site_threshold_fk"] = \
          """ALTER TABLE rc_site_threshold ADD
               (CONSTRAINT rc_site_threshold_fk FOREIGN KEY (site_index) REFERENCES rc_site(site_index) ON DELETE CASCADE)"""





        self.create['rc3_site_attr'] = """
        CREATE TABLE rc_site_attr(
            site_index INTEGER      NOT NULL,
            attr_name  VARCHAR(255) NOT NULL,
            attr_value VARCHAR(255) DEFAULT ''
            ) %s""" % tablespaceTable

        self.indexes["rc2_site_attr_un"] = \
          """ALTER TABLE rc_site_attr ADD
               (CONSTRAINT rc_site_attr_un UNIQUE (attr_name, site_index) %s)""" % tablespaceIndex

        self.constraints["rc2_site_attr_fk"] = \
          """ALTER TABLE rc_site_attr ADD
               (CONSTRAINT rc_site_attr_fk FOREIGN KEY (site_index) REFERENCES rc_site(site_index) ON DELETE CASCADE)"""

