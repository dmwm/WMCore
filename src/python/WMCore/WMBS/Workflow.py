#!/usr/bin/env python
"""
_Workflow_

A simple object representing a Workflow in WMBS.

A workflow has an owner (e.g. PA instance, CRAB server user) and
a specification. The specification describes how jobs should be
created and what the jobs are supposed to do. This description
is held external to WMBS, WMBS just stores a pointer (url) to
the specification file. A workflow can be used with many
filesets and subscriptions (e.g. repeating the same task on a
bunch of data).

workflow + fileset = subscription
"""

import logging

from WMCore.DataStructs.Workflow import Workflow as WMWorkflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.WMBSBase import WMBSBase


class Workflow(WMBSBase, WMWorkflow):
    """
    A simple object representing a Workflow in WMBS.

    A workflow has an owner (e.g. PA instance, CRAB server user) and
    a specification. The specification describes how jobs should be
    created and what the jobs are supposed to do. This description
    is held external to WMBS, WMBS just stores a pointer (url) to
    the specification file. A workflow can be used with many
    filesets and subscriptions (e.g. repeating the same task on a
    bunch of data).

    workflow + fileset = subscription
    """

    def __init__(self, spec=None, owner="unknown", dn="unknown",
                 group="unknown", owner_vogroup="DEFAULT",
                 owner_vorole="DEFAULT", name=None, task=None,
                 wfType=None, id=-1, alternativeFilesetClose=False,
                 priority=None):
        WMBSBase.__init__(self)
        WMWorkflow.__init__(self, spec=spec, owner=owner, dn=dn,
                            group=group, owner_vogroup=owner_vogroup,
                            owner_vorole=owner_vorole, name=name,
                            task=task, wfType=wfType, priority=priority)

        if self.dn == "unknown":
            self.dn = owner

        self.id = id
        self.alternativeFilesetClose = alternativeFilesetClose
        return

    def exists(self):
        """
        _exists_

        Determine whether or not a workflow exists with the given spec, owner
        and name.  Return the ID if the workflow exists, False otherwise.
        """
        action = self.daofactory(classname="Workflow.Exists")
        result = action.execute(spec=self.spec, owner=self.dn,
                                group_name=self.vogroup,
                                role_name=self.vorole,
                                name=self.name, task=self.task,
                                conn=self.getDBConn(),
                                transaction=self.existingTransaction())

        return result

    def insertUser(self):
        """
        _insertUser_

        Check if the user exists in the wmbs database and return the id to be added
        in the workflow entry. If the user does not exists then it gets created.
        """
        existingTransaction = self.beginTransaction()

        userfactory = self.daofactory(classname="Users.GetUserId")
        userid = userfactory.execute(dn=self.dn,
                                     group_name=self.vogroup,
                                     role_name=self.vorole,
                                     conn=self.getDBConn(),
                                     transaction=self.existingTransaction())
        if not userid:
            newuser = self.daofactory(classname="Users.New")
            userid = newuser.execute(dn=self.dn, hn=self.owner,
                                     owner=self.owner, group=self.group,
                                     group_name=self.vogroup,
                                     role_name=self.vorole,
                                     conn=self.getDBConn(),
                                     transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return userid

    def create(self):
        """
        _create_

        Write the workflow to the database.  If the workflow already exists in
        the database nothing will happen.
        """
        if self.exists() is not False:
            self.load()
            return

        userid = self.insertUser()

        existingTransaction = self.beginTransaction()
        action = self.daofactory(classname="Workflow.New")
        action.execute(spec=self.spec, owner=userid, name=self.name,
                       task=self.task, wfType=self.wfType,
                       alt_fs_close=self.alternativeFilesetClose,
                       priority=self.priority,
                       conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        self.id = self.exists()
        self.commitTransaction(existingTransaction)
        logging.info("Workflow id %d created for %s", self.id, self.name)
        return

    def delete(self):
        """
        _delete_

        Remove this workflow from the database.
        """
        action = self.daofactory(classname="Workflow.Delete")
        action.execute(id=self.id, conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        return

    def load(self):
        """
        _load_

        Load a workflow from WMBS.  One of the following must be provided:
          - The workflow ID
          - The workflow name and task
          - The workflow spec and owner and task
        """
        existingTransaction = self.beginTransaction()

        if self.id > 0:
            action = self.daofactory(classname="Workflow.LoadFromID")
            result = action.execute(workflow=self.id,
                                    conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
        elif self.name is not None:
            action = self.daofactory(classname="Workflow.LoadFromNameAndTask")
            result = action.execute(workflow=self.name, task=self.task,
                                    conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
        else:
            action = self.daofactory(classname="Workflow.LoadFromSpecOwner")
            result = action.execute(spec=self.spec, dn=self.dn,
                                    task=self.task, conn=self.getDBConn(),
                                    transaction=self.existingTransaction())

        self.id = result["id"]
        self.spec = result["spec"]
        self.name = result["name"]
        self.owner = result["owner"]
        self.dn = result["dn"]
        self.group = result["grp"]
        self.vorole = result["vogrp"]
        self.vogroup = result["vorole"]
        self.task = result["task"]
        self.wfType = result["type"]
        self.priority = result["priority"]

        action = self.daofactory(classname="Workflow.LoadOutput")
        results = action.execute(workflow=self.id, conn=self.getDBConn(),
                                 transaction=self.existingTransaction())

        self.outputMap = {}
        for outputID in results:
            for outputMap in results[outputID]:
                outputFileset = Fileset(id=outputMap["output_fileset"])
                if outputMap["merged_output_fileset"] is not None:
                    mergedOutputFileset = Fileset(id=outputMap["merged_output_fileset"])
                else:
                    mergedOutputFileset = None

                if outputID not in self.outputMap:
                    self.outputMap[outputID] = []

                self.outputMap[outputID].append({"output_fileset": outputFileset,
                                                 "merged_output_fileset": mergedOutputFileset})

        self.commitTransaction(existingTransaction)
        return

    def addOutput(self, outputIdentifier, outputFileset,
                  mergedOutputFileset=None):
        """
        _addOutput_

        Associate an output of this workflow with a particular fileset.
        """
        existingTransaction = self.beginTransaction()

        if self.id is False:
            self.create()

        if outputIdentifier not in self.outputMap:
            self.outputMap[outputIdentifier] = []

        self.outputMap[outputIdentifier].append({"output_fileset": outputFileset,
                                                 "merged_output_fileset": mergedOutputFileset})

        action = self.daofactory(classname="Workflow.InsertOutput")
        if mergedOutputFileset is not None:
            mergedFilesetID = mergedOutputFileset.id
        else:
            mergedFilesetID = None

        action.execute(workflowID=self.id, outputIdentifier=outputIdentifier,
                       filesetID=outputFileset.id, mergedFilesetID=mergedFilesetID,
                       conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def countWorkflowsBySpec(self):
        """
        _countWorkflowsBySpec_

        Count workflows that share our spec
        """

        existingTransaction = self.beginTransaction()
        action = self.daofactory(classname="Workflow.CountWorkflowBySpec")
        result = action.execute(spec=self.spec)
        self.commitTransaction(existingTransaction)

        return result
