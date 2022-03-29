#!/usr/bin/env python
"""
_DBSBufferFile_

A simple object representing a file in DBSBuffer.
"""
from WMComponent.DBS3Buffer.DBSBufferDataset import DBSBufferDataset

from WMCore.DataStructs.File import File as WMFile
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.WMBSBase import WMBSBase


class DBSBufferFile(WMBSBase, WMFile):

    def __init__(self, lfn=None, id=-1, size=None,
                 events=None, checksums=None, parents=None, locations=None,
                 status="NOTUPLOADED", inPhedex=0, workflowId=None, prep_id=None):

        checksums = checksums or {}
        WMBSBase.__init__(self)
        WMFile.__init__(self, lfn=lfn, size=size, events=events,
                        checksums=checksums, parents=parents, merged=True)
        self.setdefault("status", status)
        self.setdefault("in_phedex", inPhedex)
        self.setdefault("id", id)
        self.setdefault("workflowId", workflowId)

        # Parameters for the algorithm
        self.setdefault("appName", None)
        self.setdefault("appVer", None)
        self.setdefault("appFam", None)
        self.setdefault("psetHash", None)
        self.setdefault("configContent", None)
        self.setdefault("datasetPath", None)
        self.setdefault("processingVer", 0)
        self.setdefault("acquisitionEra", None)
        self.setdefault("validStatus", None)
        self.setdefault("globalTag", None)
        self.setdefault("datasetParent", None)
        self.setdefault("prep_id", None)

        if locations is None:
            self.setdefault("newlocations", set())
        else:
            self.setdefault("newlocations", self.makeset(locations))

        # The WMBS base class creates a DAO factory for WMBS, we'll need to
        # overwrite that so we can use the factory for DBSBuffer objects.
        self.daofactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                     logger=self.logger,
                                     dbinterface=self.dbi)

        return

    def exists(self):
        """
        _exists_

        Determine whether or not a file with this LFN exists inside the
        database.  Return the file's ID if it exists, False otherwise.
        """
        action = self.daofactory(classname="DBSBufferFiles.Exists")
        return action.execute(lfn=self["lfn"], conn=self.getDBConn(),
                              transaction=self.existingTransaction())

    def getStatus(self):
        """
        _getStatus_

        Retrieve the status of the file.  This can be one of the following:
          UPLOADED
          NOTUPLOADED
        """
        return self["status"]

    def getLocations(self):
        """
        _getLocations_

        Retrieve a list of locations where this file is stored.
        """
        return list(self["locations"])

    def getRuns(self):
        """
        _getRuns_

        Retrieve a list of WMCore.DataStructs.Run objects that represent which
        run/lumi sections this file contains.
        """
        return list(self["runs"])

    def load(self, parentage=0):
        """
        _load_

        The the file and all it's metadata from the database.  Either the LFN
        or the file's ID must be specified before this is called.
        """
        existingTransaction = self.beginTransaction()

        if self["id"] != -1:
            action = self.daofactory(classname="DBSBufferFiles.GetByID")
            result = action.execute(self["id"], conn=self.getDBConn(),
                                    transaction=self.existingTransaction())
        else:
            action = self.daofactory(classname="DBSBufferFiles.GetByLFN")
            result = action.execute(self["lfn"], conn=self.getDBConn(),
                                    transaction=self.existingTransaction())

        self.update(result)

        action = self.daofactory(classname='DBSBufferFiles.GetChecksum')
        result = action.execute(fileid=self['id'], conn=self.getDBConn(),
                                transaction=self.existingTransaction())
        self["checksums"] = result

        action = self.daofactory(classname="DBSBufferFiles.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn=self.getDBConn(),
                              transaction=self.existingTransaction())
        for r in runs:
            self.addRun(run=Run(r, *runs[r]))

        action = self.daofactory(classname="DBSBufferFiles.GetLocation")
        self["locations"] = action.execute(self["lfn"], conn=self.getDBConn(),
                                           transaction=self.existingTransaction())

        self["newlocations"].clear()
        self["parents"].clear()

        if parentage > 0:
            action = self.daofactory(classname="DBSBufferFiles.GetParents")
            lfns = action.execute(self["lfn"], conn=self.getDBConn(),
                                  transaction=self.existingTransaction())
            for lfn in lfns:
                parentFile = DBSBufferFile(lfn=lfn)
                parentFile.load(parentage=parentage - 1)
                self["parents"].add(parentFile)

        self.commitTransaction(existingTransaction)
        return

    def insertDatasetAlgo(self):
        """
        _insertDatasetAlgo_

        Insert the dataset and algorithm for this file into the DBS Buffer.
        """
        newAlgoAction = self.daofactory(classname="NewAlgo")
        assocAction = self.daofactory(classname="AlgoDatasetAssoc")

        existingTransaction = self.beginTransaction()

        newAlgoAction.execute(appName=self["appName"], appVer=self["appVer"],
                              appFam=self["appFam"], psetHash=self["psetHash"],
                              configContent=self["configContent"],
                              conn=self.getDBConn(),
                              transaction=True)

        dbsbufferDataset = DBSBufferDataset(self["datasetPath"],
                                            processingVer=self['processingVer'],
                                            acquisitionEra=self['acquisitionEra'],
                                            validStatus=self['validStatus'],
                                            globalTag=self.get('globalTag', None),
                                            parent=self['datasetParent'],
                                            prep_id=self['prep_id'])

        if dbsbufferDataset.exists():
            dbsbufferDataset.updateDataset()
        else:
            dbsbufferDataset.create()

        assocID = assocAction.execute(appName=self["appName"], appVer=self["appVer"],
                                      appFam=self["appFam"], psetHash=self["psetHash"],
                                      datasetPath=self["datasetPath"],
                                      conn=self.getDBConn(),
                                      transaction=True)

        self.commitTransaction(existingTransaction)
        return assocID

    def create(self):
        """
        _create_

        Insert this file and all it's metadata into the DBS Buffer.
        """

        if self.exists() != False:
            self.load()
            return

        existingTransaction = self.beginTransaction()
        assocID = self.insertDatasetAlgo()

        addAction = self.daofactory(classname="DBSBufferFiles.Add")
        addAction.execute(files=self["lfn"], size=self["size"],
                          events=self["events"],
                          datasetAlgo=assocID, status=self["status"],
                          workflowID=self["workflowId"],
                          conn=self.getDBConn(),
                          transaction=self.existingTransaction())

        if len(self["runs"]) > 0:
            lumiAction = self.daofactory(classname="DBSBufferFiles.AddRunLumi")
            lumiAction.execute(file=self["lfn"], runs=self["runs"],
                               conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        self["id"] = self.exists()
        self.updateLocations()
        self.commitTransaction(existingTransaction)

        for checksumType in self["checksums"]:
            self.setCksum(cksum=self["checksums"][checksumType],
                          cktype=checksumType)
        return

    def delete(self):
        """
        _delete_

        Remove a file from the DSBuffer database.
        """
        action = self.daofactory(classname="DBSBufferFiles.Delete")
        action.execute(file=self["lfn"], conn=self.getDBConn(),
                       transaction=self.existingTransaction())
        return

    def addChildren(self, lfns):
        """
        _addChildren_

        Set one or more lfns as the child of this file.
        """
        if not isinstance(lfns, list):
            lfns = [lfns]

        existingTransaction = self.beginTransaction()

        if not self["id"] > 0:
            raise Exception("Parent file doesn't have an id %s" % self["lfn"])

        action = self.daofactory(classname="DBSBufferFiles.HeritageLFNChild")
        action.execute(childLFNs=lfns, parentID=self["id"],
                       conn=self.getDBConn(),
                       transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def addParents(self, parentLFNs):
        """
        _addParents_

        Associate this file with it's parents.  If the parents do not exist in
        the buffer then bogus place holder files will be created so that the
        parentage information can be tracked and correctly inserted into DBS.
        """
        newAlgoAction = self.daofactory(classname="NewAlgo")
        newDatasetAction = self.daofactory(classname="NewDataset")
        assocAction = self.daofactory(classname="AlgoDatasetAssoc")
        existsAction = self.daofactory(classname="DBSBufferFiles.Exists")

        uploadFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                   logger=self.logger,
                                   dbinterface=self.dbi)
        setDatasetAlgoAction = uploadFactory(classname="SetDatasetAlgo")

        existingTransaction = self.beginTransaction()

        toBeCreated = []
        for parentLFN in parentLFNs:
            self["parents"].add(DBSBufferFile(lfn=parentLFN))
            if not existsAction.execute(lfn=parentLFN,
                                        conn=self.getDBConn(),
                                        transaction=True):
                toBeCreated.append(parentLFN)

        if len(toBeCreated) > 0:
            newAlgoAction.execute(appName="cmsRun", appVer="UNKNOWN",
                                  appFam="UNKNOWN", psetHash="NOT_SET",
                                  configContent="NOT_SET",
                                  conn=self.getDBConn(),
                                  transaction=True)

            newDatasetAction.execute(datasetPath="bogus",
                                     conn=self.getDBConn(),
                                     transaction=True)

            assocID = assocAction.execute(appName="cmsRun", appVer="UNKNOWN",
                                          appFam="UNKNOWN", psetHash="NOT_SET",
                                          datasetPath="bogus",
                                          conn=self.getDBConn(),
                                          transaction=True)

            setDatasetAlgoAction.execute(datasetAlgo=assocID, inDBS=1,
                                         conn=self.getDBConn(),
                                         transaction=True)

            addInputFiles = self.daofactory(classname="DBSBufferFiles.AddIgnore")
            addInputFiles.execute(lfns=toBeCreated, datasetAlgo=assocID,
                                  status="GLOBAL",
                                  inPhEDEx=1,
                                  conn=self.getDBConn(),
                                  transaction=True)

        action = self.daofactory(classname="DBSBufferFiles.HeritageLFNParent")
        action.execute(parentLFNs=parentLFNs, childLFN=self["lfn"],
                       conn=self.getDBConn(),
                       transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)
        return

    def updateLocations(self):
        """
        _updateLocations_

        Write any new locations to the database.  After any new locations are
        written to the database all locations will be reloaded from the
        database.
        """
        if len(self["newlocations"]) == 0:
            return

        insertAction = self.daofactory(classname="DBSBufferFiles.AddLocation")
        addAction = self.daofactory(classname="DBSBufferFiles.SetLocationByLFN")

        existingTransaction = self.beginTransaction()

        insertAction.execute(siteName=self['newlocations'],
                             conn=self.getDBConn(),
                             transaction=self.existingTransaction())

        binds = []
        for location in self["newlocations"]:
            binds.append({"lfn": self["lfn"],
                          "pnn": location})

        addAction.execute(binds=binds,
                          conn=self.getDBConn(),
                          transaction=self.existingTransaction())

        self["locations"].update(self["newlocations"])
        self["newlocations"].clear()
        self.commitTransaction(existingTransaction)
        return

    def setLocation(self, pnn, immediateSave=True):
        """
        _setLocation_

        Sets the location of a file. If immediateSave is True, commit change to
        the DB immediately, otherwise queue for addition when save() is called.
        """
        if isinstance(pnn, str):
            self["newlocations"].add(pnn)
            self["locations"].add(pnn)
        else:
            self["newlocations"].update(pnn)
            self["locations"].update(pnn)

        if immediateSave:
            self.updateLocations()

        return

    def setAlgorithm(self, appName=None, appVer=None, appFam=None,
                     psetHash=None, configContent=None):
        """
        _setAlgorithm_

        Set the DBS algorithm for this file.
        """
        self["appName"] = appName
        self["appVer"] = appVer
        self["appFam"] = appFam
        self["psetHash"] = psetHash
        self["configContent"] = configContent
        return

    def setProcessingVer(self, ver):
        """
        _setProcessingVer_

        Set the era
        """
        procVer = ver or 0
        try:
            procVer = int(procVer)
        except TypeError:
            msg = "Provided ver=%s of type %s cannot be converted to int" \
                    % (ver, type(ver))
            raise TypeError(msg) from None
        self['processingVer'] = procVer
        return

    def setAcquisitionEra(self, era):
        """
        _setAcquisitionEra_

        Set the era
        """

        self['acquisitionEra'] = era
        return

    def setDatasetPath(self, datasetPath):
        """
        _setDatasetPath_

        Set the dataset path for this file.
        """
        self["datasetPath"] = datasetPath
        return

    def setValidStatus(self, validStatus):
        """
        _setValidStatus_

        Set the valid status for the dataset to be migrated into DBS
        """

        self['validStatus'] = validStatus
        return

    def setGlobalTag(self, globalTag):
        """
        _setGlobalTag_

        Set the global Tag
        """

        self['globalTag'] = globalTag
        return

    def getGlobalTag(self):
        """
        _getGlobalTag_

        Get the global Tag
        """

        return self['globalTag']

    def setPrepID(self, prep_id):

        self['prep_id'] = prep_id
        return

    def getPrepID(self):

        return self['prep_id']

    def setDatasetParent(self, datasetParent):
        """
        _setDatasetParent_

        Set the dataset's parent path
        """

        self['datasetParent'] = datasetParent
        return

    def getDatasetParent(self):
        """
        _getDatasetParent_

        Return the datasetParent
        """

        return self['datasetParent']

    def getParentLFNs(self):
        """
        Get a flat list of parent LFNs
        """
        result = []
        parents = self['parents']
        while parents:
            result.extend(parents)
            temp = []
            for parent in parents:
                temp.extend(parent["parents"])
            parents = temp
        result.sort()  # ensure SecondaryInputFiles are in order
        return [x['lfn'] for x in result]

    def addRunSet(self, runSet):
        """
        add the set of runs.  This should be called after a file is created,
        unlike addRun which should be called before the file was created.
        runSet should be set of DataStruct.Run
        also there should be no duplicate entries in runSet.
        (May need to change in schema level not to allow duplicate record)
        """
        existingTransaction = self.beginTransaction()

        lumiAction = self.daofactory(classname="DBSBufferFiles.AddRunLumi")
        lumiAction.execute(file=self["lfn"], runs=runSet,
                           conn=self.getDBConn(),
                           transaction=self.existingTransaction())

        action = self.daofactory(classname="DBSBufferFiles.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn=self.getDBConn(),
                              transaction=self.existingTransaction())

        self["runs"].clear()
        for r in runs:
            self.addRun(run=Run(r, *runs[r]))

        self.commitTransaction(existingTransaction)
        return

    def setBlock(self, blockName):
        """
        _setBlock_

        Associate this file with a block in DBS/PhEDEx.
        """
        existingTransaction = self.beginTransaction()

        blockAction = self.daofactory(classname="DBSBufferFiles.SetBlock")
        blockAction.execute(self["lfn"], blockName, conn=self.getDBConn(),
                            transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def setCksum(self, cksum, cktype):
        """
        _setCKSum_

        Set the Checksum
        """
        if self['id'] < 0:
            self.create()

        existingTransaction = self.beginTransaction()

        action = self.daofactory(classname="DBSBufferFiles.AddChecksum")
        action.execute(fileid=self['id'], cktype=cktype, cksum=cksum, \
                       conn=self.getDBConn(), transaction=existingTransaction)

        self.commitTransaction(existingTransaction)

        return
