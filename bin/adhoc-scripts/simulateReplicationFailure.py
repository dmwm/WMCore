#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : simulateReplicationFailure.py
author     : valentin kuznetsov <vkuznet AT gmail DOT com>
Description: helper script to simulate replication failure
"""

import os
import sys
import json
import tempfile
import time
import requests

# 3rd party modules
from Utils.CertTools import ckey, cert, caBundle


def wipeDb(couchUrl, dbName, auth=None, certTuple=None, caFileName=None):
    """
    wipe out couch database
    :param couchUrl: couch url string
    :param dbName: database name
    :param auth: authentication tuple
    :param certTuple: cert and ckey tuple
    :param caFileName: CA file name
    :return: nothing
    """
    deleteResp = requests.delete(f"{couchUrl}/{dbName}", auth=auth, cert=certTuple, verify=caFileName)

    if deleteResp.status_code not in (200, 202, 404):
        raise Exception(f"Failed to delete database: {deleteResp.text}")
    print(f"Deleted database '{dbName}' (status {deleteResp.status_code})")

def createDb(couchUrl, dbName, auth, certTuple=None, caFileName=None):
    """
    creates new couchdb database
    :param couchUrl: couch url string
    :param dbName: database name
    :param auth: authentication tuple
    :param certTuple: cert and ckey tuple
    :param caFileName: CA file name
    :return: nothing
    """
    r = requests.put(f"{couchUrl}/{dbName}", auth=auth, cert=certTuple, verify=caFileName)
    if r.status_code not in (201, 412):  # 412 = already exists
        raise Exception(f"Failed to create DB {couchUrl}/{dbName}: {r.text} status code {r.status_code}")

def setupValidator(couchUrl, targetDb, auth, certTuple=None, caFileName=None):
    """
    setup validator in couch database
    :param couchUrl: couch url string
    :param targetDb: database name
    :param auth: authentication tuple
    :param certTuple: cert and ckey tuple
    :param caFileName: CA file name
    :return: nothing
    """
    designDoc = {
        "_id": "_design/rejectBadDocs",
        "validate_doc_update": (
            "function(newDoc, oldDoc, userCtx) { "
            "if (newDoc.blockMe === true) { throw({forbidden: 'Blocked by validator'}); } "
            "}"
        )
    }
    headers = {'Content-Type': 'application/json'}

    r = requests.put(f"{couchUrl}/{targetDb}/{designDoc['_id']}",
                     data=json.dumps(designDoc), headers=headers, auth=auth, cert=certTuple, verify=caFileName)
    if r.status_code not in (201, 202):
        raise Exception(f"Failed to upload validator: {r.text}")

def insertValidDocs(couchUrl, sourceDb, auth, certTuple=None, caFileName=None):
    """
    insert set of valid documents into couch database
    wipe out couch database
    :param couchUrl: couch url string
    :param sourceDb: database name
    :param auth: authentication tuple
    :param certTuple: cert and ckey tuple
    :param caFileName: CA file name
    :return: nothing
    """
    validDocIds = []
    headers = {'Content-Type': 'application/json'}
    for i in range(3):
        doc = {"_id": f"validDoc{i}", "msg": f"This is valid doc #{i}"}
        r = requests.put(f"{couchUrl}/{sourceDb}/{doc['_id']}",
                         data=json.dumps(doc), headers=headers, auth=auth, cert=certTuple, verify=caFileName)
        if r.status_code in (201, 202):
            validDocIds.append(doc['_id'])
        else:
            raise Exception(f"Failed to insert valid doc: {r.text}")
    return validDocIds

def insertInvalidDoc(couchUrl, sourceDb, auth, certTuple=None, caFileName=None):
    """
    insert invalid document into couch database
    :param couchUrl: couch url string
    :param sourceDb: database name
    :param auth: authentication tuple
    :param certTuple: cert and ckey tuple
    :param caFileName: CA file name
    :return: nothing
    """
    doc = {"_id": "badDoc", "blockMe": True, "msg": "This should fail replication"}
    headers = {'Content-Type': 'application/json'}
    r = requests.put(f"{couchUrl}/{sourceDb}/{doc['_id']}",
                     data=json.dumps(doc), headers=headers, auth=auth, cert=certTuple, verify=caFileName)
    if r.status_code not in (201, 202):
        raise Exception(f"Failed to insert bad doc: {r.text}")
    return doc['_id']

def deleteReplicationDoc(sourceUrl, replicationId, auth, certTuple=None, caFileName=None):
    """
    delete replication document
    :param sourceUrl: source url string
    :param replicationId: replication id string
    :param auth: authentication tuple
    :param certTuple: cert and ckey tuple
    :param caFileName: CA file name
    :return: nothing
    """
    docUrl = f"{sourceUrl}/_replicator/{replicationId}"
    r = requests.get(docUrl, auth=auth, cert=certTuple, verify=caFileName)

    if r.status_code == 200:
        rev = r.json()['_rev']
        delResp = requests.delete(f"{docUrl}?rev={rev}", auth=auth, cert=certTuple, verify=caFileName)
        if delResp.status_code not in (200, 202):
            raise Exception(f"Failed to delete replication doc: {delResp.text}")
        print(f"Deleted existing replication doc '{replicationId}'")
    elif r.status_code != 404:
        raise Exception(f"Failed to check replication doc: {r.text}")

def setupReplication(sourceUrl, sourceDb, targetUrl, targetDb, auth, replicationId, certTuple=None, caFileName=None):
    """
    setup replication between source and target databases
    :param sourceUrl: source couchdb url string
    :param sourceDb: source database name
    :param targetUrl: target couchdb url string
    :param targetDb: target database name
    :param auth: authentication tuple
    :param replicationId: replication id string
    :param certTuple: cert and ckey tuple
    :param caFileName: CA file name
    :return: nothing
    """
    replicationDoc = {
        "_id": replicationId,
        "source": sourceDb,
        "target": f"{targetUrl}/{targetDb}",
        "create_target": False,
        "continuous": True,
        "user_ctx": {"name": "admin", "roles": ["_admin"]}
    }
    headers = {'Content-Type': 'application/json'}
    r = requests.put(f"{sourceUrl}/_replicator/{replicationId}",
                     data=json.dumps(replicationDoc), headers=headers, auth=auth, cert=certTuple, verify=caFileName)
    if r.status_code not in (201, 202):
        raise Exception(f"Failed to create replication: {r.text}")
    print(f"Replication '{replicationId}' set up successfully with status {r.status_code}")

def checkReplicationStatus(couchUrl, replicationId, auth, certTuple=None, caFileName=None):
    """
    checks replication status for given replication id
    :param couchUrl: couch url string
    :param replicationId: replication id string
    :param auth: authentication tuple
    :param certTuple: cert and ckey tuple
    :param caFileName: CA file name
    :return: nothing
    """
    for _ in range(10):
        time.sleep(2)
        r = requests.get(f"{couchUrl}/_replicator/{replicationId}", auth=auth, cert=certTuple, verify=caFileName)
        doc = r.json()
        state = doc.get("_replication_state", "unknown")
        reason = doc.get("_replication_state_reason", "")
        print(f"Replication state: {state}")
        if reason:
            print(f"Reason: {reason}")
        if state in ("error", "completed"):
            return state, reason
    return "unknown", ""

def main():
    """
    main function for testing purposes
    """
    if os.getenv('COUCH_SOURCE_URL') == '':
        print('ERROR: no COUCH_SOURCE_URL found')
        sys.exit(1)
    if os.getenv('COUCH_TARGET_URL') == '':
        print('ERROR: no COUCH_TARGET_URL found')
        sys.exit(1)
    if os.getenv('COUCH_CREDENTIALS') == '':
        print('ERROR: no COUCH_CREDENTIALS found, should be username:password')
        sys.exit(1)

    certTuple = (cert(), ckey())
    capath = os.environ.get("X509_CERT_DIR", '/etc/grid-security/certificates')
    cacerts = caBundle(capath)

    with tempfile.NamedTemporaryFile(mode='w+', suffix=".pem", delete=True) as tfile:
        tfile.write(cacerts)
        tfile.flush()
        caFileName = tfile.name

        sourceUrl = os.getenv('COUCH_SOURCE_URL', '')
        targetUrl = os.getenv('COUCH_TARGET_URL', '')
        authEnv = os.getenv('COUCH_CREDENTIALS', '')
        auth = tuple(authEnv.split(':')) if ':' in authEnv else None
        sourceDb = 'test_db'
        targetDb = 'test_db'
        replicationId = 'replicationFailTest'

        print(f"INFO: wipe out {sourceUrl}/{sourceDb}")
        wipeDb(sourceUrl, sourceDb, auth, certTuple, caFileName)
        print(f"INFO: wipe out {targetUrl}/{targetDb}")
        wipeDb(targetUrl, targetDb, auth, certTuple, caFileName)

        print("INFO: Setting up test environment...")
        createDb(sourceUrl, sourceDb, auth, certTuple, caFileName)
        createDb(targetUrl, targetDb, auth, certTuple, caFileName)
        setupValidator(targetUrl, targetDb, auth, certTuple, caFileName)

        print("INFO: Inserting valid documents...")
        validDocIds = insertValidDocs(sourceUrl, sourceDb, auth, certTuple, caFileName)

        print("INFO: Inserting a document that will be rejected by the target...")
        badDocId = insertInvalidDoc(sourceUrl, sourceDb, auth, certTuple, caFileName)

        print("INFO: Starting replication...")
        deleteReplicationDoc(sourceUrl, replicationId, auth, certTuple, caFileName)
        setupReplication(sourceUrl, sourceDb, targetUrl, targetDb, auth, replicationId, certTuple, caFileName)

        print("INFO: Waiting for replication to complete or fail...")
        state, reason = checkReplicationStatus(sourceUrl, replicationId, auth, certTuple, caFileName)

        print("\nINFO: Successfully inserted document IDs:")
        for docId in validDocIds:
            print(f" - {docId}")

        print(f"\nWARNING: Problematic document ID (should fail replication): {badDocId}")

        print(f"\nINFO: Replication final state: {state}")
        if reason:
            print(f"ERROR: Replication failure reason: {reason}")
        else:
            print("INFO: No error reported.")

if __name__ == "__main__":
    main()

