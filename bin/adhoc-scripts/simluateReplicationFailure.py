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
import requests
import json
import time

def certs():
    # TLS client cert/key
    certFile = os.getenv('X509_USER_CERT')
    keyFile = os.getenv('X509_USER_KEY')
    cert = (certFile, keyFile) if certFile and keyFile else None

    # CA cert (if needed to trust the server)
    caCert = os.getenv('SSL_CA_CERT')  # Optional
    return cert, caCert

def wipeDb(couchUrl, dbName, auth=None):
    cert, caCert = certs()

    # Delete the test DB
    deleteResp = requests.delete(
        f"{couchUrl}/{dbName}",
        auth=auth,
        cert=cert,
        verify=caCert or True
    )

    if deleteResp.status_code not in (200, 202, 404):
        raise Exception(f"Failed to delete database: {deleteResp.text}")
    else:
        print(f"Deleted database '{dbName}' (status {deleteResp.status_code})")

def createDb(couchdbUrl, name, auth):
    cert, caCert = certs()
    r = requests.put(f"{couchdbUrl}/{name}", auth=auth, cert=cert, verify=caCert or True)
    if r.status_code not in (201, 412):  # 412 = already exists
        raise Exception(f"Failed to create DB {couchdbUrl}/{name}: {r.text} status code {r.status_code}")

def setupValidator(couchdbUrl, targetDb, auth):
    designDoc = {
        "_id": "_design/rejectBadDocs",
        "validate_doc_update": (
            "function(newDoc, oldDoc, userCtx) { "
            "if (newDoc.blockMe === true) { throw({forbidden: 'Blocked by validator'}); } "
            "}"
        )
    }
    headers = {'Content-Type': 'application/json'}
    cert, caCert = certs()

    r = requests.put(f"{couchdbUrl}/{targetDb}/{designDoc['_id']}",
                     data=json.dumps(designDoc), headers=headers, auth=auth, cert=cert, verify=caCert or True)
    if r.status_code not in (201, 202):
        raise Exception(f"Failed to upload validator: {r.text}")

def insertValidDocs(couchdbUrl, sourceDb, auth):
    cert, caCert = certs()
    validDocIds = []
    headers = {'Content-Type': 'application/json'}
    for i in range(3):
        doc = {"_id": f"validDoc{i}", "msg": f"This is valid doc #{i}"}
        r = requests.put(f"{couchdbUrl}/{sourceDb}/{doc['_id']}",
                         data=json.dumps(doc), headers=headers, auth=auth, cert=cert, verify=caCert or True)
        if r.status_code in (201, 202):
            validDocIds.append(doc['_id'])
        else:
            raise Exception(f"Failed to insert valid doc: {r.text}")
    return validDocIds

def insertInvalidDoc(couchdbUrl, sourceDb, auth):
    cert, caCert = certs()
    doc = {"_id": "badDoc", "blockMe": True, "msg": "This should fail replication"}
    headers = {'Content-Type': 'application/json'}
    r = requests.put(f"{couchdbUrl}/{sourceDb}/{doc['_id']}",
                     data=json.dumps(doc), headers=headers, auth=auth, cert=cert, verify=caCert or True)
    if r.status_code not in (201, 202):
        raise Exception(f"Failed to insert bad doc: {r.text}")
    return doc['_id']

def deleteReplicationDoc(sourceUrl, replicationId, auth):
    cert, caCert = certs()
    docUrl = f"{sourceUrl}/_replicator/{replicationId}"
    r = requests.get(docUrl, auth=auth, cert=cert, verify=caCert or True)

    if r.status_code == 200:
        rev = r.json()['_rev']
        delResp = requests.delete(f"{docUrl}?rev={rev}", auth=auth, cert=cert, verify=caCert or True)
        if delResp.status_code not in (200, 202):
            raise Exception(f"Failed to delete replication doc: {delResp.text}")
        print(f"Deleted existing replication doc '{replicationId}'")
    elif r.status_code != 404:
        raise Exception(f"Failed to check replication doc: {r.text}")

def setupReplication(sourceUrl, sourceDb, targetUrl, targetDb, auth, replicationId):
    replicationDoc = {
        "_id": replicationId,
        "source": sourceDb,
        "target": f"{targetUrl}/{targetDb}",
        "create_target": False,
        "continuous": True,
        "user_ctx": {"name": "admin", "roles": ["_admin"]}
    }
    headers = {'Content-Type': 'application/json'}
    cert, caCert = certs()
    r = requests.put(f"{sourceUrl}/_replicator/{replicationId}",
                     data=json.dumps(replicationDoc), headers=headers, auth=auth, cert=cert, verify=caCert or True)
    if r.status_code not in (201, 202):
        raise Exception(f"Failed to create replication: {r.text}")
    else:
        print(f"Replication '{replicationId}' set up successfully with status {r.status_code}")

def checkReplicationStatus(couchdbUrl, replicationId, auth):
    cert, caCert = certs()
    for _ in range(10):
        time.sleep(2)
        r = requests.get(f"{couchdbUrl}/_replicator/{replicationId}", auth=auth, cert=cert, verify=caCert or True)
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
    if os.getenv('COUCH_SOURCE_URL') == '':
        print('ERROR: no COUCH_SOURCE_URL found')
        sys.exit(1)
    if os.getenv('COUCH_TARGET_URL') == '':
        print('ERROR: no COUCH_TARGET_URL found')
        sys.exit(1)
    if os.getenv('COUCH_CREDENTIALS') == '':
        print('ERROR: no COUCH_CREDENTIALS found, should be username:password')
        sys.exit(1)

    sourceUrl = os.getenv('COUCH_SOURCE_URL', '')
    targetUrl = os.getenv('COUCH_TARGET_URL', '')
    authEnv = os.getenv('COUCH_CREDENTIALS', '')
    auth = tuple(authEnv.split(':')) if ':' in authEnv else None
    sourceDb = 'test_db'
    targetDb = 'test_db'
    replicationId = 'replicationFailTest'

    print(f"INFO: wipe out {sourceUrl}/{sourceDb}")
    wipeDb(sourceUrl, sourceDb, auth)
    print(f"INFO: wipe out {targetUrl}/{targetDb}")
    wipeDb(targetUrl, targetDb, auth)

    print("INFO: Setting up test environment...")
    createDb(sourceUrl, sourceDb, auth)
    createDb(targetUrl, targetDb, auth)
    setupValidator(targetUrl, targetDb, auth)

    print("INFO: Inserting valid documents...")
    validDocIds = insertValidDocs(sourceUrl, sourceDb, auth)

    print("INFO: Inserting a document that will be rejected by the target...")
    badDocId = insertInvalidDoc(sourceUrl, sourceDb, auth)

    print("INFO: Starting replication...")
    deleteReplicationDoc(sourceUrl, replicationId, auth)
    setupReplication(sourceUrl, sourceDb, targetUrl, targetDb, auth, replicationId)

    print("INFO: Waiting for replication to complete or fail...")
    state, reason = checkReplicationStatus(sourceUrl, replicationId, auth)

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

