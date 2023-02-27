

from argparse import ArgumentParser

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL


def cleanDeletedDoc(couchURL, totalLimit, filter, limit, type, lastSeq):
    couchURLBase, dbName = splitCouchServiceURL(couchURL)
    couchDB = CouchServer(couchURLBase).connectDatabase(dbName, False)
    couchDB["timeout"] = 3600
    _cleanDeletedDoc(couchDB, totalLimit, filter, limit, type, lastSeq)
    print("last sequence %s" % couchDB.last_seq)


def _cleanDeletedDoc(couchDB, totalLimit, filter, limit, type, lastSeq):
    cleanLimit = 0
    report = "nothing"
    start = True
    while True and (True if totalLimit < 0 else (cleanLimit <= totalLimit)):
        if start and lastSeq:
            since = lastSeq + 1
            start = False
        else:
            since = -1
        data = couchDB.changesWithFilter(filter, limit, since)
        if len(data["results"]) == 0:
            break
        if type == "purge":
            report = purgeDoc(couchDB, data, cleanLimit)
            print(report)
            print("delete sequence %s" % couchDB.last_seq)
        if type == "delete":
            report = deleteDoc(couchDB, data, cleanLimit)
            # print report
            print("delete sequence %s" % couchDB.last_seq)


def purgeDoc(couchDB, data, cleanLimit):
    purgeDict = {}
    for result in data["results"]:
        purgeDict[result["id"]] = [result["changes"][0]["rev"]]
        cleanLimit += 1
    purgeData = couchDB.purge(purgeDict)
    purgeSeq = purgeData["purge_seq"]
    return purgeSeq


def deleteDoc(couchDB, data, cleanLimit):
    for result in data["results"]:
        doc = {}
        doc["_id"] = result["id"]
        doc["_rev"] = result["changes"][0]["rev"]
        cleanLimit += 1
        couchDB.queueDelete(doc)
    return couchDB.commit(doc)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--url", dest="url",
                        help="type couch db url")
    parser.add_argument("--type", dest="type",
                        help="type purge or delete url")
    parser.add_argument("--start", dest="start",
                        help="type last seq")
    options = parser.parse_args()
    if options.url:
        cleanDeletedDoc(options.url, -1, 'WMStats/deleteFilter', 1000, options.type, int(options.start))
