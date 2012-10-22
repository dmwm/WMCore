"""
Provide the tools to format data from various sources.
"""
from collections import defaultdict

def combineListOfDict(matchKey, baseList, applyList, errorKey = None, **kwargs):
    """
    combineListOfDict provide the function to combine two lists of dictionary

    matchkey is the dictionary key word in which values will be compared,
    baseList is the base of the combine results,
    applyList will contain the item which will update the base list.
    baseList and applyList can be exchangeable without affecting the return result.
    kwargs is key value fair (key being key of baseList and applyList item,
    value being the function reference which takes 2 pararmeters and
    return meaningful result of combining non exclusive items

    values of item[matchkey] in baseList should be unique
    i.e.
    key = 'id'
    baseList = [{'id': 1, 'sales': 1000}, {'id': 2, 'sales': 2000}]
    applyList = [{'id': 2, 'sales': 1000, 'task' : 'A'}, {'id': 3, 'sales': 3000}]

    combineListOfDict('id', baseList, appliyList, sales=(lambda x, y: x+y))
    will return
    [{'id': 1, 'sales': 1000}, {'id': 2, 'sales': 3000, 'task' : 'A'}, {'id': 3, 'sales': 3000}]

    """
    # construct dict with base list
    baseListDict = defaultdict(dict)
    for baseItem in baseList:
        baseListDict[baseItem[matchKey]].update(baseItem)

    resultList = []
    errorUrls = {}
    for aItem in applyList:
        #Error handling
        if errorKey and aItem.has_key("error_url"):
            errorUrls[aItem["error_url"]] = aItem['error']
            continue
        if baseListDict.has_key(aItem[matchKey]):
            temp = {}
            # nonorthogonal case
            if kwargs:
                for key, value in kwargs.items():
                    baseListDict[aItem[matchKey]].setdefault(key, None)
                    aItem.setdefault(key, None)
                    temp[key] = value(baseListDict[aItem[matchKey]][key], aItem[key])

            baseListDict[aItem[matchKey]].update(aItem)
            baseListDict[aItem[matchKey]].update(temp)
            resultList.append(baseListDict[aItem[matchKey]])
            baseListDict.pop(aItem[matchKey])
        else:
            resultList.append(aItem)

    resultList.extend(baseListDict.values())

    if len(errorUrls) != 0:
        # update list of error urls (global wq, local wq)
        for resultDict in resultList:
            # update list of error urls (global wq, local wq)
            if resultDict.has_key(errorKey) and resultDict[errorKey]:
                if type(resultDict[errorKey]) != list:
                    errorKeyCollection = [resultDict[errorKey]]
                else:
                    errorKeyCollection = resultDict[errorKey]

                for errorUrl, errorMsg in errorUrls.items():
                    if errorUrl in resultDict[errorKey]:
                        resultDict.setdefault("error", "")
                        if len(resultDict['error']) == 0:
                            initStr = "%s"
                        else:
                            initStr = ", %s"
                        resultDict['error'] += initStr % errorMsg

    return resultList


def combineListOfDictSlow(matchKey, baseList, applyList, errorKey = None, **kwargs):
    """
    combineListOfDict provide the function to combine two lists of dictionary

    matchkey is the dictionary key word in which values will be compared,
    baseList is the base of the combine results,
    applyList will contain the item which will update the base list.
    baseList and applyList can be exchangeable without affecting the return result.
    kwargs is key value fair (key being key of baseList and applyList item,
    value being the function reference which takes 2 pararmeters and
    return meaningful result of combining non exclusive items

    values of item[matckey] in baseList should be unique
    i.e.#Error handling
            if errorKey and aItem.has_key("error_url"):
    key = 'id'
    baseList = [{'id': 1, 'sales': 1000}, {'id': 2, 'sales': 2000}]
    applyList = [{'id': 2, 'sales': 1000, 'task' : 'A'}, {'id': 3, 'sales': 3000}]

    combineListOfDict('id', baseList, appliyList, sales=(lambda x, y: x+y))
    will return
    [{'id': 1, 'sales': 1000}, {'id': 2, 'sales': 3000, 'task' : 'A'}, {'id': 3, 'sales': 3000}]

    """
    resultList = []
    for bItem in baseList:
        resultDict = {}
        resultDict.update(bItem)
        for aItem in applyList:
            #Error handling
            if errorKey and aItem.has_key("error_url"):
                if resultDict.has_key(errorKey) and resultDict[errorKey]:
                    if type(resultDict[errorKey]) != list:
                        errorKeyCollection = [resultDict[errorKey]]
                    else:
                        errorKeyCollection = resultDict[errorKey]

                    if aItem["error_url"] in resultDict[errorKey]:
                        resultDict.setdefault("error", "")
                        if len(resultDict['error']) == 0:
                            initStr = "%s"
                        else:
                            initStr = ", %s"
                        resultDict['error'] += initStr % aItem['error']
                continue

            if resultDict[matchKey] == aItem[matchKey]:
                temp = {}
                # nonorthogonal case
                if kwargs:
                    for key, value in kwargs.items():
                        resultDict.setdefault(key, None)
                        aItem.setdefault(key, None)
                        temp[key] = value(resultDict[key], aItem[key])

                resultDict.update(aItem)
                resultDict.update(temp)
        resultList.append(resultDict)

    for aItem in applyList:
        if errorKey and aItem.has_key("error_url"):
        #error case is handled above
            insertFlag = False
            continue
        insertFlag = True
        for result in resultList:
            if result[matchKey] == aItem[matchKey]:
                insertFlag = False
                break
        if insertFlag:
            resultDict = {}
            resultDict.update(aItem)
            resultList.append(resultDict)
    return resultList


def errorFormatter(url, msg):
    return [{'error_url': url, 'error': "%s - %s" % (
              url.strip('/workqueue'), msg)}]

def add(a, b):
    if a == None:
        a = 0
    if b == None:
        b = 0
    return a + b

def convertToList(a):
    if a == None:
        return []
    if type(a) != list:
        return [a]
    else:
        return a

def addToList(a, b):
    results = []
    results.extend(convertToList(a))
    results.extend(convertToList(b))
    return results

def splitCouchServiceURL(serviceURL):
    """
    split service URL to couchURL and couchdb name
    serviceURL should be couchURL/dbname format.
    """

    splitedURL = serviceURL.rstrip('/').rsplit('/', 1)

    return splitedURL[0], splitedURL[1]
