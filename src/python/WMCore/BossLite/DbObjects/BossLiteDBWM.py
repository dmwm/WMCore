#!/usr/bin/env python
"""
_BossLiteDBWM_

"""

__version__ = "$Id: BossLiteDBWM.py,v 1.5 2010/05/09 20:00:32 spigafi Exp $"
__revision__ = "$Revision: 1.5 $"

from copy import deepcopy
import threading

from WMCore.BossLite.Common.Exceptions  import DbError
from WMCore.BossLite.Common.System      import evalCustomList

from WMCore.BossLite.DbObjects.Task         import Task
from WMCore.BossLite.DbObjects.Job          import Job
from WMCore.BossLite.DbObjects.RunningJob   import RunningJob

from WMCore.BossLite.API.BossLiteDBInterface    import BossLiteDBInterface
from WMCore.WMConnectionBase    import WMConnectionBase

def dbTransaction(func):
    """
    Basic transaction decorator function
    """
    
    def wrapper(self, *args, **kwargs):
        """
        Decorator for db transaction
        """
        
        self.existingTransaction = self.engine.beginTransaction()
        try:
            res = func(self, *args, **kwargs)
            self.engine.commitTransaction(self.existingTransaction)
        except Exception, ex:
            msg = "Failure in TrackingDB class"
            msg += str(ex)
            # Is this correct?
            myThread = threading.currentThread()
            """
            ---> pylint error: 
            E:42:dbTransaction.wrapper: Instance of '_DummyThread' has no 
               'transaction' member (but some types could not be inferred)
            """
            myThread.transaction.rollback()
            raise DbError(msg)        
        return res
    return wrapper

class BossLiteDBWM(BossLiteDBInterface):
    """
    _BossLiteDBWM_
    
    This class is *strongly* specialized to use WMCore DB back-end
    """

    ##########################################################################

    def __init__(self):
        """
        __init__
        """

        # call super class init method
        super(BossLiteDBWM, self).__init__()
                                  
        # Initialize WMCore database ...
        self.engine = WMConnectionBase(daoPackage = "WMCore.BossLite")

        self.existingTransaction = None

        
    ##########################################################################
    # Methods for BossLiteAPI
    ##########################################################################
    
    def insert(self, obj):
        """
        Uses default values for non specified parameters. Note that all
        parameters can be default, a useful method to book an ID.
        """

        # get field information
        fields = self.getFields(obj)
        fieldList = ','.join([x[0] for x in fields])
        valueList = ','.join([x[1] for x in fields])

        query = 'insert into ' + obj.tableName + '(' + fieldList + ') ' + \
                       'values(' + valueList + ')'

        # execute query 
        rows = self.executeSQL(query)

        # done, return number of updated rows - is this true?
        return rows

    ##########################################################################

    def select(self, template, strict = True):
        """
        _select_
        """
        
        # get template information
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
        objectFields = [key[0] for key in fieldMapping]
        dbFields = [key[1] for key in fieldMapping]

        # get matching information from template
        fields = self.getFields(template)

        # determine if comparison is strict or not
        if strict:
            operator = '='
        else:
            operator = ' like '
        listOfFields = ' and '.join([('%s'+ operator +'%s') % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        query = 'select ' + ', '.join(dbFields) + ' from ' +  tableName + \
                ' ' + listOfFields

        results = None
        theList = []
        
        # execute query
        results = self.executeSQL(query)
        
        if results is None :
            return theList

        # get all information and build objects
        for row in results:

            # fill object
            obj = self.fillObject( row, template, objectFields )

            # add to list
            theList.append(obj)

        # return the list
        return theList

    ##########################################################################

    def selectDistinct(self, template, distinctAttr, strict = True):
        """
        _select_
        """
        
        # get template information
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
        objectFields = [key[0] for key in fieldMapping]
        distFields = [key[1] for key in fieldMapping if key[0] in distinctAttr]

        # get matching information from template
        fields = self.getFields(template)

        # determine if comparison is strict or not
        if strict:
            operator = '='
        else:
            operator = ' like '
        listOfFields = ' and '.join([('%s'+ operator +'%s') % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        query = 'select distinct (' + ', '.join(distFields) + ')' + \
                ' from ' +  tableName + \
                ' ' + listOfFields

        results = None
        theList = []
        
        # execute query
        results = self.executeSQL(query)
        
        if results is None :
            return theList

        # get all information and build objects
        for row in results:

            # fill object
            obj = self.fillObject( row, template, objectFields )

            # add to list
            theList.append(obj)

        # return the list
        return theList
    
    ##########################################################################

    def selectJoin(self, template, jTemplate, 
                   jMap=None, less=None, more=None, options=None ):
        """
        select from template and jTemplate, using join condition from jMap
        -> long, complex, useful (?)
        """

        # evaluate options
        opt = { 'strict' : True,
                'jType'  : '',
                'limit'  : None,
                'offset' : None,
                'inList' : None }
        if options is not None :
            opt.update( options )

        if more is None :
            more = {}

        if less is None :
            less = {}

        # get template information
        dbMap = template.__class__.fields
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get template information
        jDbMap = jTemplate.__class__.fields
        jMapping = jTemplate.__class__.fields.items()
        jTableName = jTemplate.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
        objectFields = [key[0] for key in fieldMapping]
        dbFields = [key[1] for key in fieldMapping]

        # get field mapping in order for join table
        jFieldMapping = [(key, value) for key, value in jMapping]
        jObjectFields = [key[0] for key in jFieldMapping]
        jDbFields = [key[1] for key in jFieldMapping]

        # get matching information from template
        fields = self.getFields(template)

        # get matching information from join template
        jFields = self.getFields(jTemplate)

        # evaluate eventual lists
        if opt['inList'] is None :
            listOfFields = ''
        else :
            for key in opt['inList'].keys() :
                k = template.__class__.fields[key]
                listOfFields = 't1.' + k + ' in (' + \
                    ','.join( str(val ) for val in opt['inList'][key]) + ') ' 
        
        if listOfFields != "" :
            listOfFields += ' and '

        # determine if comparison is strict or not
        if opt['strict']:
            operator = '='
        else:
            operator = ' like '

        # is there a set of field for more/less comparison?
        listOfFields += ' and '.join([('t1.%s'+ operator +'%s') % (key, value)
                                      for key, value in fields
                                      if key not in more and key not in less
                                ])
        jListOfFields = ' and '.join([('t2.%s'+ operator +'%s') \
                                      % (key, value)
                                      for key, value in jFields
                                      if key not in more and key not in less
                                ])

        # check for general query for all objects
        if listOfFields != "" and  jListOfFields != "":
            listOfFields = " where " + listOfFields + " and " + jListOfFields

        elif listOfFields != "":
            listOfFields = " where " + listOfFields

        elif jListOfFields != "":
            listOfFields = " where " + jListOfFields

        # evaluate more
        for key, val in more.iteritems():
            print key, jDbMap[key], val
            if key in objectFields :
                listOfFields += ' and t1.%s>%s ' % ( dbMap[key], val )
            elif key in jObjectFields :
                listOfFields += ' and t2.%s>%s ' % ( jDbMap[key], val )

        # evaluate less
        for key, val in less.iteritems():
            if key in objectFields :
                listOfFields += ' and t1.%s<%s ' % ( dbMap[key], val )
            elif key in jObjectFields :
                listOfFields += ' and t2.%s<%s ' % ( jDbMap[key], val )

        # evaluate join conditions
        jLFields = ''
        if jMap is not None :
            jLFields = ' and '.join([('t1.%s=t2.%s') % ( \
                template.__class__.fields[key], \
                jTemplate.__class__.fields[value])
                                     for key, value in jMap.iteritems()
                                     ])

        if jLFields != '':
            jLFields = ' on (' + jLFields + ') '

        # what kind of join?
        if opt['jType'] == '' :
            qJoin = ' inner join '
        elif opt['jType'] == 'left' :
            qJoin = ' left join '
        elif opt['jType'] == 'right' :
            qJoin = ' right join '

        # prepare query
        query = 'select ' + ', '.join( ['t1.'+ key for key in dbFields] ) + \
                ', ' + ', '.join( ['t2.'+ key for key in jDbFields] ) + \
                ' from ' +  tableName + ' t1 ' + qJoin + \
                jTableName + ' t2 ' + jLFields + listOfFields

        # limit?
        if opt['limit'] is not None :
            if opt['offset'] is None or int(opt['offset']) == 0 :
                query += ' limit %s' % opt['limit']
            else  :
                query += ' limit %s,%s' % (opt['offset'], opt['limit'])


        # execute query
        results = None
        theList = []
        
        # execute query
        results = self.executeSQL(query)
        
        if results is None :
            return theList

        # get all information and build objects
        size =  len( mapping )
        for row in results:
        
            # fill objects
            obj = self.fillObject( row, template, objectFields )
            jObj = self.fillObject( row[size:], jTemplate, jObjectFields )
        
            # add to list
            theList.append((obj, jObj))

        # return the list
        return theList

    ##########################################################################
    
    def update(self, template, skipAttributes = None):
        """
        _update_
        """
        
        # get template information
        tableName = template.__class__.tableName
        tableIndex = template.__class__.tableIndex
        tableIndexRes = [ template.mapping[key]
                          for key in template.__class__.tableIndex ]

        if skipAttributes is None :
            skipAttributes = {}

        # get specification for keys (if any)
        keys = [(template.mapping[key], template.data[key])
                             for key in tableIndex
                             if template.data[key] is not None]
        keysSpec = " and ".join(['%s="%s"' % (key, value)
                                 for key, value in keys
                                ])

        unlikeSpec = " and ".join(['%s!="%s"' % (key, value)
                                   for key, value in skipAttributes.iteritems()
                                   ])

        if keysSpec != "" and unlikeSpec != "" :
            keysSpec += ' and ' + unlikeSpec
        elif unlikeSpec != "" :
            keysSpec = unlikeSpec

        if keysSpec != "" :
            keysSpec = ' where ' + keysSpec

        # define update list (does not include keys)
        fields = self.getFields(template)

        listOfFields = ','.join(['%s=%s' % (key, value)
                                     for key, value in fields
                                     if key not in tableIndexRes
                                ])

        # return if there are no fields to update
        if listOfFields == "":
            return 0

        query = 'update ' + tableName + ' set  ' + listOfFields + \
                keysSpec
        
        # execute query
        rows = self.executeSQL(query)
        
        # return number of modified rows - is this true?
        return rows

    ##########################################################################
    
    def delete(self, template):
        """
        _delete_
        """
        
        # get template information
        tableName = template.__class__.tableName

        # get matching information from template
        fields = self.getFields(template)
        listOfFields = ' and '.join(['%s=%s' % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        query = 'delete from ' +  tableName + \
                ' ' + listOfFields

        # execute query
        rows = self.executeSQL(query)
        
        # return number of rows removed - is this true?
        return rows
        
    ##########################################################################

    def getFields(self, obj):
        """
        prepare field sections in query
        """
        
        # get access to default values and mappings
        defaults = obj.__class__.defaults
        mapping = obj.__class__.fields

        # build list of fields and values with non default values
        fields = [ (mapping[key], '"' + str(value).replace('"','""') + '"')
                  for key, value in obj.data.items()
                  if value != defaults[key] ]

        # return it
        return fields
    
    ##########################################################################

    def fillObject( self, dbRow, template, objectFields ):
        """
        fillObject method
        """
        
        # create a single object
        obj = type(template)()
 
        # fill fields
        for key, value in zip(objectFields, dbRow):
 
            # check for NULLs
            if value is None:
                obj[key] = deepcopy( template.defaults[key] )
 
            # check for lists
            elif type(template.defaults[key]) == list:
                try :
                    obj[key] = evalCustomList(value)
                except SyntaxError:
                    obj[key] = [ value ]
 
            # other objects get casted automatically
            else:
                obj[key] = value
 
            # mark them as existing in database
            obj.existsInDataBase = True
 
        return obj

    ##########################################################################
    
    def distinctAttr(self, template, value1 , value2, alist ,  strict = True):
        """
        _distinctAttr_
        """
        
        # get template information
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
       # objectFields = [key[0] for key in fieldMapping]
       # dbFields = [key[1] for key in fieldMapping]

        #DanieleS
        for key, val in fieldMapping:
            if key == value1:
                dbFields = [val]
                objectFields = [key]
            if key == value2:
                field = val
        #        break
        
        # get matching information from template
        # fields = self.getFields(template)
        
        # determine if comparison is strict or not
        if strict:
            operator = '='
        else:
            operator = ' like '
        listOfFields = ' or '.join([('%s'+ operator +'%s') % (field, value)
                                     for value in alist
                                ])
        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        # DanieleS.
        # prepare query
        query = 'select distinct (' + ', '.join(dbFields) + ') from ' + \
                    tableName + ' ' + listOfFields

        results = None
        theList = []
        
        # execute query
        results = self.executeSQL(query)
        
        if results is None :
            return theList

        # get all information and build objects
        for row in results:

            # create a single object
            # template = deepcopy(template)
            obj = type(template)()

            # fill fields
            for key, value in zip(objectFields, row):

                # check for NULLs
                if value is None:
                    obj[key] = deepcopy(template.defaults[key])

                # check for lists
                elif type(template.defaults[key]) == list:
                    try :
                        obj[key] = evalCustomList(value)
                    except SyntaxError:
                        obj[key] = [ value ]

                # other objects get casted automatically
                else:
                    obj[key] = value

                # mark them as existing in database
                obj.existsInDataBase = True

            # add to list
            theList.append(obj)

        # return the list
        return theList

    ##########################################################################
    
    def distinct(self, template, value1 , strict = True):
        """
        _distinct_
        """
        
        # get template information
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
       # objectFields = [key[0] for key in fieldMapping]
       # dbFields = [key[1] for key in fieldMapping]

        #DanieleS
        for key, val in fieldMapping:
            if key == value1:
                dbFields = [val]
                objectFields = [key]
                break
        # get matching information from template
        fields = self.getFields(template)

        # determine if comparison is strict or not
        if strict:
            operator = '='
        else:
            operator = ' like '
        listOfFields = ' and '.join([('%s'+ operator +'%s') % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        # DanieleS.
        # prepare query
        query = 'select distinct (' + ', '.join(dbFields) + ') from ' + \
                tableName + ' ' + listOfFields

        results = None
        theList = []
        
        # execute query
        results = self.executeSQL(query)
        
        if results is None :
            return theList

        # get all information and build objects
        for row in results:

            # create a single object
            # template = deepcopy(template)
            obj = type(template)()

            # fill fields
            for key, value in zip(objectFields, row):

                # check for NULLs
                if value is None:
                    obj[key] = deepcopy(template.defaults[key])

                # check for lists
                elif type(template.defaults[key]) == list:
                    try :
                        obj[key] = evalCustomList(value)
                    except SyntaxError:
                        obj[key] = [ value ]

                # other objects get casted automatically
                else:
                    obj[key] = value

                # mark them as existing in database
                obj.existsInDataBase = True

            # add to list
            theList.append(obj)

        # return the list
        return theList
    
    ##########################################################################
    # Methods for DbObjects
    ##########################################################################
    
    @dbTransaction
    def objExists(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            # Task DAO
            action = self.engine.daofactory(classname = 'Task.Exists')
            tmpId = action.execute(name = obj.data['name'],
                           conn = self.engine.getDBConn(),
                           transaction = self.existingTransaction)
        
        elif type(obj) == Job :
            action = self.engine.daofactory(classname = "Job.Exists")
            tmpId = action.execute(name = obj.data['name'],
                                   conn = self.engine.getDBConn(),
                                   transaction = self.existingTransaction)
        
        elif type(obj) == RunningJob :
            action = self.engine.daofactory(classname = "RunningJob.Exists")
            tmpId = action.execute(submission = obj.data['submission'],
                                jobID = obj.data['jobId'], 
                                taskID = obj.data['taskId'],
                                conn = self.engine.getDBConn(),
                                transaction = self.existingTransaction )
        
        else :
            raise NotImplementedError
        
        return tmpId
        
    ##########################################################################
    
    @dbTransaction
    def objSave(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            action = self.engine.daofactory(classname = 'Task.Save')
        
        elif type(obj) == Job :
            action = self.engine.daofactory(classname = "Job.Save")
        
        elif type(obj) == RunningJob :
            action = self.engine.daofactory(classname = "RunningJob.Save")
                
        else :
            raise NotImplementedError    
        
        action.execute(binds = obj.data,
                       conn = self.engine.getDBConn(),
                       transaction = self.existingTransaction)
        
    ##########################################################################
    
    @dbTransaction
    def objCreate(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            action = self.engine.daofactory(classname = 'Task.New')
            
        elif type(obj) == Job :
            action = self.engine.daofactory(classname = "Job.New")
            
        elif type(obj) == RunningJob :
            action = self.engine.daofactory(classname = "RunningJob.New")
            
        else :
            raise NotImplementedError 
        
        action.execute(binds = obj.data,
                       conn = self.engine.getDBConn(),
                       transaction = self.existingTransaction)
        
    ##########################################################################
    
    @dbTransaction
    def objLoad(self, obj, classname= None):
        """
        put your description here
        """

        if type(obj) == Task :
            
            if classname == 'Task.GetJobs' :
                binds = {'taskId' : obj.data['id'] }
                
            elif obj.data['id'] > 0:
                classname = "Task.SelectTask"
                binds = {'id' : obj.data['id'] }
                
            elif obj.data['name']:
                classname = "Task.SelectTask"
                binds = {'name' : obj.data['name'] }
                
            else:
                # Then you're screwed
                return []
            
            action = self.engine.daofactory(classname = classname)
            result = action.execute(binds = binds,
                                    conn = self.engine.getDBConn(),
                                    transaction = self.existingTransaction)
            
            return result
        
        elif type(obj) == Job :
            
            if obj.data['id'] > 0:
                binds = { 'id' : obj.data['id'] }
            
            elif obj.data['jobId'] > 0 and obj.data['taskId'] > 0:
                binds = { 'job_id' : obj.data['jobId'],
                          'task_id' : obj.data['taskId'] }
                
            elif obj.data['name']:
                binds = { 'name' : obj.data['name'] }
                
            else:
                # We have no identifiers.  We're screwed
                # this branch doesn't exist
                return []
            
            # action = self.engine.daofactory(classname = "Job.Load")
            action = self.engine.daofactory(classname = "Job.SelectJob")
            result = action.execute(binds = binds, 
                                    conn = self.engine.getDBConn(),
                                    transaction = self.existingTransaction)
            
            return result
        
        elif type(obj) == RunningJob :
            
            if (obj.data['jobId'] and obj.data['taskId'] and \
                                                obj.data['submission']) :
                binds = {'task_id' : obj.data['taskId'],
                         'job_id' : obj.data['jobId'],
                         'submission' : obj.data['submission'] }
                
            elif obj.data['id'] > 0:
                binds = {'id' : obj.data['id'] } 
                
            else:
                # We have nothing
                return []

            action = self.engine.daofactory( classname = "RunningJob.Load" )
            result = action.execute(binds = binds,
                                    conn = self.engine.getDBConn(),
                                    transaction = self.existingTransaction)
            
            return result
        
        else :
            raise NotImplementedError        
        
    ##########################################################################
    
    @dbTransaction
    def objUpdate(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            raise NotImplementedError
        
        elif type(obj) == Job :
            raise NotImplementedError
        
        elif type(obj) == RunningJob :
            raise NotImplementedError
        
        else :
            raise NotImplementedError        
        
    ##########################################################################
    
    @dbTransaction
    def objRemove(self, obj):
        """
        put your description here
        """

        if type(obj) == Task :
            
            action = self.engine.daofactory(classname = 'Task.Delete')
            
            # verify data is complete
            if not obj.valid(['id']):
                column = 'name'
                value = obj.data['name']
            else :
                column = 'id'
                value = obj.data['id']
        
        elif type(obj) == Job :
            
            action = self.engine.daofactory(classname = "Job.Delete")
            
            # verify data is complete
            if not obj.valid(['id']):
                column = 'name'
                value = obj.data['name']
            else :
                column = 'id'
                value = obj.data['id']
        
        elif type(obj) == RunningJob :
            
            action = self.engine.daofactory(classname = "RunningJob.Delete")
            
            value = obj.data['id']
            column = 'id'
        
        else :
            raise NotImplementedError      
        
        action.execute(value = value,
                       column = column,
                       conn = self.engine.getDBConn(),
                       transaction = self.existingTransaction)  
        
    ##########################################################################
    # Method for execute raw SQL statements through general-purpose DAO
    ##########################################################################
    
    @dbTransaction
    def executeSQL(self, query):
        """
        Method for execute raw SQL statements through general-purpose DAO
        """
        
        action = self.engine.daofactory(classname = "BLGenericDAO")
        result = action.execute(rawSql = query,
                           conn = self.engine.getDBConn(),
                           transaction = self.existingTransaction) 
        
        return result
