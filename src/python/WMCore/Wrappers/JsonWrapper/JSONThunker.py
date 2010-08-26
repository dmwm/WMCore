import sys
import types

class _EmptyClass:
    pass

class JSONThunker:
    """
    _JSONThunker_
    Converts an arbitrary object to <-> from a jsonable object.
    
    Will, for the most part "do the right thing" about various instance objects
    by storing their class information along with their data in a dict. Handles
    a recursion limit to prevent infinite recursion.
    
    self.passThroughTypes - stores a list of types that should be passed
      through unchanged to the JSON parser
      
    self.blackListedModules - a list of modules that should not be stored in
      the JSON.
    
    """
    def __init__(self):
        self.passThroughTypes = (types.NoneType,
                                 types.BooleanType,
                                 types.IntType,
                                 types.LongType,
                                 types.ComplexType,
                                 types.StringTypes,
                                 types.StringType,
                                 types.UnicodeType
                                 )
        # objects that inherit from dict should be treated as a dict
        #   they don't store their data in __dict__. There was enough
        #   of those classes that it warrented making a special case
        self.dictSortOfObjects = ( ('WMCore.Datastructs.Job', 'Job'),
                                   ('WMCore.WMBS.Job', 'Job'),
                                   ('WMCore.Database.CMSCouch', 'Document' ))
        # ditto above, but for lists
        self.listSortOfObjects = ( ('WMCore.DataStructs.JobPackage', 'JobPackage' ),
                                   ('WMCore.WMBS.JobPackage', 'JobPackage' ),)
        
        self.foundIDs = {}
        # modules we don't want JSONed
        self.blackListedModules = ('sqlalchemy.engine.threadlocal',
                                   'WMCore.Database.DBCore',
                                   'logging',
                                   'WMCore.DAOFactory',
                                   'WMCore.WMFactory',
                                   'WMFactory',
                                   'WMCore.Configuration',
                                   'WMCore.Database.Transaction',
                                   'threading',
                                   'datetime')
        
    def checkRecursion(self, data):
        """
        handles checking for infinite recursion
        """
        if (id(data) in self.foundIDs):
            if (self.foundIDs[id(data)] > 5):
                self.unrecurse(data)
                return "**RECURSION**"
            else:
                self.foundIDs[id(data)] += 1
                return data
        else:
            self.foundIDs[id(data)] = 1
            return data
           
    def unrecurse(self, data):
        """
        backs off the recursion counter if we're returning from _thunk
        """
        self.foundIDs[id(data)] = self.foundIDs[id(data)] -1
         
    def checkBlackListed(self, data):
        """
        checks to see if a given object is from a blacklisted module
        """
        try:
            # special case
            if ((data.__class__.__module__ == 'WMCore.Database.CMSCouch') and
                (data.__class__.__name__ == 'Document')):
                data.__class__ = type({})
                return data
            if (data.__class__.__module__ in self.blackListedModules):
                return "Blacklisted JSON object: module %s, name %s, str() %s" %\
                    (data.__class__.__module__,data.__class__.__name__ , str(data))
            else:
                return data
        except:
            return data

    
    def thunk(self, toThunk):
        """
        Thunk - turns an arbitrary object into a JSONable object
        """
        self.foundIDs = {}
        data = self._thunk(toThunk)
        return data
    
    def unthunk(self, data):
        """
        unthunk - turns a previously 'thunked' object back into a python object
        """
        return self._unthunk(data)
    
    def handleSetThunk(self, toThunk):
        toThunk = self.checkRecursion( toThunk )
        tempDict = {'thunker_encoded_json':True, 'type': 'set'}
        tempDict['set'] = self._thunk(list(toThunk))
        self.unrecurse(toThunk)
        return tempDict
    
    def handleListThunk(self, toThunk):
        toThunk = self.checkRecursion( toThunk )
        for k,v in enumerate(toThunk):
                toThunk[k] = self._thunk(v)
        self.unrecurse(toThunk)
        return toThunk
    
    def handleDictThunk(self, toThunk):
        toThunk = self.checkRecursion( toThunk )
        special = False
        tmpdict = {}
        for k,v in toThunk.iteritems():
            if type(k) == type(int): 
                special = True
                tmpdict['_i:%s' % k] = self._thunk(v)
            elif type(k) == type(float): 
                special = True
                tmpdict['_f:%s' % k] = self._thunk(v)
            else:
                tmpdict[k] = self._thunk(v)
        if special:
            toThunk['thunker_encoded_json'] = self._thunk(True)
            toThunk['type'] = self._thunk('dict')
            toThunk['dict'] = tmpdict
        else:
            toThunk.update(tmpdict)
        self.unrecurse(toThunk)
        return toThunk
    
    def handleObjectThunk(self, toThunk):
        toThunk = self.checkRecursion( toThunk )
        toThunk = self.checkBlackListed(toThunk)
        
        if (type(toThunk) == type("")):
            # things that got blacklisted
            return toThunk
        if (hasattr(toThunk, '__to_json__')):
            #Use classes own json thunker
            toThunk2 = toThunk.__to_json__(self)
            self.unrecurse(toThunk)
            return toThunk2
        elif ( isinstance(toThunk, dict) ):
            toThunk2 = self.handleDictObjectThunk( toThunk )
            self.unrecurse(toThunk)
            return toThunk2
        elif ( isinstance(toThunk, list) ):
            #a mother thunking list
            toThunk2 = self.handleListObjectThunk( toThunk )
            self.unrecurse(toThunk)
            return toThunk2
        else:
            try:
                thunktype = '%s.%s' % (toThunk.__class__.__module__,
                                       toThunk.__class__.__name__)
                tempDict = {'thunker_encoded_json':True, 'type': thunktype}
                tempDict[thunktype] = self._thunk(toThunk.__dict__)
                self.unrecurse(toThunk)
                return tempDict
            except Exception, e:
                tempDict = {'json_thunk_exception_' : "%s" % e }
                self.unrecurse(toThunk)
                return tempDict
            
    def handleDictObjectThunk(self, data):
        thunktype = '%s.%s' % (data.__class__.__module__,
                               data.__class__.__name__)
        tempDict = {'thunker_encoded_json':True, 
                    'is_dict': True,
                    'type': thunktype, 
                    thunktype: {}}
        
        for k,v in data.__dict__.iteritems():
            tempDict[k] = self._thunk(v)
        for k,v in data.iteritems():
            tempDict[thunktype][k] = self._thunk(v)
            
        return tempDict
    
    def handleDictObjectUnThunk(self, value, data):
        data.pop('thunker_encoded_json', False)
        data.pop('is_dict', False)
        thunktype = data.pop('type', False)
        
        for k,v in data.iteritems():
            if (k == thunktype):
                for k2,v2 in data[thunktype].iteritems():
                    value[k2] = self._unthunk(v2)
            else:
                value.__dict__[k] = self._unthunk(v)
        return value
    
    def handleListObjectThunk(self, data):
        thunktype = '%s.%s' % (data.__class__.__module__,
                               data.__class__.__name__)
        tempDict = {'thunker_encoded_json':True, 
                    'is_list': True,
                    'type': thunktype, 
                    thunktype: []}
        for k,v in enumerate(data):
            tempDict['thunktype'].append(self._thunk(v)) 
        for k,v in data.__dict__.iteritems():
            tempDict[k] = self._thunk(v)           
        return tempDict
    
    def handleListObjectUnThunk(self, value, data):
        data.pop('thunker_encoded_json', False)
        data.pop('is_list', False)
        thunktype = data.pop('type')
        tmpdict = {}
        for k,v in data[thunktype].iteritems():
            setattr(value, k, self._unthunk(v))
            
        for k,v in data.iteritems():
            if (k == thunktype):
                continue
            value.__dict__ = self._unthunk(v)
        return value
    
    def _thunk(self, toThunk):
        """
        helper function for thunk, does the actual work
        """
        
        if (type(toThunk) in self.passThroughTypes):
            return toThunk
        elif (type(toThunk) == type([])):
            return self.handleListThunk(toThunk)
        
        elif (type(toThunk) == type({})):
            return self.handleDictThunk(toThunk)
        
        elif ((type(toThunk) == type(set()))):
            return self.handleSetThunk(toThunk)
        
        elif (type(toThunk) == types.FunctionType):
            self.unrecurse(toThunk)
            return "function reference"
        elif (isinstance(toThunk, object)):
            return self.handleObjectThunk(toThunk)
        else:
            self.unrecurse(toThunk)
            raise RuntimeError, type(toThunk)
        
    def _unthunk(self, jsondata):
        """
        _unthunk - does the actual work for unthunk
        """
        if (type(jsondata) == types.UnicodeType):
            return str(jsondata)
        if (type(jsondata) == type({})):
            if ('thunker_encoded_json' in jsondata):
                # we've got a live one...
                if jsondata['type'] == 'set':
                    newSet = set()
                    for i in self._unthunk(jsondata['set']):
                        newSet.add( self._unthunk( i ) )
                    return newSet
                if jsondata['type'] == 'dict':
                    # We have a "special" dict
                    data = {}
                    for k,v in jsondata['dict'].iteritems():
                        tmp = self._unthunk(v)
                        if k.startswith('_i:'):
                            data[int(k.lstrip('_i:'))] = tmp
                        elif k.startswith('_f:'):
                            data[float(k.lstrip('_f:'))] = tmp
                        else:
                            data[k] = tmp
                    return data
                else:
                    # spawn up an instance.. good luck
                    #   here be monsters
                    #   inspired from python's pickle code
                    ourClass = self.getThunkedClass(jsondata)
                    
                    value = _EmptyClass()
                    if (hasattr(ourClass, '__from_json__')):
                        # Use classes own json loader
                        try:
                            value.__class__ = ourClass
                        except:
                            value = ourClass()
                        value = ourClass.__from_json__(value, data, self)
                    elif ('thunker_encoded_json' in jsondata and
                                     'is_dict' in jsondata):
                        try:
                            value.__class__ = ourClass
                        except:
                            value = ourClass()
                        value = self.handleDictObjectUnThunk( value, jsondata )
                    elif ( 'thunker_encoded_json' in jsondata ):
                        #print "list obj unthunk"  
                        try:
                            value.__class__ = ourClass
                        except:
                            value = ourClass()
                        value = self.handleListObjectUnThunk( value, jsondata )
                    else:
                        #print "did we get here"
                        try:
                            value.__class__ = getattr(ourClass, name).__class__
                            #print "changed the class to %s " % value.__class__
                        except Exception, ex:
                            #print "Except1 in requests %s " % ex
                            try:
                                #value = _EmptyClass()
                                value.__class__ = ourClass
                            except Exception, ex2:
                                #print "Except2 in requests %s " % ex2
                                #print type(ourClass)
                                try:
                                    value = ourClass();
                                except:
                                    #print 'megafail'
                                    pass
                        
                        #print "name %s module %s" % (name, module)
                        value.__dict__ = data
                #print "our value is %s "% value
                return value
            else:
                #print 'last ditch attempt'
                data = {}
                for k,v in jsondata.iteritems():
                    data[k] = self._unthunk(v)
                return data
 
        else:
            return jsondata
        
    def getThunkedClass(self, jsondata):
        """
        Work out the class from it's thunked json representation
        """
        module = jsondata['type'].rsplit('.',1)[0]
        name = jsondata['type'].rsplit('.',1)[1]
        if (module == 'WMCore.Services.Requests') and (name == JSONThunker):
            raise RuntimeError, "Attempted to unthunk a JSONThunker.."
        
        __import__(module)
        mod = sys.modules[module]
        ourClass = getattr(mod, name)
        return ourClass