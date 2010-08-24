from WMCore.WMBS.Actions.Action import BaseAction

class CreateWMBSAction(BaseAction):
    name = "CreateWMBS"
    
    def printschema(self, dbinterface = None, table = None):
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        if table:
            try:
                sql = action.create[table]
                print table, sql
            except KeyError:
                print '%s not in the schema' % table               
        else:
            for i in action.create.keys():
                print i, action.create[i]