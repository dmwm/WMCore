from WMCore.WMBS.Actions.Action import BaseAction

class NewFilesetAction(BaseAction):
    name = "NewFileset"
        
    def execute(self, fileset = None, dbinterface = None):
        """
        import the approriate SQL object and execute it
        """ 
        dia = dbinterface.engine.dialect
        #if isinstance(dia, OracleDialect):
        #    from WMCore.WMBS.Oracle import OracleDialect as WMBSOracle
        #    return WMBSOracle (self.logger, engine)
        #el
        if isinstance(dia, self.dialects['sqlite']):
            from WMCore.WMBS.SQLite.NewFilesetSQL import NewFilesetSQL
        elif isinstance(dia, self.dialects['mysql']):
            from WMCore.WMBS.MySQL.NewFilesetSQL import NewFilesetSQL
        else:
            raise TypeError, "unknown connection type"
             
        action = NewFilesetSQL(self.logger, dbinterface)
        return action.execute(fileset)