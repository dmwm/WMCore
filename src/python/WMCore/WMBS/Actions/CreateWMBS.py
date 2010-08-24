from WMCore.WMBS.Actions.Action import BaseAction

class CreateWMBSAction(BaseAction):
    name = "CreateWMBS"
        
    def execute(self, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        dia = dbinterface.engine.dialect
        #if isinstance(dia, OracleDialect):
        #    from WMCore.WMBS.Oracle import OracleDialect as WMBSOracle
        #    return WMBSOracle (self.logger, engine)
        #el
        if isinstance(dia, self.dialects['sqlite']):
            from WMCore.WMBS.SQLite.CreateWMBSSQL import CreateWMBS
        elif isinstance(dia, self.dialects['mysql']):
            from WMCore.WMBS.MySQL.CreateWMBSSQL import CreateWMBS
        else:
            raise TypeError, "unknown connection type"
             
        action = CreateWMBS(self.logger, dbinterface)
        try:
            action.execute()
            return True
        except:
            return False