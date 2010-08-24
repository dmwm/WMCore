from WMCore.WMBS.Actions.Action import BaseAction
class ListFilesetAction(BaseAction):
    name = "ListFileset"
        
    def execute(self, dbinterface = None):
        """
        import the approriate SQL object and execute it
        """ 
        dia = dbinterface.engine.dialect
        #if isinstance(dia, OracleDialect):
        #    from WMCore.WMBS.Oracle import OracleDialect as WMBSOracle
        #    return WMBSOracle (self.logger, engine)
        #el
        if isinstance(dia, self.dialects['sqlite']):
            from WMCore.WMBS.SQLite.ListFilesetSQL import ListFileset
        elif isinstance(dia, self.dialects['mysql']):
            from WMCore.WMBS.MySQL.ListFilesetSQL import ListFileset
        action = ListFileset(self.logger, dbinterface)
        return action.execute()