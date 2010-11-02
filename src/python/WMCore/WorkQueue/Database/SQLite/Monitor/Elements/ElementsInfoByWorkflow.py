"""
_ElementsInfoByWorkflow_

SQLite implementation of Monitor.Elements.ElementsInfoByWorkflow
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Elements.ElementsInfoByWorkflow \
     import ElementsInfoByWorkflow as ElementsInfoByWorkflowMySQL

class ElementsInfoByWorkflow(ElementsInfoByWorkflowMySQL):
    pass