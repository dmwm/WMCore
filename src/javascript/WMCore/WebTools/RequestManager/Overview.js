var overviewTable = function(divID){

    var dataSchema = {
        fields: [{key: "request_name"},
                 {key: "status"},
                 {key: "type"},
                 {key: "global_queue"},
                 {key: "local_queue"},
                 {key: "pending"},
                 {key: "cooloff"},
                 {key: "running"},
                 {key: "success"},
                 {key: "failure"},
                 {key: "couch_doc_base", label: "summary"}]
        };

    var dataUrl = "/reqMgr/overview"
    var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema)
    //writeDebugObject(dataSource)
    //writeEval(dataSource.responseType)
    var dataTable = WMCore.WebTools.createDataTable(divID, dataSource, WMCore.WebTools.createDefaultTableDef(dataSchema.fields), WMCore.WebTools.createDefaultTableConfig(), 100000)
}