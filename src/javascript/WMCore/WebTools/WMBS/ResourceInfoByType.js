WMCore.namespace("WMBS.ResourceInfoByType")

WMCore.WMBS.ResourceInfoByType.resourceDT = function(divID){
    var dataSchema = {
        resultsList: 'results',
        fields: [{
            key: "site"
        }, {
            key: "['data'][0]['type']"
        }, {
            key: "['data'][0]['max_slots']",
            label: "threshold"
        }, {
            key: "['data'][0]['task_running_jobs']",
            label: "running"
        }, {
            key: "['data'][1]['type']"
        }, {
            key: "['data'][1]['max_slots']",
            label: "threshold"
        }, {
            key: "['data'][1]['task_running_jobs']",
            label: "running"
        }, {
            key: "['data'][2]['type']"
        }, {
            key: "['data'][2]['max_slots']",
            label: "threshold"
        }, {
            key: "['data'][2]['task_running_jobs']",
            label: "running"
        }, {
            key: "['data'][3]['type']"
        }, {
            key: "['data'][3]['max_slots']",
            label: "threshold"
        }, {
            key: "['data'][3]['task_running_jobs']",
            label: "running"
        }, {
            key: "['data'][4]['type']"
        }, {
            key: "['data'][4]['max_slots']",
            label: "threshold"
        }, {
            key: "['data'][4]['task_running_jobs']",
            label: "running"
        }, //common number
        {
            key: "['data'][0]['total_running_jobs']",
            label: "running"
        }, {
            key: "['data'][0]['total_slots']",
            label: "slots"
        }]
    };
    
    
    var tableDef = [{
        key: "site"
    }, {
        label: "Job Type",
        children: [{
            label: "Processing",
            children: [ //{key: "['data'][2]['type']"},
            {
                key: "['data'][2]['max_slots']",
                label: "threshold"
            }, {
                key: "['data'][2]['task_running_jobs']",
                label: "running"
            }]
        }, {
            label: "Merge",
            children: [ //{key: "['data'][1]['type']"},
            {
                key: "['data'][1]['max_slots']",
                label: "threshold"
            }, {
                key: "['data'][1]['task_running_jobs']",
                label: "running"
            }]
        }, {
            label: "Skim",
            children: [ //{key: "['data'][0]['type']"},
            {
                key: "['data'][0]['max_slots']",
                label: "threshold"
            }, {
                key: "['data'][0]['task_running_jobs']",
                label: "running"
            }]
        }, {
            label: "LogCollect",
            children: [ //{key: "['data'][4]['type']"},
            {
                key: "['data'][4]['max_slots']",
                label: "threshold"
            }, {
                key: "['data'][4]['task_running_jobs']",
                label: "running"
            }]
        }, {
            label: "CleanUp",
            children: [ //{key: "['data'][3]['type']"},
            {
                key: "['data'][3]['max_slots']",
                label: "threshold"
            }, {
                key: "['data'][3]['task_running_jobs']",
                label: "running"
            }]
        }, ]
    }, //common number.
    {
        label: "Total",
        children: [{
            key: "['data'][0]['total_slots']",
            label: "slots"
        }, {
            key: "['data'][0]['total_running_jobs']",
            label: "running"
        }]
    }]
    
    var dataUrl = "/wmbsservice/wmbs/listthresholdsforsubmit"
    
    var dataSource = WMCore.createDataSource(dataUrl, dataSchema)
    dataSource.responseType = YAHOO.util.DataSource.TYPE_JSON
    var dataTable = WMCore.createDataTable(divID, dataSource, tableDef,
                                WMCore.createDefaultTableConfig(), 100000)
}
