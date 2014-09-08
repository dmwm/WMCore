WMStats.namespace("HistoryModel");

WMStats.HistoryModel = new WMStats._ModelBase('requestHistory', {}, WMStats.History);

WMStats.HistoryModel.setOptions = function() {
    var current = Math.round((new Date()).getTime() / 1000);
    this._options= {'reduce': false,
                    'startkey': [current, {}],
                    'endkey': [current - 600*60*24],
                    'include_docs': true,
                    'descending': true};
};

WMStats.HistoryModel.setTrigger(WMStats.CustomEvents.HISTORY_LOADED);
