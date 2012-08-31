WMStats.namespace("SiteModel")

WMStats.SiteModel = new WMStats._ModelBase('latestAgentSite', 
                                         {"reduce": true, "group_level": 2}, 
                                         WMStats.Sites, WMStats.SiteTable);

WMStats.SiteModel.constructSiteKey = function(data) {
    /*
     * assemple keys from data for lasted site summary.
     * key format is [timestamp, site, agent_url]
     */
    var keys = [];
    for (var i in data.rows){
        keys.push([data.rows[i].value.max, data.rows[i].key[0], 
                   data.rows[i].key[1]]);
    }
    return keys;
}

WMStats.SiteModel.callback = function(siteKeys) {
    var options = {"keys": WMStats.SiteModel.constructSiteKey(siteKeys), "reduce": true, 
                       "group": true};

    WMStats.Couch.view('timeWithAgentSite', options, jQuery.proxy(this.dataReady, this), 'json')
}

