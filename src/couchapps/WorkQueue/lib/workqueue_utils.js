// Useful functions for workqueue couchapp

var workqueue_utils = {

	commonInputDataSites: function(element) {
		// Determine sites which contain all the input data (inc. parents)
		var sites = this.commonSites(element, "Inputs");
		if (element.ParentData.length) {
			var parents = this.commonSites(element, "ParentData");
			for (var i = 0; i < sites.length; i++) {
    			if (parents.indexOf(sites[i]) === -1) {
    				sites.splice(i, 1);
    				continue;
    			}
    		}
		}
		return sites;		
	},

	commonInputParentDataSites: function(element) {
		// Determine sites which contain all the input parent data
		return this.commonSites(element, "ParentData");
	},

	commonSites: function(element, label) {
		// return common sites in element.label structure
    	var common_sites = [];
    	var first = true
    	for (var data in element.Inputs) {
    		if (!element.Inputs.hasOwnProperty(data)) {
    			continue;
    		}
    		var locations = element[label][data];
    		if (first) {
    			common_sites = locations;
    			first = false;
    			continue;
    		} else {
    			// remove any site that doesnt host a data item
    			for (var i = 0; i < common_sites.length; i++) {
    				if (locations.indexOf(common_sites[i]) === -1) {
    					common_sites.splice(i, 1);
    					continue;
    				}
    			}
    		}
    	}
    	return common_sites
    },

};

//CommonJS bindings
if( typeof(exports) === 'object' ) {
   exports.commonInputDataSites = workqueue_utils.commonInputDataSites;
};