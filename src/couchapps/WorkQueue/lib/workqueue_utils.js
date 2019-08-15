// Useful functions for workqueue couchapp

var workqueue_utils = {

	commonInputDataSites: function(element) {
		// Determine sites which contain all the input data
		if (element["NoInputUpdate"] === true) {
			return element["SiteWhitelist"];
		}
		return this.commonSites(element, "Inputs");
	},

	commonInputParentDataSites: function(element) {
		// Determine sites which contain all the input parent data
		if (element["NoInputUpdate"] === true) {
			return element["SiteWhitelist"];
		}
		return this.commonSites(element, "ParentData");
	},

	commonPileupDataSites: function(element) {
		// Determine sites which contain something of the pileup data
		if (element["NoPileupUpdate"] === true) {
			return element["SiteWhitelist"];
		}
        return this.commonSites(element, "PileupData");
	},

	commonSites: function(element, label) {
	    // just need to find ONE common site
		var site_white_list = element["SiteWhitelist"];
		for (var data in element[label]) {
			var common_sites = [];
			if (!element[label][data].length) {
				// input data has no locations
				return common_sites;
			}
			var locations = element[label][data];
			// remove any site that doesnt host a data item
			for (var i = 0; i < locations.length; i++) {
				if (site_white_list.indexOf(locations[i]) > -1) {
					common_sites.push(locations[i]);
					break;
				}
			}
			if (!common_sites.length) {
				return common_sites;
			}
		}
		// everything is fine then
		return site_white_list;
	},

};

//CommonJS bindings
if( typeof(exports) === 'object' ) {
	exports.commonInputDataSites = workqueue_utils.commonInputDataSites;
	exports.commonInputParentDataSites = workqueue_utils.commonInputParentDataSites;
	exports.commonPileupDataSites = workqueue_utils.commonPileupDataSites;
};