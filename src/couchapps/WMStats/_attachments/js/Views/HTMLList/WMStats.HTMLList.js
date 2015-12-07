WMStats.namespace("HTMLList");

WMStats.HTMLList = function(format) {

    this._format = format;
};

WMStats.HTMLList.prototype = {
    create: function (data, containerDiv) {
         $(containerDiv).html(this._format(data));
    }
};
