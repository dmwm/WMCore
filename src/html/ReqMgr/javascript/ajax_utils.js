function ajaxApproveRequests(base, method, ids) {
    // base is a URL base, e.g. https://cmsweb.cern.ch
    // method is request method, e.g. /request
    // ids are request ids which needs to be approved
    new Ajax.Updater('response', base+'/'+method,
    { method: 'get' ,
      parameters : {'ids': ids},
      onException: function() {return;},
      onComplete : function() {return;}
    });
}
