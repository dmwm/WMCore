function cleanConfirmation () {
  var doc = document.getElementById('confirmation');
  doc.innerHTML='';
  doc.className='';
}
function ajaxRequest(path, parameters) {
    // path is an URI binded to certain server method
    // parameters is dict of parameters passed to the server function
    new Ajax.Updater('response', path,
    { method: 'post' ,
      parameters : parameters,
      onCreate: function() {
          var doc = document.getElementById('confirmation');
          doc.innerHTML='Your request has been submitted';
          doc.className='tools-alert tools-alert-blue confirmation shadow';
      },
      onException: function() {
          var doc = document.getElementById('confirmation');
          doc.innerHTML='ERROR! Your request has been failed';
          doc.className='tools-alert tools-alert-red confirmation shadow';
          setTimeout(cleanConfirmation, 5000);
      },
      onComplete : function(response) {
          var doc = document.getElementById('confirmation');
          if  (response.status==200 || response.status==201) {
              doc.innerHTML='SUCCESS! Your request has been processed with status '+response.status;
              doc.className='tools-alert tools-alert-green confirmation shadow';
          } else {
              doc.innerHTML='WARNING! Your request has been processed with status '+response.status;
              doc.className='tools-alert tools-alert-yellow confirmation shadow';
          }
          setTimeout(cleanConfirmation, 5000);
      }
    });
}
