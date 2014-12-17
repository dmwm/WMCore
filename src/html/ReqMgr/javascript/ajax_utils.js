function ajaxRequest(path, parameters) {
    // path is an URI
    // parameters is JSON dict
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
          setTimeout(function(){
              var doc = document.getElementById('confirmation');
              doc.innerHTML='';
              doc.className='';
          }, 5000);
      },
      onComplete : function() {
          var doc = document.getElementById('confirmation');
          doc.innerHTML='SUCCESS! Your request has been proceesed';
          doc.className='tools-alert tools-alert-green confirmation shadow';
          setTimeout(function(){
              var doc = document.getElementById('confirmation');
              doc.innerHTML='';
              doc.className='';
          }, 5000);
      }
    });
}
