function(keys, values, rereduce) {
       if (rereduce) {
               return sum(values);
       } else {
               if (values.length > 0){
                       return values.length;
               }
       }
}

