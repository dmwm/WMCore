rm database.lite
yes | mysqladmin -u root drop wmbs; mysqladmin -u root create wmbs
rm test.py.log; time python test.py 
