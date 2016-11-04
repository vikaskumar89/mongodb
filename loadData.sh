cd /temp/mongodb/mongo/bin/                                                                                                                                                                             
./mongoimport --db d8 --collection order --type csv --headerline --file /temp/mongodb/d8/table1.csv
./mongoimport --db d8 --collection customer --type csv --headerline --file /temp/mongodb/d8/table2.csv
./mongoimport --db d8 --collection wdpayment --type csv --headerline --file /temp/mongodb/d8/table3.csv
./mongoimport --db d8 --collection stock --type csv --headerline --file /temp/mongodb/d8/table4.csv
