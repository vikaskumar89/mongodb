from pymongo import Connection
import datetime
connection = Connection()
db = connection['warehouse8']
print "Connected to Database"
order= db.order
for x in xrange(1,11):
    for y in xrange(1,11):
        for z in xrange(1,11):
            k = 1
            ls = []
            while(k<=2):
                odc ={
                "OL_NUMBER":k,
                "OL_ALL_LOCAL":1,
                "OL_I_ID":z,
                "OL_I_NAME":"I_NAME",
                "OL_I_PRICE":11.00,
                "OL_SUPPLY_W_ID":x,
                "OL_DELIVERY_D":datetime.datetime.utcnow(),
                "OL_AMOUNT":11.00*k,
                "OL_QUANTITY":k,
                "OL_DIST_INFO":"OL_DIST_INFO"
                } 
                k = k+1
                ls.append(odc)
            record = {
                "W_ID":x,
                "D_ID":y,
                "O_ID":z,
                "C_ID":y,
                "C_FIRST_NAME":"Vikas",
                "C_MIDDLE_NAME":"Kumar",
                "C_LAST_NAME":"Sherawat",
                "O_CARRIER_ID":11,
                "O_ENTRY_D":datetime.datetime.utcnow(),
                "O_OL_CNT":2,
                "ORDERLINE": ls
                }
            ids = order.insert(record)
#order.createIndex({"W_ID":1,"D_ID":1,"O_ID":1})
print str(x*y*z), " Orders successfully Inserted "
