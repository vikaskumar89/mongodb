from pymongo import Connection
connection = Connection()
db = connection['warehouse8']
print "Connected to Database"
stock= db.stock
for i in xrange(1,11):
    w_ytd = 100+10*i
    for j in xrange(1,101):
        record = {
            "_id":{"W_ID":i,"I_ID":j},
            "S_QUANTITY":101,
            "S_YTD":99,
            "S_ORDER_CNT":11,
            "S_REMOTE_CNT":11,
            "S_DIST_01":"S_DIST_01",
            "S_DIST_02":"S_DIST_02",
            "S_DIST_03":"S_DIST_03",
            "S_DIST_04":"S_DIST_04",
            "S_DIST_05":"S_DIST_05",
            "S_DIST_06":"S_DIST_06",
            "S_DIST_07":"S_DIST_07",
            "S_DIST_08":"S_DIST_08",
            "S_DIST_09":"S_DIST_09",
            "S_DIST_10":"S_DIST_10",
            "S_DATA":"S_DATA",
            "I_PRICE":111,
            "I_NAME":"I_NAME",
            "I_DATA":"I_DATA"}
        ids = stock.insert(record)
print str(j)," items inserted in each ,",str(i), " warehouse"
