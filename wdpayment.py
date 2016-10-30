from pymongo import Connection
connection = Connection()
db = connection['warehouse8']
print "Connected to Database"
wdpayment= db.wdpayment
for i in xrange(1,11):
    w_ytd = 100.2+10*i
    for j in xrange(1,11):
        d_ytd = 10.2+j
        record = {
            "W_ID":i,
            "D_ID":j,
            "W_YTD":w_ytd,
            "D_YTD":d_ytd,
            "D_NEXT_O_ID":1
        }
        ids = wdpayment.insert(record)
        print ids
