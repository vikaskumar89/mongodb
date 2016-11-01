from pymongo import Connection
connection = Connection()
db = connection['warehouse8']
print "Connected to Database"
customer= db.customer
count = 1
for i in xrange(1,11):
    for j in xrange(1,11):
        record = {
        "W_ID":1,
        "D_ID":j,
        "C_ID":i,
        "C_FIRST_NAME":"Vikas"+str(i),
        "C_MIDDLE_NAME":"Kumar",
        "C_LAST_NAME":"Sherawat",
        "C_STREET_1":"Street1",
        "C_STREET_2":"Street2",
        "C_CITY":"Delhi",
        "C_STATE":"Delhi",
        "C_ZIP":"110017",
        "C_PHONE":"123476",
        "C_SINCE":"2001",
        "C_CREDIT":10,
        "C_CREDIT_LIM":90,
        "C_DISCOUNT":.15,
        "C_BALANCE":500.0+count,
        "C_YTD_PAYMENT":700.0,
        "C_PAYMENT_CNT":34,
        "C_DELIVERY_CNT":34,
        "C_DATA":"random data",
        "W_NAME":"W1",
        "W_TAX":.05,
        "W_STREET_1":"W_STREET_1",
        "W_STREET_2":"W_STREET_2",
        "W_CITY":"W_CITY",
        "W_STATE":"W_STATE",
        "W_ZIP":"W_ZIP",
        "D_NAME":"D"+str(j),
        "D_TAX":.03,
        "D_STREET_1":"D_STREET_1",
        "D_STREET_2":"D_STREET_2",
        "D_CITY":"D_CITY",
        "D_STATE":"D_STATE",
        "D_ZIP":"D_ZIP"
        }
        ids = customer.insert(record)
        print ids
        count += 1
