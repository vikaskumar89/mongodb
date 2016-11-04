use d8
db.customer.createIndex( { "W_ID": 1, "D_ID": 1,"C_ID":1 } )
db.stock.createIndex( { "W_ID": 1, "I_ID": 1} )
db.wdpayment.createIndex( { "W_ID": 1} )
db.wdpayment.createIndex( { "W_ID": 1, "D_ID": 1} )
db.order.createIndex( { "W_ID": 1, "D_ID": 1,"O_ID":-1} )
db.order.createIndex( { "W_ID": 1, "D_ID": 1,"C_ID":-1} )
db.order.createIndex( { "W_ID": 1, "D_ID": 1,"O_CARRIER_ID":1} )
db.customer.ensureIndex({"C_BALANCE":-1})
