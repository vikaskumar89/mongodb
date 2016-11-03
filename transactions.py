from pymongo import Connection
from popularitem import ItemInfo,NewOrder,popular
from bson.objectid import ObjectId
import copy
import datetime
import os
from decimal import Decimal
import time
import operator
from operator import itemgetter
from pprint import pprint

class Transactions:
    def __init__(self,fid):
        print "Inside init"
        connection = MongoClient('localhost', 27017)
        #connection = Connection()
        db = connection['warehouse8']
        self.customer = db.customer
        self.stock = db.stock
        self.wdpayment = db.wdpayment
        self.order  = db.order
        self.dnum = 10
        self.fname = 'output/'+str(fid)+'-output.txt'
        if os.path.isfile(self.fname):
            os.remove(self.fname)
        self.file = open(self.fname,'a')
    
    def neworder(self,c,w,d,lis):
        print "Inside New Order"
        count = 1
        all_local = 1
        for j in lis:
            item = j.split(",")
            if w != item[1]:
                all_local = 0
                break
        total = Decimal(0)
        entry_d = datetime.datetime.now()
        bulk = self.stock.initialize_ordered_bulk_op()
        obulk = self.order.initialize_ordered_bulk_op()
        oid = 0
        for row in self.wdpayment.find({"D_ID" : d,"W_ID" : w },{"D_NEXT_O_ID":1}):
            oid = row['D_NEXT_O_ID']

        olist = []
        for j in lis:
            item = j.split(",")
            olist.append(int(item[0]))	

        itemdc = {}
        for row in self.stock.find({"W_ID":w,"I_ID":{"$in":olist}},{"I_PRICE":1,"I_NAME":1,"S_QUANTITY":1,"I_ID":1}):
            price = row['I_PRICE']
            name = row['I_NAME']
            squantity = row['S_QUANTITY']
            itemdc[row['I_ID']] = ItemInfo(price,name,squantity)

        count = 1
        itemout = ""

        wtax = Decimal(0)
        dtax = Decimal(0)
        cdiscount= Decimal(0)
        fname = ""
        mname = ""
        lname = ""
        csince = ''
        credit = Decimal(0)

        for row in self.customer.find({ "D_ID" : d, "C_ID" : c, "W_ID" : w },{"W_TAX":1,"D_TAX":1,"C_DISCOUNT":1,"C_FIRST_NAME":1,"C_MIDDLE_NAME":1,"C_SINCE":1,"C_CREDIT":1,"C_LAST_NAME":1}):
            wtax = row['W_TAX']             
            dtax = row['D_TAX']
            cdiscount = row['C_DISCOUNT']
            fname = row['C_FIRST_NAME']
            mname = row['C_MIDDLE_NAME']
            lname = row['C_LAST_NAME']
            csince = row['C_SINCE']
            credit = row['C_CREDIT']
        k = 1
        for j in lis:
            oldc = {}
            item = j.split(",")
            i_id = int(item[0])	
            sw_id = int(item[1])	
            i_quant = int(item[2])
            oldc["OL_I_ID"] = i_id
            count = count + 1
            oldc["OL_I_NAME"] = itemdc[i_id].name
            oldc["OL_ALL_LOCAL"] = all_local
            oldc["OL_SUPPLY_W_ID"] = sw_id
            oldc["OL_DELIVERY_D"] = None 
            oldc["OL_QUANTITY"] = i_quant
            oldc["OL_I_PRICE"] = itemdc[i_id].price
            oldc["OL_DIST_INFO"] = "S_DIST_"+str(d)
            amount = itemdc[i_id].price * i_quant
            total += amount
            oldc["OL_AMOUNT"] = amount
            #orderList.append(oldc)
            rcnt = 0 if sw_id == w else 1
            aquant = squantity-i_quant
            if aquant < 10:
                quant = 100-i_quant
                bulk.find({"W_ID":w, "I_ID":i_id}).update({"$inc":{"S_YTD":i_quant, "S_ORDER_CNT":1, "S_REMOTE_CNT":rcnt, "S_QUANTITY":quant}})
            else:
                quant = aquant
                bulk.find({"W_ID":w, "I_ID":i_id}).update({"$inc":{"S_YTD":i_quant, "S_ORDER_CNT":1, "S_REMOTE_CNT":rcnt, "S_QUANTITY":-i_quant}})
            itemout += str(i_id)+","+itemdc[i_id].name+","+str(sw_id)+","+str(amount)+","+str(i_quant)+","+str(quant)+"\n"
            record = {
                    "W_ID":w,
                    "D_ID":d,
                    "O_ID":oid,
                    "C_ID":c,
                    "C_FIRST_NAME":fname,
                    "C_MIDDLE_NAME":mname,
                    "C_LAST_NAME":lname,
                    "O_CARRIER_ID":None,
                    "O_ENTRY_D":datetime.datetime.now(),
                    "O_OL_CNT":len(lis),
                    "OL_NUMBER":k,
                    "ORDERLINE": oldc
            }
            k += 1
            obulk.insert(record)
        
        bulk.execute()
        obulk.execute()

        total= total * (1+Decimal(wtax)+Decimal(dtax)) * Decimal((1-cdiscount))


        self.wdpayment.update({"D_ID" : d, "W_ID" : w },{"$inc":{"D_NEXT_O_ID":1}})
        out = "Customer Identifier: "+str(w)+","+str(d)+","+str(c)+"\n"
        out += str(cdiscount)+","+str(csince)+","+str(credit)+"\n"
        out += str(wtax)+","+str(dtax)+"\n"
        out += str(oid)+","+str(entry_d)+"\n"
        out += str(len(lis))+","+str(total)+"\n"
        out += itemout+"\n"
        self.file.write(out)



    def delivery(self,w,carrier):
        print "Inside delivery:"
        orderdc = {}
        cntdc = {}
        custdc = {}
        oldc = {}
        for i in range(1,self.dnum+1):
            rows = self.order.find( {"W_ID": w,"D_ID": i ,"O_CARRIER_ID": None },{"O_ID":1,"O_OL_CNT":1,"C_ID":1} ).limit(1)
            for row in rows:
                orderdc[i] = row['O_ID']
                cntdc[i] = row['O_OL_CNT']
                custdc[i] = row['C_ID']
        amountdc = {}
        for did, oid in orderdc.iteritems():
            pipeline = [{ "$match": {     "$and": [         {"W_ID":2},         {"D_ID":1}, {"O_ID":5}     ] } }, { "$group": { "_id" : None, "sum" : {"$sum": "$ORDERLINE.OL_AMOUNT" } } }]
            result = self.order.aggregate(pipeline)
            #result = self.order.aggregate({ "$match": {     "$and": [         {"W_ID":2},         {"D_ID":1}, {"O_ID":5}     ] } }, { "$group": { "_id" : None, "sum" : {"$sum": "$ORDERLINE.OL_AMOUNT" } } })
            #result = self.order.aggregate({"$match":{"$and":[{"W_ID":2},{"D_ID":1}, {"O_ID":5}]}},{"$group":{"_id" : None,"sum" : {"$sum": "$ORDERLINE.OL_AMOUNT" } } })
            amount = result['result'][0]['sum']
            amountdc[did] = amount
    
                

        bulk = self.order.initialize_ordered_bulk_op()
        cbulk = self.customer.initialize_ordered_bulk_op()

        for did,oid in orderdc.iteritems():
            #bulk.find({"W_ID": w,"D_ID":did,"O_ID":oid}).update({"$set":{"O_CARRIER_ID":carrier}})
            bulk.find({"W_ID": w,"D_ID":did,"O_ID":oid}).update({"$set":{"O_CARRIER_ID":carrier, "ORDERLINE.OL_DELIVERY_D":datetime.datetime.now()}})
            cbulk.find({"W_ID": w, "D_ID":did, "C_ID":custdc[did]}).update({"$inc":{"C_BALANCE":amountdc[did],"C_DELIVERY_CNT":1}})
        bulk.execute()
        cbulk.execute() 




 
    def payment(self,c,w,d,payment):
        print "Inside Payment"
        self.wdpayment.update({"W_ID" : w },{"$inc":{"W_YTD":payment}},multi= True)
        self.wdpayment.update({"D_ID" : d, "W_ID" : w },{"$inc":{"D_YTD":payment}})
        self.customer.update({ "D_ID" : d, "C_ID" : c, "W_ID" : w },{"$inc":{"C_BALANCE":-payment, "C_YTD_PAYMENT":payment, "C_PAYMENT_CNT":1}})

        out = "Customer Identifier: "+str(w)+","+str(d)+","+str(c)+"\n"
        for row in self.customer.find( { "D_ID" : d, "C_ID" : c, "W_ID" : w }).limit(1):
            out += "Name :"+row['C_FIRST_NAME']+","+row['C_MIDDLE_NAME']+","+row['C_LAST_NAME']+"\n"
            out += row['C_STREET_1']+","+row['C_STREET_2']+","+row['C_CITY']+","+ row['C_STATE']+","+row['C_ZIP']+","+row['C_PHONE']+","+row['C_SINCE']+","+str(row['C_CREDIT'])+","+str(row['C_CREDIT_LIM'])+","+str(row['C_DISCOUNT'])+","+str(row['C_BALANCE'])+"\n"
            out += row['W_STREET_1']+","+row['W_STREET_2']+","+row['W_CITY']+","+ row['W_STATE']+","+row['W_ZIP']+"\n"
            out += row['D_STREET_1']+","+row['D_STREET_2']+","+row['D_CITY']+","+ row['D_STATE']+","+row['D_ZIP']+"\n"
            out += str(payment)
        self.file.write(out)

    def orderstatus(self, w, d, c):
        print "Inside OrderStatus"
        result = self.order.find({"W_ID":w,"D_ID":d,"C_ID":c}).sort("_id",-1).limit(1)
        out = ""
        for row in result:
            out = "Name :"+row['C_FIRST_NAME']+row['C_MIDDLE_NAME']+row['C_LAST_NAME']+"\n"
            out += str(row['O_ID'])+"\t"+str(row['O_CARRIER_ID'])+"\t"+str(row['O_ENTRY_D'])+"\n"
            item  = row['ORDERLINE']
            out += str(item['OL_I_ID'])+"\t"
            out += str(item['OL_SUPPLY_W_ID'])+"\t"
            out += str(item['OL_DELIVERY_D'])+"\t"
            out += str(item['OL_QUANTITY'])+"\t"
            out += str(item['OL_AMOUNT'])+"\n"
            self.file.write(out)
	
    def stocklevel(self,w,d,t,l):
        print "Inside stock level,Threshold is\t",w,d,t,l
        oid = 0
        for row in self.wdpayment.find({ "D_ID" : d,"W_ID" : w },{"D_NEXT_O_ID":1}):
            oid = row['D_NEXT_O_ID']
        oid = oid-l
        count = 0
        itemset = set()
        for row in self.order.find({"W_ID":w,"D_ID":d,"O_ID":{"$gt":oid}},{"ORDERLINE":1}):
            item = row['ORDERLINE']
            itemid = item["OL_I_ID"]
            if itemid in itemset:
                continue
            itemset.add(itemid)
        itemset = list(itemset)
        for row in self.stock.find({"W_ID":w,"I_ID":{"$in":itemset}},{"S_QUANTITY"}):
            if row['S_QUANTITY'] < t:
                count += 1
        out = "Total Item less than threshold is: "+str(count)
        self.file.write(out)
        

    def popularItem(self,w,d,l):
        print "Inside Popular Item"
        oid = 0
        for row in self.wdpayment.find({ "D_ID" : d,"W_ID" : w },{"D_NEXT_O_ID":1}):
            oid = row['D_NEXT_O_ID']
        oid = oid-l
        count = 0
        orderdc = dict()
        for row in self.order.find({"W_ID":w,"D_ID":d,"O_ID":{"$gt":oid}},{"ORDERLINE":1,"O_ID":1,"O_ENTRY_D":1,"C_ID":1,"C_FIRST_NAME":1,"C_MIDDLE_NAME":1,"C_LAST_NAME":1}):
            items= row['ORDERLINE']
            oid = row['O_ID']
            entry_d = row['O_ENTRY_D']
            cid = row['C_ID']
            fname = row['C_FIRST_NAME']
            mname = row['C_MIDDLE_NAME']
            lname = row['C_LAST_NAME']
            quan = items['OL_QUANTITY'] 
            item = items['OL_I_ID']
            name = items['OL_I_NAME']
            if oid in orderdc:
                pItem = orderdc[oid]
                if pItem.quantity < quan:
                    pItem.item = item
                    pItem.quantity = quan
                    pItem.entry = entry_d
                    pItem.name = name
                    orderdc[oid] = pItem

            else:
                pItem = popular(oid,item,name,quan,entry_d,cid,fname,mname,lname)
                orderdc[oid] = pItem
        out = ""
        for obj in orderdc.itervalues():
            out += "Order Id and Entry Date:"+str(obj.oid)+","+str(obj.entry)+"\n"
            out += "Customers who placed the order:"+obj.fname+","+obj.mname+","+obj.lname+"\n"
            out += "Item name and quantity of popular item:"+obj.name+","+str(obj.quantity)+"\n"
        self.file.write(out)
             
        

    def topbalance(self):
        print "Inside top balance"
        customer = self.customer
        result = customer.find({},{"C_FIRST_NAME":1,"C_MIDDLE_NAME":1,"C_LAST_NAME":1,"W_NAME":1,"D_NAME":1,"C_BALANCE":1}).sort("C_BALANCE",-1).limit(10)
        out =""
        for rows in result:
            out += rows['C_FIRST_NAME']+" "+rows['C_MIDDLE_NAME']+" "+rows['C_LAST_NAME']+"\t"+""+str(rows['C_BALANCE'])+"\n"
            out += rows['W_NAME']+":"+rows['D_NAME']+"\n"
        self.file.write(out)
   
