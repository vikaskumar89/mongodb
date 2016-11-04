from pymongo import Connection,MongoClient
from popularitem import ItemInfo,NewOrder,popular
from bson.objectid import ObjectId
import copy
import datetime
import os
from decimal import Decimal
import time
import operator
from operator import itemgetter

class Transactions:
    def __init__(self,fid):
        #Node 26: 192.168.48.245
        connection = MongoClient('localhost', 27017)
        db = connection['d8']
        self.customer = db.customer
        self.stock = db.stock
        self.wdpayment = db.wdpayment
        self.order  = db.order
        self.dnum = 10
        self.ntime = 0.0
        self.ptime = 0.0
        self.dtime = 0.0
        self.otime = 0.0
        self.stime = 0.0
        self.Itime = 0.0
        self.Ttime = 0.0
        self.nc = 0
        self.pc = 0
        self.dc = 0
        self.oc = 0
        self.sc = 0
        self.Ic = 0
        self.Tc = 0
        self.fname = 'output/'+str(fid)+'-output.txt'
        if os.path.isfile(self.fname):
            os.remove(self.fname)
        self.file = open(self.fname,'a')
    
    def neworder(self,c,w,d,lis):
        ti = time.time()
        self.nc += 1
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
        for row in self.wdpayment.find({"W_ID" : w,"D_ID" : d },{"D_NEXT_O_ID":1}).limit(1):
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

        for row in self.customer.find({ "W_ID" : w, "D_ID" : d, "C_ID" : c },{"W_TAX":1,"D_TAX":1,"C_DISCOUNT":1,"C_FIRST_NAME":1,"C_MIDDLE_NAME":1,"C_SINCE":1,"C_CREDIT":1,"C_LAST_NAME":1}):
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
            total += Decimal(amount)
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


        self.wdpayment.update({"W_ID" : w, "D_ID" : d },{"$inc":{"D_NEXT_O_ID":1}})
        out = "Customer Identifier: "+str(w)+","+str(d)+","+str(c)+"\n"
        out += str(cdiscount)+","+str(csince)+","+str(credit)+"\n"
        out += str(wtax)+","+str(dtax)+"\n"
        out += str(oid)+","+str(entry_d)+"\n"
        out += str(len(lis))+","+str(total)+"\n"
        out += itemout+"\n"
        self.file.write(out)
        self.ntime += time.time()-ti



    def delivery(self,w,carrier):
        ti = time.time()
        self.dc += 1
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
        sti = time.time()
        #print "Time taken in selecting oldest order:",sti-ti
        for did, oid in orderdc.iteritems():
            pipeline = [{ "$match": {     "$and": [         {"W_ID":w},         {"D_ID":did}, {"O_ID":oid}     ] } }, { "$group": { "_id" : None, "sum" : {"$sum": "$ORDERLINE.OL_AMOUNT" } } }]
            result = self.order.aggregate(pipeline)
            amount = result['result'][0]['sum']
            amountdc[did] = amount
    
                
        tti = time.time()
        #print "Time taken in calculating the total amount: ",tti-sti

        bulk = self.order.initialize_ordered_bulk_op()
        cbulk = self.customer.initialize_ordered_bulk_op()

        for did,oid in orderdc.iteritems():
            bulk.find({"W_ID": w,"D_ID":did,"O_ID":oid}).update({"$set":{"O_CARRIER_ID":carrier, "ORDERLINE.OL_DELIVERY_D":datetime.datetime.now()}})
            cbulk.find({"W_ID": w, "D_ID":did, "C_ID":custdc[did]}).update({"$inc":{"C_BALANCE":amountdc[did],"C_DELIVERY_CNT":1}})
        try:
            bulk.execute()
            cbulk.execute()
        except Exception:
            out = 'Exception Occured in bulk execution'
            self.file.write(out)
        finally:
            #print "Time taken in bulk update: ",time.time()-ti
            self.dtime += time.time()-ti


    def payment(self,c,w,d,payment):
        ti = time.time()
        self.pc += 1
        self.wdpayment.update({"W_ID" : w },{"$inc":{"W_YTD":payment}},multi= True)
        self.wdpayment.update({"W_ID" : w, "D_ID" : d },{"$inc":{"D_YTD":payment}})
        self.customer.update({ "W_ID" : w, "D_ID" : d, "C_ID" : c },{"$inc":{"C_BALANCE":-payment, "C_YTD_PAYMENT":payment, "C_PAYMENT_CNT":1}})

        out = "Customer Identifier: "+str(w)+","+str(d)+","+str(c)+"\n"
        for row in self.customer.find( { "W_ID" : w, "D_ID" : d, "C_ID" : c }).limit(1):
            out += "Name :"+row['C_FIRST_NAME']+","+row['C_MIDDLE_NAME']+","+row['C_LAST_NAME']+"\n"
            out += row['C_STREET_1']+","+row['C_STREET_2']+","+row['C_CITY']+","+ row['C_STATE']+","+str(row['C_ZIP'])+","+str(row['C_PHONE'])+","+row['C_SINCE']+","+str(row['C_CREDIT'])+","+str(row['C_CREDIT_LIM'])+","+str(row['C_DISCOUNT'])+","+str(row['C_BALANCE'])+"\n"
            out += row['W_STREET_1']+","+row['W_STREET_2']+","+row['W_CITY']+","+ row['W_STATE']+","+str(row['W_ZIP'])+"\n"
            out += row['D_STREET_1']+","+row['D_STREET_2']+","+row['D_CITY']+","+ row['D_STATE']+","+str(row['D_ZIP'])+"\n"
            out += str(payment)
        self.file.write(out)
        self.ptime += time.time()-ti

    def orderstatus(self, w, d, c):
        ti = time.time()
        self.oc += 1
        result = self.order.find({"W_ID":w,"D_ID":d,"C_ID":c}).limit(1)
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
            self.otime += time.time()-ti

	
    def stocklevel(self,w,d,t,l):
        ti = time.time()
        self.sc += 1
        oid = 0
        for row in self.wdpayment.find({ "W_ID" : w,"D_ID" : d },{"D_NEXT_O_ID":1}).limit(1):
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
        self.stime += time.time()-ti
        

    def popularItem(self,w,d,l):
        ti = time.time()
        self.Ic += 1
        oid = 0
        for row in self.wdpayment.find({"W_ID" : w,"D_ID" : d },{"D_NEXT_O_ID":1}).limit(1):
            oid = row['D_NEXT_O_ID']
        oid = oid-l
        count = 0
        orderset = set()
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
            if oid in orderset:
                pItem = orderdc[oid]
                if pItem.quantity < quan:
                    pItem.item = item
                    pItem.quantity = quan
                    pItem.entry = entry_d
                    pItem.name = name
                    orderdc[oid] = pItem

            else:
                orderset.add(oid)
                pItem = popular(oid,item,name,quan,entry_d,cid,fname,mname,lname)
                orderdc[oid] = pItem
        out = ""
        for obj in orderdc.itervalues():
            out += "Order Id and Entry Date:"+str(obj.oid)+","+str(obj.entry)+"\n"
            out += "Customers who placed the order:"+obj.fname+","+obj.mname+","+obj.lname+"\n"
            out += "Item name and quantity of popular item:"+obj.name+","+str(obj.quantity)+"\n"
        self.file.write(out)
        self.Itime += time.time()-ti
             
        

    def topbalance(self):
        ti = time.time()
        self.Tc += 1
        customer = self.customer
        result = customer.find({},{"C_FIRST_NAME":1,"C_MIDDLE_NAME":1,"C_LAST_NAME":1,"W_NAME":1,"D_NAME":1,"C_BALANCE":1}).sort("C_BALANCE",-1).limit(10)
        out =""
        for rows in result:
            out += rows['C_FIRST_NAME']+" "+rows['C_MIDDLE_NAME']+" "+rows['C_LAST_NAME']+"\t"+""+str(rows['C_BALANCE'])+"\n"
            out += rows['W_NAME']+":"+rows['D_NAME']+"\n"
        self.file.write(out)
        self.Ttime += time.time()-ti
   
