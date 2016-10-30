from pymongo import Connection
from popularitem import PopularItem,ItemInfo,NewOrder
from bson.objectid import ObjectId
import copy
import datetime
import os
from decimal import Decimal
import time
import operator
from operator import itemgetter

class Transactions:
    def __init__(self):
        print "Inside init"
        connection = Connection()
        db = connection['warehouse8']
        self.customer = db.customer
        self.stock = db.stock
        self.wdpayment = db.wdpayment
        self.order  = db.order
    
    def neworder(self,c,w,d,lis):
        print "Inside New Order"

    def delivery(self,w,carrier):
        print "Inside delivery"
    
    def payment(self,c,w,d,payment):
        print "Inside Payment"
    
    def orderstatus(self, w, d, c):
        print "Inside OrderStatus"
	
    def stocklevel(self,w,d,t,l):
        print "Inside stock level,Threshold is\t",t
        oid = 0
        for row in self.wdpayment.find({"_id" : { "D_ID" : 1,"W_ID" : 1 }},{"D_NEXT_O_ID":1}):
            oid = row['D_NEXT_O_ID']
        oid = oid-l
        count = 0
        itemset = set()
        for row in self.order.find({"W_ID":1,"D_ID":1,"O_ID":{"$gt":oid}},{"ORDERLINE":1}):
            orderline = row['ORDERLINE']
            for item in orderline:
                itemid = item['OL_I_ID']
                if itemid in itemset:
                    continue
                itemset.add(itemid)
        itemset = list(itemset)
        for row in self.stock.find({"W_ID":w,"I_ID":{"$in":itemset}},{"S_QUANTITY"}):
            if row['S_QUANTITY'] < t:
                count += 1
        print "Total Item less than threshold is: ",count
        

    def popularItem(self,w,d,l):
        print "Inside Popular Item"
        oid = 0
        for row in self.wdpayment.find({"_id" : { "D_ID" : 1,"W_ID" : 1 }},{"D_NEXT_O_ID":1}):
            oid = row['D_NEXT_O_ID']
        oid = oid-l
        count = 0
        orderdc = dict()
        for row in self.order.find({"W_ID":1,"D_ID":1,"O_ID":{"$gt":oid}},{"ORDERLINE":1,"O_ID":1,"O_ENTRY_D":1,"C_ID":1,"C_FIRST_NAME":1,"C_MIDDLE_NAME":1,"C_LAST_NAME":1}):
            orderline = row['ORDERLINE']
            oid = row['O_ID']
            entry_d = row['O_ENTRY_D']
            cid = row['C_ID']
            fname = row['C_FIRST_NAME']
            mname = row['C_MIDDLE_NAME']
            lname = row['C_LAST_NAME']
            for items in orderline:
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
                    pItem = PopularItem(oid,item,name,quan,entry_d,cid,fname,mname,lname)
                    orderdc[oid] = pItem
        out = ""
        for obj in orderdc.itervalues():
            out += "Order Id and Entry Date:"+str(obj.oid)+","+str(obj.entry)+"\n"
            out += "Customers who placed the order:"+obj.fname+","+obj.mname+","+obj.lname+"\n"
            out += "Item name and quantity of popular item:"+obj.name+","+str(obj.quantity)+"\n"
        print out
             
        

    def topbalance(self):
        print "Inside top balance"
        customer = self.customer
        result = customer.find({},{"C_FIRST_NAME":1,"C_MIDDLE_NAME":1,"C_LAST_NAME":1,"W_NAME":1,"D_NAME":1,"C_BALANCE":1}).sort("C_BALANCE",-1).limit(2)
        out =""
        for rows in result:
            out += rows['C_FIRST_NAME']+" "+rows['C_MIDDLE_NAME']+" "+rows['C_LAST_NAME']+"\t"+""+str(rows['C_BALANCE'])+"\n"
            out += rows['W_NAME']+":"+rows['D_NAME']+"\n"
        print out
   
