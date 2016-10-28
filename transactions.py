from pymongo import Connection
import copy
import datetime
import os
from decimal import Decimal
import time
import operator
from operator import itemgetter

class Transactions:
    def _init__(self,fid):
        print "Inside init"
        self.connection = Connection()
        self.db = self.connection['warehouse8']
    
    
    def neworder(self,c,w,d,lis):
        print "Inside New Order"

    def delivery(self,w,carrier):
        print "Inside delivery"
    
    def payment(self,c,w,d,payment):
        print "Inside Payment"
    
    def orderstatus(self, w, d, c):
        print "Inside OrderStatus"
	
    def stocklevel(self,w,d,t,l):
        print "Inside stock level"

    def popularItem(self,w,d,l):
        print "Inside Popular Item"

    def topbalance(self):
        print "Inside top balance"

   
