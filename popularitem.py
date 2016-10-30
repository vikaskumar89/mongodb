class PopularItem:
	
	def __init__(self,oid,item,name,quantity,entry,cid,fname,mname,lname):
		self.oid = oid
		self.item = item
		self.name = name
		self.quantity = quantity
		self.entry = entry
		self.cid = cid
        self.fname = fname
        self.lname = lname
        self.mname = mname


class ItemInfo:

	def __init__(self,item,name,count):
		self.item = item
		self.name = name
		self.count = count

class NewOrder:

	def __init__(self,itemid,name,sw_id,quantity,amount,stock):
		self.itemid = itemid
		self.name = name
		self.sw_id = sw_id
		self.quantity = quantity
		self.amount = amount
		self.stock = stock

class Balance:
	
	def __init__(self, w,d,c,bal):
		self.w = w
		self.d = d
		self.c = c
		self.bal = bal
	def __cmp__(self, other):
		return cmp(self.bal, other.bal)


