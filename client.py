from __future__ import division
import os
import sys
import time
from transactions import Transactions
import threading
import time
import traceback

class MyThread (threading.Thread):

        def __init__(self):
                threading.Thread.__init__(self)
		self.countfile = 'count.txt'

        def run(self, id):
		self.id = id
		start = time.time()
		count = 0
		try:
			fname = os.getcwd()+'/testdata/'+str(self.id)+'.txt'
                	count = self.readFile(fname)
		except Exception as e:
			print "Thread:"+str(self.id)+" interrupted"
			traceback.print_exc()
		finally:
			rtime = time.time()-start
			throughput =count/rtime
			out = "File finished:"+str(self.id)+","+str(count)+","+str(rtime)+","+str(throughput)
			print out
		with open(self.countfile, "a") as f:
			    f.write(str(count)+","+str(throughput) + '\n')


	def readFile(self,fname):
		with open(fname) as f:
			content = [x.strip('\n') for x in f.readlines()]
		t = Transactions()
		i = 0
		tcount = 0
		while i < len(content):
			transx = content[i].split(",");
			transType = transx[0]
			tcount =  tcount + 1
			if transType == 'N':
				M = int(transx[4])
				c = int(transx[1])
				w = int(transx[2])
				d = int(transx[3])
				lis = content[i+1:i+M+1]
				t.neworder(c,w,d,lis)
				i = i + M
			elif transType == 'D':
				w = int(transx[1])
				c = int(transx[2])
				t.delivery(w,c)
			elif transType == 'P':
				w = int(transx[1])
				d = int(transx[2])
				c = int(transx[3])
				p = int(float(transx[4]))
				t.payment(c,w,d,p)
			elif transType == 'S':
				w = int(transx[1])
				d = int(transx[2])
				th  = int(transx[3])
				l = int(transx[4])
				t.stocklevel(w,d,th,l)
			elif transType == 'O':
				w = int(transx[1])
				d = int(transx[2])
				c = int(transx[3])
				t.orderstatus(w,d,c)
			elif transType == 'I':
				w = int(transx[1])
				d = int(transx[2])
				l = int(transx[3])
				t.popularItem(w,d,l)
			elif transType == 'T':
				a = 0
				t.topbalance()
			else:
				print 'Input Mistmatch'
			i = i+1;i
		#if t.ntime != 0:
		#	print self.id,"\tNew Order:\t",t.ntime,"\t\t",t.nc,"\t",t.nc/t.ntime
		#if t.ptime != 0:
		#	print self.id,"\tPayment :\t",t.ptime,"\t\t",t.pc,"\t",t.pc/t.ptime
		#if t.dtime != 0:
		#	print self.id,"\tDelivery:\t",t.dtime,"\t\t",t.dc,"\t",t.dc/t.dtime
		#if t.otime != 0:
		#	print self.id,"\tOrder Status:\t",t.otime,"\t\t",t.oc,"\t",t.oc/t.otime
		#if t.stime != 0:
		#	print self.id,"\tStock Level:\t",t.stime,"\t\t",t.sc,"\t",t.sc/t.stime
		#if t.Itime != 0:
		#	print self.id,"\tPopular Item:\t",t.Itime,"\t\t",t.Ic,"\t",t.Ic/t.Itime
		#if t.Ttime != 0:
		#	print self.id,"\tTop Balance:\t",t.Ttime,"\t\t",t.Tc,"\t",t.Tc/t.Ttime
		return tcount;

class MainThread:
        totaltime =0.0
        totalcount =0
        starttime = 0.0
        totalfiles =0
        filesfinish = 0


