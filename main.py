from multiprocessing import Pool,Process
# import pathos.multiprocessing as mp
import time
import datetime
from client import MyThread, MainThread
import sys
import os.path


if __name__ == '__main__':
    MainThread.starttime = time.time()
    start_time = time.time()
    begin = int(sys.argv[1])
    end = int(sys.argv[2])

    totalfiles = end + 1 - begin
    countfile = 'count.txt'
    if os.path.isfile(countfile):
	os.remove(countfile)
    ls = []
    print "Spawned ",totalfiles," process simultaneously at ",datetime.datetime.now()
    print "======================================================================================================================="
    for i in xrange(begin, end+1):
	thread = MyThread()
	process = Process(target=thread.run, args=(i,))
	process.start()
	ls.append(process)
    
    for i in ls:
	i.join()

    end_time = time.time()
    timetaken = end_time - start_time
    thru = []
    with open(countfile, 'r') as f:
	content = [x.strip('\n') for x in f.readlines()] 
    count= 0
    for c in content:
	    thru.append(float(c.split(',')[1]))
	    val = int(c.split(',')[0])
            count += val
    print "======================================================================================================================="
    print "|\t\t\t\t\t\tFINAL RESULT\t\t\t\t\t\t\t\t"
    print "|"
    print "|"
    print "|\tMax Throughput:\t\t",max(thru)
    print "|\tMin Throughput:\t\t",min(thru)
    print "|\tAvg Throughput:\t\t",sum(thru)/len(thru)
    print "|\tTotal transacation:\t",count
    print "|\tTotal Time taken:\t",timetaken
    print "|\tTotal Throughput:\t",count/timetaken
    print "======================================================================================================================="
