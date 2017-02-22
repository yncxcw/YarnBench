import matplotlib.pyplot as pl
import numpy as np

def draw_cdf(datas):
    datas = np.array(datas)
    count,bv=np.histogram(datas,bins=20)
    cdf=np.cumsum(count)
    cdf=cdf/float(count.sum())
    X=bv[1:]
    Y=cdf
    pl.figure()
    pl.plot(X,Y)
    



def draw_time_series(fname):
    file=open(fname,"r")
    X=[]
    Y=[]
    for line in file.readlines():
        item=line.strip().split(",")
        X.append(float(item[0]))
        Y.append(float(item[1]))
    pl.figure()
    pl.plot(X,Y)


fname="./10GB_TPCH/queue_default_absoluteUsedCapacity.csv"
draw_time_series(fname)

fname="./10GB_TPCH/queue_default_numApplications.csv"
draw_time_series(fname)

pl.show()
    
