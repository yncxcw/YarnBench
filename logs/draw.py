import matplotlib.pyplot as pl
import numpy as np

def get_cdf(datas):
    datas = np.array(datas)
    count,bv=np.histogram(datas,bins=20)
    cdf=np.cumsum(count)
    cdf=cdf/float(count.sum())
    X=bv[1:]
    Y=cdf
    return X,Y
    

def get_time_series(fname):
    file=open(fname,"r")
    X=[]
    Y=[]
    for line in file.readlines():
        item=line.strip().split(",")
        X.append(float(item[0]))
        Y.append(float(item[1]))
    return X,Y

def draw_plot(X,Y):
    pl.figure()
    pl.plot(X,Y)


fname="./10GB_TPCH/queue_default_absoluteUsedCapacity.csv"
X,Y=get_time_series(fname)
draw_plot(X,Y)

X,Y=get_cdf(Y)
draw_plot(X,Y)


fname="./10GB_TPCH/queue_default_numApplications.csv"
X,Y=get_time_series(fname)
draw_plot(X,Y)

X,Y=get_cdf(Y)
draw_plot(X,Y)



pl.show()
    
