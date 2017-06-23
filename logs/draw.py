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


def get_queue_run(fname):
    file=open(fname,"r")
    X=[]
    Y=[]
    Z=[]
    count=0
    for line in file.readlines():
        item=line.strip().split(",")
        X.append(count)
        Y.append(int(item[3]))
        Z.append(int(item[4]))
        count=count+1
    return X,Y,Z

def draw_plot(X,Y):
    pl.figure()
    pl.plot(X,Y)


def draw_plots(X,Y,Z):
    pl.figure()
    pl.plot(X,Y)
    pl.plot(X,Z)

fname="./SPARK_2/job_default.csv"
X,Y,Z=get_queue_run(fname)
draw_plots(X,Y,Z)
pl.show()
    
