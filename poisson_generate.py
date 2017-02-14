#!/bin/python
import math
import random
import time
def test():
    p = 1.0
    k = 0
    e = math.exp(-1)

    while p>=e:
        u = random.random()
        p*=u
        k+=1

    k=k-1

    return k

i = 100

while i > 0:

    print test()
    time.sleep(5)
    i=i-1


