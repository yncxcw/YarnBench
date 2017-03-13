

name="pagerank2-2.log"
cont="1488909341469_0002"

fr=open(name,"r")
fw=open(name+"_p","w")



for line in fr.readlines():
    if cont in line and "ContainerImpl" in line:
        fw.write(line) 



fr.close()
fw.close()


