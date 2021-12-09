import pyrap.tables as pt

oldtable = "/scratch/jason/chiles_original.ms"
newtable = "/scratch/jason/chiles_adios.ms"
t = pt.table(oldtable)
dmdef = t.getdminfo()

print("Original Table: **************************")
for i in dmdef:
    print(i)
    print(dmdef[i])

dmdef["*17"]["TYPE"] = "AdiosStMan"
print("New Table: ***********************")
for i in dmdef:
    print(i)
    print(dmdef[i])

t.copy(newtable, True, True, dminfo=dmdef)
