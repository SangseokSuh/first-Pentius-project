#  generate Pentius Inventory file for APW from MySQL database

import mysql.connector
 
con = mysql.connector.connect(
    host='localhost', port = 3305, 
    user='gone', password='fishing',
    database='pentius'
    )

cur = con.cursor()

# Write SQL query to select desired fields
select1 = """select b.partno, cast((b.onhand2 - b.allo2) as unsigned) as available 
from 
    (select a.partno, sum(a.onhand) as onhand2, sum(a.allo) as allo2 
     from 
       (select p.num as partno, pco.QTY as onhand, 0 as allo 
       from part p left outer join customfieldview pty on (pty.cfid = 55 and p.id = pty.RECORDID)  
            left outer join customfieldview srg on (srg.cfid = 56 and p.id = srg.RECORDID) 
            left outer join customfieldview sst on (sst.cfid = 4 and p.id = sst.RECORDID)  
            left outer join partcost pco on (p.id = pco.partid) 
       where pty.info in ('A','C','F','H','L','T','W') 
         and p.num not like 'PWC%' 
         and srg.info = 'U' 
         and pco.qty > 0 
         
       union all 
       select productnum as partno, 0 as onhand, sum(qtytofulfill) as allo 
       from soitem 
       where typeid = 10 and statusid = 10 
       group by productnum 
       ) a 
     group by a.partno 
     ) b 
where b.onhand2 > b.allo2 
order by b.partno
 """

cur.execute(select1)

f = open("C:\\Users\\pentius\\Documents\\Inventory_APW\\Pentiusinventory.txt", "w")

for i in range(len(cur.description)):
    desc = cur.description[i]
    f.write(desc[0].ljust(16))

fieldIndices = range(len(cur.description))
for row in cur:
    f.write('\n') 
    for fieldIndex in fieldIndices:
        fieldValue = str(row[fieldIndex])
        if fieldIndex == 0:
          f.write(fieldValue.ljust(16))  
        else:
          f.write(fieldValue.rjust(9))

f.close()