
# Pentius inventory update to the text file for Hanson inventory feed on eBay2   2/16/2023

import mysql.connector

con = mysql.connector.connect(
    host='localhost', port = 3305, 
    user='gone', password='fishing',
    database='pentius'
    )

import pandas as pd

# Load existing excel file into a pandas dataframe
existing_df = pd.read_excel('C:\\Users\\pentius\\Documents\\Inventory_Hanson\\weekly_AISIN_GMB_PENTIUS\\PEN_inventory_Hanson.xlsx')

# Update the qty field to 0
existing_df['QTYONHAND'] = 0

# Write SQL query to select desired fields
query = """select b.partno as PARTNUMBER, cast((b.onhand2 - b.allo2) as unsigned) as AVAILABLE
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

# Use pandas read_sql() method to fetch the result set and store it in a dataframe
df = pd.read_sql(query, con)

# Merge the dataframes based on the partnumber key
merged_df = pd.merge(existing_df, df, on='PARTNUMBER', how='left')

# Update the quantity in the merged dataframe and drop the added column
merged_df.loc[merged_df['AVAILABLE'].notnull(), 'QTYONHAND'] = merged_df['AVAILABLE']
merged_df.drop('AVAILABLE', axis=1, inplace=True)

# Save the updated dataframe to the existing excel file and the text file
merged_df.to_excel('C:\\Users\\pentius\\Documents\\Inventory_Hanson\\weekly_AISIN_GMB_PENTIUS\\PEN_inventory_Hanson.xlsx', index=False)
merged_df.to_csv('C:\\Users\\pentius\\Documents\\Inventory_Hanson\\PEN_inventory_Hanson.txt', sep='\t', header=False, index=False)

# Close the connection to the MySQL database
con.close()