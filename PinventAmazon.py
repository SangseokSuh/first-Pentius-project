
# Pentius inventory update to the csv file for dropship inventory feed on Amazon   3/3/2023

import mysql.connector

con = mysql.connector.connect(
    host='localhost', port = 3305, 
    user='gone', password='fishing',
    database='pentius'
    )

import pandas as pd

# Load existing csv file into a pandas dataframe
existing_df = pd.read_csv('C:\\Users\\pentius\\Documents\\Inventory_DS_Amazon\\amazon-ds-inventory_230207.csv')

# Update the qty field to 0
existing_df['Available units'] = 0

# Write SQL query to select desired fields
query = """select s.sku as SKU, round(sum(s.qty),0) as qty
from (     
select a.sku as sku, 
       (case when sum(a.onhand - a.allo) > 60 then sum(a.onhand - a.allo) else 0 end) as qty
from
       (select p.num as sku, pco.QTY as onhand, 0 as allo
       from part p left outer join partcost pco on (p.id = pco.partid)
       where pco.qty > 0
         
       union all
       select productnum as sku, 0, sum(qtytofulfill) as allo
       from soitem
       where typeid = 10 and statusid = 10
       group by sku
       ) a
group by a.sku

union all
select (case when b.sku like 'PA%' then concat(b.sku, '-6PK')
             when b.sku like 'PC%' then concat(b.sku, '-6PK')
             when b.sku like 'PF%' then concat(b.sku, '-6PK')
             when b.sku like 'PH%' then concat(b.sku, '-6PK')
             when b.sku like 'PL%' then concat(b.sku, '-12PK')
             when b.sku like 'PW%' then concat(b.sku, '-10PK')
             else b.sku end
       ) as sku,
       (case when b.sku like 'PA%' and sum(b.onhand - b.allo)/6 > 10 then sum(b.onhand - b.allo)/6 
             when b.sku like 'PC%' and sum(b.onhand - b.allo)/6 > 10 then sum(b.onhand - b.allo)/6 
             when b.sku like 'PF%' and sum(b.onhand - b.allo)/6 > 10 then sum(b.onhand - b.allo)/6 
             when b.sku like 'PH%' and sum(b.onhand - b.allo)/6 > 10 then sum(b.onhand - b.allo)/6 
             when b.sku like 'PL%' and sum(b.onhand - b.allo)/12 > 5 then sum(b.onhand - b.allo)/12 
             when b.sku like 'PW%' and sum(b.onhand - b.allo)/10 > 6 then sum(b.onhand - b.allo)/10 
             else 0 end
       ) as qty
from
       (select p.num as sku, pco.QTY as onhand, 0 as allo
       from part p left outer join partcost pco on (p.id = pco.partid)
       where pco.qty > 0
         
       union all
       select productnum as sku, 0, sum(qtytofulfill) as allo
       from soitem
       where typeid = 10 and statusid = 10
       group by sku
       ) b
group by b.sku
) s
group by s.sku
 """

# Use pandas read_sql() method to fetch the result set and store it in a dataframe
df = pd.read_sql(query, con)

# Merge the dataframes based on the SKU key
merged_df = pd.merge(existing_df, df, on='SKU', how='left')

# Update the quantity in the merged dataframe and drop the added column
merged_df.loc[merged_df['qty'].notnull(), 'Available units'] = merged_df['qty']
merged_df.drop('qty', axis=1, inplace=True)

# Save the updated dataframe to the existing csv file 
merged_df.to_csv('C:\\Users\\pentius\\Documents\\Inventory_DS_Amazon\\amazon-ds-inventory_1.csv', index=False)

# Save the dataframe to the excel file 
df.to_excel('C:\\Users\\pentius\\Documents\\Inventory_DS_Amazon\\Pentius_inventory_1.xlxs', index=False)

# Close the connection to the MySQL database
con.close()