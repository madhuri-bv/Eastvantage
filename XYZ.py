import pandas as pd
import sqlite3

# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect("assignment.db")
df_sales = pd.read_sql_query("SELECT * from sales", con)
df_customers = pd.read_sql_query("SELECT * from customers where age>= 18 and age<=35", con)
df_orders = pd.read_sql_query("SELECT * from orders", con)
df_items = pd.read_sql_query("SELECT * from items", con)

df_orders_items = pd.merge(df_orders, df_items[['item_name', 'item_id']], how="left", on=["item_id"])
df_sales_customers = pd.merge(df_sales, df_customers[['customer_id', 'age']], how="left", on=["customer_id"])
df_final = pd.merge(df_sales_customers, df_orders_items, how="left", on=["sales_id"])
result = df_final.groupby(["customer_id","item_name", "age"])["quantity"].sum().reset_index()

# print(result.head(5))
# print(type(result))

result.to_csv('output.csv', sep =';')




