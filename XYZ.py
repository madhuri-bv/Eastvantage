import pandas as pd
import sqlite3

# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect("assignment.db")

# store each table in pandas data frame

df_sales = pd.read_sql_query("SELECT * from sales", con)
df_customers = pd.read_sql_query("SELECT * from customers where age>= 18 and age<=35", con)
df_orders = pd.read_sql_query("SELECT * from orders", con)
df_items = pd.read_sql_query("SELECT * from items", con)

# Join orders and items and store in dataframe
df_orders_items = pd.merge(df_orders, df_items[['item_name', 'item_id']], how="left", on=["item_id"])

# Join customers and sales and store in dataframe
df_sales_customers = pd.merge(df_sales, df_customers[['customer_id', 'age']], how="left", on=["customer_id"])

# Generate the final result dataframe
df_final = pd.merge(df_sales_customers, df_orders_items, how="left", on=["sales_id"])

# Find the sume of quantity of each item purchased by customer

result = df_final.groupby(["customer_id","item_name", "age"])["quantity"].sum().reset_index()

# print(result.head(5))
# print(type(result))

#Generate output in csv file separated by ;

result.to_csv('output.csv', sep =';')




