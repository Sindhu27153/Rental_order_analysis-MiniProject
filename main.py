#connect python to MYSQL using pymysql
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from urllib.parse import quote_plus
import streamlit as st
import matplotlib.pyplot as plt
import altair as alt
import plotly.express as px
import seaborn as sns

orders_df = pd.read_csv("D:/Rental Order Analysis/env/Scripts/orders.csv")
products_df = pd.read_csv("D:/Rental Order Analysis/env/Scripts/products.csv")

#details to connect to database
host = '127.0.0.1'
user = 'root'
password = 'India@123'
Database = 'RetailOrders'

# URL-encode the password
encoded_password = quote_plus(password)
#connecting to Mysql

connection = pymysql.connect(host=host, user=user, password=password,database=Database)
cursor = connection.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {Database}")
cursor.execute(f"use {Database}")
connection.commit()

   
def run_query_1(query_1):
    try:
        connection = pymysql.connect(host=host, user=user, password=password,database=Database)
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {Database}")
        cursor.execute(f"use {Database}")
        connection.commit()


        # Connect to MySQL server using sqlalchemy create_engine with help of pymysql
        connection_url = f"mysql+pymysql://{user}:{encoded_password}@{host}/{Database}"

        engine = create_engine(connection_url)

        engine_connection = engine.connect()

        # Insert order DataFrame into SQL, creating table if it doesn't exist
        orders_df.to_sql('orders', con=engine_connection, if_exists='replace', index=False)

        # Insert product DataFrame into SQL, creating table if it doesn't exist
        products_df.to_sql('products', con=engine_connection, if_exists='replace', index=False)


        with connection.cursor() as cursor:
            cursor.execute(query_1)
            result = cursor.fetchall()
            column_name= [column[0] for column in cursor.description]
            
        connection.close()
        return pd.DataFrame(result,columns=column_name)
    except ValueError:
         return "Select a Query"
def run_query_2(query_2):
    try:
        connection = pymysql.connect(host=host, user=user, password=password,database=Database)
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {Database}")
        cursor.execute(f"use {Database}")
        connection.commit()


        # Connect to MySQL server using sqlalchemy create_engine with help of pymysql
        connection_url = f"mysql+pymysql://{user}:{encoded_password}@{host}/{Database}"

        engine = create_engine(connection_url)

        engine_connection = engine.connect()

        # Insert order DataFrame into SQL, creating table if it doesn't exist
        orders_df.to_sql('orders', con=engine_connection, if_exists='replace', index=False)

        # Insert product DataFrame into SQL, creating table if it doesn't exist
        products_df.to_sql('products', con=engine_connection, if_exists='replace', index=False)


        with connection.cursor() as cursor:
            cursor.execute(query_2)
            result = cursor.fetchall()
            column_name= [column[0] for column in cursor.description]
            
        connection.close()
        return pd.DataFrame(result,columns=column_name)
    except ValueError:
         return "Select a Query"
    
st.title("Rental Order Analysis")

queries_1 = {

"Top_10 High Revenue products":'''select sub_category,
                                round(sum(sale_price*quantity),3) as High_Revenue_Products
                                from products 
                                Group by sub_category 
                                order by High_Revenue_Products desc 
                                limit 10;''',


"Top 5 cities with the highest profit margins" : '''select o.city,round((sum(p.profit)/sum(p.sale_price*p.quantity))*100,2) as High_Profit_margins from orders o
                                join products p 
                                on o.product_id = p.product_id
                                group by o.city
                                order by High_profit_margins desc 
                                limit 5;''',

"Total discount given for each category" : '''select category,
                                round(sum(discount_price*quantity),4) as total_discount from products 
                                group by category
                                order by total_discount desc ;''',

"Average sale price per product category" : """select category,round(avg(sale_price),4) as Average_Sale_Price from products
                                group by category;""",

"Region with the highest average sale price" : """select o.region,round(avg(p.sale_price),4) as Highest_Average_Sale_Price from orders o
                                join products p
                                on o.product_id = p.product_id
                                group by o.region
                                order by  Highest_Average_Sale_Price desc
                                limit 1;""",

"Total profit per category" : """select category,round(sum(sale_price*quantity) - sum(cost_price*quantity)   ,4) as Total_Profit
                                from products 
                                group by category;""",


"Top 3 segments with the highest quantity of orders" : """select o.segment,sum(p.quantity) as high_quantity_orders from orders o
                                join products p 
                                on o.product_id = p.product_id
                                group by segment order by high_quantity_orders desc
                                limit 3;""",

"Average discount percentage given per region" : """ select o.region,concat(round(avg(p.discount_percent),2),'%') as average_discount_percentage 
                                from orders o
                                join products p 
                                on o.product_id = p.product_id
                                group by region;""",

"Product category with the highest total profit" : """select category,round(sum(sale_price*quantity) - sum(cost_price*quantity)   ,4) as Highest_Total_Profit
                                from products 
                                group by category
                                order by Highest_Total_Profit desc
                                limit 1;""",


"Total revenue generated per year" : """select year(o.order_date) as year_ ,round(sum(p.sale_price*p.quantity),4) as  Total_Revenue 
                                from orders o
                                join products p 
                                on o.product_id = p.product_id
                                group by year_;""",

}
queries_2 = {

"Region with lowest profit" : """select o.region,o.country,o.city,sum(p.profit) as Profit from orders o 
                                join products p 
                                on o.product_id = p.product_id
                                group by o.region ,o.country,o.city
                                order by Profit asc
                                limit 1;""",

"Revenue between months across different years" : """select
                                extract(year from o.order_date) as year, 
                                extract(month from o.order_date) as Month,
                                round(sum(p.sale_price*p.quantity),2) as Monthly_Revenue 
                                from products p 
                                join orders o 
                                on p.product_id = o.product_id
                                group by month,year
                                order by Month;""",


"categorize products into perfomance tiers" :"""select sub_category,
                                            round(sum(sale_price*quantity),2) as Revenue,
                                            case when round(sum(sale_price*quantity),2) > 1000000 then "High"
                                            when round(sum(sale_price*quantity),2) > 500000 then "Medium"
                                            else "Low"
                                            end as Perfomance_category
                                            from products
                                            group by sub_category
                                            order by revenue desc;""",
                            


"Revenue vs discount_price" : """select category,round(sum(sale_price*quantity),2) as Revenue,
                            discount_percent as discount_percentage
                            from products
                            group by category,discount_percent
                            order by discount_percent desc;""",

"Ship_mode with highest profit" : """select o.ship_mode,count(*) as shipping_count ,
                            round(sum(p.profit),3) as Profit,
                            dense_rank() over(order by round(sum(p.profit),3) desc) as shipment_rank
                            from orders o 
                            join products p 
                            on o.product_id = p.product_id
                            group by o.ship_mode;""",
"Total profit for each segments": """select o.segment,round(sum(p.profit),3) as profit from orders o
                            join products p 
                            on p.product_id = o.product_id
                            group by o.segment
                            order by profit desc
                            limit 3;""",

"Top 10  State with highest average revenue" : """select o.state,round(avg(sale_price*quantity),2) as Average_Revenue
                            from orders o
                            join products p 
                            on p.product_id = o.product_id
                            group by state
                            order by Average_Revenue desc
                            limit 10;""",

"Total profit generated per month for last year" : """select year(order_date) as year_,month(o.order_date) as Month_,
                            monthname(o.order_date) as Month_Name,
                            round(sum(p.profit),4) as  Total_Profit
                            from orders o
                            join products p 
                            on o.product_id = p.product_id
                            where year(o.order_date) = '2023'
                            group by year_,Month_,Month_name
                            order by month_ asc;""",

"postal code with Highest order Quantity" : """select o.postal_code,o.city,sum(p.quantity) as Quantity from orders o
                            join products p 
                            on o.product_id = p.product_id
                            group by postal_code,city
                            order by quantity desc limit 10;""",

"Top 10 sub_category with lowest revenue" : """select sub_category,round(sum(sale_price*quantity),2) as Lowest_Revenue
                            from products 
                            group by sub_category 
                            order by lowest_Revenue asc limit 10;"""
}



selected_query_1 = st.selectbox("Select a Query to Find Insights",list(queries_1.keys()))
selected_query_2 = st.selectbox("Select a Query to Find Insights",list(queries_2.keys()))
if st.button("Run Query_1"):
        query_1 = queries_1[selected_query_1]
        
        result_df_1 = run_query_1(query_1)
        
      
        if selected_query_1 == "Top_10 High Revenue products":
            st.subheader("Top 10 High Revenue Products")
            st.bar_chart(result_df_1.set_index('sub_category')['High_Revenue_Products'])
            
            top_product = result_df_1.iloc[0]
            st.markdown(f"**Top Product:** {top_product['sub_category']} with revenue of ${top_product['High_Revenue_Products']}")


        if selected_query_1 == "Top 5 cities with the highest profit margins":
            st.subheader("Top 5 cities with the Highest Profit Margins")

            st.bar_chart(result_df_1.set_index('city')['High_Profit_margins'])

            top_city = result_df_1.iloc[0]
            st.markdown(f"**Top City:**{top_city['city']} with High Profit Margin of {top_city['High_Profit_margins']}")


        if selected_query_1 == "Total discount given for each category":
            st.subheader("Total discount given for each category")
            fig,ax = plt.subplots()
            ax.pie(result_df_1['total_discount'],labels=result_df_1['category'],autopct='%1.1f%%',explode=(0,0,0.1),shadow=True)
            
            st.pyplot(fig)

        if selected_query_1 == "Average sale price per product category" :
              # Create a bar chart with Altair
            st.subheader("Average sale price per product category")
            chart = alt.Chart(result_df_1).mark_bar().encode(
                x='category',  
                y='Average_Sale_Price', 
                color='category', 
            )
            st.altair_chart(chart)

        if selected_query_1 =="Region with the highest average sale price":
            st.subheader("Region with the highest average sale price")
            
            st.table(result_df_1)
        
        if selected_query_1 == "Total profit per category":
            st.subheader("Total profit per category")
            
            st.bar_chart(result_df_1.set_index('category')["Total_Profit"],stack=True,color=["#AEEA94"],width=700,height=300)

        if selected_query_1 =="Top 3 segments with the highest quantity of orders":
            st.subheader("Top 3 segments with the highest quantity of orders")
            fig,ax = plt.subplots()
            ax.pie(result_df_1['high_quantity_orders'],labels=result_df_1['segment'],autopct='%1.1f%%',explode=(0,0,0.1),shadow=True)
            
            st.pyplot(fig)

        if selected_query_1 =="Average discount percentage given per region":
            
            chart = alt.Chart(result_df_1).mark_bar().encode(
                x='region',  
                y='average_discount_percentage', 
                color='region')
            st.altair_chart(chart,use_container_width=True)

        if selected_query_1 =="Product category with the highest total profit":
            st.subheader("Product category with the highest total profit")
            st.dataframe(result_df_1)

        if selected_query_1 == "Total revenue generated per year":
            st.subheader("Total revenue generated per year")
            st.bar_chart(result_df_1.set_index('year_')['Total_Revenue'],color=['#69247C'],use_container_width=False,width=600,height=300)
if st.button("Run_query_2"):
        query_2 = queries_2[selected_query_2]

        result_df_2 = run_query_2(query_2)

        if selected_query_2 == "Region with lowest profit":
            st.subheader("Region with lowest profit")
            st.dataframe(result_df_2)

        if selected_query_2 =="Revenue between months across different years" :
            st.subheader("Revenue between months across different years")
        
            fig = px.line(result_df_2,x='Month',y='Monthly_Revenue',color='Month')
            st.plotly_chart(fig)
            
        if selected_query_2== "categorize products into perfomance tiers" :

            pivot_df = result_df_2.pivot(index="sub_category",columns="Perfomance_category",values="Revenue")
            plt.figure(figsize=(8,6))
            sns.heatmap(pivot_df,annot=True,cmap="coolwarm",cbar=True)
            st.pyplot(plt)

        if selected_query_2== "Revenue vs discount_price" :
            fig = px.line(result_df_2, x="discount_percentage", y="Revenue")

            st.plotly_chart(fig)
            
        if selected_query_2 == "Ship_mode with highest profit" :
           st.subheader("Ship mode with highest profit")
           chart = alt.Chart(result_df_2).mark_bar().encode(
                x='ship_mode',  
                y='Profit', 
                color='shipping_count')
            
           st.altair_chart(chart)


        if selected_query_2== "Total profit for each segments":
            st.subheader("Total profit for each segments")
            fig,ax = plt.subplots()
            ax.pie(result_df_2['profit'],labels=result_df_2['segment'],autopct='%1.1f%%',explode=(0,0,0.1),shadow=True)
            st.pyplot(fig)

        if selected_query_2 == "Top 10  State with highest average revenue":
            st.subheader("Top 10  State with highest average revenue")
            chart = alt.Chart(result_df_2).mark_bar().encode(
                x='state',  
                y='Average_Revenue')
            
            st.altair_chart(chart)

        if selected_query_2 == "Total profit generated per month for last year" :
            st.subheader("Total profit generated per month for last year")
            fig = px.line(result_df_2,x='Month_Name',y='Total_Profit')
            st.plotly_chart(fig)


        if selected_query_2 == "postal code with Highest order Quantity":
            st.subheader("postal code with Highest order Quantity")
            st.bar_chart(result_df_2.set_index('postal_code')['Quantity'],color='#79D7BE',horizontal=True,use_container_width=False,width=800,height=300)
            
        
        if selected_query_2 == "Top 10 sub_category with lowest revenue":
            st.bar_chart(result_df_2.set_index('sub_category')['Lowest_Revenue'],color='Lowest_Revenue')

    

       