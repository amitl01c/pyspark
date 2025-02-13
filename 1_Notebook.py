# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC Getting the file path for pyspark

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# df.display() it gives in tabuler format
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark funtions

# COMMAND ----------

df.__getattr__("Item_Identifier")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading JSON format

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema',True).option('header',True).load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display(
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Defination

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
                Item_Identfifier STRING,
                Item_Weight STRING,
                Item_Fat_Content STRING,
                Item_Visibility DOUBLE,
                Item_Type STRING,
                Item_MRP DOUBLE,
                Outlet_Identifier STRING,
                Outlet_Establishment_Year INTEGER,
                Outlet_Size STRING,
                Outlet_Location_Type STRING,
                Outlet_Type STRING,
                Item_Outlet_Sales DOUBLE
                '''

# COMMAND ----------

df = spark.read.format('csv')\
    .schema(my_ddl_schema)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### StructTpe() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([

                                StructField('Item_Identifier', StringType(),True),
                                StructField('Item_Weight', StringType(),True),
                                StructField('Item_Fat_Content', StringType(),True),
                                StructField('Item_Visibility', StringType(),True),
                                StructField('Item_Type', StringType(),True),
                                StructField('Item_MRP', StringType(),True),
                                StructField('Outlet_Identifier', StringType(),True),
                                StructField('Outlet_Establishment_Year', StringType(),True),
                                StructField('Outlet_Size', StringType(),True),
                                StructField('Outlet_Location_Type', StringType(),True),
                                StructField('Outlet_Type', StringType(),True),
                                StructField('Item_Outlet_Sales', StringType(),True)
])

# COMMAND ----------

df = spark.read.format('csv')\
                        .schema(my_struct_schema)\
                        .option('header',True)\
                        .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT FUNTION
# MAGIC WHICH IS SAME AS SELECT STATEMENT IN SQL

# COMMAND ----------

df_select = df.select('Item_Identifier', 'Item_Type','Item_MRP')
df_select.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('Item_Identifier').alias('primary_key')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER/WHERE 
# MAGIC FOR DATA FILTERING 
# MAGIC FOR SLICING DATA ON CONDITION BASIS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1
# MAGIC FOR SINGLE CONDITION

# COMMAND ----------

df.filter((col('Item_Weight') < 10)) .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2
# MAGIC FOR MULTIPLE CONDITIONS

# COMMAND ----------

df.filter((col('Item_Fat_Content') == 'Low Fat') & (col('Item_Type') == 'Hard Drinks')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Scenario 3
# MAGIC in it we use isin() funtion and isNull() for null check

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(
            (col('Item_Fat_Content') == 'Low Fat') 
          # & (col('Item_Type') == 'Hard Drinks') 
          & (col('Outlet_Size').isNull())
          &(col('Outlet_Location_Type').isin('Tier 1','Tier 2'))
          ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Identifier','Item_Id').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## withColumn
# MAGIC SCENARIO -1

# COMMAND ----------

df.withColumn('multiply', col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO -2

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
                            .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type Casting

# COMMAND ----------

 df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/ OrdrBy

# COMMAND ----------

df.sort(col('Item_Weight').desc(),col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2
# MAGIC  ** Sorting on the basis of multiple column**
# MAGIC Here in ascending =[] lis we pass a boolean value 0 or 1 
# MAGIC where, 0 for descending and 1 for ascending
# MAGIC
# MAGIC it sorts data from left to right column 

# COMMAND ----------

df.sort(['Item_Visibility','Item_weight'],ascending =[0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit 
# MAGIC work as same as SQL

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Function
# MAGIC Used to remove column from the table
# MAGIC
# MAGIC We can remove multiple columns 
# MAGIC
# MAGIC Scenario 1
# MAGIC

# COMMAND ----------

df.drop('Item_Weight').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop_Duplicates
# MAGIC ** Scenario 1**
# MAGIC
# MAGIC It will remove duplicate row from the table

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario 2**
# MAGIC Remove duplicates based on columns

# COMMAND ----------

df.drop_duplicates(subset=['Item_type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct
# MAGIC
# MAGIC Shows only unique value
# MAGIC

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION and UNION BY NAME
# MAGIC
# MAGIC

# COMMAND ----------

df_1 = spark.createDataFrame([
  ('1', 'Amit'),
  ('2', 'Sachin'),
  ('3','Rajat')
],['id','name'])

data = [('3','Hemant'),
        ('5','Satyam')]
schema = 'id string,name string'

df_2 = spark.createDataFrame(data,schema)

# COMMAND ----------

df_1.display()
df_2.display()

# COMMAND ----------

df_1.union(df_2).display()
df_1.unionAll(df_2).display()

# COMMAND ----------

data = [('Hemant','4'),
        ('Satyam','5')]
schema = 'name string,id string'

df_3 = spark.createDataFrame(data,schema)
df_3.display()

# COMMAND ----------

df_1.union(df_3).display()

df_1.unionByName(df_3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## STRING FUNCTIONS
# MAGIC In it we covers hte given below functions
# MAGIC
# MAGIC initcap(column_name) : funtion used to make first character in capital letter in which we have to pass the column name
# MAGIC upp
# MAGIC er(column_name) and lower(column_name)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df.select(initcap('Item_type')).display()
df.select(lower('Item_type')).display()
df.select(upper('Item_type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Funtion
# MAGIC
# MAGIC current_date()
# MAGIC
# MAGIC date_add()
# MAGIC
# MAGIC date_sub()

# COMMAND ----------

from pyspark.sql.functions import current_date
df = df.withColumn("Current_date",current_date()).display()

# COMMAND ----------

df2 = df.withColumn('week_after',date_add('Current_date',7))
# df = df.withColumn('week_after',date_add('Current_date',-7))
df2.display()

# COMMAND ----------

df1 = df.withcolumn('week_before',date_sub('Current_date',7)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATEDIFF

# COMMAND ----------

df.withColumbn('datediff',datediff('Current Date','week_before'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date_format

# COMMAND ----------

df = df.withColumn("curent_date",date_format(current_date(),'mm-dd-yyyy')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### NULL HANDLING IN PYSPARK
# MAGIC
# MAGIC   We have two option 
# MAGIC   1. Drop null values 
# MAGIC       df.dropna() in it parameters we can pass are 'any' and 'all'
# MAGIC   2. Fill null values

# COMMAND ----------


df.dropna('any').display()

# COMMAND ----------

# MAGIC %md
# MAGIC drop null value for a particular column

# COMMAND ----------

df.dropna(subset=['Outlet_size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replacing null values
# MAGIC
# MAGIC for particular column

# COMMAND ----------

df.fillna(0.0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPLIT AND INDEXING
# MAGIC SPLIT : This funtion is used to split and convert into a list

# COMMAND ----------

df = df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Indexing

# COMMAND ----------

df.withColumn('Outlet_type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXPLODE AND EXPLODE_OUTER FUNTIOS
# MAGIC explode('colun_name') : used to fetch an each element of a list from a column that contains list and associat with the row it ![belongs](path)

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.withColumn('Outlet_type',explode('Outlet_Type')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### ARRAY_CONTAINS
# MAGIC This function is used to check wheather a column which contain list as an element in that list contian the given element or not

# COMMAND ----------

df_exp.withColumn('Type_1_Flage',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GROUP_BY
# MAGIC Used for the argregation of data

# COMMAND ----------

df.groupBy('Item_type').sum('Item_MRP').display()
# df.select(groupBy('Item_Type').sum('Item_MRP'))

# COMMAND ----------

df.groupBy('Item_type').agg(avg('Item_MRP')).display()
df.groupBy('Item_type').avg('Item_MRP').display()


# COMMAND ----------

df.groupBy('Item_type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()


# COMMAND ----------

# MAGIC %md 
# MAGIC ### COLLECT_LIST()
# MAGIC   This function is used to make a list of different tuples of a column according to group by clause
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### PIVOT
# MAGIC

# COMMAND ----------

df.groupBy('Item_type').pivot('Outlet_Size').agg(avg('Item_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN-OTHERWISE FUNCTION
# MAGIC   Used to put conditions
# MAGIC
# MAGIC   And for case statements
# MAGIC
# MAGIC ##### Scenario 1

# COMMAND ----------

df1 = df.withColumn('veg flag',when(col('Item_Type') == 'Meat','Non-Veg').otherwise('Veg'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2

# COMMAND ----------

df2 = df1.withColumn('veg_exp_flag', when(((col('veg flag') == 'Veg')  & (col('Item_MRP') < 100)),'Veg Inexpensive')\
                              .when(((col('veg flag') == 'Veg')  & (col('Item_MRP') > 100)),'Veg Expensive').otherwise('Non-veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS
# MAGIC ##### INNER JOIN
# MAGIC
# MAGIC ##### LEFT JOIN
# MAGIC ##### RIGHT JOIN
# MAGIC ##### ANTI JOIN

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)
     

df1.display()
     

df2.display()
     

# COMMAND ----------

        ####### INNER JOIN #####
df1.join(df2, df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

        ######     LEFT JOIN   #############
df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

        ##### RIGHT JOIN #####
df1.join(df2,df1['dept_id']==df2['dept_id'],'RIGHT').display()        

# COMMAND ----------

            #### ANIT JOIN ####
df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import desc
df = spark.createDataFrame([(2, "Alice"), (5, "Bob")]).toDF("age", "name")
df2 = spark.createDataFrame([Row(height=80, name="Tom"), Row(height=85, name="Bob")])
df3 = spark.createDataFrame([Row(age=2, name="Alice"), Row(age=5, name="Bob")])
df4 = spark.createDataFrame([
    Row(age=10, height=80, name="Alice"),
    Row(age=5, height=None, name="Bob"),
    Row(age=None, height=None, name="Tom"),
    Row(age=None, height=None, name=None),
])
df.display()
df2.display()
df3.display()
df4.display()

# COMMAND ----------

df.join(df2, 'name').show()
df.join(df4, ['name', 'age']).select(df.name, df.age).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #WINDOW FUNCTIONS
# MAGIC ###Row Number()
# MAGIC ###Rank()
# MAGIC ###Dense Rank()
# MAGIC ###Cumulative Sum()

# COMMAND ----------

# Row Number : Used to give a unique identifier to each column of the data frame

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowCol', row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

duplicate_data_df = df.withColumns({'rank':dense_rank().over(Window.orderBy('Item_Identifier')),'row_number': row_number().over(Window.orderBy('Item_Identifier'))}).display()


# COMMAND ----------



# COMMAND ----------








