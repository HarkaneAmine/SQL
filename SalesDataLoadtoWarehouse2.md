```sql
-- After creating a data lakehouse 'sales_dlh' and imported the csv file that contains the data we need "sales.csv", we will upload the data into a staging table 'sales_stg' in the data lakehouse.
-- this will not run
/*
copy into sales_stg
from 'https://onelake.dfs.fabric.microsoft.com/sales_ws/sales_dlh.Lakehouse/Files/sales.csv'
with (
    FILE_TYPE ='CSV'
    ,FIRSTROW = 2
    ,FIELDTERMINATOR =','
    ,ROWTERMINATOR ='\n'
    ,ENCODING = 'UTF8'
    --,QUOTECHAR = '"'
    --,ERROR_HANDLING
)
*/
```






```sql
--drop table sales_schema.Dim_Customer
drop table sales_schema.Fact_Sales 
drop table sales_schema. Dim_Customer
drop table sales_schema.Dim_Item
-- drop SCHEMA sales_schema
```






```sql
SELECT *
FROM [sales_dwh].[INFORMATION_SCHEMA].[TABLES]
```








```sql
-- To organize our data warehouse we will create a dedicated schema 'sales_schema' that will group the tables.

if not exists( select * from sys.schemas where name = 'sales_schema')
begin
exec('create schema sales_schema') -- for creating a schema after using the if not exists check, we need to pass by exec function
end
```






```sql
-- After Creating the staging table and the data warehouse 'sales_dwh' and the 'sales_schema', we wil create the Dimensional and fact tables to organize our data for analytics.

-- Create Dim_Customer Table
if not exists (select * from INFORMATION_SCHEMA.TABLES where table_schema ='sales_schema' and table_name = 'Dim_Customer')
CREATE TABLE sales_schema.Dim_Customer (
    CustomerID varchar(255) not null
    ,CustomerName varchar(255)
    ,EmailAddress varchar(255) not null
    --,Constraint PK_Dim_Customer primary key (CustomerID) -- Contraints cannot be declred inside the create in fabric
)

-- Create Dim_Item Table
if not exists (select * from INFORMATION_SCHEMA.TABLES where table_schema = 'sales_schema' and table_name ='Dim_Item')
create table sales_schema.Dim_Item(
    ItemID varchar(255) not null
    ,ItemName varchar(255) not null
)

-- Create Fact_Sales Table 
if not exists (select* from INFORMATION_SCHEMA.TABLES where table_schema = 'sales_schema' and table_name = 'Fact_Sales' )
create table sales_schema.Fact_Sales (
    CustomerID varchar(255) not null
    ,ItemID varchar(255) not null
    ,SalesOrderNumber varchar(30) null
    ,SalesOrderLineNumber int null 
    ,OrderDate date null 
    ,Quantity int null 
    ,TaxAmount float null
    ,UnitPrice float null 
)

```






```sql
SELECT *
FROM [sales_dwh].[INFORMATION_SCHEMA].[TABLES]
where table_schema = 'sales_schema'
```








```sql
-- Defining primary key for Dim_Customer
alter table sales_schema.Dim_Customer
add constraint PK_Dim_Customer primary key Nonclustered (CustomerID) not enforced

-- Defining primary key for Dim_Item
alter table sales_schema.Dim_Item 
add constraint PK_Dim_Item primary key nonclustered (ItemID) not enforced

-- Defining foreign keys for Fact_Sales
alter table sales_schema.Fact_Sales
add constraint FK_Dim_Item foreign key (ItemID) references sales_schema.Dim_Item (ItemID) not enforced

alter table sales_schema.Fact_Sales
add constraint FK_Dim_Customer foreign key (CustomerID) references sales_schema.Dim_Customer(CustomerID) not enforced
```






```sql
-- Data in the staging table Sales_stg 
select top 5 *
from sales_dlh.dbo.sales_stg
```








```sql
-- Load data to the warehouse using a stored procedure

create or alter procedure sales_schema.LoadDataFromStaging (@OrderYear int) as 
begin

-- Insert customers data into Dim_Customer while avoiding duplicates.
insert into sales_schema.Dim_Customer (CustomerID, CustomerName,EmailAddress)
select distinct 
    CustomerName
    ,CustomerName
    ,EmailAddress
from sales_dlh.dbo.sales_stg
where year(OrderDate)=@OrderYear
and not exists (
    select * 
    from sales_schema.Dim_Customer c 
    inner join sales_dlh.dbo.sales_stg s
    on  c.CustomerName = s.CustomerName
    and c.EmailAddress = s.EmailAddress   
    )


-- Insert items data into Dim_Item
insert into sales_schema.Dim_Item(ItemID,ItemName)
select distinct Item,Item
from sales_dlh.dbo.sales_stg
where year(OrderDate) = @OrderYear
and not exists (
    select *
    from sales_schema.Dim_Item i
    inner join sales_dlh.dbo.sales_stg s
    on i.ItemName = s.Item
)

-- insert sales data in Fact_Sales
insert into sales_schema.Fact_Sales(CustomerID,ItemID,SalesOrderNumber,SalesOrderLineNumber,OrderDate,Quantity,TaxAmount,UnitPrice)
select distinct
    cast(CustomerName as varchar(255))
    ,cast(Item as varchar(255))
    ,cast(SalesOrderNumber as varchar(30))
    ,cast(SalesOrderLineNumber as int) 
    ,cast(OrderDate as date) 
    ,cast(Quantity as int) 
    ,cast(TaxAmount as float)
    ,cast(UnitPrice as float)
from sales_dlh.dbo.sales_stg
where year(OrderDate) = @OrderYear

end

-- running the procedure
exec sales_schema.LoadDataFromStaging 2021
```






```sql

```
