-- Databricks notebook source
-- MAGIC %sql
-- MAGIC create schema sample

-- COMMAND ----------

create table emp(id int, name string, age int, dept string)

-- COMMAND ----------

describe detail emp

-- COMMAND ----------

describe extended emp

-- COMMAND ----------

drop table emp

-- COMMAND ----------

Create table emp(id int, name string, age int, dept string) location "dbfs:/mnt/saunextadls/raw/delta/saijal/emp"

-- COMMAND ----------

describe extended emp

-- COMMAND ----------

describe history emp

-- COMMAND ----------

select * from emp

-- COMMAND ----------

insert into table emp values(1,'a',23,'DE')

-- COMMAND ----------

select * from emp

-- COMMAND ----------

describe history emp

-- COMMAND ----------

insert into table emp values(2,'b',23,'DE')

-- COMMAND ----------

 

insert into table emp values(3,'c',23,'DE'),
(4,'d',23,'DE')

-- COMMAND ----------

describe history emp

-- COMMAND ----------

select * from emp

-- COMMAND ----------

delete from emp where id= 1

-- COMMAND ----------

select * from emp

-- COMMAND ----------

Update emp
set dept='DS'
where id= 4

-- COMMAND ----------

describe history emp

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from emp version as of 3

-- COMMAND ----------

create table oldemp as 
select * from emp version as of 3

-- COMMAND ----------

select * from emp timestamp as of '2023-09-27T08:41:25.000+0000'

-- COMMAND ----------


