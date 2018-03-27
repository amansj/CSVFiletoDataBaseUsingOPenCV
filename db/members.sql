--Error Table
CREATE TABLE errortable
(
rownno number(10),
errorinfo varchar2(250)
);
select * from errortable;
select count(*) from pg;
truncate table errortable;
truncate table pg

--DESC PG
CREATE TABLE  "DESCPG" 
   ("CSV_COLID" VARCHAR2(30), 
	"DB_COLID" VARCHAR2(30), 
	"DATATYPE" VARCHAR2(20)
   ) ;
-- Drop Data
drop table data
-- TABLE DATA
create table data 
(
"DB_A" NUMBER(15,0),
"DB_C" NUMBER(15,4),
"DB_BD" NUMBER(20,0),
"NAME" varchar2(30),
"COLID" number(10)
);
-- DESC DATA
CREATE TABLE  "DESCDATA" 
   ("FORMULA" VARCHAR2(100), 
	"DB_COLID" VARCHAR2(30), 
	"DATATYPE" VARCHAR2(20)
   ) ;
   truncate table descdata
   desc data
INSERT INTO DESCDATA VALUES(':A','DB_A','Decimal(15,0)');
INSERT INTO DESCDATA VALUES('(((:C+:D)/:A)*:2)','DB_C','Decimal(15,4)');
INSERT INTO DESCDATA VALUES(':B+:D','DB_BD','Decimal(20,0)');
INSERT INTO DESCDATA VALUES(':FirstName+:" "+:LastName','NAME','String');
INSERT INTO DESCDATA VALUES(':colid','COLID','Decimal(10,0)');
SELECT * from DESCDATA
select * from data
select * from errortable