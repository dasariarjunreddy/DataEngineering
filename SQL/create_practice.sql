create table dept(depno int,dname varchar(20),manager int)
alter table dept alter column depno int not null;
alter table dept add constraint prim primary key(depno);
create table emp(eno int primary key,ename char(20),depno int constraint foreign_key foreign key (depno) references dept(depno) on delete cascade);
/* to drop a constraint you should have a name for constraint */alter table emp drop constraint foreign_key;

/*throughs an error cannot delete dept bcoz emp references to dept  */ drop table dept;

insert into dept values (1,'engg',100),(2,'hr',200),(3,'op',300);

select * from dept;

/*Msg 547, Level 16, State 0, Line 13
The INSERT statement conflicted with the FOREIGN KEY constraint "foreign_key". The conflict occurred in database "master", table "dbo.dept", column 'depno'.
The statement has been terminated.

Completion time: 2021-09-20T16:54:15.4924892+05:30
*/

insert into emp values (100,'Arjun',1),(101,'Chandra',1),(200,'elma',2),(201,'Prabhu',4);

insert into emp values (100,'Arjun',1),(101,'Chandra',1),(200,'elma',2),(201,'Prabhu',2);

select * from emp as e join dept as d on e.depno= d.depno where d.depno=1;

/*doesn't work in MSSQL, works in MySQL*/select eno from emp group by depno;

select * from emp.dbo.employees;


          
EXECUTE sp_helpindex dept



create table team(tname varchar(20))
insert into team values ('Arjun'),('Chandra'),('elma'),('Prabhu');

select * from (select t.tname as A,d.tname as B from team t cross join team d where t.tname!=d.tname) f ;

select distinct
       case when a.tname<=b.tname then a.tname else b.tname end id1,
       case when a.tname<=b.tname then b.tname else a.tname end id2
from team a cross join team b where a.tname!=b.tname

SELECT  t1.tname, t2.tname
FROM    team t1, team t2
WHERE   t1.tname !=(SELECT t2.tname)