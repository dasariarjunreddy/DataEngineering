
use window_fun;

/*2nd highest salary without using window function replace 2 with any number that you 
want eg: for 4th highest salary replace it with 4*/

SELECT first_name, salary,DEPARTMENT_ID 
	FROM emp.dbo.employees e1 
	WHERE 2-1 = (SELECT COUNT(DISTINCT salary) 
	FROM emp.dbo.employees e2 WHERE e2.salary > e1.salary and e1.DEPARTMENT_ID=e2.DEPARTMENT_ID)
	order by DEPARTMENT_ID

select department_id,avg(salary) from emp.dbo.employees where avg(salary)>1200 group by DEPARTMENT_ID
--using case when 
select case (order_id%2)
	   when 0 then 'even'
	   else 'odd' 
	   end as number_type,* from Orders

select * from Orders s where
	1-1 = (select count(distinct order_amount) 
			from Orders r 
			where s.order_amount<r.order_amount and s.city=r.city);

	 

--practice
select * from orders where order_amount in (select max(order_amount) from orders group by year(order_date))

select * from orders a where order_amount in (select max(order_amount) 
from orders b where year(a.order_date)=year(b.order_date ))

select * from (select r.city,r._count,rank() over(order by _count desc) as trans 
from (select distinct city,count(city) over (partition by city) as _count from orders) as r) as q where q.trans=1


select * from (select r.storeid,r.cust_id,r._count,rank() over(order by _count desc) as trans 
from (select storeid, distinct cust_id,count(cust_id) over (partition by cust_id) as _count from orders) as r) as q 
where q.trans=1

select * from orders order by order_date
select city,customer_name,order_amount,sum(order_amount) over(partition by city order by order_date) from orders


/*different joins with null value*/
create table tab1(col int);
create table tab2(col int);

insert into tab1 values (1),(1),(0),(null);
insert into tab2 values (1),(0),(null),(null);

select * from tab1 t1 join tab2 t2 on t1.col=t2.col;

select * from tab1 t1 full outer join tab2 t2 on t1.col=t2.col;

select * from tab1 t1  left outer join tab2 t2 on t1.col=t2.col;

select * from tab1 t1  right outer join tab2 t2 on t1.col=t2.col;


create table tab3(col int);
create table tab4(col int);

insert into tab3 values (1),(1),(1);
insert into tab4 values (1),(1),(1);

select * from tab3 t1 join tab3 t2 on t1.col=t2.col;

select * from tab3 t1 full outer join tab4 t2 on t1.col=t2.col;

select * from tab3 t1  left outer join tab4 t2 on t1.col=t2.col;

select * from tab3 t1  right outer join tab4 t2 on t1.col=t2.col;



/*flatten the table */

create table marks(rollno int,sub varchar(4),mark int);
truncate table marks;
insert into marks values(1,'math',10),(2,'math',20),(1,'sc',15),(2,'sc',14),(3,'math',16),(1,'lang',18);

select * from marks
select * from (select * from marks) x pivot (max(mark) for sub in (math,sc,lang)) y
select sum(mark) from marks
-- other approach 

--Declare Variable  
declare @pivot_column nvarchar(max);
declare @query nvarchar(max);

--Select Pivot Column
select @pivot_column=COALESCE(@pivot_column+',','')+QUOTENAME(sub) from
	(select distinct sub from marks) a;
select @pivot_column
/*Create Dynamic Query*/  
select @query='select rollno, '+@pivot_column+'from (select rollno,sub,mark from marks) x
pivot(max(mark) for sub in ('+@pivot_column+'))as y order by rollno';


/*Execute Query*/  
EXECUTE  sp_executesql  @query ;