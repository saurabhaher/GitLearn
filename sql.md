# SQL

### OFFSET AND FETCH&#x20;

```sql
select * from Person.Person
order by BusinessEntityID
OFFSET 0 ROWS
FETCH NEXT 10 ROWS ONLY
```

### Subquery

\
Use  IN , EXISTS, ANY, ALL\
IN - does equation to  a set of values\
ANY -\
SELECT \* FROM employee  WHERE salary > ANY (2000, 3000, 4000)  \
you can use    =, <>, <, >, <=,>=  for comparision\
ALL - returns true if the expression matches with all values



### Set Operations

UNION - union of 2 tables without duplicates\
UNION ALL - union with duplicates\
INTERSECT - intersection of 2 sets\
EXCEPT - find the difference  - remaining T1 when we remove T2 from T1

![](<.gitbook/assets/image (6).png>)\


### CTE

Recursive CTE

<pre class="language-sql"><code class="lang-sql"><strong>
</strong><strong>// print the numbers without using built in function
</strong><strong>with numbers as
</strong>(
select 1 as n  -- base query
union all
select n+1 from numbers where n &#x3C; 3  - recursive query
)
select * from numbers
</code></pre>

```sql
// Print the hierarchy with top manager
with emp_hierarchy as 
(
select EmployeeKey, FirstName, LastName, ParentEmployeeKey, Title, 1 as lvl from dbo.DimEmployee where ParentEmployeeKey is null
union all
select e.EmployeeKey, e.FirstName, e.LastName, e.ParentEmployeeKey, e.Title, h.lvl+1 as lvl from emp_hierarchy h
join dbo.DimEmployee e on h.EmployeeKey = e.ParentEmployeeKey
)
select * from emp_hierarchy order by lvl asc
```

<pre class="language-sql"><code class="lang-sql">// Find hierarchy from low to up
<strong>
</strong>with emp_hierarchy as 
(
select EmployeeKey, FirstName, LastName, ParentEmployeeKey, Title, 1 as lvl from dbo.DimEmployee where EmployeeKey = 1
union all
select e.EmployeeKey, e.FirstName, e.LastName, e.ParentEmployeeKey, e.Title, h.lvl+1 as lvl from emp_hierarchy h
join dbo.DimEmployee e on h.ParentEmployeeKey = e.EmployeeKey
)
select * from emp_hierarchy order by lvl asc
</code></pre>





```
// Ankit Bansal SQL

--------------------------------------------------------------------------------

--Complex Problem 1

create table icc_world_cup ( Team_1 Varchar(20), Team_2 Varchar(20), Winner Varchar(20) ); 
INSERT INTO icc_world_cup values('India','SL','India'); INSERT INTO icc_world_cup values('SL','Aus','Aus'); 
INSERT INTO icc_world_cup values('SA','Eng','Eng'); INSERT INTO icc_world_cup values('Eng','NZ','NZ'); 
INSERT INTO icc_world_cup values('Aus','India','India'); select * from icc_world_cup;


with cte1 as
(
select team_1 as team,
case when winner = team_1 then 'Y' ELSE 'N' END as ct
from icc_world_cup
),
cte2 as 
(
select 
team_2 as team, 
case when winner = team_2 then 'Y' ELSE 'N' END as ct
from icc_world_cup
),
cte3 as
(
select * from cte1 union all SELECT * from cte2
)
SELECT team, 
count(1) TOTAL,
COUNT(CASE WHEN ct = 'Y' THEN 1 END) as SUCCESS,
COUNT(CASE WHEN ct = 'N' THEN 1 END) As FAILURE
from cte3 GROUP by team;

--------------------------------------------------------------------------------

--Complex Problem 2 

create table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
);
SELECT * from  customer_orders;
insert into customer_orders values(1,100,cast('2022-01-01' as date),2000),(2,200,cast('2022-01-01' as date),2500),(3,300,cast('2022-01-01' as date),2100)
,(4,100,cast('2022-01-02' as date),2000),(5,400,cast('2022-01-02' as date),2200),(6,500,cast('2022-01-02' as date),2700)
,(7,100,cast('2022-01-03' as date),3000),(8,400,cast('2022-01-03' as date),1000),(9,600,cast('2022-01-03' as date),3000)
;


select order_date, 
count(1) as total, 
count(case when rn =1 then 1 end) as new,
count(1) - count(case when rn =1 then 1 end) as old
from
(
select *, 
row_number() over( partition by customer_id order by order_date asc) as rn
from customer_orders
)z group by order_date;

--------------------------------------------------------------------------------

--Complex Problem 3 

create table entries ( 
name varchar(20),
address varchar(20),
email varchar(20),
floor int,
resources varchar(10));

insert into entries 
values ('A','Bangalore','A@gmail.com',1,'CPU'),('A','Bangalore','A1@gmail.com',1,'CPU'),('A','Bangalore','A2@gmail.com',2,'DESKTOP')
,('B','Bangalore','B@gmail.com',2,'DESKTOP'),('B','Bangalore','B1@gmail.com',2,'DESKTOP'),('B','Bangalore','B2@gmail.com',1,'MONITOR');

select * from entries

select name, count(1)as total,
max(floor) as floor
--STRING_AGG(resources,',') as rs
from entries group by name


-------------------------------------------------------------------------------------------------

--Complex problem 4 - Get Nth Sunday after today
DECLARE @N int;
SET @N = 3;
SELECT DATEADD(DAY,7*(@N-1) +(8 - DATEPART(WEEKDAY,GETDATE() )), CAST(GETDATE() AS DATE))


-------------------------------------------------------------------------------------------------

--Complex problem 8 - Find the player having max score in each group, in case of same score player will least id has first preference



create table players
(player_id int,
group_id int)

insert into players values (15,1);
insert into players values (25,1);
insert into players values (30,1);
insert into players values (45,1);
insert into players values (10,2);
insert into players values (35,2);
insert into players values (50,2);
insert into players values (20,3);
insert into players values (40,3);

create table matches
(
match_id int,
first_player int,
second_player int,
first_score int,
second_score int)

insert into matches values (1,15,45,3,0);
insert into matches values (2,30,25,1,2);
insert into matches values (3,30,15,2,0);
insert into matches values (4,40,20,5,2);
insert into matches values (5,35,50,1,1);

select * from matches
select * from players
;
with cte as 
(
select match_id, first_player as player_id, first_score as score  from matches
union all
select match_id, second_player as player_id, second_score as score  from matches
),
cte2 as
(
select player_id, sum(score) as total_score from cte group by player_id
),
cte3 as 
(
select cte2.player_id, 
cte2.total_score, 
players.group_id,
DENSE_RANK() over (partition by players.group_id order by cte2.total_score desc, cte2.player_id asc) as dr
from cte2 join players on cte2.player_id = players.player_id
)
select * from cte3 where dr = 1


------------------------------------------------------------------------------------

--Complex problem 8 - Leetcode Market Analysis II - Find the second favourate

create table users (
user_id int,
 join_date date,
 favorite_brand  varchar(50));

 create table orders (
 order_id       int     ,
 order_date     date    ,
 item_id        int     ,
 buyer_id       int     ,
 seller_id      int 
 );

 create table items
 (
 item_id        int     ,
 item_brand     varchar(50)
 );

 insert into users values (1,'2019-01-01','Lenovo'),(2,'2019-02-09','Samsung'),(3,'2019-01-19','LG'),(4,'2019-05-21','HP');

 insert into items values (1,'Samsung'),(2,'Lenovo'),(3,'LG'),(4,'HP');

 insert into orders values (1,'2019-08-01',4,1,2),(2,'2019-08-02',2,1,3),(3,'2019-08-03',3,2,3),(4,'2019-08-04',1,4,2)
 ,(5,'2019-08-04',1,3,4),(6,'2019-08-05',2,2,4);


select * from orders
select * from users 
select * from items
;


with cte as 
(
select *,
dense_rank() over (partition by seller_id order by order_date asc) as dr
from orders
)
select
user_id, 
case when item_brand = favorite_brand then 'Y' else 'N' end as SecondFavourate
from users u 
left join cte c on u.user_id = c.seller_id and dr = 2
left join items i on c.item_id = i.item_id

-------------------------------------------------------------------------------

-- - Find the task duration - group continueous date

create table tasks (
date_value date,
state varchar(10)
)

insert into tasks values ('2019-01-01','S'),('2019-01-02','S'),('2019-01-03','S'),('2019-01-04','F'),('2019-01-05','F'),('2019-01-06','S')

with cte as
(
select *,
row_number() over (partition by state order by date_value asc) as rn
from tasks 
),
cte2 as
(
select *, dateadd(day, -1*rn, date_value) as dateN
from cte 
)select state,dateN, min(date_value),max(date_value) from cte2 group by state,dateN

---------------------------------------------------------------------------------------


--Complex problem  - User purchase platform


create table spending
(
user_id int,
spend_date date,
platform varchar(10),
amount int
);

insert into spending
values (1,'2019-07-01','mobile',100),
(1,'2019-07-01','desktop',100),
(2,'2019-07-01','mobile',100),
(2,'2019-07-02','mobile',100),
(3,'2019-07-01','desktop',100),
(3,'2019-07-01','desktop',100)


select * from spending;

select spend_date, count(1) from spending group by spend_date, platform

---------------------------------------------------------------------------------------
--Frequently brought together

create table orders_2
(
order_id int,
customer_id int,
product_id int,
);

insert into orders_2 VALUES 
(1, 1, 1),
(1, 1, 2),
(1, 1, 3),
(2, 2, 1),
(2, 2, 2),
(2, 2, 4),
(3, 1, 5);

create table products (
id int,
name varchar(10)
);
insert into products VALUES 
(1, 'A'),
(2, 'B'),
(3, 'C'),
(4, 'D'),
(5, 'E');

select * from orders_2

with cte as
(select * from orders_2 left join products on orders_2.product_id = products.id),
cte2 as(
select c.order_id, c.name+''+d.name as product
from cte c join cte d on c.order_id = d.order_id where c.product_id< d.product_id
) 
select product, count(1) from cte2 group by product


-----------------------------------------------------------------------------------------

create table users_2
(
user_id integer,
name varchar(20),
join_date date
);
insert into users_2
values (1, 'Jon', CAST('2-14-20' AS date)), 
(2, 'Jane', CAST('2-14-20' AS date)), 
(3, 'Jill', CAST('2-15-20' AS date)), 
(4, 'Josh', CAST('2-15-20' AS date)), 
(5, 'Jean', CAST('2-16-20' AS date)), 
(6, 'Justin', CAST('2-17-20' AS date)),
(7, 'Jeremy', CAST('2-18-20' AS date));

create table events
(
user_id integer,
type varchar(10),
access_date date
);

insert into events values
(1, 'Pay', CAST('3-1-20' AS date)), 
(2, 'Music', CAST('3-2-20' AS date)), 
(2, 'P', CAST('3-12-20' AS date)),
(3, 'Music', CAST('3-15-20' AS date)), 
(4, 'Music', CAST('3-15-20' AS date)), 
(1, 'P', CAST('3-16-20' AS date)), 
(3, 'P', CAST('3-22-20' AS date));

select * from events;
select * from users_2;
with cte as
(
select e.user_id, e.type,e.access_date,u.name, u.join_date, 
DATEDIFF(day, u.join_date, e.access_date) diff
from events e inner join users_2 u on e.user_id  = u.user_id
),
cte2 as
(
select c.*, c2.user_id  user2 , c2.type type2 from cte c left join cte c2 on c.user_id = c2.user_id
where c2.type= 'Music' and c.diff<= 30
)
select count(case when type='P' then 1 end)*1.0/count(case when type='Music' then 1 end) from cte2 

--------------------------------------------------------------------------------------------------------

--Customer churn and retention analysis

create table transactions (
order_id int, 
cust_id int,
order_date date ,
amount int
);


delete from transactions;

insert into transactions values 
(1,1,'2020-01-15',150),
(2,1,'2020-02-10',150),
(3,2,'2020-01-16',150),
(4,2,'2020-02-25',150),
(5,3,'2020-01-10',150),
(6,3,'2020-02-20',150),
(7,4,'2020-01-20',150),
(8,5,'2020-02-20',150),
(9,5,'2020-03-01',150),
(10,5,'2020-03-20',150),
(11,5,'2020-04-02',150)

select month(t1.order_date) as month,count(distinct t1.cust_id) - count(distinct t2.cust_id) as churn ,  count(distinct t2.cust_id) as retention
from transactions t1 
left join 
transactions t2 on t1.cust_id = t2.cust_id and datediff(month, t2.order_date, t1.order_date ) =1 
group by month(t1.order_date)

--------------------------------------------------------------------------------------------------------

-- Get Second most recent activity

create table UserActivity
(
username varchar(20),
activity varchar(20),
startDate Date,
endDate Date
)

insert into UserActivity values 

('Alice','Travel', '2020-02-12','2020-02-20'),
('Alice','Dancing', '2020-02-21','2020-02-23'),
('Alice','Travel', '2020-02-24','2020-02-28'),
('Bob','Travel', '2020-02-11','2020-02-18')

select * from UserActivity


with cte as 
(
select *, rank() over (partition by username order by startDate asc) as rn , 
count(username) over (partition by username ) as cn 
from UserActivity
) 
select * from cte where rn = 2 
union
select * from cte where rn = 1 and cn =1

-----------------------------------------------------------------------------------------------------------

--Scenerio based Sql question - Total charges as per billing 


create table billings 
(
emp_name varchar(10),
bill_date date,
bill_rate int
);

insert into billings values
('Sachin','01-JAN-1990',25)
,('Sehwag' ,'01-JAN-1989', 15)
,('Dhoni' ,'01-JAN-1989', 20)
,('Sachin' ,'05-Feb-1991', 30)
;

create table HoursWorked 
(
emp_name varchar(20),
work_date date,
bill_hrs int
);
insert into HoursWorked values
('Sachin', '01-JUL-1990' ,3)
,('Sachin', '01-AUG-1990', 5)
,('Sehwag','01-JUL-1990', 2)
,('Sachin','01-JUL-1991', 4)

--select * from billings;
--select * from HoursWorked;

with cte as
(
select *, lead(bill_date,1, '2999-01-01') over(partition by emp_name order by bill_date asc) as next_date from billings 
),
cte2 as
(
select h.*, cte.bill_rate, cte.bill_date, cte.emp_name as em_name, cte.next_date
from HoursWorked h left join cte on h.emp_name = cte.emp_name 
and h.work_date >= cte.bill_date and h.work_date < cte.next_date
)
select emp_name, sum(bill_hrs * bill_rate) as total_rate  from cte2 group by emp_name

------------------------------------------------------------------------------------------------------------

--Spotify case study 

CREATE table activity
(
user_id varchar(20),
event_name varchar(20),
event_date date,
country varchar(20)
);
delete from activity;
insert into activity values (1,'app-installed','2022-01-01','India')
,(1,'app-purchase','2022-01-02','India')
,(2,'app-installed','2022-01-01','USA')
,(3,'app-installed','2022-01-01','USA')
,(3,'app-purchase','2022-01-03','USA')
,(4,'app-installed','2022-01-03','India')
,(4,'app-purchase','2022-01-03','India')
,(5,'app-installed','2022-01-03','SL')
,(5,'app-purchase','2022-01-03','SL')
,(6,'app-installed','2022-01-04','Pakistan')
,(6,'app-purchase','2022-01-04','Pakistan');

select * from activity; 

--1. find total active users each day 
select event_date, count(distinct user_id)  from activity group by event_date

--2. find active users per week
select datepart(week, event_date), count(distinct user_id)  from activity group by datepart(week, event_date)

--3. date wise users who made purchase on same day they installed a app
with cte as 
(
select distinct event_date as event_date from activity
),
cte2 as
(
select  a.event_date, count(a.user_id) as ct from activity a 
inner join activity b on a.user_id = b.user_id and
a.event_date = b.event_date and
a.event_name ='app-installed' and b.event_name ='app-purchase'
group by a.event_date
)
select cte.event_date, coalesce(ct, 0) as count from  cte left join cte2 on cte.event_date = cte2.event_date


--4. Percentage of paid users from India, USA and others
with cte as
(
select user_id, 
case when country not in ('India','USA') then 'Others' else country end as country_n
from activity where event_name ='app-purchase'
),
cte2 as
(
select country_n , count(user_id) as ct from cte group by country_n 
)
select  *, (ct*100.0/(sum(ct) over())) as sm  from cte2 


--5. how many who installed yesterday have purchased today
with cte as 
(
select distinct event_date as event_date from activity
),
cte2 as(
select a.event_date, count(a.user_id) as ct from activity a 
inner join activity b on a.user_id = b.user_id and datediff(day, b.event_date, a.event_date) =1 and
a.event_name ='app-purchase' and b.event_name ='app-installed' group by a.event_date
)
select cte.event_date, coalesce(ct, 0) as count from  cte left join cte2 on cte.event_date = cte2.event_date

-------------------------------------------------------------------------------------------------------


--consecutive empty sets  
-- Hint  - the seat has to be empty and previous or next 2 rows has to be empty

create table bms (seat_no int ,is_empty varchar(10));
insert into bms values
(1,'N')
,(2,'Y')
,(3,'N')
,(4,'Y')
,(5,'Y')
,(6,'Y')
,(7,'N')
,(8,'Y')
,(9,'Y')
,(10,'Y')
,(11,'Y')
,(12,'N')
,(13,'Y')
,(14,'Y');

--method 1
with cte as
(
select *,
case when is_empty ='Y' then 1 else 0 end Num
from bms),
cte2 as
(
select *,
sum(Num) over(order by seat_no asc rows between 2 preceding and 0 following) as pre,
sum(Num) over(order by seat_no asc rows between 1 preceding and 1 following) as mid,
sum(Num) over(order by seat_no asc rows between 0 preceding and 2 following) as nxt
from cte
)
select *,
case when pre = 3 or mid = 3 or nxt =3  then 'Y' else 'N' end tr
from cte2 


--method 2  -- Substract the consecutive numbers




```

```
```















