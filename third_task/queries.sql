-- 1
select name, COUNT(category_id) as amount
from category 
	inner join film_category
	using(category_id)
group by name
order by amount desc;

-- 2

select
  actor_id,
  first_name,
  last_name,
  COUNT(rental_id) as rental_amount
from actor 
	inner join film_actor using(actor_id)
	inner join film using(film_id)
	inner join inventory using(film_id)
	inner join rental using(inventory_id)
group by actor.actor_id
order by rental_amount desc
limit 10;


-- 3

select name, SUM(amount) as price
from category 
	inner join film_category using(category_id)
	inner join film using(film_id)
	inner join inventory using(film_id)
	inner join rental using(inventory_id)
	inner join payment using(rental_id)
group by name
order by SUM(amount) desc
limit 1;

-- 4

select film_id, title
from film 
	left join inventory using(film_id)
where inventory.film_id is null
group by film_id
order by film_id;

-- 5

with count_of_actor as(
	select 
		first_name, 
		last_name, 
		COUNT(actor_id) as amount
	from actor 
	inner join film_actor using(actor_id)
	inner join film using(film_id)
	inner join film_category using(film_id)
	inner join category using(category_id)
	where name = 'Children'
	group by actor_id),
count_of_actor_ranked as(
select 
	first_name, 
	last_name, 
	amount, 
	rank() over(order by amount desc) as actor_rank
from count_of_actor
)

select first_name, last_name, amount from count_of_actor_ranked
where actor_rank <= 3;

-- 6

select city, SUM(active) as active, COUNT(active) - SUM(active) as not_active
from city 
	inner join address using(city_id)
	inner join customer using(address_id)
group by city
order by not_active desc;


-- 7 

with city_rental_hours as(
	select name, city, DATE_PART('hour', return_date - rental_date) hours
	from category
	inner join film_category using(category_id)
	inner join film using(film_id)
	inner join inventory using(film_id)
	inner join rental using(inventory_id)
	inner join customer using(customer_id)
	inner join address using(address_id)
	inner join city using(city_id)
)

(
select name, sum(hours) from city_rental_hours
where city like '%a%'
group by name
order by sum(hours) desc
limit 1
)
union
(
select name, sum(hours) from city_rental_hours
where city like '%-%'
group by name
order by sum(hours) desc
limit 1
);