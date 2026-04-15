-- Databricks notebook source
create database netflix_db

-- COMMAND ----------

use netflix_db;
show tables

-- COMMAND ----------

select * from netflix
limit 10

-- COMMAND ----------

select count(*) as total_content from netflix

-- COMMAND ----------

SELECT DISTINCT type from netflix

-- COMMAND ----------

select * from netflix
where type = 'William Wyler'


-- COMMAND ----------

DELETE FROM netflix
WHERE type = 'William Wyler';

-- COMMAND ----------

select * from netflix
where type = 'William Wyler';
select count(*) as total_content from netflix

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Business problems

-- COMMAND ----------

select type, count(*) as total_number from netflix
group by type

-- COMMAND ----------

SELECT type, rating from (
select type, rating, count(*) as total_number,rank() over(partition by type order by count(*) desc) as ranking from netflix
group by type, rating
) AS T1
where ranking = 1

-- COMMAND ----------

select * from netflix
where type = 'Movie' and release_year = 2020

-- COMMAND ----------

select country, count(*) as Total_of_content from netflix
group by country
limit 10

-- COMMAND ----------

SELECT individual_country, COUNT(*) AS total_content
FROM netflix
LATERAL VIEW EXPLODE(SPLIT(country, ',')) AS individual_country
WHERE individual_country IS NOT NULL
GROUP BY individual_country
ORDER BY total_content DESC
limit 5

-- COMMAND ----------

select * from netflix
where type = 'Movie' 
and 
duration = (select max(duration) from netflix )

-- COMMAND ----------

SELECT 
    *
FROM netflix
WHERE type = 'Movie'
ORDER BY SPLIT_PART(duration, ' ', 1)::INT DESC;

-- COMMAND ----------

Select * from netflix_db.netflix
where try_to_date(date_added, 'MMMM d, yyyy') >= current_date - Interval '5 years'

-- COMMAND ----------

SELECT *
FROM netflix_db.netflix
WHERE CAST(SPLIT_PART(date_added, ' ', -1) AS INT) >= year(current_date) - 5

-- COMMAND ----------

Select * from netflix_db.netflix
where director ILIKE '%Rajiv Chilaka%'

-- COMMAND ----------

SELECT *
FROM netflix_db.netflix
LATERAL VIEW EXPLODE(SPLIT(director, ',')) AS director_name
WHERE director_name = 'Rajiv Chilaka';

-- COMMAND ----------

SELECT 
    *
FROM netflix_db.netflix
WHERE type = 'TV Show'
AND SPLIT_PART(duration, ' ', 1)::INT > 5;

-- COMMAND ----------

SELECT 
    TRIM(genre) AS Genre,
    COUNT(*) AS total_content
FROM netflix_db.netflix
LATERAL VIEW EXPLODE(SPLIT(listed_in, ',')) AS genre
GROUP BY TRIM(genre)
ORDER BY total_content DESC;

-- COMMAND ----------

Select 
CAST(SPLIT_PART(date_added, ' ', -1) AS INT) as year,
count(*) as total_release,
round(total_release/(select count(*) from netflix_db.netflix where country = 'India') * 100, 2) as avg_content_per_year
from netflix_db.netflix
where country = 'India'
group by year
order by year

-- COMMAND ----------

Select * from netflix_db.netflix
where type = 'Movie'
ANd listed_in ILIKE '%Documentaries%'

-- COMMAND ----------

Select * from netflix_db.netflix
where director IS NULL

-- COMMAND ----------

select * from netflix_db.netflix
where cast ilike '%Salman Khan%'
and release_year > extract(year from current_date) - 10

-- COMMAND ----------

SELECT 
    TRIM(casts) AS Casts,
    COUNT(*) AS total_content
FROM netflix_db.netflix
LATERAL VIEW EXPLODE(SPLIT(cast, ',')) AS casts
WHERE country ILIKE '%India%'
GROUP BY TRIM(casts)
ORDER BY total_content DESC
LIMIT 10;

-- COMMAND ----------

with new_table
as (
Select *,
CASE When description ilike '%kill%' or description ilike '%violence%' Then 'Bad Content' Else 'Good Content' End as category
from netflix_db.netflix
)

select category, count(*) as total_content
from new_table
group by category

-- COMMAND ----------

SELECT 
    category,
    COUNT(*) AS content_count
FROM (
    SELECT 
        CASE 
            WHEN description ILIKE '%kill%' OR description ILIKE '%violence%' THEN 'Bad'
            ELSE 'Good'
        END AS category
    FROM netflix_db.netflix
) AS categorized_content
GROUP BY category;