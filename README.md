# Netflix Data Analysis using SQL
![](https://github.com/najirh/netflix_sql_project/blob/main/logo.png)

## Overview
This project involves a comprehensive analysis of Netflix's movies and TV shows data using SQL. The goal is to extract valuable insights and answer various business questions based on the dataset. The following README provides a detailed account of the project's objectives, business problems, solutions, findings, and conclusions.

## Objectives

- Analyze the distribution of content types (movies vs TV shows).
- Identify the most common ratings for movies and TV shows.
- List and analyze content based on release years, countries, and durations.
- Explore and categorize content based on specific criteria and keywords.

## Dataset

The data for this project is sourced from the Kaggle dataset:

- **Dataset Link:** [Movies Dataset](https://www.kaggle.com/datasets/shivamb/netflix-shows?resource=download)
- **Dashboard Link:** [Netflix_Dashboard](https://dbc-fd1be9cd-e896.cloud.databricks.com/dashboardsv3/01f138e84eee17c38be10bd980076f82/published?o=7474660876172019)

## Schema

```sql
CREATE DATABASE netflix_db;

DROP TABLE IF EXISTS netflix;
CREATE TABLE netflix
(
    show_id      VARCHAR(5),
    type         VARCHAR(10),
    title        VARCHAR(250),
    director     VARCHAR(550),
    casts        VARCHAR(1050),
    country      VARCHAR(550),
    date_added   VARCHAR(55),
    release_year INT,
    rating       VARCHAR(15),
    duration     VARCHAR(15),
    listed_in    VARCHAR(250),
    description  VARCHAR(550)
);

use netflix_db;
show tables;

select * from netflix
limit 10;

```

## Business Problems and Solutions

### 1. Count the Number of Movies vs TV Shows

```sql
SELECT DISTINCT type from netflix;

select * from netflix
where type = 'William Wyler';

DELETE FROM netflix
WHERE type = 'William Wyler';

SELECT 
    type,
    COUNT(*)
FROM netflix
GROUP BY 1;
```

**Objective:** Determine the distribution of content types on Netflix.

### 2. Find the Most Common Rating for Movies and TV Shows
```sql
SELECT type, rating from (
select type, rating, count(*) as total_number,
rank() over(partition by type order by count(*) desc) as ranking
from netflix
group by type, rating
) AS T1
where ranking = 1;
```
OR
```sql
WITH RatingCounts AS (
    SELECT 
        type,
        rating,
        COUNT(*) AS rating_count
    FROM netflix
    GROUP BY type, rating
),
RankedRatings AS (
    SELECT 
        type,
        rating,
        rating_count,
        RANK() OVER (PARTITION BY type ORDER BY rating_count DESC) AS rank
    FROM RatingCounts
)
SELECT 
    type,
    rating AS most_frequent_rating
FROM RankedRatings
WHERE rank = 1;
```

**Objective:** Identify the most frequently occurring rating for each type of content.

### 3. List All Movies Released in a Specific Year (e.g., 2020)

```sql
select * from netflix
where type = 'Movie' and release_year = 2020;
```

**Objective:** Retrieve all movies released in a specific year.

### 4. Find the Top 5 Countries with the Most Content on Netflix
```sql
SELECT trim(individual_country), COUNT(*) AS total_content
FROM netflix_db.netflix
LATERAL VIEW EXPLODE(SPLIT(country, ',')) AS individual_country
WHERE individual_country IS NOT NULL
GROUP BY trim(individual_country)
ORDER BY total_content DESC
limit 5
```
OR
```sql
SELECT * 
FROM
(
    SELECT 
        UNNEST(STRING_TO_ARRAY(country, ',')) AS country,
        COUNT(*) AS total_content
    FROM netflix
    GROUP BY 1
) AS t1
WHERE country IS NOT NULL
ORDER BY total_content DESC
LIMIT 5;
```

**Objective:** Identify the top 5 countries with the highest number of content items.

### 5. Identify the Longest Movie

```sql
SELECT 
    *
FROM netflix
WHERE type = 'Movie'
ORDER BY SPLIT_PART(duration, ' ', 1)::INT DESC;
```

**Objective:** Find the movie with the longest duration.

### 6. Find Content Added in the Last 5 Years
```sql
SELECT *
FROM netflix_db.netflix
WHERE CAST(SPLIT_PART(date_added, ' ', -1) AS INT) >= year(current_date) - 5
```
OR
```sql
SELECT *
FROM netflix
WHERE TO_DATE(date_added, 'Month DD, YYYY') >= CURRENT_DATE - INTERVAL '5 years';
```

**Objective:** Retrieve content added to Netflix in the last 5 years.

### 7. Find All Movies/TV Shows by Director 'Rajiv Chilaka'
```sql
Select * from netflix_db.netflix
where director ILIKE '%Rajiv Chilaka%'
```
OR
```sql
SELECT *
FROM netflix_db.netflix
LATERAL VIEW EXPLODE(SPLIT(director, ',')) AS director_name
WHERE director_name = 'Rajiv Chilaka';
```
OR
```sql
SELECT *
FROM (
    SELECT 
        *,
        UNNEST(STRING_TO_ARRAY(director, ',')) AS director_name
    FROM netflix
) AS t
WHERE director_name = 'Rajiv Chilaka';
```

**Objective:** List all content directed by 'Rajiv Chilaka'.

### 8. List All TV Shows with More Than 5 Seasons

```sql
SELECT *
FROM netflix
WHERE type = 'TV Show'
  AND SPLIT_PART(duration, ' ', 1)::INT > 5;
```

**Objective:** Identify TV shows with more than 5 seasons.

### 9. Count the Number of Content Items in Each Genre
```sql
SELECT 
    TRIM(genre) AS Genre,
    COUNT(*) AS total_content
FROM netflix_db.netflix
LATERAL VIEW EXPLODE(SPLIT(listed_in, ',')) AS genre
GROUP BY TRIM(genre)
ORDER BY total_content DESC;
```
OR
```sql
SELECT 
    UNNEST(STRING_TO_ARRAY(listed_in, ',')) AS genre,
    COUNT(*) AS total_content
FROM netflix
GROUP BY 1;
```

**Objective:** Count the number of content items in each genre.

### 10.Find each year and the average numbers of content release in India on netflix. 
return top 5 year with highest avg content release!
```sql
Select 
CAST(SPLIT_PART(date_added, ' ', -1) AS INT) as year,
count(*) as total_release,
round(total_release/(select count(*) from netflix_db.netflix where country = 'India') * 100, 2) as avg_content_per_year
from netflix_db.netflix
where country = 'India'
group by year
order by year
```
OR
```sql
SELECT 
    country,
    release_year,
    COUNT(show_id) AS total_release,
    ROUND(
        COUNT(show_id)::numeric /
        (SELECT COUNT(show_id) FROM netflix WHERE country = 'India')::numeric * 100, 2
    ) AS avg_release
FROM netflix
WHERE country = 'India'
GROUP BY country, release_year
ORDER BY avg_release DESC
LIMIT 5;
```

**Objective:** Calculate and rank years by the average number of content releases by India.

### 11. List All Movies that are Documentaries

```sql
SELECT * 
FROM netflix
WHERE listed_in LIKE '%Documentaries';
```

**Objective:** Retrieve all movies classified as documentaries.

### 12. Find All Content Without a Director

```sql
SELECT * 
FROM netflix
WHERE director IS NULL;
```

**Objective:** List content that does not have a director.

### 13. Find How Many Movies Actor 'Salman Khan' Appeared in the Last 10 Years

```sql
SELECT * 
FROM netflix
WHERE casts LIKE '%Salman Khan%'
  AND release_year > EXTRACT(YEAR FROM CURRENT_DATE) - 10;
```

**Objective:** Count the number of movies featuring 'Salman Khan' in the last 10 years.

### 14. Find the Top 10 Actors Who Have Appeared in the Highest Number of Movies Produced in India
```sql
SELECT 
    TRIM(casts) AS Casts,
    COUNT(*) AS total_content
FROM netflix_db.netflix
LATERAL VIEW EXPLODE(SPLIT(cast, ',')) AS casts
WHERE country ILIKE '%India%'
GROUP BY TRIM(casts)
ORDER BY total_content DESC
LIMIT 10;
```
OR
```sql
SELECT 
    UNNEST(STRING_TO_ARRAY(casts, ',')) AS actor,
    COUNT(*)
FROM netflix
WHERE country = 'India'
GROUP BY actor
ORDER BY COUNT(*) DESC
LIMIT 10;
```

**Objective:** Identify the top 10 actors with the most appearances in Indian-produced movies.

### 15. Categorize Content Based on the Presence of 'Kill' and 'Violence' Keywords
```sql
with new_table
as (
Select *,
CASE When description ilike '%kill%' or description ilike '%violence%' Then 'Bad Content' Else 'Good Content' End as category
from netflix_db.netflix
)

select category, count(*) as total_content
from new_table
group by category
```
OR
```sql
SELECT 
    category,
    COUNT(*) AS content_count
FROM (
    SELECT 
        CASE 
            WHEN description ILIKE '%kill%' OR description ILIKE '%violence%' THEN 'Bad'
            ELSE 'Good'
        END AS category
    FROM netflix
) AS categorized_content
GROUP BY category;
```

**Objective:** Categorize content as 'Bad' if it contains 'kill' or 'violence' and 'Good' otherwise. Count the number of items in each category.

## Findings and Conclusion

- **Content Distribution:** The dataset contains a diverse range of movies and TV shows with varying ratings and genres.
- **Common Ratings:** Insights into the most common ratings provide an understanding of the content's target audience.
- **Geographical Insights:** The top countries and the average content releases by India highlight regional content distribution.
- **Content Categorization:** Categorizing content based on specific keywords helps in understanding the nature of content available on Netflix.

This analysis provides a comprehensive view of Netflix's content and can help inform content strategy and decision-making.

