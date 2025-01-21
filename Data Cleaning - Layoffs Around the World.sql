-- Data Cleaning

select *
from layoffs_staging;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values / Blank Values
-- 4. Remove any columns or rows that aren't necessary
--     Rarely will use this. Only if a column is completly blank and is not needed in the future
--     		Create stagin table to use for manipulation FIRST!! DO all work here so OG data stays in tact. 

-- 1. REMOVE DUPES 

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
;

WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

select *
from layoffs_staging;

WITH duplicate_cte
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
select *  
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL, 
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT into layoffs_staging3
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

select *
from layoffs_staging3
where row_num > 1;

delete
from layoffs_staging3
where row_num > 1;

select *
from layoffs_staging3;

-- end remove dupes

-- 2. Standardiizing Data
select *
from layoffs_staging3;

select company, TRIM(company)
from layoffs_staging3;

update layoffs_staging3
set company = trim(company);

select distinct industry
from layoffs_staging3
order by 1;

select *
from layoffs_staging3
where industry like 'crypto%';

update layoffs_staging3
set industry = 'Crypto'
where industry like 'crypto%';

-- to check work
select *
from layoffs_staging3;

select distinct country
from layoffs_staging3
order by 1;

select *
from layoffs_staging3
where country like 'united states%';

update layoffs_staging3
set country = 'United States'
where country like 'United States%';
-- OR --
select distinct country, TRIM(Trailing '.' from country)
from layoffs_staging3
order by 1;
-- with --
update layoffs_staging3
set country = TRIM(Trailing '.' from country)
where country like 'United States%';

select distinct country
from layoffs_staging3
order by 1;

-- time series -- 
select `date`,
str_to_date(`date`,'%m/%d/%Y') as 'date'
from layoffs_staging3;

update layoffs_staging3
set `date` = str_to_date(`date`,'%m/%d/%Y');

select *
from layoffs_staging3;

-- alter changes data type, ONLY DO THIS ON STAGING TABLES
alter table layoffs_staging3
modify column `date` date;

-- 3. Null Values / Blank Values
select *
from layoffs_staging3;

-- finding null values with conditions
select *
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

-- finding null and blank values
select *
from layoffs_staging3;

select *
from layoffs_staging3;

-- Goal: to update one company's industry at once if another row with same company exits
-- updates one company at a time. 
update layoffs_staging3
set industry = 'Travel'
where company = 'Airbnb';

-- OR to find all at once --
select *
from layoffs_staging3 t1
join layoffs_staging3 t2
	on t1.company = t2.company
    and t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging3
set industry = null
where industry = '';

-- and update all at once --
update layoffs_staging3 t1
join layoffs_staging3 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


-- Delete rows that are note necessary for this data analysis. 
-- in this case remove companys where total_laid_off and percentage_laid_off is null or ''
-- This is not always best practice. you must be CONFIDENT in deleting data that it is not necessary down the line
select * 
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

select * 
from layoffs_staging3;

-- drop column from table
alter table layoffs_staging3
drop column row_num;