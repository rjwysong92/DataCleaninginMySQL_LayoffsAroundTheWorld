-- MySQL Exploratory Data Analysis

select *
from layoffs_staging3;


select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging3;

select *
from layoffs_staging3
where percentage_laid_off = 1
order by total_laid_off DESC;

select *
from layoffs_staging3
where percentage_laid_off = 1
order by funds_raised_millions DESC;

select company, sum(total_laid_off)
from layoffs_staging3
group by company
order by 2 desc;

select min(`date`), max(`date`) 
from layoffs_staging3;

select industry, sum(total_laid_off)
from layoffs_staging3
group by industry
order by 2 desc;

select *
from layoffs_staging3;

select country, sum(total_laid_off)
from layoffs_staging3
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging3
group by year(`date`)
order by 2 desc;

select stage, sum(total_laid_off)
from layoffs_staging3
group by stage
order by 2 desc;

select company, avg(percentage_laid_off)
from layoffs_staging3
group by company
order by 2 desc;

-- rolling total layoffs by month and year
select substring(`date`, 1, 7) as `month`, sum(total_laid_off)
from layoffs_staging3
where substring(`date`, 1, 7) is not null
group by `month`
order by 1 Asc;

-- rolling total month by month for term of data
with Rolling_Total as 
-- ^ name of table
(
-- selects date in year-mo format, sums each month in column labeled 'total_off'
select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging3
-- removes null values
where substring(`date`, 1, 7) is not null
-- orderes table by month in chronological order
group by `month`
order by 1 Asc
)
--  shows two columns other than date. Rolling total and total for each month
select `month`, total_off
, sum(total_off) over(order by `month`) as rolling_total
from Rolling_Total;


-- breakdown first by company then by month/year 
select company, year(`date`), sum(total_laid_off)
from layoffs_staging3
group by company, year(`date`)
order by 3 desc;

-- Shows year totals by company, then rank and ordered
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging3
group by company, year(`date`)
-- order by 3 desc
), 
company_year_rank as
(
select *, dense_rank() over (partition by years 
	order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking <= 5
order by 4 asc
;


select *
from layoffs_staging3;