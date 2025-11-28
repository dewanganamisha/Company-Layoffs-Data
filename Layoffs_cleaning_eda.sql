-- Data Cleaning

select * 
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values 
-- 4. Remove any column


-- Removing Duplicates
CREATE TABLE layoffs_stagging 
LIKE layoffs;

Rename table layoffs_stagging 
to layoffs_staging;

insert layoffs_staging 
select * from layoffs;

select * from layoffs;

with duplicate_cte as ( 
select *,
row_number() over(partition by 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
as row_num
from layoffs_staging )
select * from duplicate_cte 
where row_num>1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over(partition by 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
as row_num
from layoffs_staging;

select * from layoffs_staging2;

delete from layoffs_staging2 
where row_num > 1;

select * from layoffs_staging2 
where row_num>1;

-- Standardizing

select distinct company 
from layoffs_staging2;

select company, trim(company) 
from layoffs_staging2;

update layoffs_staging2 
set company= trim(company);

select distinct industry 
from layoffs_staging2
order by 1;

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct location 
from layoffs_staging2
order by 1;

select distinct country 
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2 
set country = trim(trailing '.' from country)
where  country like 'United States%';

select distinct `date`
from layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2 
set `date`=str_to_date(`date`,'%m/%d/%Y');

select `date` from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;

select distinct industry 
from layoffs_staging2;

select * 
from layoffs_staging2
where industry is null or industry ='';

select * 
from layoffs_staging2
where company='Airbnb';

update layoffs_staging2 
set industry = null 
where industry ='';

select t1.company,t1.industry,t2.industry
from layoffs_staging2 t1 
join layoffs_staging t2 
on t1.company=t2.company
where (t1.industry is null )
and t2.industry is not null;

update layoffs_staging2 t1 
join layoffs_staging2 t2 
on t1.company=t2.company
set t1.industry=t2.industry
where (t1.industry is null )
and t2.industry is not null;

select * from layoffs_staging2 
where total_laid_off is null and 
percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null and 
percentage_laid_off is null;

-- Remove Column

alter table layoffs_staging2 
drop column row_num;

select * from layoffs_staging2;

-- Exploratory Data Analysis

select max(total_laid_off) , max(percentage_laid_off)
from layoffs_staging2;

select * from layoffs_staging2
where percentage_laid_off=1;

select * from layoffs_staging2
where percentage_laid_off=1 
order by funds_raised_millions desc;

select company , sum(total_laid_off) 
from layoffs_staging2
group by 1
order by 2 desc;

select min(`date`) , max(`date`)
from layoffs_staging2;

select industry,sum(total_laid_off) 
from layoffs_staging2
group by 1
order by 2 desc;

select year(`date`) ,sum(total_laid_off) 
from layoffs_staging2
group by 1
order by 1 desc;

select stage, sum(total_laid_off) 
from layoffs_staging2
group by 1
order by 2 desc;

select substring(`date`,1,7) ,sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by 1
order by 1 desc;

with cte1 as (
select substring(`date`,1,7) as `month` ,sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by 1)
select month,total_off,sum(total_off) over(order by month asc) as rolling_total
from cte1;

select company,year(`date`) , sum(total_laid_off) 
from layoffs_staging2
group by 1,2
order by 3 desc;

with cte1 as (
select company, year(`date`) as `year`,sum(total_laid_off) as total_off
from layoffs_staging2
group by 1,2),
cte2 as( 
select * , 
dense_rank() over(partition by year order by total_off desc) as ranking 
from cte1 )
select company,`year`,total_off 
from cte2 
where ranking <=5
and `year` is not null;