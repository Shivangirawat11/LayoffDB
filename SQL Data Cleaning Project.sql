-- 	Data cleaning --

Select *
from layoffs;

-- 1. Remove duplicates --
-- 2. Standardize the Data --
-- 3. Null Values or Blank Values --
-- 4. Remove any columns --


Create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off,'date')as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, 
industry, total_laid_off, percentage_laid_off,'date', stage,
country, funds_raised_millions )as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num>1;

select *
from layoffs_staging
where company='Casper'
;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, 
industry, total_laid_off, percentage_laid_off,'date', stage,
country, funds_raised_millions )as row_num
from layoffs_staging
)
Delete
from duplicate_cte
where row_num>1; 

-- We cannot delete any data from a CTE
-- it's like a update query, this was just an example --


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

select *
from layoffs_staging2;

Insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, 
industry, total_laid_off, percentage_laid_off,'date', stage,
country, funds_raised_millions )as row_num
from layoffs_staging;

Delete
from layoffs_staging2
where row_num>1;

select * 
from layoffs_staging2
where row_num>1;

select * 
from layoffs_staging2;

-- Standardizing data-- 
-- Finding issues in your data and fixing it--

select company, trim(company)
from layoffs_staging2;  

Update layoffs_staging2
set company=trim(company);

select distinct industry
from layoffs_staging2
order by 1;  

select * 
from layoffs_staging2
where industry like'crypto%';

update layoffs_staging2
set industry='Crypto' 
where industry like 'Crypto%';

select *
from layoffs_staging2;

Select distinct location
from layoffs_staging2
order by 1;

Select distinct country
from layoffs_staging2
order by 1;

Select *
from layoffs_staging2
where country like 'United States%'
order by 1;

Select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

Update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'United States%';

select * 
from layoffs_staging2;

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

Update layoffs_staging2
set `date`= str_to_date(`date`,'%m/%d/%Y');

Alter table layoffs_staging2
modify column `date` DATE;

-- Null Values & Blank values--

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company like 'Bally%';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
and t1.location=t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

Update layoffs_staging2
set industry = null
where industry='';


Update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

 
delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * 
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

