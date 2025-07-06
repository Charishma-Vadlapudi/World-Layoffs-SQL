--- Data Cleaning
--Objective:
1.Remove duplicate rows if any
2.Standardize the data,fix errors and format columns
3.Look at null values and see what can be done
4.Remove columns which are not necessary for further processes or Remove blank columns.

Creating a staging table to process the data

CREATE TABLE world_layoffs.layoffs_staging LIKE world_layoffs.layoffs;
SELECT * FROM layoffs_staging;
INSERT layoffs_staging SELECT * FROM layoffs;

1.Removing Duplicates

1a.Creating a column with row number

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)
AS RowNumber FROM layoffs_staging; 

1b.Checking for RowNumber>1 which are duplicates
WITH duplicates_cte AS
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)
AS RowNumber FROM layoffs_staging)
SELECT *,RowNumber 
FROM duplicates_cte 
WHERE RowNumber>1;
These are the values we want to delete

WITH duplicates_cte AS
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)
AS RowNumber FROM layoffs_staging)
SELECT *,RowNumber 
FROM duplicates_cte 
WHERE RowNumber>1 
DELETE FROM duplicates_cte;

We cannot delete from duplicates_cte table.

Alternately,we can add a new column to the table layoffs_staging and delete those with RowNumber>1.

ALTER TABLE layoffs_staging ADD RowNumber INT;

SELECT * FROM layoffs_staging;

CREATE TABLE layoffs_staging2
(`company` text,
`location` text,
`industry` text,
`total_laid_off` INT,
`percentage_laid_off` INT,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO layoffs_staging2
( `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)
AS row_num FROM layoffs_staging;

Now,We can delete row_num>1 to remove duplicates.

DELETE FROM layoffs_staging2
WHERE row_num>=2;

Check whether everything got deleted or not

SELECT * FROM layoffs_staging2 WHERE row_num>1;

2.Standardize the data(Finding Issues in data and fixing them)

Checking each row

SELECT * FROM layoffs_staging2

2a.Company column has white space before start of the name.

SELECT DISTINCT company,TRIM(COMPANY) FROM layoffs_staging2 ORDER BY company;

This query worked for removing the white space.So,let us now update the table

UPDATE layoffs_staging2
SET company = TRIM(COMPANY)

Let us look at location:

SELECT DISTINCT location FROM layoffs_staging2 ORDER BY location;

Everything looks good in location column.Let us look at country column

SELECT DISTINCT country FROM layoffs_staging2 ORDER BY country;

2b.We have United States with . in one of the country names.Let us change it to 1 name.

SELECT DISTINCT country,TRIM(TRAILING '.'FROM country) FROM layoffs_staging2;

The above query removed the '.' from United states.Now,let us update the table

UPDATE layoffs_staging2
SET country= TRIM(TRAILING '.'FROM country);

SELECT * FROM layoffs_staging2;

2c.In Industry column, crypto is repeated with crypto currency,Crypto etc...So,let us change it to single unique name

UPDATE layoffs_staging2
SET industry ='Crypto'
WHERE industry like 'Crypto%';

2d.Now,Date column is in text format.Let us change it to Date format

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

The date is in date format.But in information tab ,it is still in text.

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2;

3.Let us now look at null values=We have blank values,null values in industry column,total_laid_off column,percentage)laid_off column.

We can populate industry column values if for the same company and same location,if any other data is available.
So,Let us see them.

SELECT * 
FROM layoffs_staging2 
WHERE industry ='' 
or industry IS NULL;

From the above query,Airbnb,`Bally's Interactive`,Carvana,Juul has missing rows in industry column.

Updating industry column with blank values to null values.

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry ='';


SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2
ON t1.company=t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company=t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2 WHERE industry IS NULL;

Looks like only Ballys Interactive is the only 1 row with Industry is null.

Also,let us look at those having total_laid_off,percentage_laid_off both as null values.

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

From the above query,it looks like 361 rows have null values in both total_laid_off and percentage)laid_off.
But laid off date is mentioned.
So,For EDA,we shall remove these values.

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

4.Useless columns.The row number,we have added while removing duplicates is no longer useful in EDA.
So,let us remove it.

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2;


-------EXPLORATORY DATA ANALYSIS-------------

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

The above query gives that 12,000 people were laid off from a company

SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging2;

percentage_laid_off=1 means the company went out of business.

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off=1

The above queries gives 221 companies were shut down.

---Query to give companies who have laid off all of their people ordered by funds raised by the company

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

----Total people laid off from each company

SELECT company,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

---Query-Top 5 companies with most number of laid off people 

SELECT company, total_laid_off
FROM layoffs_staging2
ORDER BY 2 DESC
LIMIT 5;

---Query to find the period of layoffs

SELECT MIN(`date`),MAX(`date`)
FROM layoffs_staging2;

It means laying off started on 3rd November 2020 till 3rd June 2023

---Query-Industry which got hit the most by laid off

SELECT industry,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

Consumer,Retail had the most people laid off from.

---Query-Which country got hit the most

SELECT country,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

US,India,Netherlands are the top countries

---Query-On which date,the layoff is most

SELECT `date`,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 2 DESC;

---Query-The laid off people on each year

SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

---Query-Stage of company with maximum layoffs

SELECT stage,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

Post-IPO stage companies have maximum layoffs.

---Query-Rolling sum of Lay-offs per YEAR per month

WITH CTE AS (
SELECT SUBSTRING(`date`,1,7) AS Yearly_Monthly,
SUM(total_laid_off) As LaidOffPerMonth
FROM layoffs_staging2
GROUP BY 1
ORDER BY 1)
SELECT Yearly_Monthly,LaidOffPerMonth,
SUM(LaidOffPerMonth) OVER(ORDER BY Yearly_Monthly) AS Rolling_Total
FROM CTE WHERE Yearly_Monthly IS NOT NULL;

---Query-To find companies with most layoffs per year

WITH company_total AS(
SELECT company,YEAR(date) AS years,SUM(total_laid_off) AS LaidOffPerCompany
FROM layoffs_staging2
GROUP BY company,YEAR(date)),
company_ranking AS(
SELECT company,years,DENSE_RANK() OVER(PARTITION BY years ORDER BY LaidOffPerCompany DESC) AS Ranking
FROM company_total)
SELECT company,years,Ranking 
FROM company_ranking
WHERE Ranking<=3 AND years IS NOT NULL
ORDER BY years,3;

