---------------------------------------------------------------Delete unused columns---------------------------------------------------------------

ALTER TABLE dbo.fact_swap
DROP COLUMN column1, Unnamed_0

-----------------------------------------------------------------CREATING TABLE STEP------------------------------------------------------------------

---------------------------------------------------------------Create dim_wallets table---------------------------------------------------------------
SELECT * FROM dbo.dim_wallets

WITH result AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY wallet_address ORDER BY date) AS seq_num
	FROM dbo.fact_swap
)
SELECT
	result.wallet_address,
	result.platform,
	result.country_code,
	result.os
INTO dim_wallets
FROM result
WHERE result.seq_num = 1;

ALTER TABLE dbo.dim_wallets
ALTER COLUMN wallet_address INT NOT NULL;

ALTER TABLE dbo.dim_wallets
ADD CONSTRAINT dim_wallet_pk PRIMARY KEY (wallet_address);

--------------------------------------------------------------- Create dim_countries Table ---------------------------------------------------------------
SELECT DISTINCT country_code, country AS country_name, [latitude_average], [longitude_average]
INTO [dim_countries]
FROM dbo.fact_swap;
-----
ALTER TABLE [dim_countries]
ALTER COLUMN country_code NVARCHAR(50) NOT NULL;
---Set primary key of countries table
alter table [dim_countries]
add constraint dim_countries_pk primary key (country_code);

--------------------------------------------------------------- Create dim_token Table ---------------------------------------------------------------

WITH source AS (
	SELECT
		source_token,
		source_kind,
		source_anchor
	FROM dbo.fact_swap
),

dest AS (
	SELECT
		dest_token,
		dest_kind,
		dest_anchor
	FROM dbo.fact_swap
),

union_table AS (
	SELECT 
		source.source_token AS TokenID,
		source.source_kind AS token_kind,
		source.source_anchor AS token_anchor
	FROM source
	UNION
	SELECT * FROM dest
)

SELECT
	*
INTO dim_token
FROM union_table;

ALTER TABLE dbo.dim_token
ALTER COLUMN TokenID INT NOT NULL;

ALTER TABLE dbo.dim_token
ADD CONSTRAINT dim_token_pk	PRIMARY KEY (TokenID);

--------------------------------------------------------------- Create dim_date Table ---------------------------------------------------------------

SELECT DISTINCT date AS datekey,
		DAY(date) AS day,
		MONTH(date) AS month,
		YEAR(date) AS year,
		CASE 
			WHEN DATEPART(dw,date) = 1 THEN 'monday'
			WHEN DATEPART(dw,date) = 2 THEN 'tuesday'
			WHEN DATEPART(dw,date) = 3 THEN 'wednesday'
			WHEN DATEPART(dw,date) = 4 THEN 'thursday'
			WHEN DATEPART(dw,date) = 5 THEN 'friday'
			WHEN DATEPART(dw,date) = 6 THEN 'saturday'
			WHEN DATEPART(dw,date) = 7 THEN 'sunday'
		END AS weekday
INTO dim_date
FROM dbo.fact_swap
-----
---- Set datekey to be not null
ALTER TABLE dim_date
ALTER COLUMN datekey DATE NOT NULL;
---- Set datekey to be primary key
alter table dim_date
add constraint dim_date_pk primary key (datekey);



--------------------------------------------------------------- Create fact_price table ---------------------------------------------------------------
-----

WITH source_token AS (
	SELECT DISTINCT
		source_token AS token_ID,
		date AS datekey,
		source_price AS price
	FROM dbo.fact_swap
),

dest_token AS (
	SELECT DISTINCT
		dest_token AS token_ID,
		date AS datekey,
		dest_price AS price
	FROM dbo.fact_swap
),

union_table AS (
	SELECT DISTINCT
		source_token.token_ID,
		source_token.datekey,
		source_token.price
	FROM source_token
	UNION
	SELECT DISTINCT
		dest_token.token_ID,
		dest_token.datekey,
		dest_token.price
	FROM dest_token
)

SELECT
	AVG(price) AS price,
	union_table.token_ID,
	union_table.datekey
INTO fact_price
FROM union_table
GROUP BY union_table.token_ID, union_table.datekey


---- Set datekey to be not null
alter table fact_price
alter column datekey date not null;

---- Set token_id to be not null
alter table fact_price
alter column token_id int not null;

alter table fact_price
add constraint fact_price_pk primary key (token_id, datekey);


------------------------------------------------------------------ MODIFYING STEP --------------------------------------------------------------------

--------------------------------------------------------------- Modify fact_swap Table ---------------------------------------------------------------

SELECT
	*
FROM dbo.fact_swap

--- add column 'fee' as revenue of platform
alter table dbo.fact_swap
add fee as volume*0.02;

alter table dbo.fact_swap
drop column 
	source_price,
	dest_price,
	latitude_average, 
	longitude_average,
	country,source_anchor,
	dest_anchor,source_kind,
	dest_kind,
	os,
	country_code,
	platform;

---- fact_swap to dim_wallet
alter table dbo.fact_swap
add constraint 
	fk_fact_swap_dim_wallet foreign key (wallet_address) references dim_wallets (wallet_address);

---- fact_swap to dim_token
alter table dbo.fact_swap
add constraint fk_fact_swap_dim_token foreign key (source_token) references dim_token (tokenID);
---- fact_swap to dim_token
alter table dbo.fact_swap
add constraint fk_fact_swap_dim_token_dest foreign key (dest_token) references dim_token (tokenID);

---- fact_swap to dim_date
alter table dbo.fact_swap
add constraint fk_fact_swap_dim_date foreign key (date) references dim_date (datekey);


--------------------------------------------------------------- Modify fact_price table ---------------------------------------------------------------

alter table fact_price
add constraint fk_fact_price_dim_date foreign key (datekey) references dim_date (datekey);


--------------------------------------------------------------- Modify dim_wallets table ---------------------------------------------------------------

alter table dim_wallets
add constraint fk_dim_wallets_dim_countries foreign key (country_code) references dim_countries (country_code);



--select * from information_schema.columns
--select top (10) * from dbo.fact_swap;



-------------------------------------------------EXPLORING AND ANALYSIS STEP-------------------------------------------------

---------------------------------------------------Revenue Increase by Day---------------------------------------------------

--SELECT date, SUM(fee) FROM dbo.fact_swap
--GROUP BY date
--ORDER BY date

WITH result AS (
	SELECT
		SUM(fee) AS rev_by_day,
		CAST(date AS DATE) AS date
	FROM dbo.fact_swap
	GROUP BY CAST(date AS DATE)
),

rev_next_day AS (
	SELECT
		rev_by_day,
		date,
		LEAD(date) OVER (ORDER BY date) AS day_next,
		LEAD(rev_by_day) OVER (ORDER BY date) AS rev_next_day
	FROM result
)

SELECT
	date,
	ROUND(((rev_next_day - rev_by_day)/rev_by_day),2) AS DOD
INTO rev_increase_by_day
FROM rev_next_day
ORDER BY date
OFFSET 1 row


SELECT
	*
FROM rev_increase_by_day

--DROP TABLE dbo.rev_increase_by_day

------------------------------------------------------------Revenue by Country---------------------------------------------------------
--SELECT * FROM dbo.fact_swap
--SELECT * FROM dbo.dim_countries

SELECT TOP 30
	country_name,
	ROUND(SUM(fee), 2) AS revenue
INTO rev_by_country
FROM dbo.fact_swap
JOIN dbo.dim_wallets ON dim_wallets.wallet_address = dbo.fact_swap.wallet_address
JOIN dbo.dim_countries ON dim_countries.country_code = dim_wallets.country_code
GROUP BY country_name
HAVING country_name IS NOT NULL
ORDER BY SUM(fee) DESC

--SELECT * FROM dbo.rev_by_country
--SELECT * FROM dbo.rev_by_token_swap

--------------------------------------------------------Revenue by token_swap------------------------------------------------------------

SELECT TOP 10
	source_token,
	dest_token,
	ROUND(SUM(fee), 2) AS revenue
INTO rev_by_token_swap
FROM dbo.fact_swap
GROUP BY source_token, dest_token
ORDER BY SUM(fee) DESC



-------------------------------------------------Transaction Count and Transaction Volume by Day-------------------------------------------------

SELECT
	COUNT(DISTINCT(txn_id)) AS count_transaction,
	SUM(volume) AS volume_transaction,
	CAST(date AS DATE) transaction_day
INTO transaction_table_by_day
FROM dbo.fact_swap
GROUP BY CAST(date AS DATE)
ORDER BY transaction_day

SELECT * FROM dbo.transaction_table_by_day ORDER BY transaction_day asc
SELECT * FROM dbo.transaction_table_by_country ORDER BY count_transaction DESC
-------------------------------------------------Transaction count and Transaction volume by country-------------------------------------------------

SELECT
	COUNT(DISTINCT(txn_id)) AS count_transaction,
	SUM(volume) AS volume_transaction,
	dc.country_name
INTO transaction_table_by_country
FROM dbo.fact_swap fs
INNER JOIN dbo.dim_wallets dw ON dw.wallet_address = fs.wallet_address
INNER JOIN dbo.dim_countries dc ON dc.country_code = dw.country_code
WHERE dc.country_name IS NOT NULL
GROUP BY dc.country_name
ORDER BY dc.country_name

SELECT * FROM transaction_table_by_country



-------------------------------------------------Number of transaction by token_type-------------------------------------------------

SELECT
	COUNT(fs.txn_id) AS transaction_count,
	dt.token_kind
INTO trans_count_by_token_type
FROM dbo.fact_swap fs
INNER JOIN dbo.dim_token dt ON dt.TokenID = fs.dest_token
GROUP BY dt.token_kind

-------------------------------------------------Number of transaction by platform----------------------------------------------------

SELECT
	COUNT(fs.txn_id),
	dw.platform
INTO transaction_count_by_platform
FROM dbo.fact_swap fs
INNER JOIN dbo.dim_wallets dw ON dw.wallet_address = fs.wallet_address
GROUP BY dw.platform


SELECT * FROM dbo.trans_count_by_token_type
SELECT * FROM dbo.dim_token