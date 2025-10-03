-- ================================================
-- Title: Cleaning Data
-- Author: Matthew Beeun
-- Date: 22nd September, 2025
-- Description: 
--   This script removes duplicates, handles missing values,
--   and standardizes the structure of the daily activity dataset
--   for the Bellabeat case study. Prepares data for merging.
-- Tools: SQL Server Management Studio
-- Dataset: Last 6 tables.
-- ================================================

-- 13. Clean and insert into bellabeat_cleaned.minuteMETsNarrow_merged
INSERT INTO bellabeat_cleaned.minuteMETsNarrow_merged (
	Id,
	ActivityMinute,
	METs
)
SELECT
	Id,
	CONVERT(DATE, ActivityMinute) AS ActivityMinute,
	CAST(METs AS INT) AS METs
FROM
--remove duplicates in primary key using rownumber in subquery
	(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityMinute) ranking
	FROM
		google_project_case_study.minuteMETsNarrow_merged
	WHERE Id IS NOT NULL
	)t
WHERE ranking = 1;

--SELECT * FROM bellabeat_cleaned.minuteMETsNarrow_merged

--14. Clean and insert into bellabeat_cleaned.minuteSleep_merged
INSERT INTO bellabeat_cleaned.minuteSleep_merged (
	Id,
	date,
	value,
	logId
)
SELECT
	Id,
	CONVERT(DATE, date) AS date,
	CAST(value AS INT) AS Value,
	logId
FROM
	(SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY Id ORDER BY date) ranking
	--ROW_NUMBER() OVER(PARTITION BY logId ORDER BY date) logranking
	FROM google_project_case_study.minuteSleep_merged
	WHERE Id IS NOT NULL)t	
WHERE ranking = 1;

--SELECT * FROM bellabeat_cleaned.minuteSleep_merged.

--15. Clean and insert into bellabeat_cleaned.minuteStepsNarrow_merged
INSERT INTO bellabeat_cleaned.minuteStepsNarrow_merged (
	Id,
	ActivityMinute,
	Steps
)
SELECT
	Id,
	CONVERT(DATE, ActivityMinute) AS ActivityMinute,
	CAST(Steps AS INT) AS Steps
FROM
	(SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityMinute) ranking
	--ROW_NUMBER() OVER(PARTITION BY logId ORDER BY date) logranking
	FROM google_project_case_study.minuteStepsNarrow_merged
	WHERE Id IS NOT NULL
	)t
WHERE ranking = 1

--SELECT * FROM bellabeat_cleaned.minuteStepsNarrow_merged

--16. Clean and Insert into bellabeat_cleaned.minuteStepsWide_merged
INSERT INTO bellabeat_cleaned.minuteStepsWide_merged (
	Id,
	ActivityHour,
	Steps00,
	Steps01,
	Steps02,
	Steps03,
	Steps04,
	Steps05,
	Steps06,
	Steps07,
	Steps08,
	Steps09,
	Steps10,
	Steps11,
	Steps12,
	Steps13,
	Steps14,
	Steps15,
	Steps16,
	Steps17,
	Steps18,
	Steps19,
	Steps20,
	Steps21,
	Steps22,
	Steps23,
	Steps24,
	Steps25,
	Steps26,
	Steps27,
	Steps28,
	Steps29,
	Steps30,
	Steps31,
	Steps32,
	Steps33,
	Steps34,
	Steps35,
	Steps36,
	Steps37,
	Steps38,
	Steps39,
	Steps40,
	Steps41,
	Steps42,
	Steps43,
	Steps44,
	Steps45,
	Steps46,
	Steps47,
	Steps48,
	Steps49,
	Steps50,
	Steps51,
	Steps52,
	Steps53,
	Steps54,
	Steps55,
	Steps56,
	Steps57,
	Steps58,
	Steps59
)
SELECT
	Id,
	CONVERT(DATE, ActivityHour) AS ActivityHour,
	CAST(Steps00 AS INT) Steps00,
	CAST(Steps01 AS INT) Steps01,
	CAST(Steps02 AS INT) Steps02,
	CAST(Steps03 AS INT) Steps03,
	CAST(Steps04 AS INT) Steps04,
	CAST(Steps05 AS INT) Steps05,
	CAST(Steps06 AS INT) Steps06,
	CAST(Steps07 AS INT) Steps07,
	CAST(Steps08 AS INT) Steps08,
	CAST(Steps09 AS INT) Steps09,
	CAST(Steps10 AS INT) Steps10,
	CAST(Steps11 AS INT) Steps11,
	CAST(Steps12 AS INT) Steps12,
	CAST(Steps13 AS INT) Steps13,
	CAST(Steps14 AS INT) Steps14,
	CAST(Steps15 AS INT) Steps15,
	CAST(Steps16 AS INT) Steps16,
	CAST(Steps17 AS INT) Steps17,
	CAST(Steps18 AS INT) Steps18,
	CAST(Steps19 AS INT) Steps19,
	CAST(Steps20 AS INT) Steps20,
	CAST(Steps21 AS INT) Steps21,
	CAST(Steps22 AS INT) Steps22,
	CAST(Steps23 AS INT) Steps23,
	CAST(Steps24 AS INT) Steps24,
	CAST(Steps25 AS INT) Steps25,
	CAST(Steps26 AS INT) Steps26,
	CAST(Steps27 AS INT) Steps27,
	CAST(Steps28 AS INT) Steps28,
	CAST(Steps29 AS INT) Steps29,
	CAST(Steps30 AS INT) Steps30,
	CAST(Steps31 AS INT) Steps31,
	CAST(Steps32 AS INT) Steps32,
	CAST(Steps33 AS INT) Steps33,
	CAST(Steps34 AS INT) Steps34,
	CAST(Steps35 AS INT) Steps35,
	CAST(Steps36 AS INT) Steps36,
	CAST(Steps37 AS INT) Steps37,
	CAST(Steps38 AS INT) Steps38,
	CAST(Steps39 AS INT) Steps39,
	CAST(Steps40 AS INT) Steps40,
	CAST(Steps41 AS INT) Steps41,
	CAST(Steps42 AS INT) Steps42,
	CAST(Steps43 AS INT) Steps43,
	CAST(Steps44 AS INT) Steps44,
	CAST(Steps45 AS INT) Steps45,
	CAST(Steps46 AS INT) Steps46,
	CAST(Steps47 AS INT) Steps47,
	CAST(Steps48 AS INT) Steps48,
	CAST(Steps49 AS INT) Steps49,
	CAST(Steps50 AS INT) Steps50,
	CAST(Steps51 AS INT) Steps51,
	CAST(Steps52 AS INT) Steps52,
	CAST(Steps53 AS INT) Steps53,
	CAST(Steps54 AS INT) Steps54,
	CAST(Steps55 AS INT) Steps55,
	CAST(Steps56 AS INT) Steps56,
	CAST(Steps57 AS INT) Steps57,
	CAST(Steps58 AS INT) Steps58,
	CAST(Steps59 AS INT) Steps59	
FROM
(SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityHour) ranking
	--ROW_NUMBER() OVER(PARTITION BY logId ORDER BY date) logranking
	FROM google_project_case_study.minuteStepsWide_merged
	WHERE Id IS NOT NULL
	)t
WHERE ranking = 1

--SELECT * FROM bellabeat_cleaned.minuteStepsWide_merged

-- 17. Clean and insert into bellabeat_cleaned.sleepDay_merged
INSERT INTO bellabeat_cleaned.sleepDay_merged (
	Id,
	SleepDay,
	TotalSleepRecords,
	TotalMinutesAsleep,
	TotalTimeInBed
)
SELECT
	Id,
	CONVERT(DATE, SleepDay) AS SleepDay,
	CAST(TotalSleepRecords AS INT) AS TotalSleepRecords,
	CAST(TotalMinutesAsleep AS INT) AS TotalMinutesASleep,
	CAST(TotalTimeInBed AS INT) AS TotalTimeInBed
FROM 
	(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY SleepDay) ranking
	FROM google_project_case_study.sleepDay_merged
	WHERE Id IS NOT NULL
	)t

--SELECT * FROM bellabeat_cleaned.sleepDay_merged

-- 18. Clean and insert into bellabeat_cleaned.weightLogInfo_merged
INSERT INTO bellabeat_cleaned.weightLogInfo_merged (
	Id,
	Date,
	WeightKg,
	WeightPounds,
	Fat,
	BMI,
	IsManualReport,
	LogId
)
SELECT
	Id,
	CONVERT(DATE, Date) AS Date,
	TRY_CAST(WeightKg AS FLOAT) AS WeightKg,
	TRY_CAST(WeightPounds AS FLOAT) AS WeightPounds,
	TRY_CAST(Fat AS FLOAT) AS Fat,
	TRY_CAST(BMI AS FLOAT) AS BMI,
	IsManualReport,
	LogId
FROM
	(SELECT
			*,
			ROW_NUMBER() OVER(PARTITION BY Id ORDER BY Date) ranking
		FROM google_project_case_study.weightLogInfo_merged
		WHERE Id IS NOT NULL
		)t
WHERE ranking = 1

SELECT * FROM bellabeat_cleaned.weightLogInfo_merged
