-- ================================================
-- Title: Clean Activity Data
-- Author: Matthew Beeun
-- Date: [Insert Date]
-- Description: 
--   This script removes duplicates, handles missing values,
--   and standardizes the structure of the daily activity dataset
--   for the Bellabeat case study. Prepares data for merging.
-- Tools: SQL Server Management Studio
-- Dataset: First 12 tables.
-- ================================================
--1. Inserting the cleaned data from google use case to bellabeat cleaned.
INSERT INTO bellabeat_cleaned.dailyActivity_merged (
    Id,
    ActivityDate,
    TotalSteps,
    TotalDistance,
    TrackerDistance,
    LoggedActivitiesDistance,
    VeryActiveDistance,
    ModeratelyActiveDistance,
    LightActiveDistance,
    SedentaryActiveDistance,
    VeryActiveMinutes,
    FairlyActiveMinutes,
    LightlyActiveMinutes,
    SedentaryMinutes,
    Calories,
    ActiveMinutes,
    CaloriesPerStep,
    StepDensity,
    ActivityFlag
)
-- your SELECT follows here
SELECT
	Id,
	CONVERT(DATE, ActivityDate) AS ActivityDate,
	TotalSteps,
	TotalDistance,
	TrackerDistance,
	LoggedActivitiesDistance,
	VeryActiveDistance,
	ModeratelyActiveDistance,
	LightActiveDistance,
	SedentaryActiveDistance,
	VeryActiveMinutes,
	FairlyActiveMinutes,
	LightlyActiveMinutes,
	SedentaryMinutes,
	Calories,

	-- sum of all movements-related minutes
    (VeryActiveMinutes + FairlyActiveMinutes + LightlyActiveMinutes) AS ActiveMinutes,

	--Efficiency of movements
      CASE 
           WHEN TotalSteps = 0 THEN NULL
           ELSE Calories / TotalSteps
      END AS CaloriesPerStep,
	
	-- Steps per unit of distance
       CASE 
           WHEN TotalDistance = 0 THEN NULL
           ELSE TotalSteps / TotalDistance
       END AS StepDensity,

	-- show active steps taken
	   	CASE 
	  WHEN TotalSteps = 0 AND Calories = 0 THEN 'NoActivity'
	  ELSE 'Active'
	END AS ActivityFlag

FROM (
	 SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY Id  ORDER BY ActivityDate) AS Ranking
	FROM google_project_case_study.dailyActivity_merged
	WHERE Id IS NOT NULL
	)t
WHERE Ranking = 1


--2. Cleaning and inserting into the bellabeat_cleaned.dailyCalories_-merged
INSERT INTO bellabeat_cleaned.dailyCalories_merged(
	Id,
	ActivityDay,
	Calories
)
SELECT
	Id,
	CONVERT(DATE,ActivityDay) AS ActivityDay,
	Calories
FROM
	(

	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityDay) AS Ranking
	FROM google_project_case_study.dailyCalories_merged
	WHERE Id IS NOT NULL
	)t
WHERE Ranking = 1;

-- SELECT * FROM bellabeat_cleaned.dailyCalories_merged

--3. Cleaning and inserting into the table bellabeat_cleaned.dailyIntensities
INSERT INTO bellabeat_cleaned.dailyIntensities_merged(
	Id,
	ActivityDay,
	SedentaryMinutes,
	LightlyActiveMinutes,
	FairlyActiveMinutes,
	VeryActiveMinutes,
	SedentaryActiveDistance,
	LightActiveDistance,
	ModeratelyActiveDistance,
	VeryActiveDistance,
	ActiveMinutes,
	SedentaryRatio
)
SELECT
	Id,
	CONVERT(DATE, ActivityDay),
	SedentaryMinutes,
	LightlyActiveMinutes,
	FairlyActiveMinutes,
	VeryActiveMinutes,
	SedentaryActiveDistance,
	LightActiveDistance,
	ModeratelyActiveDistance,
	VeryActiveDistance,
	(LightlyActiveMinutes+FairlyActiveMinutes+VeryActiveMinutes) AS ActiveMinutes,
	(SedentaryMinutes/1440) AS SedentaryRatio
FROM
	-- Cleaning and removing duplicates
	(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityDay) Ranking
	FROM google_project_case_study.dailyIntensities_merged
	WHERE Id IS NOT NULL
	)t
WHERE Ranking = 1;

--SELECT * FROM bellabeat_cleaned.dailyIntensities_merged


--4. Cleaning and inserting into the table bellabeat_cleaned.dailySteps
INSERT INTO bellabeat_cleaned.dailySteps_merged(
	Id,
	ActivityDay,
	StepTotal,
	FlagStep
)
SELECT
	Id,
	CONVERT(DATE, ActivityDay) AS ActivityDay,
	StepTotal,
	CASE 
		WHEN StepTotal = 0 THEN 'NoSteps'
		ELSE 'StepsRecorded'
	END AS FlagStep
FROM
	(SELECT 
		*,
	ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityDay) AS Ranking
	FROM google_project_case_study.dailySteps_merged
	WHERE Id IS NOT NULL)t
WHERE Ranking = 1;

-- SELECT * FROM bellabeat_cleaned.dailySteps_merged

-- 5. Remaining duplicates in primary key from the table google_project_case_study.heartrate_seconds_merged

INSERT INTO bellabeat_cleaned.heartrate_seconds_merged(
	Id,
	Time,
	Value
) 
SELECT
	Id,
	CONVERT(DATE, Time) AS Time,
	CAST(Value AS INT) AS Value
FROM
	(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY Time) AS Ranking
	FROM google_project_case_study.heartrate_seconds_merged
	WHERE Id IS NOT NULL
)t
WHERE Ranking = 1;

-- SELECT * FROM bellabeat_cleaned.heartrate_seconds_merged

-- 6. Clean and insert into the bellabeat_cleaned.hourlyCalories_merged
INSERT INTO bellabeat_cleaned.hourlyCalories_merged(
	Id,
	ActivityHour,
	Calories
)
SELECT
	Id,
	CONVERT(DATE, ActivityHour) ActivityHour,
	CAST(Calories AS INT) Calories
FROM
-- using the subquery to remove duplicates in primary key
	(
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityHour) ranking
	FROM google_project_case_study.hourlyCalories_merged
	WHERE Id IS NOT NULL
	)t
WHERE ranking = 1;

SELECT * FROM bellabeat_cleaned.hourlyCalories_merged

-- 7. Clean and insert into bellabeat_cleaned.hourlyIntensities_merged
INSERT INTO bellabeat_cleaned.hourlyIntensities_merged (
    Id,
    ActivityHour,
    TotalIntensity,
    AverageIntensity
)
SELECT
	Id,
	CONVERT(DATE, ActivityHour) AS ActivityHour,
	CAST(TotalIntensity AS INT) AS TotalIntensity,
	CAST(AverageIntensity AS FLOAT) AS AverageIntensity
FROM
	(
	  SELECT
	  *,
	  ROW_NUMBER() OVER (PARTITION BY Id ORDER BY ActivityHour) AS ranking
	  FROM google_project_case_study.hourlyIntensities_merged
	  WHERE Id IS NOT NULL AND TotalIntensity IS NOT NULL AND AverageIntensity IS NOT NULL
	)t
WHERE ranking = 1;

--SELECT * FROM bellabeat_cleaned.hourlyIntensities_merged

--8. Clean and insert into bellabeat_cleaned.hourlySteps_merged
INSERT INTO bellabeat_cleaned.hourlySteps_merged(
	Id,
	ActivityHour,
	StepTotal
)
SELECT
	Id,
	CONVERT(DATE, ActivityHour) AS ActivityHour,
	CAST(StepTotal AS INT) AS StepTotal
FROM
	(SELECT
	* ,
	ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityHour) AS ranking
	FROM google_project_case_study.hourlySteps_merged
	WHERE Id IS NOT NULL AND StepTotal IS NOT NULL
	)t
WHERE ranking = 1

--SELECT * FROM bellabeat_cleaned.hourlySteps_merged

-- 9. Clean and insert into bellabeat_cleaned.minuteCaloriesNarrow_merged
INSERT INTO bellabeat_cleaned.minuteCaloriesNarrow_merged(
	Id,
	ActivityMinute,
	Calories
)
SELECT
	Id,
	CONVERT(DATE, ActivityMinute) AS ActivityMinute,
	CAST(Calories AS FLOAT) AS Calories
FROM
		(
		SELECT
			*,
			ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityMinute) AS ranking
		FROM google_project_case_study.minuteCaloriesNarrow_merged 
		WHERE Id IS NOT NULL
		--AND Calories IS NOT NULL
		)t
WHERE ranking = 1	

--SELECT * FROM bellabeat_cleaned.minuteCaloriesNarrow_merged

--10. Clean and insert into bellabeat_cleaned.minuteCaloriesWide_merged
INSERT INTO bellabeat_cleaned.minuteCaloriesWide_merged (
	Id,
	ActivityHour,
	Calories00,
	Calories01,
	Calories02,
	Calories03,
	Calories04,
	Calories05,
	Calories06,
	Calories07,
	Calories08,
	Calories09,
	Calories10,
	Calories11,
	Calories12,
	Calories13,
	Calories14,
	Calories15,
	Calories16,
	Calories17,
	Calories18,
	Calories19,
	Calories20,
	Calories21,
	Calories22,
	Calories23,
	Calories24,
	Calories25,
	Calories26,
	Calories27,
	Calories28,
	Calories29,
	Calories30,
	Calories31,
	Calories32,
	Calories33,
	Calories34,
	Calories35,
	Calories36,
	Calories37,
	Calories38,
	Calories39,
	Calories40,
	Calories41,
	Calories42,
	Calories43,
	Calories44,
	Calories45,
	Calories46,
	Calories47,
	Calories48,
	Calories49,
	Calories50,
	Calories51,
	Calories52,
	Calories53,
	Calories54,
	Calories55,
	Calories56,
	Calories57,
	Calories58,
	Calories59	
)
SELECT
	Id,
	CONVERT(DATE, ActivityHour) AS ActivityHour,
	CAST(Calories00 AS FLOAT) Calories00,
	CAST(Calories01 AS FLOAT) Calories01,
	CAST(Calories02 AS FLOAT) Calories02,
	CAST(Calories03 AS FLOAT) Calories03,
	CAST(Calories04 AS FLOAT) Calories04,
	CAST(Calories05 AS FLOAT) Calories05,
	CAST(Calories06 AS FLOAT) Calories06,
	CAST(Calories07 AS FLOAT) Calories07,
	CAST(Calories08 AS FLOAT) Calories08,
	CAST(Calories09 AS FLOAT) Calories09,
	CAST(Calories10 AS FLOAT) Calories10,
	CAST(Calories11 AS FLOAT) Calories11,
	CAST(Calories12 AS FLOAT) Calories12,
	CAST(Calories13 AS FLOAT) Calories13,
	CAST(Calories14 AS FLOAT) Calories14,
	CAST(Calories15 AS FLOAT) Calories15,
	CAST(Calories16 AS FLOAT) Calories16,
	CAST(Calories17 AS FLOAT) Calories17,
	CAST(Calories18 AS FLOAT) Calories18,
	CAST(Calories19 AS FLOAT) Calories19,
	CAST(Calories20 AS FLOAT) Calories20,
	CAST(Calories21 AS FLOAT) Calories21,
	CAST(Calories22 AS FLOAT) Calories22,
	CAST(Calories23 AS FLOAT) Calories23,
	CAST(Calories24 AS FLOAT) Calories24,
	CAST(Calories25 AS FLOAT) Calories25,
	CAST(Calories26 AS FLOAT) Calories26,
	CAST(Calories27 AS FLOAT) Calories27,
	CAST(Calories28 AS FLOAT) Calories28,
	CAST(Calories29 AS FLOAT) Calories29,
	CAST(Calories30 AS FLOAT) Calories30,
	CAST(Calories31 AS FLOAT) Calories31,
	CAST(Calories32 AS FLOAT) Calories32,
	CAST(Calories33 AS FLOAT) Calories33,
	CAST(Calories34 AS FLOAT) Calories34,
	CAST(Calories35 AS FLOAT) Calories35,
	CAST(Calories36 AS FLOAT) Calories36,
	CAST(Calories37 AS FLOAT) Calories37,
	CAST(Calories38 AS FLOAT) Calories38,
	CAST(Calories39 AS FLOAT) Calories39,
	CAST(Calories40 AS FLOAT) Calories40,
	CAST(Calories41 AS FLOAT) Calories41,
	CAST(Calories42 AS FLOAT) Calories42,
	CAST(Calories43 AS FLOAT) Calories43,
	CAST(Calories44 AS FLOAT) Calories44,
	CAST(Calories45 AS FLOAT) Calories45,
	CAST(Calories46 AS FLOAT) Calories46,
	CAST(Calories47 AS FLOAT) Calories47,
	CAST(Calories48 AS FLOAT) Calories48,
	CAST(Calories49 AS FLOAT) Calories49,
	CAST(Calories50 AS FLOAT) Calories50,
	CAST(Calories51 AS FLOAT) Calories51,
	CAST(Calories52 AS FLOAT) Calories52,
	CAST(Calories53 AS FLOAT) Calories53,
	CAST(Calories54 AS FLOAT) Calories54,
	CAST(Calories55 AS FLOAT) Calories55,
	CAST(Calories56 AS FLOAT) Calories56,
	CAST(Calories57 AS FLOAT) Calories57,
	CAST(Calories58 AS FLOAT) Calories58,
	CAST(Calories59 AS FLOAT) Calories59
FROM
	(
	SELECT 
		* ,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityHour) As ranking
	FROM google_project_case_study.minuteCaloriesWide_merged
	WHERE Id IS NOT NULL)t
WHERE ranking  = 1

--SELECT * FROM bellabeat_cleaned.minuteCaloriesWide_merged

--11. Clean and insert data into bellabeat_cleaned.minuteIntensitiesNarrow_merged
INSERT INTO bellabeat_cleaned.minuteIntensitiesNarrow_merged(
	Id,
	ActivityMinute,
	Intensity
)
SELECT
	Id,
	CONVERT(DATE, ActivityMinute) AS ActivityMinute,
	CAST(Intensity AS INT) AS Intensity
FROM
	(
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityMinute) ranking
	FROM google_project_case_study.minuteIntensitiesNarrow_merged
	WHERE Id IS NOT NULL)t
WHERE ranking = 1;

--SELECT * FROM bellabeat_cleaned.minuteIntensitiesNarrow_merged.

--12. Clean and insert data into the table bellabeat_cleaned.minutesIntensitiesWide_merged
INSERT INTO bellabeat_cleaned.minuteIntensitiesWide_merged(
	Id,
	ActivityHour,
	Intensity00,
	Intensity01,
	Intensity02,
	Intensity03,
	Intensity04,
	Intensity05,
	Intensity06,
	Intensity07,
	Intensity08,
	Intensity09,
	Intensity10,
	Intensity11,
	Intensity12,
	Intensity13,
	Intensity14,
	Intensity15,
	Intensity16,
	Intensity17,
	Intensity18,
	Intensity19,
	Intensity20,
	Intensity21,
	Intensity22,
	Intensity23,
	Intensity24,
	Intensity25,
	Intensity26,
	Intensity27,
	Intensity28,
	Intensity29,
	Intensity30,
	Intensity31,
	Intensity32,
	Intensity33,
	Intensity34,
	Intensity35,
	Intensity36,
	Intensity37,
	Intensity38,
	Intensity39,
	Intensity40,
	Intensity41,
	Intensity42,
	Intensity43,
	Intensity44,
	Intensity45,
	Intensity46,
	Intensity47,
	Intensity48,
	Intensity49,
	Intensity50,
	Intensity51,
	Intensity52,
	Intensity53,
	Intensity54,
	Intensity55,
	Intensity56,
	Intensity57,
	Intensity58,
	Intensity59	
)
SELECT
	Id,
	CONVERT(DATE, ActivityHour) ActivityHour,
	CAST(Intensity00 AS FLOAT) Intensity00,
	CAST(Intensity01 AS FLOAT) Intensity01,
	CAST(Intensity02 AS FLOAT) Intensity02,
	CAST(Intensity03 AS FLOAT) Intensity03,
	CAST(Intensity04 AS FLOAT) Intensity04,
	CAST(Intensity05 AS FLOAT) Intensity05,
	CAST(Intensity06 AS FLOAT) Intensity06,
	CAST(Intensity07 AS FLOAT) Intensity07,
	CAST(Intensity08 AS FLOAT) Intensity08,
	CAST(Intensity09 AS FLOAT) Intensity09,
	CAST(Intensity10 AS FLOAT) Intensity10,
	CAST(Intensity11 AS FLOAT) Intensity11,
	CAST(Intensity12 AS FLOAT) Intensity12,
	CAST(Intensity13 AS FLOAT) Intensity13,
	CAST(Intensity14 AS FLOAT) Intensity14,
	CAST(Intensity15 AS FLOAT) Intensity15,
	CAST(Intensity16 AS FLOAT) Intensity16,
	CAST(Intensity17 AS FLOAT) Intensity17,
	CAST(Intensity18 AS FLOAT) Intensity18,
	CAST(Intensity19 AS FLOAT) Intensity19,
	CAST(Intensity20 AS FLOAT) Intensity20,
	CAST(Intensity21 AS FLOAT) Intensity21,
	CAST(Intensity22 AS FLOAT) Intensity22,
	CAST(Intensity23 AS FLOAT) Intensity23,
	CAST(Intensity24 AS FLOAT) Intensity24,
	CAST(Intensity25 AS FLOAT) Intensity25,
	CAST(Intensity26 AS FLOAT) Intensity26,
	CAST(Intensity27 AS FLOAT) Intensity27,
	CAST(Intensity28 AS FLOAT) Intensity28,
	CAST(Intensity29 AS FLOAT) Intensity29,
	CAST(Intensity30 AS FLOAT) Intensity30,
	CAST(Intensity31 AS FLOAT) Intensity31,
	CAST(Intensity32 AS FLOAT) Intensity32,
	CAST(Intensity33 AS FLOAT) Intensity33,
	CAST(Intensity34 AS FLOAT) Intensity34,
	CAST(Intensity35 AS FLOAT) Intensity35,
	CAST(Intensity36 AS FLOAT) Intensity36,
	CAST(Intensity37 AS FLOAT) Intensity37,
	CAST(Intensity38 AS FLOAT) Intensity38,
	CAST(Intensity39 AS FLOAT) Intensity39,
	CAST(Intensity40 AS FLOAT) Intensity40,
	CAST(Intensity41 AS FLOAT) Intensity41,
	CAST(Intensity42 AS FLOAT) Intensity42,
	CAST(Intensity43 AS FLOAT) Intensity43,
	CAST(Intensity44 AS FLOAT) Intensity44,
	CAST(Intensity45 AS FLOAT) Intensity45,
	CAST(Intensity46 AS FLOAT) Intensity46,
	CAST(Intensity47 AS FLOAT) Intensity47,
	CAST(Intensity48 AS FLOAT) Intensity48,
	CAST(Intensity49 AS FLOAT) Intensity49,
	CAST(Intensity50 AS FLOAT) Intensity50,
	CAST(Intensity51 AS FLOAT) Intensity51,
	CAST(Intensity52 AS FLOAT) Intensity52,
	CAST(Intensity53 AS FLOAT) Intensity53,
	CAST(Intensity54 AS FLOAT) Intensity54,
	CAST(Intensity55 AS FLOAT) Intensity55,
	CAST(Intensity56 AS FLOAT) Intensity56,
	CAST(Intensity57 AS FLOAT) Intensity57,
	CAST(Intensity58 AS FLOAT) Intensity58,
	CAST(Intensity59 AS FLOAT) Intensity59
FROM
		(SELECT *,
		ROW_NUMBER() OVER(PARTITION BY Id ORDER BY ActivityHour) ranking
		FROM google_project_case_study.minuteIntensitiesWide_merged
		WHERE Id IS NOT NULL)t
WHERE ranking = 1


--SELECT * FROM bellabeat_cleaned.minuteIntensitiesWide_merged

