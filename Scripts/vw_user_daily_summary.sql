-- ============================================================
-- Title: vw_user_daily_summary
-- Author: Matthew Beeun
-- Date: 23rd September, 2025
-- Description:
--   This view consolidates daily user metrics including activity,
--   sleep, calories burned, and BMI. It serves as a unified source
--   for behavioral analysis and segmentation in the Bellabeat case study.
--
-- Purpose:
--   - Simplify downstream analysis and reporting
--   - Enable visualization tools to query clean, joined data
--   - Support stakeholder insights into user engagement patterns
--
-- Source Tables:
--   11 tables affected for all narrow merged except the wide merged

-- Notes:
--   - Nulls came in during the joins.
--   - Joins performed on Id and ActivityDate
-- ============================================================



--CREATE SCHEMA bellabeat
CREATE VIEW bellabeat.leaf_user_summary AS 

(SELECT
	DAM.Id,
	DAM.ActivityDate,
	DAM.TotalSteps,
	DAM.TotalDistance,
	DAM.VeryActiveDistance,
	DAM.ModeratelyActiveDistance,
	DAM.LightActiveDistance,
	DAM.SedentaryActiveDistance,
	DAM.VeryActiveMinutes,
	DAM.FairlyActiveMinutes,
	DAM.LightlyActiveMinutes,
	DAM.SedentaryMinutes,
	DAM.Calories AS DailyCalories,
	DAM.ActiveMinutes,
	DAM.CaloriesPerStep,
	DAM.StepDensity,
	DAM.ActivityFlag,
	DSM.StepTotal AS DailyStepTotal,
	DSM.FlagStep,
	HSM.StepTotal AS HourlyStepTotal,
	HIM.AverageIntensity,
	HIM.TotalIntensity,
	SDM.TotalMinutesAsleep,
	SDM.TotalSleepRecords, 
	SDM.TotalTimeInBed,
	MSM.value AS MinutesValue,
	DIM.SedentaryRatio,
	HRM.Value AS HourlyValue, 
	HCM.Calories AS HourlyCalories,
	MCN.Calories AS MinutesCalories,
	WLM.BMI,
	WLM.Fat,
	WLM.IsManualReport,
	WLM.WeightKg,
	WLM.WeightPounds
FROM bellabeat_cleaned.dailyActivity_merged AS DAM
	LEFT JOIN bellabeat_cleaned.dailySteps_merged AS DSM
	ON DAM.Id = DSM.Id AND DAM.ActivityDate = DSM.ActivityDay
	LEFT JOIN bellabeat_cleaned.hourlySteps_merged AS HSM
	ON DAM.Id = HSM.Id AND DAM.ActivityDate = HSM.ActivityHour
	LEFT JOIN bellabeat_cleaned.hourlyIntensities_merged AS HIM
	ON DAM.Id = HIM.Id AND DAM.ActivityDate = HIM.ActivityHour
	LEFT JOIN bellabeat_cleaned.sleepDay_merged AS SDM
	ON DAM.Id = SDM.Id AND DAM.ActivityDate = SDM.SleepDay
	LEFT JOIN bellabeat_cleaned.minuteSleep_merged AS MSM
	ON DAM.Id = MSM.Id AND DAM.ActivityDate = MSM.date
	LEFT JOIN bellabeat_cleaned.dailyIntensities_merged AS DIM
	ON DAM.Id = DIM.Id AND DAM.ActivityDate = DIM.ActivityDay
	LEFT JOIN bellabeat_cleaned.heartrate_seconds_merged AS HRM
	ON DAM.Id = HRM.Id AND DAM.ActivityDate = HRM.Time
	--WHERE MSM.Value IS NOT NULL OR HRM.Value IS NOT NULL
	LEFT JOIN bellabeat_cleaned.hourlyCalories_merged AS HCM
	ON DAM.Id = HCM.Id AND DAM.ActivityDate = HCM.ActivityHour
	LEFT JOIN bellabeat_cleaned.minuteCaloriesNarrow_merged AS MCN
	ON DAM.Id = MCN.Id AND DAM.ActivityDate = MCN.ActivityMinute
	LEFT JOIN bellabeat_cleaned.weightLogInfo_merged AS WLM
	ON DAM.Id = WLM.Id AND DAM.ActivityDate = WLM.Date
)	

-- SELECT * FROM bellabeat.leaf_user_summary