USE [msdb]
GO
/****** Object:  UserDefinedFunction [dbo].[fn_schedule_next_run]    Script Date: 31/03/2025 13:37:23 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[fn_schedule_next_run]
(
	@schedule_id int,
    @current_datetime DATETIME
)
RETURNS DATETIME
AS
BEGIN
/*
This will calculate and return the next rundate/time for any schedule in [msdb].[dbo].[sysschedules] (@from_msdb = 1)
or from local table [schedule] (@from_msdb = 0). If there is no next rundate/time, the function returns NULL.

The fields in [msdb].[dbo].[sysschedules] (and the local table [schedule]) are defined as follows 
(as seen on https://docs.microsoft.com/en-us/sql/relational-databases/system-tables/dbo-sysschedules-transact-sql?view=sql-server-ver15) 

freq_type - int

	How frequently a job runs for this schedule.

	1 = One time only
	4 = Daily
	8 = Weekly
	16 = Monthly
	32 = Monthly, relative to freq_interval
	64 = Runs when the SQL Server Agent service starts
	128 = Runs when the computer is idle

freq_interval - int	

	Days that the job is executed. Depends on the value of freq_type. 
	The default value is 0, which indicates that freq_interval is unused. 
	See the table below for the possible values and their effects.

freq_subday_type - int

	Units for the freq_subday_interval. The following are the possible 
	values and their descriptions.

	1 : At the specified time
	2 : Seconds
	4 : Minutes
	8 : Hours

freq_subday_interval - int

	Number of freq_subday_type periods to occur between each execution of the job.

freq_relative_interval - int	

	When freq_interval occurs in each month, if freq_type is 32 (monthly relative). 
	Can be one of the following values:

	0 = freq_relative_interval is unused
	1 = First
	2 = Second
	4 = Third
	8 = Fourth
	16 = Last

freq_recurrence_factor - int

	Number of weeks or months between the scheduled execution of a job. 
	freq_recurrence_factor is used only if freq_type is 8, 16, or 32. 
	If this column contains 0, freq_recurrence_factor is unused.

active_start_date - int

	Date on which execution of a job can begin. The date is formatted as YYYYMMDD. NULL indicates today's date.

active_end_date - int

	Date on which execution of a job can stop. The date is formatted YYYYMMDD.

active_start_time - int

	Time on any day between active_start_date and active_end_date that job begins executing. 
	Time is formatted HHMMSS, using a 24-hour clock.

active_end_time - int

	Time on any day between active_start_date and active_end_date that job stops executing. 
	Time is formatted HHMMSS, using a 24-hour clock.

Value of freq_type				Effect on freq_interval
-------------------------------------------------------
	1 (once)					freq_interval is unused (0)
	4 (daily)					Every freq_interval days
	8 (weekly)					freq_interval is one or more of the following:
									1 = Sunday
									2 = Monday
									4 = Tuesday
									8 = Wednesday
									16 = Thursday
									32 = Friday
									64 = Saturday
	16 (monthly)				On the freq_interval day of the month
	32 (monthly, relative)		freq_interval is one of the following:
									1 = Sunday
									2 = Monday
									3 = Tuesday
									4 = Wednesday
									5 = Thursday
									6 = Friday
									7 = Saturday
									8 = Day
									9 = Weekday
									10 = Weekend day
	64 (starts when SQL Server Agent service starts)	freq_interval is unused (0)
	128 (runs when computer is idle)					freq_interval is unused (0)

-------------------------------------------------------------
This is more or less the algorithm used to find the next time
-------------------------------------------------------------
if freq_subday_type is not in (0, 1, 2, 4, 8)
	set time = null
if now < active_start_time
	set time = active_start_time, today = true
if now > active_end_time
	set time = active_start_time, today = false
if now >= active_start_time and now <= active_end_time
	if freq_subday_type = 1
		set time = active_start_time, today = false
	if freq_subday_type = 2
		set seconds = seconds since active_start_time until now
		if (seconds mod freq_subday_interval) = 0
			set time = active_start_time + seconds, today = true
		else
			set time = active_start_time + freq_subday_interval * ((seconds div freq_subday_interval) + 1), today = true
			if time > active_end_time
				set time = active_start_time, today = false
	if freq_sub_type = 4
		set minutes = minutes since active_start_time until now
		if (minutes mod freq_subday_interval) = 0 
			if (seconds(now) <= seconds(active_start_time))
				set time = active_start_time + minutes, today = true
			else
				set time = active_start_time + freq_subday_interval * ((minutes div freq_subday_interval) + 1), today = true
				if (time > active_end_time)
					set time = active_start_time, today = false
		else
			set time = active_start_time + freq_subday_interval * ((minutes div freq_subday_interval) + 1), today = true
			if (time > active_end_time)
				set time = active_start_time, today = false
	if freq_sub_type = 8
		set hours = hours since active_start_time until now 
		if (hours mod freq_subday_interval) = 0
			if (minutes(now) < minutes(active_start_time)) or (minutes(now) = minutes(active_start_time) and seconds(now) <= seconds(active_start_time))
				set time = active_start_time + hours, today = true
			else
				set time = active_start_time + freq_subday_interval * ((hours div freq_subday_interval) + 1), today = true
				if (time > active_end_time)
					set time = active_start_time, today = false
		else
			set time = active_start_time + freq_subday_interval * ((hours div freq_subday_interval) + 1), today = true
			if (time > active_end_time)
				set time = active_start_time, today = false
				
-------------------------------------------------------------
This is more or less the algorithm used to find the next date
-------------------------------------------------------------
if today > active_end_date then return null
set date = active_start_date
if freq_type = 1 (once) => 
	if date == today then return date else return null
if freq_type = 4 (daily) => 
	if date == today then return date
	if freq_interval = 0 then return null
	while (date < today && date < active_end_date) date += freq_interval
	if date < active_end_date then return date else return null
if freq_type = 8 (weekly) =>
	while (date < today && date < active_end_date) date += 7 * freq_recurrence_factor
	while (date < active_end_date)
		set next_week_date = date + 8
		while (date < next_week_date && weekday(date) not in freq_interval) date += 1
		if (date < next_week_date) ==> return date
		date += next_week_date - 8 + 7 * freq_recurrence_factor
	end while
	return null
if freq_type = 16 (monthly) =>
	while (day(date) < freq_interval && date < active_end_dateday) date += 1
	while (date < today && date < active_end_date) set month(date) = month(date) + freq_recurrence_factor
	if (date < active_end_date) then return date else return null
if freq_type = 32 (monthly relative) =>
	while (date < active_end_date)
		set year = year(date)
		set month = month(date)
		set day = first/second/third/fourth/last (freq_relative_interval) mo/tu/we/th/fr/sa/su/day/weekday/weekendday (freq_interval) of month/year
		if (date == today) return date
		set month(date) = month(date) + freq_recurrence_factor ; set year accordingly
	end
	return null;

*/

	DECLARE
		@freq_type int,
		@freq_interval int,
		@freq_subday_type int,
		@freq_subday_interval int,
		@freq_relative_interval int,
		@freq_recurrence_factor int,
		@active_start_date int,
		@active_end_date int,
		@active_start_time int,
		@active_end_time int;

	SELECT
		@freq_type = [freq_type],
		@freq_interval = [freq_interval],
		@freq_subday_type = [freq_subday_type],
		@freq_subday_interval = [freq_subday_interval],
		@freq_relative_interval = [freq_relative_interval],
		@freq_recurrence_factor = [freq_recurrence_factor],
		@active_start_date = [active_start_date],
		@active_end_date = [active_end_date],
		@active_start_time = [active_start_time],
		@active_end_time = [active_end_time]
	FROM 
		[msdb].[dbo].[sysschedules]
	WHERE
		[schedule_id] = @schedule_id


	-- 'AT TIME ZONE' since SQL Server 2016
	DECLARE @CurrentDate DATE = @current_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
	DECLARE @CurrentTime TIME = @current_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
	DECLARE @CurrentDateAndTime DATETIME = CAST(@CurrentDate AS DATETIME) + CAST(@CurrentTime AS DATETIME)

	DECLARE @StartDateAndTime DATETIME = CAST(@CurrentDate AS datetime) + STUFF(STUFF(RIGHT('000000' + CAST(@active_start_time AS VARCHAR), 6), 5, 0, ':'), 3, 0, ':')
	DECLARE @EndDateAndTime DATETIME = CAST(@CurrentDate AS datetime) + STUFF(STUFF(RIGHT('000000' + CAST(@active_end_time AS VARCHAR), 6), 5, 0, ':'), 3, 0, ':')

	DECLARE @NextTime DATETIME = NULL
	DECLARE @ForToday BIT = 0
	DECLARE @Seconds INT
	DECLARE @Minutes INT
	DECLARE @Hours INT

	IF (@freq_subday_type NOT IN (0, 1, 2, 4, 8) OR @freq_subday_interval < 0) BEGIN
		SET @NextTime = NULL
		SET @ForToday = 1
	END
	ELSE IF @CurrentDateAndTime < @StartDateAndTime BEGIN
		SET @NextTime = @StartDateAndTime
		SET @ForToday = 1
	END
	ELSE IF (@CurrentDateAndTime > @EndDateAndTime) BEGIN
		SET @NextTime = @StartDateAndTime
		SET @ForToday = 0
	END
	ELSE IF (@freq_subday_type IN (0, 1)) BEGIN
		-- At the specified time
		SET @NextTime = @StartDateAndTime
		SET @ForToday = 0
	END
	ELSE IF (@freq_subday_type = 2) BEGIN
		-- Every @freq_subday_interval seconds
		SET @Seconds = DATEDIFF(SECOND, @StartDateAndTime, @CurrentDateAndTime)
		IF (@Seconds % @freq_subday_interval = 0) BEGIN
			SET @NextTime = @CurrentDateAndTime
			SET @ForToday = 1
		END
		ELSE BEGIN
			SET @Seconds = @freq_subday_interval * ((@Seconds / @freq_subday_interval) + 1)
			SET @NextTime = DATEADD(SECOND, @Seconds, @StartDateAndTime)
			IF (@NextTime <= @EndDateAndTime) BEGIN
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @NextTime = @StartDateAndTime
				SET @ForToday = 0
			END
		END
	END
	ELSE IF (@freq_subday_type = 4) BEGIN
		-- Every @freq_subday_interval minutes
		SET @Minutes = DATEDIFF(MINUTE, @StartDateAndTime, @CurrentDateAndTime)
		IF (@Minutes % @freq_subday_interval = 0) BEGIN
			IF (DATEPART(SECOND, @CurrentDateAndTime) <= DATEPART(SECOND, @StartDateAndTime)) BEGIN
				SET @NextTime = DATEADD(MINUTE, @Minutes, @StartDateAndTime)
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @Minutes = @freq_subday_interval * ((@Minutes / @freq_subday_interval) + 1)
				SET @NextTime = DATEADD(MINUTE, @Minutes, @StartDateAndTime)
				IF (@NextTime <= @EndDateAndTime) BEGIN
					SET @ForToday = 1
				END
				ELSE BEGIN
					SET @NextTime = @StartDateAndTime
					SET @ForToday = 0
				END
			END
		END
		ELSE BEGIN
			SET @Minutes = @freq_subday_interval * ((@Minutes / @freq_subday_interval) + 1)
			SET @NextTime = DATEADD(MINUTE, @Minutes, @StartDateAndTime)
			IF (@NextTime <= @EndDateAndTime) BEGIN
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @NextTime = @StartDateAndTime
				SET @ForToday = 0
			END
		END
	END
	ELSE IF (@freq_subday_type = 8) BEGIN
		-- Every @freq_subday_interval hours
		SET @Hours = DATEDIFF(HOUR, @StartDateAndTime, @CurrentDateAndTime)
		IF (@Hours % @freq_subday_interval = 0) BEGIN
			IF (DATEPART(MINUTE, @CurrentDateAndTime) <= DATEPART(MINUTE, @StartDateAndTime) OR (DATEPART(MINUTE, @CurrentDateAndTime) = DATEPART(MINUTE, @StartDateAndTime) AND DATEPART(SECOND, @CurrentDateAndTime) <= DATEPART(SECOND, @StartDateAndTime))) BEGIN
				SET @NextTime = DATEADD(HOUR, @Hours, @StartDateAndTime)
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @Hours= @freq_subday_interval * ((@Hours / @freq_subday_interval) + 1)
				SET @NextTime = DATEADD(HOUR, @Hours, @StartDateAndTime)
				IF (@NextTime <= @EndDateAndTime) BEGIN
					SET @ForToday = 1
				END
				ELSE BEGIN
					SET @NextTime = @StartDateAndTime
					SET @ForToday = 0
				END
			END
		END
		ELSE BEGIN
			SET @Hours= @freq_subday_interval * ((@Hours / @freq_subday_interval) + 1)
			SET @NextTime = DATEADD(HOUR, @Hours, @StartDateAndTime)
			IF (@NextTime <= @EndDateAndTime) BEGIN
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @NextTime = @StartDateAndTime
				SET @ForToday = 0
			END
		END
	END

	SET @NextTime = CASE WHEN @NextTime IS NOT NULL AND @ForToday <> 1 THEN DATEADD(DAY, 1, @NextTime) ELSE @NextTime END

	DECLARE @StartDate DATE = STUFF(STUFF(RIGHT('00000000' + CAST(@active_start_date AS VARCHAR), 8), 7, 0, '-'), 5, 0, '-')
	DECLARE @EndDate DATE = STUFF(STUFF(RIGHT('00000000' + CAST(@active_end_date AS VARCHAR), 8), 7, 0, '-'), 5, 0, '-')
	DECLARE @Today DATE = @NextTime
	DECLARE @RunningDate DATE = @NextTime
	DECLARE @NextDate DATE = NULL

	IF (@NextTime IS NULL) BEGIN
		GOTO DONE
	END

	IF (@freq_type = 1) BEGIN
		-- 1 = One time only
		SET @NextDate = CASE WHEN (@RunningDate = @Today) THEN @RunningDate ELSE NULL END
		GOTO DONE
	END

	IF (@RunningDate > @EndDate) BEGIN
		GOTO DONE
	END

	SET @RunningDate = @StartDate
	IF (@freq_type = 4) BEGIN
		-- 4 = Daily
		IF (@RunningDate = @Today) BEGIN
			SET @NextDate = @RunningDate
			GOTO DONE
		END
		IF (@freq_interval = 0) GOTO DONE
		WHILE (@RunningDate < @Today AND @RunningDate < @EndDate) BEGIN
			SET @RunningDate = DATEADD(DAY, @freq_interval, @RunningDate)
		END
		SET @NextDate = CASE WHEN (@RunningDate <= @EndDate) THEN @RunningDate ELSE NULL END
		GOTO DONE
	END

	IF (@freq_type = 8) BEGIN
		-- 8 = Weekly
		DECLARE @WhichWeekdays INT = @freq_interval
		DECLARE @EveryXWeeks INT = @freq_recurrence_factor
		DECLARE @NextWeekDate DATE

		IF ((@WhichWeekdays & 127) = 0) BEGIN
			-- if no day  of the week selected, allow any day of the week
			SET @WhichWeekdays = 127
		END
		IF (@EveryXWeeks <= 0) BEGIN
			-- if "occurs every x week" is not set, set "occurs every x week" to 1
			SET @EveryXWeeks = 1
		END
		IF (@StartDate > @Today) BEGIN
			SET @NextWeekDate = @StartDate
		END
		ELSE BEGIN
			-- set the starting week to the week of @Today
			SET @NextWeekDate = DATEADD(DAY, 7 * @EveryXWeeks * (DATEDIFF(DAY, @StartDate, @Today) / (7 * @EveryXWeeks)), @StartDate)
		END
		SET @RunningDate = @NextWeekDate
		IF (DATEDIFF(DAY, @RunningDate, @Today) >= 7) BEGIN
			SET @NextWeekDate = DATEADD(DAY, (7 * @EveryXWeeks), @NextWeekDate)
			SET @RunningDate = @NextWeekDate
		END
		ELSE BEGIN
			WHILE (@RunningDate < @Today) BEGIN
				SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
			END
		END
		IF (@RunningDate >= @Today AND @RunningDate < @EndDate AND (0 <> (POWER(2, DATEPART(WEEKDAY, @RunningDate) - 1) & @WhichWeekdays))) BEGIN
			SET @NextDate = @RunningDate
			GOTO DONE
		END
		WHILE (@RunningDate < @EndDate) BEGIN
			IF (DATEDIFF(DAY, @NextWeekDate, @RunningDate) >= 7) BEGIN
				SET @NextWeekDate = DATEADD(DAY, (7 * @EveryXWeeks), @NextWeekDate)
				SET @RunningDate = @NextWeekDate
			END
			ELSE BEGIN
				WHILE (DATEDIFF(DAY, @NextWeekDate, @RunningDate) < 7) BEGIN
					IF (@RunningDate < @EndDate AND (0 <> (POWER(2, DATEPART(WEEKDAY, @RunningDate) - 1) & @WhichWeekdays))) BEGIN
						SET @NextDate = @RunningDate
						GOTO DONE
					END
					SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
				END
			END
		END
		GOTO DONE
	END

	IF (@freq_type = 16) BEGIN
		-- 16 = Monthly
		DECLARE @DayOfTheMonth INT = @freq_interval
		DECLARE @EveryXMonths INT = @freq_recurrence_factor
		DECLARE @NextMonthDate DATE

		IF (@DayOfTheMonth > 31) BEGIN
			GOTO DONE
		END
		IF (@EveryXMonths <= 0) BEGIN
			-- if "occurs every x months" is not set, set "occurs every month"
			SET @EveryXMonths = 1
		END

		IF (@StartDate > @Today) BEGIN
			SET @NextMonthDate = DATEADD(DAY, 1 - DATEPART(DAY, @StartDate), @StartDate)
		END
		ELSE BEGIN
			-- set the next month date (is nearest 1st to today of month
			SET @NextMonthDate = DATEADD(
				MONTH, 
				@EveryXMonths 
					* (DATEDIFF(MONTH, 
						DATEADD(DAY, 1 - DATEPART(DAY, @StartDate), @StartDate), 
						DATEADD(DAY, 1 - DATEPART(DAY, @Today), @Today)) 
						/ @EveryXMonths), 
				DATEADD(DAY, 1 - DATEPART(DAY, @StartDate), @StartDate))
		END
		WHILE (DATEADD(MONTH, 1, @NextMonthDate) < @Today) BEGIN
			SET @NextMonthDate = DATEADD(MONTH, @EveryXMonths, @NextMonthDate)
		END
		SET @RunningDate = @NextMonthDate
		WHILE (@RunningDate < @Today AND @RunningDate < @EndDate) BEGIN
			SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
		END

		-- to avoid an endless loop for the below special cases of schedules, we need to count the maximum number of months
		--		- @DayOfTheMonth = 30 or 31 
		--		- @StartDate somewhere in february
		--		- @EveryXMonths = 12
		-- or also 
		--		- @DayOfTheMonth = 31 
		--		- @StartDate somewhere in a month with 30 days
		--		- @EveryXMonths = 12
		DECLARE @MaxMonths INT = 48

		IF (DATEPART(DAY, @RunningDate) <> @DayOfTheMonth) BEGIN
			WHILE (DATEPART(DAY, @RunningDate) <> @DayOfTheMonth AND @RunningDate < @EndDate AND @MaxMonths > 0) BEGIN
				IF (DATEPART(DAY, DATEADD(DAY, 1, @RunningDate)) > 1) BEGIN
					-- we are stil in same month
					SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
				END
				ELSE BEGIN
					-- we passed to the next month
					SET @NextMonthDate = DATEADD(MONTH, @EveryXMonths, @NextMonthDate)
					SET @RunningDate = @NextMonthDate
					SET @MaxMonths = @MaxMonths - @EveryXMonths
				END
			END
		END
		IF (DATEPART(DAY, @RunningDate) = @DayOfTheMonth ) BEGIN
			SET @NextDate = @RunningDate
		END
		GOTO DONE
	END

	IF (@freq_type = 32) BEGIN
		-- 32 = Monthly, relative to freq_interval
		DECLARE @FirstUpToLastType INT = @freq_relative_interval
		DECLARE @DayType INT = @freq_interval

		IF (@FirstUpToLastType = 0) BEGIN
			GOTO DONE
		END
		IF (@FirstUpToLastType = 5) BEGIN
			-- last su/mo/tu/we/th/fr/sa/day/weekday/weekendday
			SET @RunningDate = DATEADD(DAY, -1, DATEADD(MONTH, 1, DATEADD(DAY, 1 - DAY(@RunningDate), @RunningDate)))
			WHILE (@RunningDate < @EndDate) BEGIN
				IF (@DayType IN (1,2,3,4,5,6,7)) BEGIN
					-- 1 = Sunday
					-- 2 = Monday
					-- 3 = Tuesday
					-- 4 = Wednesday
					-- 5 = Thursday
					-- 6 = Friday
					-- 7 = Saturday
					WHILE (DATEPART(WEEKDAY, @RunningDate) <> @DayType) BEGIN
						SET @RunningDate = DATEADD(DAY, -1, @RunningDate)
					END
				END
				ELSE IF (@DayType = 9) BEGIN
					-- 9 = Weekday
					WHILE (DATEPART(WEEKDAY, @RunningDate) IN (1, 7)) BEGIN
						SET @RunningDate = DATEADD(DAY, -1, @RunningDate)
					END
				END
				ELSE IF (@DayType = 10) BEGIN
					-- 10 = Weekend day
					WHILE (DATEPART(WEEKDAY, @RunningDate) NOT IN (1, 7)) BEGIN
						SET @RunningDate = DATEADD(DAY, -1, @RunningDate)
					END
				END

				IF (@RunningDate >= @Today AND @RunningDate <= @EndDate) BEGIN
					SET @NextDate = @RunningDate
					GOTO DONE
				END
				IF (@RunningDate < @EndDate) BEGIN
					GOTO DONE
				END
				-- try next month - should succeed if less than @EndDate
				SET @RunningDate = DATEADD(DAY, -1, DATEADD(MONTH, 1, DATEADD(DAY, 1 - DAY(@RunningDate), @RunningDate)))
			END
			GOTO DONE
		END
		ELSE BEGIN
			-- freq_relative_interval = 1,2,3,4
			-- first/second/third/fourth su/mo/tu/we/th/fr/sa/day/weekday/weekendday
			SET @RunningDate = DATEADD(DAY, 1 - DAY(@RunningDate), @RunningDate)
			WHILE (@RunningDate <= @EndDate) BEGIN
				IF (@DayType IN (1,2,3,4,5,6,7)) BEGIN
					-- 1 = Sunday
					-- 2 = Monday
					-- 3 = Tuesday
					-- 4 = Wednesday
					-- 5 = Thursday
					-- 6 = Friday
					-- 7 = Saturday
					WHILE (DATEPART(WEEKDAY, @RunningDate) <> @DayType) BEGIN
						SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
					END
				END
				ELSE IF (@DayType = 9) BEGIN
					-- 9 = Weekday
					WHILE (DATEPART(WEEKDAY, @RunningDate) NOT IN (2,3,4,5,6)) BEGIN
						SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
					END
				END
				ELSE IF (@DayType = 10) BEGIN
					-- 10 = Weekend day
					WHILE (DATEPART(WEEKDAY, @RunningDate) NOT IN (1, 7)) BEGIN
						SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
					END
				END

				IF (@FirstUpToLastType > 1) BEGIN
					-- for second, third, fourth and last
					IF (@DayType IN (1,2,3,4,5,6,7)) BEGIN
						-- 1-7 = Weekday Sunday to Saturday
						SET @RunningDate = DATEADD(DAY, 7 * (@FirstUpToLastType - 1), @RunningDate)
					END
					ELSE IF (@DayType = 8) BEGIN
						-- 8 = Day
						SET @RunningDate = DATEADD(DAY, @FirstUpToLastType - 1, @RunningDate)
					END
					ELSE IF (@DayType = 9) BEGIN
						-- 9 = Weekday
						-- add 1, 2 or 3 days for second, third or fourth weekday unless we are 
						-- Friday (second weekday), Thursday (third weekday) or Wednesday 
						-- (fourth weekday) in which cases we add 2 more days to skip weekend
						SET @RunningDate = DATEADD(DAY, @FirstUpToLastType - 1 + 
							CASE @FirstUpToLastType 
								WHEN 2 THEN CASE WHEN DATEPART(WEEKDAY, @RunningDate) <= 6 THEN 2 ELSE 0 END
								WHEN 3 THEN CASE WHEN DATEPART(WEEKDAY, @RunningDate) <= 5 THEN 2 ELSE 0 END
								WHEN 4 THEN CASE WHEN DATEPART(WEEKDAY, @RunningDate) <= 4 THEN 2 ELSE 0 END
								ELSE 0
							END, @RunningDate)
					END
					ELSE IF (@DayType = 10) BEGIN
						-- 10 = Weekend day
						-- this implementation will take the first day of the next weekend, not the next weekend day
						-- add 7, 14 or 21 days for next weekend unless we are Sunday where we need to add 
						-- 1 day less to have the Saturday of the next weekend 
						SET @RunningDate = DATEADD(DAY, 7 * (@FirstUpToLastType - 1) - CASE WHEN DATEPART(WEEKDAY, @RunningDate) = 6 THEN 1 ELSE 0 END, @RunningDate)
					END
				END
				IF (@RunningDate >= @Today AND @RunningDate <= @EndDate) BEGIN
					SET @NextDate = @RunningDate
					GOTO DONE
				END
				IF (@RunningDate < @EndDate) BEGIN
					GOTO DONE
				END
				-- try next month - should succeed if less than @EndDate
				SET @RunningDate = DATEADD(MONTH, 1, DATEADD(DAY, 1 - DAY(@RunningDate), @RunningDate))
			END
		END
	END

DONE:
	IF (@NextDate IS NULL OR @NextTime IS NULL) BEGIN
		RETURN NULL
	END

	RETURN CAST(@NextDate AS DATETIME) + CAST(CAST(@NextTime AS TIME) AS DATETIME)
END