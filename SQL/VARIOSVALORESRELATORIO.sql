declare @param varchar(100)
set @param = '73417,72086,73170'
set @param = concat(@param,',')


;with cte as 
		(	
			select left(@param,CHARINDEX(',',@param)-1) param,SUBSTRING(@param,CHARINDEX(',',@param)+1,LEN(@param)) param1 
			union all 
			select left(param1,CHARINDEX(',',param1)-1) param,SUBSTRING(param1,CHARINDEX(',',param1)+1,LEN(param1)) param1
			from cte  
			where len(param1)>1
		)
select * from cte
