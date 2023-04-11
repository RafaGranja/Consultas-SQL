DECLARE @LOGIN VARCHAR(MAX)
SET @LOGIN = 'FLUIG'
   
SELECT
NULL AS NESTED_LEVEL,
A.SESSION_ID AS SESSION_ID,
'(' + CAST(COALESCE(E.WAIT_DURATION_MS, B.WAIT_TIME) AS VARCHAR(20)) + 'MS)' + COALESCE(E.WAIT_TYPE, B.WAIT_TYPE) + COALESCE((CASE 
WHEN COALESCE(E.WAIT_TYPE, B.WAIT_TYPE) LIKE 'PAGE%LATCH%' THEN ':' + DB_NAME(LEFT(E.RESOURCE_DESCRIPTION, CHARINDEX(':', E.RESOURCE_DESCRIPTION) - 1)) + ':' + SUBSTRING(E.RESOURCE_DESCRIPTION, CHARINDEX(':', E.RESOURCE_DESCRIPTION) + 1, 999)
WHEN COALESCE(E.WAIT_TYPE, B.WAIT_TYPE) = 'OLEDB' THEN '[' + REPLACE(REPLACE(E.RESOURCE_DESCRIPTION, ' (SPID=', ':'), ')', '') + ']'
ELSE ''
END), '') AS WAIT_INFO,
COALESCE(E.WAIT_DURATION_MS, B.WAIT_TIME) AS WAIT_TIME_MS,
NULLIF(B.BLOCKING_SESSION_ID, 0) AS BLOCKING_SESSION_ID,
COALESCE(F.BLOCKED_SESSION_COUNT, 0) AS BLOCKED_SESSION_COUNT,
X.TEXT,
A.TOTAL_ELAPSED_TIME,
A.[DEADLOCK_PRIORITY],
(CASE B.TRANSACTION_ISOLATION_LEVEL
WHEN 0 THEN 'UNSPECIFIED' 
WHEN 1 THEN 'READUNCOMMITTED' 
WHEN 2 THEN 'READCOMMITTED' 
WHEN 3 THEN 'REPEATABLE' 
WHEN 4 THEN 'SERIALIZABLE' 
WHEN 5 THEN 'SNAPSHOT'
END) AS TRANSACTION_ISOLATION_LEVEL,
A.LAST_REQUEST_START_TIME,
A.LOGIN_NAME,
A.NT_USER_NAME,
A.ORIGINAL_LOGIN_NAME,
A.[HOST_NAME],
(CASE WHEN D.NAME IS NOT NULL THEN 'SQLAGENT - TSQL JOB (' + D.[NAME] + ' - ' + SUBSTRING(A.[PROGRAM_NAME], 67, LEN(A.[PROGRAM_NAME]) - 67) +  ')' ELSE A.[PROGRAM_NAME] END) AS [PROGRAM_NAME]
FROM
SYS.DM_EXEC_SESSIONS AS A WITH (NOLOCK)
LEFT JOIN SYS.DM_EXEC_REQUESTS AS B WITH (NOLOCK) ON A.SESSION_ID = B.SESSION_ID
LEFT JOIN MSDB.DBO.SYSJOBS AS D ON RIGHT(D.JOB_ID, 10) = RIGHT(SUBSTRING(A.[PROGRAM_NAME], 30, 34), 10)
LEFT JOIN (
SELECT
    SESSION_ID, 
    WAIT_TYPE,
    WAIT_DURATION_MS,
    RESOURCE_DESCRIPTION,
    ROW_NUMBER() OVER(PARTITION BY SESSION_ID ORDER BY (CASE WHEN WAIT_TYPE LIKE 'PAGE%LATCH%' THEN 0 ELSE 1 END), WAIT_DURATION_MS) AS RANKING
FROM 
    SYS.DM_OS_WAITING_TASKS
) E ON A.SESSION_ID = E.SESSION_ID AND E.RANKING = 1
LEFT JOIN (
SELECT
    BLOCKING_SESSION_ID,
    COUNT(*) AS BLOCKED_SESSION_COUNT
FROM
    SYS.DM_EXEC_REQUESTS
WHERE
    BLOCKING_SESSION_ID <> 0
GROUP BY
    BLOCKING_SESSION_ID
) F ON A.SESSION_ID = F.BLOCKING_SESSION_ID
LEFT JOIN SYS.SYSPROCESSES AS G WITH(NOLOCK) ON A.SESSION_ID = G.SPID
OUTER APPLY SYS.DM_EXEC_SQL_TEXT(COALESCE(G.[SQL_HANDLE],B.[SQL_HANDLE])) AS X
WHERE A.SESSION_ID <> @@SPID
AND LOGIN_NAME LIKE CONCAT('%',@LOGIN,'%') AND TEXT NOT LIKE '(@P0%'

DECLARE @TABLE TABLE(
SPID INT,
STATUS VARCHAR(MAX),
LOGIN VARCHAR(MAX),
HOSTNAME VARCHAR(MAX),
BLKBY VARCHAR(MAX),
DBNAME VARCHAR(MAX),
COMMAND VARCHAR(MAX),
CPUTIME INT,
DISKIO INT,
LASTBATCH VARCHAR(MAX),
PROGRAMNAME VARCHAR(MAX),
SPID_1 INT,
REQUESTID INT
)

INSERT INTO @TABLE EXEC SP_WHO2

SELECT  A.SPID,A.STATUS,A.LOGIN,A.HOSTNAME,A.BLKBY,A.DBNAME,A.CPUTIME,A.DISKIO,A.LASTBATCH,TEXT
FROM    @TABLE A
INNER JOIN SYS.SYSPROCESSES B
ON A.SPID=B.SPID
OUTER APPLY SYS.DM_EXEC_SQL_TEXT(B.SQL_HANDLE)
WHERE A.LOGIN LIKE CONCAT('%',@LOGIN,'%') AND TEXT NOT LIKE '(@P0%'

EXEC MASTER.DBO.SP_WHOISACTIVE

EXEC MASTER.DBO.STPVERIFICA_LOCKS