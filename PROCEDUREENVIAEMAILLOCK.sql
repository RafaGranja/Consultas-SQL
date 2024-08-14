USE [master]
GO
/****** Object:  StoredProcedure [dbo].[SP_DELP_ENVIA_EMAIL_LOCKS]    Script Date: 03/05/2024 09:09:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[SP_DELP_ENVIA_EMAIL_LOCKS]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	CREATE TABLE ##MONITORAMENTO_LOCKS
    (
        [NESTED_LEVEL] INT,
        [SESSION_ID] SMALLINT,
        [WAIT_INFO] NVARCHAR(4000),
        [WAIT_TIME_MS] BIGINT,
        [BLOCKING_SESSION_ID] SMALLINT,
        [BLOCKED_SESSION_COUNT] INT,
        [OPEN_TRANSACTION_COUNT] INT,
        [SQL_TEXT] NVARCHAR(MAX),
        [SQL_COMMAND] NVARCHAR(MAX),
        [TOTAL_ELAPSED_TIME] INT,
        [DEADLOCK_PRIORITY] INT,
        [TRANSACTION_ISOLATION_LEVEL] VARCHAR(50),
        [LAST_REQUEST_START_TIME] DATETIME,
        [LOGIN_NAME] NVARCHAR(128),
        [NT_USER_NAME] NVARCHAR(128),
        [ORIGINAL_LOGIN_NAME] NVARCHAR(128),
        [HOST_NAME] NVARCHAR(128),
        [PROGRAM_NAME] NVARCHAR(128)
    )

	DECLARE @MONITORAMENTO_LOCKS TABLE
    (
        [NESTED_LEVEL] INT,
        [SESSION_ID] SMALLINT,
        [WAIT_INFO] NVARCHAR(4000),
        [WAIT_TIME_MS] BIGINT,
        [BLOCKING_SESSION_ID] SMALLINT,
        [BLOCKED_SESSION_COUNT] INT,
        [OPEN_TRANSACTION_COUNT] INT,
        [SQL_TEXT] XML,
        [SQL_COMMAND] XML,
        [TOTAL_ELAPSED_TIME] INT,
        [DEADLOCK_PRIORITY] INT,
        [TRANSACTION_ISOLATION_LEVEL] VARCHAR(50),
        [LAST_REQUEST_START_TIME] DATETIME,
        [LOGIN_NAME] NVARCHAR(128),
        [NT_USER_NAME] NVARCHAR(128),
        [ORIGINAL_LOGIN_NAME] NVARCHAR(128),
        [HOST_NAME] NVARCHAR(128),
        [PROGRAM_NAME] NVARCHAR(128)
    )

	INSERT INTO @MONITORAMENTO_LOCKS EXEC MASTER.DBO.STPVERIFICA_LOCKS

	INSERT INTO ##MONITORAMENTO_LOCKS 
	SELECT [NESTED_LEVEL] ,
        [SESSION_ID] ,
        [WAIT_INFO],
        [WAIT_TIME_MS],
        [BLOCKING_SESSION_ID] ,
        [BLOCKED_SESSION_COUNT] ,
        [OPEN_TRANSACTION_COUNT] ,
        CAST([SQL_TEXT] AS NVARCHAR(MAX)) ,
        CAST([SQL_COMMAND] AS NVARCHAR(MAX)) ,
        [TOTAL_ELAPSED_TIME] ,
        [DEADLOCK_PRIORITY] ,
        [TRANSACTION_ISOLATION_LEVEL] ,
        [LAST_REQUEST_START_TIME] ,
        [LOGIN_NAME] ,
        [NT_USER_NAME] ,
        [ORIGINAL_LOGIN_NAME] ,
        [HOST_NAME] ,
        [PROGRAM_NAME] 
	FROM @MONITORAMENTO_LOCKS


	DECLARE @html NVARCHAR(MAX);
	DECLARE @subject_ NVARCHAR(MAX);
	SELECT @subject_ =CONCAT('Consultas em Lock do servidor ',@@SERVERNAME);

	EXEC stpExport_Table_HTML_Output
		@Ds_Tabela = '##MONITORAMENTO_LOCKS',
		@Ds_Saida = @html OUTPUT

	EXEC msdb.dbo.sp_send_dbmail 
		@profile_name = 'Email', 
		@recipients = 'rafael.granja@delp.com.br;', 
		@subject = @subject_, 
		@body = @html, 
		@body_format = 'HTML', 
		@query_no_truncate = 1, 
		@attach_query_result_as_file = 0;

	DROP TABLE ##MONITORAMENTO_LOCKS

END
