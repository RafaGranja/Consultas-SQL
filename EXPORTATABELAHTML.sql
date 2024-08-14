USE [master]
GO
/****** Object:  StoredProcedure [dbo].[stpExport_Table_HTML_Output]    Script Date: 03/05/2024 09:10:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		rafael.granja
-- Create date: 22/04/2024
-- Description:	transforma tabela em html
-- =============================================
ALTER PROCEDURE [dbo].[stpExport_Table_HTML_Output]
    @Ds_Tabela [varchar](max),
    @Fl_Aplica_Estilo_Padrao BIT = 1,
	@Ds_Alinhamento VARCHAR(10) = 'left',
	@Ds_OrderBy VARCHAR(MAX) = '',
    @Ds_Saida VARCHAR(MAX) OUTPUT
AS
BEGIN

			SET NOCOUNT ON
        
			DECLARE
				@query NVARCHAR(MAX),
				@Database sysname,
				@Nome_Tabela sysname

    
    
			IF (LEFT(@Ds_Tabela, 1) = '#')
			BEGIN
				SET @Database = 'tempdb.'
				SET @Nome_Tabela = @Ds_Tabela
			END
			ELSE BEGIN
				SET @Database = LEFT(@Ds_Tabela, CHARINDEX('.', @Ds_Tabela))
				SET @Nome_Tabela = SUBSTRING(@Ds_Tabela, LEN(@Ds_Tabela) - CHARINDEX('.', REVERSE(@Ds_Tabela)) + 2, LEN(@Ds_Tabela))
			END

    
			SET @query = '
			SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
			FROM ' + @Database + 'INFORMATION_SCHEMA.COLUMNS 
			WHERE TABLE_NAME = ''' + @Nome_Tabela + '''
			ORDER BY ORDINAL_POSITION'
    
    
			IF (OBJECT_ID('tempdb..#Colunas') IS NOT NULL) DROP TABLE #Colunas
			CREATE TABLE #Colunas (
				ORDINAL_POSITION int, 
				COLUMN_NAME sysname, 
				DATA_TYPE nvarchar(128), 
				CHARACTER_MAXIMUM_LENGTH int,
				NUMERIC_PRECISION tinyint, 
				NUMERIC_SCALE int
			)

			INSERT INTO #Colunas
			EXEC(@query)

    
    
			IF (@Fl_Aplica_Estilo_Padrao = 1)
			BEGIN
    
			SET @Ds_Saida = '<html>

		<head>
			<title>Titulo</title>
			<style type="text/css">				

				 table { border: outset 2.25pt; }
                thead { background: #b92113; }
                th { color: #fff; padding: 10px;}
                td { padding: 3.0pt 3.0pt 3.0pt 3.0pt; text-align:' + @Ds_Alinhamento + '; }
			</style>
		</head>';
    
			END
       
    
			SET @Ds_Saida = ISNULL(@Ds_Saida, '') + '
		<table border="1" cellpadding="0">
			<thead>
				<tr>'
											

			DECLARE @totalColunas INT 
			SET @totalColunas = (SELECT COUNT(*) FROM #Colunas)

			-- Cabeçalho da tabela
			DECLARE @contadorColuna INT 			
			SET @contadorColuna = 1
						
			declare
				@nomeColuna sysname,
				@tipoColuna sysname
    	
	
			WHILE(@contadorColuna <= @totalColunas)
			BEGIN

				SELECT @nomeColuna = COLUMN_NAME
				FROM #Colunas
				WHERE ORDINAL_POSITION = @contadorColuna


				SET @Ds_Saida = ISNULL(@Ds_Saida, '') + '
					<th>' + @nomeColuna + '</th>'


				SET @contadorColuna = @contadorColuna + 1

			END
			

			SET @Ds_Saida = ISNULL(@Ds_Saida, '') + '
				</tr>
			</thead>
			<tbody>'

    
			-- Conteúdo da tabela

			DECLARE @saida VARCHAR(MAX)

			SET @query = '
		SELECT @saida = (
			SELECT '


			SET @contadorColuna = 1

			WHILE(@contadorColuna <= @totalColunas)
			BEGIN

				SELECT 
					@nomeColuna = COLUMN_NAME,
					@tipoColuna = DATA_TYPE
				FROM 
					#Colunas
				WHERE 
					ORDINAL_POSITION = @contadorColuna



				IF (@tipoColuna IN ('int', 'bigint', 'float', 'numeric', 'decimal', 'bit', 'tinyint', 'smallint', 'integer'))
				BEGIN
        
					SET @query = @query + '
			ISNULL(CAST([' + @nomeColuna + '] AS VARCHAR(MAX)), '''') AS [td]'
    
				END
				ELSE BEGIN
        
					SET @query = @query + '
			ISNULL([' + @nomeColuna + '], '''') AS [td]'
    
				END
    
        
				IF (@contadorColuna < @totalColunas)
					SET @query = @query + ','

        
				SET @contadorColuna = @contadorColuna + 1

			END



			SET @query = @query + '
		FROM ' + @Ds_Tabela + (CASE WHEN ISNULL(@Ds_OrderBy, '') = '' THEN '' ELSE ' 
		ORDER BY ' END) + @Ds_OrderBy + '
		FOR XML RAW(''tr''), Elements
		)'
    
    
			EXEC tempdb.sys.sp_executesql
				@query,
				N'@saida NVARCHAR(MAX) OUTPUT',
				@saida OUTPUT


			-- Identação
			SET @saida = REPLACE(@saida, '<tr>', '
				<tr>')

			SET @saida = REPLACE(@saida, '<td>', '
					<td>')

			SET @saida = REPLACE(@saida, '</tr>', '
				</tr>')


			SET @Ds_Saida = ISNULL(@Ds_Saida, '') + @saida


    
			SET @Ds_Saida = ISNULL(@Ds_Saida, '') + '
			</tbody>
		</table>'
    
            
END

