USE tempdb;
GO

-- altera o arquivo de data
DBCC SHRINKFILE(tempdev, 1,TRUNCATEONLY ) ;
GO

-- altera o arquivo de log
DBCC SHRINKFILE(templog, 1,TRUNCATEONLY ) ;
GO

-- altera todos os arquivos da base da dados
DBCC SHRINKDATABASE (tempdb,1,TRUNCATEONLY);
GO

-- reconfigura tamanho inicial dos arquivos
ALTER DATABASE [tempdb] MODIFY FILE ( NAME = N'tempdev', SIZE = 19922944KB , FILEGROWTH = 10% )
GO
ALTER DATABASE [tempdb] MODIFY FILE ( NAME = N'templog', SIZE = 512000KB , FILEGROWTH = 10% )
GO

-- configured size
SELECT 
name, file_id, type_desc, size * 8 / 1024 [TempdbSizeInMB]
FROM sys.master_files
WHERE DB_NAME(database_id) = 'tempdb'
ORDER BY type_desc DESC, file_id 


-- current size
SELECT 
name, file_id, type_desc, size * 8 / 1024 [TempdbSizeInMB]
FROM tempdb.sys.database_files 
ORDER BY type_desc DESC, file_id


-- available size
SELECT name ,size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)/128.0 AS AvailableSpaceInMB
FROM sys.database_files;

