SELECT INDICE,IDCRIACAO,TIPODESENHO   		
			,CASE WHEN COMPONENTE>0 THEN 1 ELSE 0 END AS COMPONENTE  		
			,CASE WHEN PROCESSO>0 THEN 1 ELSE 0 END AS PROCESSO  		
			,CASE WHEN LISTA>0 THEN 1 ELSE 0 END AS LISTA  		
			,CASE WHEN	COMPORLISTA='SIM' THEN 1 ELSE 0 END AS COMPORLISTA 
				FROM (  SELECT INDICE,OS,IDCRIACAO,TIPODESENHO,(SELECT COUNT(ID) FROM Z_CRM_ML001019  					
						WHERE OSCOMPONENTES=KE.OS AND IDCRIACAOCOMPONENTES=KE.IDCRIACAO) COMPONENTE,				
						(SELECT COUNT(ID) FROM Z_CRM_ML001021  					
						WHERE OSPROCESSO=KE.OS AND IDCRIACAOPROCESSO=KE.IDCRIACAO) PROCESSO, 
						(SELECT	SUM(ID) FROM( SELECT COUNT(ID) ID FROM Z_CRM_LISTAMATERIAIS  					
												WHERE NUMOSSALVOS=KE.OS AND IDCRIACAOSALVOS=KE.IDCRIACAO
												UNION
												SELECT COUNT(ID) ID FROM Z_CRM_ML001019  					
												WHERE OSCOMPONENTES=KE.OS AND IDCRIACAOCOMPONENTES=KE.IDCRIACAO 
												AND CODIGOPRDCOMPONENTES!='' ) L ) LISTA,  				
						 KE.COMPORLISTA  		
						 FROM Z_CRM_ML001005 KE  	
						 WHERE  		 
						 KE.OS='3.07840.32.001' ) R  	
	ORDER BY CAST('/' + REPLACE(INDICE, '.', '/') + '/' AS HIERARCHYID)


	select * from Z_CRM_ML001005






SELECT INDICE,IDCRIACAO,TIPODESENHO   		
			,CASE WHEN COMPONENTE>0 THEN 1 ELSE 0 END AS COMPONENTE  		
			,CASE WHEN PROCESSO>0 THEN 1 ELSE 0 END AS PROCESSO  		
			,CASE WHEN LISTA>0 THEN 1 ELSE 0 END AS LISTA  		
			,CASE WHEN	COMPORLISTA='SIM' THEN 1 ELSE 0 END AS COMPORLISTA
				FROM (  SELECT INDICE,OS,IDCRIACAO,TIPODESENHO,(SELECT COUNT(ID) FROM Z_CRM_EX001019  					
						WHERE OSCOMPONENTES=KE.OS AND IDCRIACAOCOMPONENTES=KE.IDCRIACAO 
						AND (CODIGOPRDCOMPONENTES IS NOT NULL AND CODIGOPRDCOMPONENTES!='' )) COMPONENTE,				
						(SELECT COUNT(ID) FROM Z_CRM_EX001021  					
						WHERE OSPROCESSO=KE.OS AND IDCRIACAOPROCESSO=KE.IDCRIACAO) PROCESSO, 
						(SELECT COUNT(ID) ID FROM Z_CRM_LISTAMATERIAIS  					
						WHERE NUMOSSALVOS=KE.OS AND IDCRIACAOSALVOS=KE.IDCRIACAO) LISTA,		
						 KE.COMPORLISTA  		
						 FROM Z_CRM_EX001005 KE  	
						 WHERE  		 
						 KE.OS='3.07840.32.001' AND ITEMEXCLUSIVO!=2 AND KE.EXECUCAO=1) R  	
ORDER BY CAST('/' + REPLACE(INDICE, '.', '/') + '/' AS HIERARCHYID)
 

 SELECT INDICE,IDCRIACAO,TIPODESENHO ,CASE WHEN COMPONENTE>0 THEN 1 ELSE 0 END AS COMPONENTE ,CASE WHEN PROCESSO>0 THEN 1 ELSE 0 END AS PROCESSO  		 ,CASE WHEN LISTA>0 THEN 1 ELSE 0 END AS LISTA  		 ,CASE WHEN	COMPORLISTA='SIM' THEN 1 ELSE 0 END AS COMPORLISTA  FROM (  SELECT INDICE,OS,IDCRIACAO,TIPODESENHO,(SELECT COUNT(ID) FROM Z_CRM_EX001019   WHERE OSCOMPONENTES=KE.OS AND IDCRIACAOCOMPONENTES=KE.IDCRIACAO) COMPONENTE, (SELECT COUNT(ID) FROM Z_CRM_EX001021  					 WHERE OSPROCESSO=KE.OS AND IDCRIACAOPROCESSO=KE.IDCRIACAO) PROCESSO,  (SELECT	SUM(ID) FROM( SELECT COUNT(ID) ID FROM Z_CRM_LISTAMATERIAIS  	 WHERE NUMOSSALVOS=KE.OS AND IDCRIACAOSALVOS=KE.IDCRIACAO UNION  SELECT COUNT(ID) ID FROM Z_CRM_EX001019  					 WHERE OSCOMPONENTES=KE.OS AND IDCRIACAOCOMPONENTES=KE.IDCRIACAO  AND CODIGOPRDCOMPONENTES!='' ) L ) LISTA,  				 KE.COMPORLISTA   		 FROM Z_CRM_EX001005 KE  	WHERE  		 KE.OS='3.07840.32.001'  AND KE.CODTRFPAI='1.20' 1) R  	ORDER BY CAST('/' + REPLACE(INDICE, '.', '/') + '/' AS HIERARCHYID) 