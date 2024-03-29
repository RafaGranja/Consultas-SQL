 SELECT 
     INDICE, 
     IDCRIACAO, 
     TIPODESENHO, 
 CASE 
         WHEN COMPONENTE > 0 THEN 1 
         ELSE 0 
     END AS COMPONENTE, 
 CASE 
         WHEN PROCESSO > 0 THEN 1 
         ELSE 0 
     END AS PROCESSO, 
 CASE 
         WHEN LISTA > 0 THEN 1 
         ELSE 0 
     END AS LISTA, 
 CASE 
         WHEN COMPORLISTA = 'SIM' THEN 1 
         ELSE 0 
     END AS COMPORLISTA, 
 CASE 
         WHEN TEMFILHOS = 'SIM' THEN 1 
         ELSE 0 
     END AS TEMFILHOS 
 FROM 
     ( 
         SELECT 
             INDICE, 
             KE.OS, 
             KE.IDCRIACAO, 
             TIPODESENHO, 
 			C.ID COMPONENTE, 
             P.ID PROCESSO, 
             L.LISTA, 
             KE.COMPORLISTA, 
             CASE 
                 WHEN N.NIVEL IS NULL THEN 'NAO' 
                 ELSE 'SIM' 
             END TEMFILHOS 
         FROM 
             Z_CRM_ML001005 KE 
 			LEFT JOIN ( SELECT OS,NIVEL,MIN(IDCRIACAO) IDCRIACAO FROM Z_CRM_ML001005 GROUP BY OS,NIVEL ) N ON N.OS=KE.OS AND N.NIVEL=KE.NIVEL 
 			LEFT JOIN ( 
 				SELECT 
 								NUMOSSALVOS,IDCRIACAOSALVOS,SUM(ID) LISTA 
 							FROM 
 								( 
 									SELECT 
 									   NUMOSSALVOS,IDCRIACAOSALVOS, COUNT(ID) ID 
 									FROM 
 										Z_CRM_LISTAMATERIAIS 
 									GROUP BY NUMOSSALVOS,IDCRIACAOSALVOS 
 									UNION 
 									SELECT 
 										OSCOMPONENTES,IDCRIACAOCOMPONENTES,COUNT(ID) ID 
 									FROM 
 										Z_CRM_ML001019 
 									WHERE CODIGOPRDCOMPONENTES != '' 
 									GROUP BY OSCOMPONENTES,IDCRIACAOCOMPONENTES ) R GROUP BY NUMOSSALVOS,IDCRIACAOSALVOS ) L ON L.NUMOSSALVOS=KE.OS AND L.IDCRIACAOSALVOS=KE.IDCRIACAO 
 			LEFT JOIN (SELECT OSPROCESSO,IDCRIACAOPROCESSO,COUNT(ID) ID FROM Z_CRM_ML001021 GROUP BY OSPROCESSO,IDCRIACAOPROCESSO ) P ON P.OSPROCESSO=KE.OS AND P.IDCRIACAOPROCESSO=KE.IDCRIACAO 
 			LEFT JOIN (SELECT OSCOMPONENTES,IDCRIACAOCOMPONENTES,COUNT(ID) ID FROM Z_CRM_ML001019 GROUP BY OSCOMPONENTES,IDCRIACAOCOMPONENTES ) C ON C.OSCOMPONENTES=KE.OS AND C.IDCRIACAOCOMPONENTES=KE.IDCRIACAO 
         WHERE 
             KE.OS = '3.07840.32.001' 
     ) R 
 ORDER BY 
     CAST( 
         '/' + REPLACE(INDICE, '.', '/') + '/' AS HIERARCHYID 
     ) 





