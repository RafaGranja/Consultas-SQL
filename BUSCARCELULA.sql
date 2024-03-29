SELECT TRFC.CELULA,C.DESCRICAO FROM  MPRJ PRJ
     INNER JOIN MTRF TRF
               ON TRF.CODCOLIGADA = PRJ.CODCOLIGADA
			   AND TRF.IDPRJ = PRJ.IDPRJ
       LEFT JOIN MTRFCOMPL TRFC
               ON TRF.CODCOLIGADA = TRFC.CODCOLIGADA
                  AND TRF.IDPRJ = TRFC.IDPRJ
                  AND TRF.IDTRF = TRFC.IDTRF
		LEFT JOIN GCONSIST C
               ON C.CODTABELA = 'CELULA'
                  AND C.APLICACAO = 'M'
                  AND C.CODINTERNO = TRFC.CELULA
		WHERE PRJ.CODCOLIGADA = 1
                  AND PRJ.POSICAO IN ( 1, 4 ) AND TRF.CODTRFAUX='52344/23'