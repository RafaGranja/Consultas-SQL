" SELECT     COMPONENTE, "+
"            QUANTIDADE, "+
"            APONTADO, "+
"            ESTOQUE_ATUAL, "+
"            ESTOQUE_ALOCADO, "+
"            ESTOQUE_SALDO, "+
"            (QUANTIDADE - APONTADO) SALDO,CODTB2FAT "+
" FROM       (       SELECT P.CODIGOPRD+ ' - '+ P.DESCRICAO COMPONENTE, MP.QUANTIDADE, "+
" 							ISNULL(( "+
" 								SELECT SUM(QUANTIDADE) "+
" 								FROM   KATVORDEMMP MMP "+
" 								WHERE  MMP.IDPRODUTO = MP.IDPRODUTO "+
" 								AND    MMP.CODCOLIGADA = A.CODCOLIGADA            "+
" 								AND    MMP.CODFILIAL = A.CODFILIAL                  "+
" 								AND    MMP.CODESTRUTURA = A.CODESTRUTURA                  "+
" 								AND    MMP.CODORDEM = A.CODORDEM                  "+
" 								AND    MMP.IDATIVIDADE = A.IDATVORDEM                  "+
" 								AND    MMP.EFETIVADO = 1  ),0)  "+
" 							+              "+
" 							ISNULL(( "+
" 								SELECT SUM(MMP.QUANTIDADE) FROM KCOMPSUBSTITUTO KS                     "+
" 								INNER JOIN KATVORDEM AMP "+
" 								ON         KS.CODCOLIGADA = AMP.CODCOLIGADA "+
" 								AND        KS.CODFILIAL = AMP.CODFILIAL "+
" 								AND        KS.CODESTRUTURA = AMP.CODESTRUTURA "+
" 								AND        KS.CODATIVIDADE= AMP.CODATIVIDADE                     "+
" 								INNER JOIN KATVORDEMMP MMP "+
" 								ON         KS.CODCOLIGADA = MMP.CODCOLIGADA "+
" 								AND        KS.CODFILIAL = MMP.CODFILIAL "+
" 								AND        KS.CODESTRUTURA = MMP.CODESTRUTURA "+
" 								AND        AMP.CODORDEM = MMP.CODORDEM "+
" 								AND        AMP.IDATVORDEM = MMP.IDATIVIDADE "+
" 								AND        KS.IDPRD = MMP.IDPRODUTO WHERE KS.IDPRDORIGEM = MP.IDPRODUTO           "+
" 								AND        AMP.CODCOLIGADA = A.CODCOLIGADA               "+
" 								AND        AMP.CODFILIAL = A.CODFILIAL                 "+
" 								AND        AMP.CODESTRUTURA = A.CODESTRUTURA                "+
" 								AND        AMP.CODORDEM = A.CODORDEM                 "+
" 								AND        AMP.IDATVORDEM = A.IDATVORDEM                 "+
" 								AND        MMP.EFETIVADO = 1 ),0) APONTADO, "+
" 							E.SALDOFISICO2 ESTOQUE_ATUAL, E.SALDOFISICO3 ESTOQUE_ALOCADO, E.SALDOFISICO2 - E.SALDOFISICO3 ESTOQUE_SALDO,A.CODCOLIGADA, "+  
" 							(SELECT CODTB2FAT FROM TPRODUTODEF WHERE CODCOLIGADA=A.CODCOLIGADA AND IDPRD=P.IDPRD) CODTB2FAT "+
" 						FROM         KATVORDEM A         "+
" 						INNER JOIN KATVORDEMMP MP "+
" 						ON         MP.CODCOLIGADA = A.CODCOLIGADA "+
" 						AND        MP.CODFILIAL = A.CODFILIAL "+
" 						AND        MP.CODESTRUTURA = A.CODESTRUTURA "+
" 						AND        MP.IDATIVIDADE = A.IDATVORDEM "+
" 						AND        MP.CODORDEM = A.CODORDEM         "+
" 						INNER JOIN TPRD P "+
" 						ON         MP.CODCOLIGADA = P.CODCOLIGADA "+
" 						AND        MP.IDPRODUTO = P.IDPRD          "+
" 						INNER JOIN KITEMORDEM KO "+
" 						ON         A.CODCOLIGADA = KO.CODCOLIGADA "+
" 						AND        A.CODFILIAL = KO.CODFILIAL "+
" 						AND        A.CODESTRUTURA = KO.CODESTRUTURA "+
" 						AND        A.CODORDEM = KO.CODORDEM         "+
" 						LEFT JOIN  TPRDLOC E "+
" 						ON         E.CODCOLIGADA = P.CODCOLIGADA "+
" 						AND        E.CODFILIAL = A.CODFILIAL "+
" 						AND        E.IDPRD = P.IDPRD "+
" 						AND        E.CODLOC IN ('23','25','27') "+
"                       WHERE      A.CODCOLIGADA = "+codColigada+" "+ 
" 						AND        A.CODFILIAL = "+codFilial+" "+ 
" 						AND        A.CODORDEM = '"+codOrdem+"'  "+  
"                       AND        MP.EFETIVADO=0  "+
" 						AND        P.CODIGOPRD NOT LIKE '01.053.%' "+
" 						AND        P.CODIGOPRD NOT LIKE '01.019.%' "+
" 						AND        P.CODIGOPRD NOT LIKE '01.020.%' "+
" 						AND        A.IDATVORDEM<> ISNULL(( "+
" 													  SELECT IDATVORDEM "+
" 													  FROM   ZMDPLANOAPROVEITAMENTOCORTE "+
" 													  WHERE  CODCOLIGADA = A.CODCOLIGADA "+
" 													  AND    CODORDEM=A.CODORDEM "+
" 													  AND    CODATIVIDADE=A.CODATIVIDADE "+
" 													  AND    QTDEAPONTADA IS NOT NULL "+
" 													  AND    QTDEAPONTADA<>0),'')         "+
" 						UNION ALL         "+
" 						SELECT COMPONENTE, QUANTIDADE,                       "+
" 							ISNULL(( "+
" 								SELECT SUM(QUANTIDADE)  "+
" 								FROM KATVORDEMMP MMP                                 "+
" 								INNER JOIN TPRD PT "+
" 								ON         PT.CODCOLIGADA = MMP.CODCOLIGADA "+
" 								AND        PT.IDPRD = MMP.IDPRODUTO WHERE PT.DESCRICAO = APONT.COMPONENTE                  "+
" 								AND        MMP.CODCOLIGADA = APONT.CODCOLIGADA                         "+
" 								AND        MMP.CODFILIAL = APONT.CODFILIAL                              "+
" 								AND        MMP.CODORDEM = APONT.CODORDEM                              "+
" 								AND        MMP.IDATIVIDADE = APONT.IDATIVIDADE                             "+
" 								AND        MMP.EFETIVADO = 1  ),0) APONTADA, ESTOQUE_ATUAL, ESTOQUE_ALOCADO, ESTOQUE_SALDO,APONT.CODCOLIGADA, "+
" 							(SELECT CODTB2FAT FROM TPRODUTODEF WHERE CODCOLIGADA=APONT.CODCOLIGADA AND IDPRD=APONT.IDPRD) CODTB2FAT "+
" 						FROM ( "+
" 							SELECT     MP.CODCOLIGADA, "+
" 										MP.CODFILIAL, "+
" 										MP.CODORDEM, "+
" 										MP.IDATIVIDADE, "+
" 										P.DESCRICAO COMPONENTE, "+
" 										SUM(MP.QUANTIDADE) QUANTIDADE, "+
" 										SUM(E.SALDOFISICO2) ESTOQUE_ATUAL, "+
" 										SUM(E.SALDOFISICO3) ESTOQUE_ALOCADO, "+
" 										SUM(E.SALDOFISICO2) - SUM(E.SALDOFISICO3) ESTOQUE_SALDO,P.IDPRD "+
" 							FROM KATVORDEM A                 "+
" 							INNER JOIN KATVORDEMMP MP "+
" 							ON         MP.CODCOLIGADA = A.CODCOLIGADA "+
" 							AND        MP.CODFILIAL = A.CODFILIAL "+
" 							AND        MP.CODESTRUTURA = A.CODESTRUTURA "+
" 							AND        MP.IDATIVIDADE = A.IDATVORDEM "+
" 							AND        MP.CODORDEM = A.CODORDEM                 "+
" 							INNER JOIN TPRD P "+
" 							ON         MP.CODCOLIGADA = P.CODCOLIGADA "+
" 							AND        MP.IDPRODUTO = P.IDPRD                   "+
" 							INNER JOIN KITEMORDEM KO "+
" 							ON         A.CODCOLIGADA = KO.CODCOLIGADA "+
" 							AND        A.CODFILIAL = KO.CODFILIAL "+
" 							AND        A.CODESTRUTURA = KO.CODESTRUTURA "+
" 							AND        A.CODORDEM = KO.CODORDEM     "+
" 							LEFT JOIN  TPRDLOC E "+
" 							ON         E.CODCOLIGADA = P.CODCOLIGADA "+
" 							AND        E.CODFILIAL = A.CODFILIAL "+
" 							AND        E.IDPRD = P.IDPRD "+
" 							AND        E.CODLOC IN ('23','25','27')  "+
"                           WHERE      A.CODCOLIGADA = "+codColigada+" "+ 
" 						    AND        A.CODFILIAL = "+codFilial+" "+ 
" 						    AND        A.CODORDEM = '"+codOrdem+"'  "+  
"                           AND        MP.EFETIVADO=0                 "+
" 							AND        P.CODIGOPRD NOT LIKE '01.053.%' "+
" 							AND        P.CODIGOPRD NOT LIKE '01.019.%' "+
" 							AND        P.CODIGOPRD NOT LIKE '01.020.%' "+
" 							AND        A.IDATVORDEM<> ISNULL(( "+
" 											SELECT IDATVORDEM "+
" 											FROM   ZMDPLANOAPROVEITAMENTOCORTE "+
" 											WHERE  CODCOLIGADA = A.CODCOLIGADA "+
" 											AND    CODORDEM=A.CODORDEM "+
" 											AND    CODATIVIDADE=A.CODATIVIDADE "+
" 											AND    QTDEAPONTADA IS NOT NULL "+
" 											AND    QTDEAPONTADA<>0),'')                 "+
" 							GROUP BY   MP.CODCOLIGADA, "+
" 										MP.CODFILIAL, "+
" 										MP.CODORDEM, "+
" 										MP.IDATIVIDADE, "+
" 										P.DESCRICAO,P.IDPRD ) AS APONT "+
" 										) APONTAMENTOS "+
" WHERE  ((QUANTIDADE - APONTADO) > 0 AND CODTB2FAT!='0184')  OR (APONTADO <= 0 AND CODTB2FAT='0184') "