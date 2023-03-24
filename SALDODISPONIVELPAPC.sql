SELECT DISTINCT PL.NUMLOTE,
                PL.IDLOTE,
                PL.DATAVALIDADE,
                ( ( ISNULL(E.SALDOFISICO2, 0) ) - ( ISNULL(PC.QTDEMP, 0) ) )
                SALDODISPONIVEL,
                P.CONTROLADOPORLOTE,
                P.IDPRD,
                P.CODIGOPRD,
                P.DESCRICAO
  FROM KESTRUTURA KE
       INNER JOIN KCOMPONENTE KC
               ON KE.CODCOLIGADA = KC.CODCOLIGADA
                  AND KE.CODFILIAL = KC.CODFILIAL
                  AND KE.CODESTRUTURA = KC.CODESTRUTURA
       INNER JOIN TPRODUTO P
               ON KE.CODCOLIGADA = P.CODCOLPRD
                  AND P.IDPRD = KC.IDPRODUTO
       INNER JOIN TLOTEPRD PL
               ON KE.CODCOLIGADA = PL.CODCOLIGADA
                  AND P.IDPRD = PL.IDPRD
                  AND PL.IDSTATUS = 6
       LEFT JOIN (SELECT CODCOLIGADA,
                         IDPRD,
                         IDLOTE,
                         Sum(SALDOFISICO2) SALDOFISICO2,
                         CODFILIAL
                    FROM TLOTEPRDLOC
                   WHERE CODLOC IN ( '21', '22', '23' )
                   GROUP BY CODCOLIGADA,
                            IDPRD,
                            IDLOTE,
                            CODFILIAL) E
              ON KE.CODCOLIGADA = E.CODCOLIGADA
                 AND P.IDPRD = E.IDPRD
                 AND PL.IDLOTE = E.IDLOTE
                 AND KE.CODFILIAL = E.CODFILIAL
       LEFT JOIN (SELECT Z.CODCOLIGADA,
                         Z.CODFILIAL,
                         Sum(Z.QTDEMP) QTDEMP,
                         Z.NUMLOTE,
                         Z.IDLOTE
                    FROM (SELECT CODCOLIGADA,
                                 CODFILIAL,
                                 QTDEMP,
                                 NUMLOTE,
                                 IDLOTE
                            FROM ZMDPLANOAPROVEITAMENTOCORTE
                           WHERE NUMPLANOCORTE NOT IN (SELECT NUMPLANOCORTE
                                                         FROM
                                 ZMDPLANOAPROVEITAMENTOCORTE
                                                        WHERE
                                 QTDEAPONTADA IS NOT NULL
                                                        GROUP BY NUMPLANOCORTE)
                                 AND NUMPLANOCORTE != ''
                           GROUP BY CODCOLIGADA,
                                    CODFILIAL,
                                    QTDEMP,
                                    NUMLOTE,
                                    IDLOTE,
                                    NUMPLANOCORTE) Z
                   GROUP BY Z.CODCOLIGADA,
                            Z.CODFILIAL,
                            Z.NUMLOTE,
                            Z.IDLOTE)PC
              ON PC.CODCOLIGADA = KE.CODCOLIGADA
                 AND PC.CODFILIAL = KE.CODFILIAL
                 AND PL.NUMLOTE = PC.NUMLOTE
                 AND PL.IDLOTE = PC.IDLOTE
 WHERE KE.CODCCUSTO = '3.07840.32.001'
       AND KE.CODFILIAL = '7'
       AND ( P.CODIGOPRD LIKE '01.%'
              OR P.CODIGOPRD LIKE '02.%'
              OR P.CODIGOPRD LIKE '03.%' )
       AND PL.NUMLOTE = '212054'
UNION
SELECT DISTINCT PL.NUMLOTE,
                PL.IDLOTE,
                PL.DATAVALIDADE,
                ( ( ISNULL(E.SALDOFISICO2, 0) ) - ( ISNULL(PC.QTDEMP, 0) ) )
                SALDODISPONIVEL,
                P.CONTROLADOPORLOTE,
                P.IDPRD,
                P.CODIGOPRD,
                P.DESCRICAO
  FROM KESTRUTURA KE
       INNER JOIN KCOMPONENTE KC
               ON KE.CODCOLIGADA = KC.CODCOLIGADA
                  AND KE.CODFILIAL = KC.CODFILIAL
                  AND KE.CODESTRUTURA = KC.CODESTRUTURA
       INNER JOIN KCOMPSUBSTITUTO KS
               ON KE.CODCOLIGADA = KS.CODCOLIGADA
                  AND KC.IDPRODUTO = KS.IDPRDORIGEM
                  AND KE.CODESTRUTURA = KS.CODESTRUTURA
       INNER JOIN TPRODUTO P
               ON KE.CODCOLIGADA = P.CODCOLPRD
                  AND P.IDPRD = KS.IDPRD
       INNER JOIN TLOTEPRD PL
               ON KE.CODCOLIGADA = PL.CODCOLIGADA
                  AND P.IDPRD = PL.IDPRD
                  AND PL.IDSTATUS = 6
       LEFT JOIN (SELECT CODCOLIGADA,
                         IDPRD,
                         IDLOTE,
                         Sum(SALDOFISICO2) SALDOFISICO2,
                         CODFILIAL
                    FROM TLOTEPRDLOC
                   WHERE CODLOC IN ( '21', '22', '23' )
                   GROUP BY CODCOLIGADA,
                            IDPRD,
                            IDLOTE,
                            CODFILIAL) E
              ON KE.CODCOLIGADA = E.CODCOLIGADA
                 AND P.IDPRD = E.IDPRD
                 AND PL.IDLOTE = E.IDLOTE
                 AND KE.CODFILIAL = E.CODFILIAL
       LEFT JOIN (SELECT Z.CODCOLIGADA,
                         Z.CODFILIAL,
                         Sum(Z.QTDEMP) QTDEMP,
                         Z.NUMLOTE,
                         Z.IDLOTE
                    FROM (SELECT CODCOLIGADA,
                                 CODFILIAL,
                                 QTDEMP,
                                 NUMLOTE,
                                 IDLOTE
                            FROM ZMDPLANOAPROVEITAMENTOCORTE
                           WHERE NUMPLANOCORTE NOT IN (SELECT NUMPLANOCORTE
                                                         FROM
                                 ZMDPLANOAPROVEITAMENTOCORTE
                                                        WHERE
                                 QTDEAPONTADA IS NOT NULL
                                                        GROUP BY NUMPLANOCORTE)
                                 AND NUMPLANOCORTE != ''
                           GROUP BY CODCOLIGADA,
                                    CODFILIAL,
                                    QTDEMP,
                                    NUMLOTE,
                                    IDLOTE,
                                    NUMPLANOCORTE) Z
                   GROUP BY Z.CODCOLIGADA,
                            Z.CODFILIAL,
                            Z.NUMLOTE,
                            Z.IDLOTE) PC
              ON PC.CODCOLIGADA = KE.CODCOLIGADA
                 AND PC.CODFILIAL = KE.CODFILIAL
                 AND PL.NUMLOTE = PC.NUMLOTE
                 AND PL.IDLOTE = PC.IDLOTE
 WHERE KE.CODCCUSTO = '3.07840.32.001'
       AND KE.CODFILIAL = '7'
       AND ( P.CODIGOPRD LIKE '01.%'
              OR P.CODIGOPRD LIKE '02.%'
              OR P.CODIGOPRD LIKE '03.%' )
       AND PL.NUMLOTE = '212054' 


	   SELECT Z.CODCOLIGADA,
                         Z.CODFILIAL,
                         Sum(Z.QTDEMP) QTDEMP,
                         Z.NUMLOTE,
                         Z.IDLOTE,Z.NUMPLANOCORTE
                    FROM (SELECT CODCOLIGADA,
                                 CODFILIAL,
                                 QTDEMP,
                                 NUMLOTE,
                                 IDLOTE,NUMPLANOCORTE
                            FROM ZMDPLANOAPROVEITAMENTOCORTE
                           WHERE NUMPLANOCORTE NOT IN (SELECT NUMPLANOCORTE
                                                         FROM
                                 ZMDPLANOAPROVEITAMENTOCORTE
                                                        WHERE
                                 QTDEAPONTADA IS NOT NULL
                                                        GROUP BY NUMPLANOCORTE)
                                 AND NUMPLANOCORTE != ''
                           GROUP BY CODCOLIGADA,
                                    CODFILIAL,
                                    QTDEMP,
                                    NUMLOTE,
                                    IDLOTE,
                                    NUMPLANOCORTE) Z
									where NUMLOTE = '212054' 
                   GROUP BY Z.CODCOLIGADA,
                            Z.CODFILIAL,
                            Z.NUMLOTE,
                            Z.IDLOTE,z.NUMPLANOCORTE