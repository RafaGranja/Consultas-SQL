SELECT *,
       (SELECT LOGNECESSIDADE
        FROM   ZMDNECESSIDADEPAPC
        WHERE  NSEQPEDIDO = R.NSEQPEDIDO) LOGNECESSIDADE
FROM   (SELECT DISTINCT Z.CODCOLIGADA,
                        Z.CODFILIAL,
                        K.CODCCUSTO                                    OS,
                        N.NUMDESENHO,
                        N.CODIGOPRD
                        CODESTRUTURA,
                        N.EXECUCAO,
                        K.CODORDEM                                     OP,
                        A.DSCATIVIDADE,
                        Z.CODATIVIDADE,
                        KA.IDATVORDEM,
                        N.POSICAODESENHO                               POSICAO,
                        N.DESCRICAO                                    ITEM,
                        N.MATERIAL,
                        ( Z.QUANTIDADE ) - ISNULL(Z3.QUANTIDADE, 0)
                        QUANTIDADE,
                        Z.QTDATENDIDA,
                        KA.QTPREVISTA
                        QTDEPLANEJADA,
                        Z.QUANTIDADE
                        QTDORIGINAL,
                        Z.SEMANANECESSIDADE,
                        Z.SEMANAREPROGRAMACAO,
                        Z.PRIORIDADEREPROGRAMACAO,
                        Z.STATUS
                               STATUSNECESSIDADE,
                        Z.RECCREATEDBY,
                        Z.RECCREATEDON,
                        ( KA.QTPREVISTA - Z.QUANTIDADE )               SALDOED,
                        Z.QUANTIDADE
                        QUANTIDADEED,
                        N.BITOLA,
                        CONVERT(VARCHAR(10), Z.DATANECESSIDADE, 103)
                        DATANECESSIDADE,
                        Z.PRIORIDADE,
                        Z.RECMODIFIEDBY,
                        Z.PAPC,
                        Z.COMPLEMENTO,
                        Z.OBS,
                        CONVERT(VARCHAR(10), Z.DATAREPROGRAMACAO, 103)
                               DATAREPROGRAMACAO,
                        ( Z.QUANTIDADE - ISNULL(Z.QTDATENDIDA, 0) )    SALDO,
                        Z.NSEQPEDIDO,
                        N.PESOBRUTO,
                        N.PESOLIQUIDO
        FROM   ZMDNECESSIDADEPAPC Z
               INNER JOIN KORDEM K
                       ON K.CODCOLIGADA = Z.CODCOLIGADA
                          AND K.CODFILIAL = Z.CODFILIAL
                          AND K.CODORDEM = Z.CODORDEM
               INNER JOIN KORDEMCOMPL KC
                       ON K.CODCOLIGADA = KC.CODCOLIGADA
                          AND K.CODFILIAL = KC.CODFILIAL
                          AND K.CODORDEM = KC.CODORDEM
               INNER JOIN KATVORDEM KA
                       ON KA.CODCOLIGADA = Z.CODCOLIGADA
                          AND KA.CODFILIAL = Z.CODFILIAL
                          AND KA.CODORDEM = Z.CODORDEM
                          AND KA.CODATIVIDADE = Z.CODATIVIDADE
               INNER JOIN FLUIG.DBO.Z_CRM_EX001005 N
                       ON N.OS = K.CODCCUSTO COLLATE LATIN1_GENERAL_CI_AS
                          AND N.CODIGOPRD = Z.CODESTRUTURA COLLATE
                                            LATIN1_GENERAL_CI_AS
                          AND N.EXECUCAO = KC.NUMEXEC
                          AND N.ITEMEXCLUSIVO <> 2
                          AND N.CODCOLIGADA = KA.CODCOLIGADA
                          AND N.CODFILIAL = KA.CODFILIAL
               INNER JOIN KATIVIDADE A
                       ON A.CODCOLIGADA = Z.CODCOLIGADA
                          AND A.CODATIVIDADE = Z.CODATIVIDADE
                          AND A.CODFILIAL = Z.CODFILIAL
               LEFT JOIN (
			   SELECT SUM(QUANTIDADE) QUANTIDADE,CODCOLIGADA,CODFILIAL,CODORDEM,CODATIVIDADE 
			   FROM ZMDPLANOAPROVEITAMENTOCORTE 
			   GROUP BY CODCOLIGADA,CODFILIAL,CODORDEM,CODATIVIDADE)AS  Z3
                      ON Z3.CODCOLIGADA = Z.CODCOLIGADA
                         AND Z3.CODFILIAL = Z.CODFILIAL
                         AND Z3.CODORDEM = Z.CODORDEM
                         AND Z3.CODATIVIDADE = Z.CODATIVIDADE
        WHERE  K.CODCCUSTO LIKE '3.07854.32.001%'
               AND ( (
       SUBSTRING(ISNULL(Z.SEMANAREPROGRAMACAO, Z.SEMANANECESSIDADE), 1,
         CHARINDEX('/', ISNULL(Z.SEMANAREPROGRAMACAO, Z.SEMANANECESSIDADE)) - 1)
         >= DATEPART(WEEK, GETDATE())
         AND ( SUBSTRING(ISNULL(Z.SEMANAREPROGRAMACAO, Z.SEMANANECESSIDADE),
         CHARINDEX('/', ISNULL(Z.SEMANAREPROGRAMACAO, Z.SEMANANECESSIDADE))
                                                                 + 1,
         DATALENGTH(ISNULL(Z.SEMANAREPROGRAMACAO, Z.SEMANANECESSIDADE))) =
               YEAR(GETDATE()) ) )
        OR (
       SUBSTRING(ISNULL(Z.SEMANAREPROGRAMACAO, Z.SEMANANECESSIDADE),
         CHARINDEX('/', ISNULL(Z.SEMANAREPROGRAMACAO, Z.SEMANANECESSIDADE))
                                                                 + 1,
         DATALENGTH(ISNULL(Z.SEMANAREPROGRAMACAO, Z.SEMANANECESSIDADE))) >
       YEAR(GETDATE()) )
        OR ( Z.QUANTIDADE - ISNULL(Z.QTDATENDIDA, 0) > 0 ) )) R WHERE POSICAO='2.6'