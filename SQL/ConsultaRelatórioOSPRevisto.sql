
SELECT * FROM(
    SELECT
		KE.INDICE ,KE.DESCOS COLLATE SQL_Latin1_General_CP1_CI_AI DESCOS,
        CONVERT(VARCHAR,KA.CODESTRUTURA)CODESTRUTURA,
        (SELECT CONCAT(CODTRFPAI COLLATE SQL_Latin1_General_CP1_CI_AI,' - ',NOMETRFPAI COLLATE SQL_Latin1_General_CP1_CI_AI) FROM FLUIG.DBO.Z_CRM_ML001005 WHERE CODTRFITEM=KE.CODTRFITEM AND NIVEL='' AND OS=KE.OS) CODTAREFA,
        T.NOMEFANTASIA,KO.NUMEXEC,SUM(CONVERT(FLOAT,KA.TEMPO)) TEMPOMIN,
        CONVERT(NVARCHAR, CONVERT(DATETIME,ROUND(SUM((CONVERT(FLOAT,KA.TEMPO)/60)/24),4)),114) TEMPOHORAS, CASE K.REPROCESSAMENTO WHEN 1 THEN 'SIM' ELSE 'NAO' END AS RETRABALHO
            FROM
                KATVORDEM KA
                    INNER JOIN KATVORDEMCOMPL KL ON
                        KL.CODCOLIGADA = KA.CODCOLIGADA
                        AND KL.CODFILIAL=KA.CODFILIAL
                        AND KL.CODORDEM=KA.CODORDEM
                        AND KL.IDATVORDEM=KA.IDATVORDEM
                    INNER JOIN KORDEM K ON
                        K.CODCOLIGADA = KA.CODCOLIGADA
                        AND K.CODFILIAL=KA.CODFILIAL
                        AND K.CODORDEM=KA.CODORDEM
                    INNER JOIN KORDEMCOMPL KO ON
                        KO.CODCOLIGADA = KA.CODCOLIGADA
                        AND KO.CODFILIAL=KA.CODFILIAL
                        AND KO.CODORDEM=KA.CODORDEM
                    INNER JOIN KATIVIDADE ATV ON
                        ATV.CODCOLIGADA=KA.CODCOLIGADA
                        AND ATV.CODFILIAL=KA.CODFILIAL
                        AND ATV.CODATIVIDADE=KA.CODATIVIDADE
                    INNER JOIN TPRD T ON
                        T.CODCOLIGADA=KA.CODCOLIGADA
                        AND T.CODIGOPRD=KA.CODESTRUTURA
                    INNER JOIN FLUIG.DBO.Z_CRM_EX001005 KE ON
                        KA.CODESTRUTURA=KE.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI
                        AND KO.NUMEXEC=KE.EXECUCAO
                    INNER JOIN KESTRUTURA KES ON
                        KES.CODCOLIGADA=KA.CODCOLIGADA
                        AND KES.CODFILIAL=KA.CODFILIAL
                        AND KES.CODESTRUTURA=KA.CODESTRUTURA
                    
            WHERE
                K.CODCCUSTO='3.07840.32.001'
                AND K.STATUS!=6
                AND KA.STATUS!=6
                AND KES.STATUSESTR!=0
            GROUP BY
                KA.CODESTRUTURA,T.NOMEFANTASIA,KO.NUMEXEC,KL.RETRABALHO ,K.REPROCESSAMENTO,CODTRFITEM,OS,KE.INDICE,NOMETRFPAI,KE.DESCOS
    UNION



       SELECT
			E.INDICE,
            E.DESCOS COLLATE SQL_Latin1_General_CP1_CI_AI DESCOS,E.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI CODESTRUTURA,
            (SELECT CONCAT(CODTRFPAI COLLATE SQL_Latin1_General_CP1_CI_AI,' - ',NOMETRFPAI COLLATE SQL_Latin1_General_CP1_CI_AI)  FROM FLUIG.DBO.Z_CRM_ML001005 WHERE CODTRFITEM=E.CODTRFITEM AND NIVEL='' AND OS=E.OS) CODTAREFA,
            CONCAT(E.DESCRICAO COLLATE SQL_Latin1_General_CP1_CI_AI,' - ',E.POSICAODESENHO COLLATE SQL_Latin1_General_CP1_CI_AI,' - ',E.NUMDESENHO COLLATE SQL_Latin1_General_CP1_CI_AI) NOMEFANTASIA,
            0 NUMEXEC,SUM(CONVERT(FLOAT,A.MINUTOSGASTOS)*CONVERT(FLOAT,E.TOTALQTDE)) TEMPOMIN,CONVERT(NVARCHAR, CONVERT(DATETIME,ROUND(SUM(((CONVERT(FLOAT,A.MINUTOSGASTOS)*CONVERT(FLOAT,E.TOTALQTDE))/60))/24,4)),114) TEMPOHORAS,'NAO' RETRABALHO
                FROM
                    FLUIG.DBO.Z_CRM_ML001005 E
                    INNER JOIN FLUIG.DBO.Z_CRM_ML001021 A ON
                        A.IDCRIACAOPROCESSO=E.IDCRIACAO
                        AND E.OS=A.OSPROCESSO
                WHERE
                    E.OS='3.07840.32.001'
                GROUP BY E.CODIGOPRD,E.DESCRICAO,E.NUMDESENHO,E.POSICAODESENHO,CODTRFITEM,OS,E.INDICE,NOMETRFPAI,DESCOS ) R



   ORDER BY CAST('/' + REPLACE(INDICE, '.', '/') + '/' AS HIERARCHYID),CODTAREFA,CODESTRUTURA,NUMEXEC ASC