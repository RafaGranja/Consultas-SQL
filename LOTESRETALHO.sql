SELECT * FROM ( 
SELECT T1.CODCOLIGADA AS 'COLIGADA',T2.NUMEROMOV AS 'NUMERO MOVIMENTO',
COALESCE((FORMAT(T2.RECCREATEDON,'D','PT-BR')+' '+CONVERT(VARCHAR(20),T2.RECCREATEDON,108)),'') DATARECEBIMENTO,
T2.CODTMV AS 'CODIGO DO MOVIMENTO',T5.NOME 'DESCRIÇÃO DO CÓDIGO',T4.CODIGOPRD AS 'CODIGO DO PRODUTO',
T3.IDLOTE AS 'ID DO LOTE', T3.NUMLOTE AS 'NUMERO DO LOTE',T6.CODLOC AS 'LOCAL DO LOTE',
T4.NOMEFANTASIA AS 'NOME DO PRODUTO',T1.QUANTIDADE2 'SALDO RECEBIDO',SUM(T6.SALDOFISICO2) 'SALDO ATUAL' 
FROM TITMLOTEPRD T1 
INNER JOIN TMOV T2 ON
T1.CODCOLIGADA=T2.CODCOLIGADA
AND T1.IDMOV=T2.IDMOV
INNER JOIN TLOTEPRD T3 ON
T3.CODCOLIGADA=T2.CODCOLIGADA
AND T3.IDLOTE=T1.IDLOTE
INNER JOIN TPRD T4 ON
T4.CODCOLIGADA=T2.CODCOLIGADA
AND T4.IDPRD=T3.IDPRD
INNER JOIN TTMV T5 ON
T5.CODCOLIGADA=T4.CODCOLIGADA
AND T5.CODTMV=T2.CODTMV 
INNER JOIN TLOTEPRDLOC T6 ON
T6.CODCOLIGADA=T5.CODCOLIGADA
AND T6.IDLOTE=T3.IDLOTE
WHERE T2.IDMOV = ( SELECT TOP 1 IDMOV FROM TITMLOTEPRD WHERE IDLOTE=T1.IDLOTE AND QUANTIDADE2>0 AND RECCREATEDON IS NOT NULL ORDER BY RECCREATEDON ASC )
AND T4.CODTB2FAT IN ('0181','0182','0183') AND T6.CODLOC='22'
GROUP BY T1.CODCOLIGADA,T2.NUMEROMOV,T2.RECCREATEDON,T2.CODTMV,T5.NOME,T4.CODIGOPRD,T3.IDLOTE,T3.NUMLOTE,T6.CODLOC,T1.QUANTIDADE2,T4.NOMEFANTASIA ) R
WHERE [SALDO RECEBIDO] != [SALDO ATUAL] AND [SALDO ATUAL]>0
ORDER BY [ID DO LOTE]
