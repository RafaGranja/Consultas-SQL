ALTER TABLE KMAOOBRAALOC NOCHECK CONSTRAINT FKKMAOOBRAALOC_KATVORDPROC


	;WITH CTE AS (
	
		SELECT K.*,KA.INDICEPROCESSO IDPROC,KA2.IDATVORDEM IDATV2,(SELECT MAX(INDICEPROCESSO) FROM KMAOOBRAALOC WHERE CODORDEM=KA2.CODORDEM 
		AND CODCOLIGADA=KA2.CODCOLIGADA AND CODFILIAL=KA2.CODFILIAL AND IDATVORDEM=KA2.IDATVORDEM
		)+ROW_NUMBER() OVER(PARTITION BY K.CODORDEM,K.IDATVORDEM ORDER BY K.INDICEPROCESSO) IDID FROM KMAOOBRAALOC K 
		INNER JOIN KATVORDEMPROCESSO  KA ON
		K.CODCOLIGADA=KA.CODCOLIGADA
		AND K.CODFILIAL=KA.CODFILIAL
		AND K.CODORDEM=KA.CODORDEM
		AND K.IDATVORDEM=KA.IDATVORDEM
		AND K.INDICEPROCESSO=KA.INDICEPROCESSO
		INNER JOIN KATVORDEM KAA ON
		KAA.CODCOLIGADA=K.CODCOLIGADA
		AND KAA.CODFILIAL=K.CODFILIAL
		AND KAA.CODORDEM=K.CODORDEM
		AND KAA.IDATVORDEM=K.IDATVORDEM
		INNER JOIN KATVORDEM KA2 ON
		KA2.CODCOLIGADA=K.CODCOLIGADA
		AND KA2.CODFILIAL=K.CODFILIAL
		AND KA2.CODORDEM='62712/23'
		AND KA2.CODATIVIDADE=KAA.CODATIVIDADE
		WHERE
		K.CODORDEM='70627/23' 

	)
	SELECT * INTO #TEMP FROM CTE 

	UPDATE K SET CODORDEM='62712/23',IDATVORDEM=KA.IDATV2,INDICEPROCESSO=KA.IDID FROM KMAOOBRAALOC K 
	INNER JOIN #TEMP KA ON
	K.CODCOLIGADA=KA.CODCOLIGADA
	AND K.CODFILIAL=KA.CODFILIAL
	AND K.CODORDEM=KA.CODORDEM
	AND K.IDATVORDEM=KA.IDATVORDEM
	AND K.INDICEPROCESSO=KA.INDICEPROCESSO
	WHERE K.CODORDEM='70627/23' 

	UPDATE K SET CODORDEM='62712/23',IDATVORDEM=KA.IDATV2,INDICEPROCESSO=KA.IDID FROM KATVORDEMPROCESSO K 
	INNER JOIN #TEMP KA ON
	K.CODCOLIGADA=KA.CODCOLIGADA
	AND K.CODFILIAL=KA.CODFILIAL
	AND K.CODORDEM=KA.CODORDEM
	AND K.IDATVORDEM=KA.IDATVORDEM
	AND K.INDICEPROCESSO=KA.INDICEPROCESSO
	WHERE K.CODORDEM='70627/23' 

	UPDATE K SET CODORDEM='62712/23',IDATVORDEM=KA.IDATV2 FROM KPARADAATV K 
	INNER JOIN #TEMP KA ON
	K.CODCOLIGADA=KA.CODCOLIGADA
	AND K.CODFILIAL=KA.CODFILIAL
	AND K.CODORDEM=KA.CODORDEM
	AND K.IDATVORDEM=KA.IDATVORDEM
	WHERE K.CODORDEM='70627/23' 

	DROP TABLE #TEMP

ALTER TABLE KMAOOBRAALOC WITH CHECK CHECK CONSTRAINT FKKMAOOBRAALOC_KATVORDPROC


UPDATE K SET CODORDEM='62712/23',IDATV=KA2.IDATVORDEM FROM KLOGAPONTAMENTO K 
INNER JOIN KATVORDEM KAA ON
KAA.CODCOLIGADA=K.CODCOLIGADA
AND KAA.CODFILIAL=K.CODFILIAL
AND KAA.CODORDEM=K.CODORDEM
AND KAA.IDATVORDEM=K.IDATV
INNER JOIN KATVORDEM KA2 ON
KA2.CODCOLIGADA=K.CODCOLIGADA
AND KA2.CODFILIAL=K.CODFILIAL
AND KA2.CODORDEM='62712/23'
AND KA2.CODATIVIDADE=KAA.CODATIVIDADE
WHERE K.CODORDEM='70627/23' 

UPDATE KLOGPRODUCAO SET CODORDEM='62712/23' WHERE CODORDEM='70627/23' 

UPDATE KLOGAPONTAMENTO SET CODORDEM='62712/23' WHERE CODORDEM='70627/23' 


------ CASO NECESSÁRIO DELETAR A ORDEM DE ORIGEM
--DELETE FROM KATVORDEMCUSTOMAOOBRA WHERE CODORDEM='70627/23'
--DELETE FROM KATVORDEMCUSTOPOSTO WHERE CODORDEM='70627/23'
--DELETE FROM KATVORDEMCUSTOEQP WHERE CODORDEM='70627/23'
--DELETE FROM KATVORDEMHISTORICOCUSTO WHERE CODORDEM='70627/23'
--DELETE FROM KATVORDEMMP WHERE CODORDEM ='70627/23' 
--DELETE FROM KATVORDEMCOMPL WHERE CODORDEM='70627/23' 
--DELETE FROM KATVORDEM WHERE CODORDEM ='70627/23' 
--DELETE FROM KITEMORDEM WHERE CODORDEM ='70627/23' 
--DELETE FROM KORDEMCOMPL WHERE CODORDEM ='70627/23' 
--DELETE FROM KORDEM WHERE CODORDEM='70627/23'


