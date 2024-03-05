SELECT OS,CODSEMANA,SUM(VALOR) HORAS,NOME FROM ( SELECT * FROM ( SELECT [IDCM]
      ,[CODSEMANAHIST]
      ,[OS]
      ,[DESCRICAO]
      ,[DESENHO]
      ,[ORDEM]
      ,[OPERACAO]
      ,[ATIVIDADE]
      ,CODSEMANA,CASE 
        WHEN SEMANA = 'CODSEMANA1' THEN VALOR1
        WHEN SEMANA = 'CODSEMANA2' THEN VALOR2
        WHEN SEMANA = 'CODSEMANA3' THEN VALOR3
        WHEN SEMANA = 'CODSEMANA4' THEN VALOR4
        WHEN SEMANA = 'CODSEMANA5' THEN VALOR5
        WHEN SEMANA = 'CODSEMANA6' THEN VALOR6
        WHEN SEMANA = 'CODSEMANA7' THEN VALOR7
        WHEN SEMANA = 'CODSEMANA8' THEN VALOR8
        WHEN SEMANA = 'CODSEMANA9' THEN VALOR9
        WHEN SEMANA = 'CODSEMANA10' THEN VALOR10
        WHEN SEMANA = 'CODSEMANA11' THEN VALOR11
        WHEN SEMANA = 'CODSEMANA12' THEN VALOR12
        WHEN SEMANA = 'CODSEMANA13' THEN VALOR13
        WHEN SEMANA = 'CODSEMANA14' THEN VALOR14
        WHEN SEMANA = 'CODSEMANA15' THEN VALOR15
        WHEN SEMANA = 'CODSEMANA16' THEN VALOR16
        WHEN SEMANA = 'CODSEMANA17' THEN VALOR17
        WHEN SEMANA = 'CODSEMANA18' THEN VALOR18
        WHEN SEMANA = 'CODSEMANA19' THEN VALOR19
        WHEN SEMANA = 'CODSEMANA20' THEN VALOR20
        WHEN SEMANA = 'CODSEMANA21' THEN VALOR21
        WHEN SEMANA = 'CODSEMANA22' THEN VALOR22
        WHEN SEMANA = 'CODSEMANA23' THEN VALOR23
        WHEN SEMANA = 'CODSEMANA24' THEN VALOR24
        WHEN SEMANA = 'CODSEMANA25' THEN VALOR25
        WHEN SEMANA = 'CODSEMANA26' THEN VALOR26
        WHEN SEMANA = 'CODSEMANA27' THEN VALOR27
        WHEN SEMANA = 'CODSEMANA28' THEN VALOR28
        WHEN SEMANA = 'CODSEMANA29' THEN VALOR29
        WHEN SEMANA = 'CODSEMANA30' THEN VALOR30
        WHEN SEMANA = 'CODSEMANA31' THEN VALOR31
        WHEN SEMANA = 'CODSEMANA32' THEN VALOR32
        WHEN SEMANA = 'CODSEMANA33' THEN VALOR33
        WHEN SEMANA = 'CODSEMANA34' THEN VALOR34
        WHEN SEMANA = 'CODSEMANA35' THEN VALOR35
        WHEN SEMANA = 'CODSEMANA36' THEN VALOR36
        WHEN SEMANA = 'CODSEMANA37' THEN VALOR37
        WHEN SEMANA = 'CODSEMANA38' THEN VALOR38
        WHEN SEMANA = 'CODSEMANA39' THEN VALOR39
        WHEN SEMANA = 'CODSEMANA40' THEN VALOR40
        WHEN SEMANA = 'CODSEMANA41' THEN VALOR41
        WHEN SEMANA = 'CODSEMANA42' THEN VALOR42
        WHEN SEMANA = 'CODSEMANA43' THEN VALOR43
        WHEN SEMANA = 'CODSEMANA44' THEN VALOR44
        WHEN SEMANA = 'CODSEMANA45' THEN VALOR45
        WHEN SEMANA = 'CODSEMANA46' THEN VALOR46
        WHEN SEMANA = 'CODSEMANA47' THEN VALOR47
        WHEN SEMANA = 'CODSEMANA48' THEN VALOR48
        WHEN SEMANA = 'CODSEMANA49' THEN VALOR49
        WHEN SEMANA = 'CODSEMANA50' THEN VALOR50
        WHEN SEMANA = 'CODSEMANA51' THEN VALOR51
        WHEN SEMANA = 'CODSEMANA52' THEN VALOR52
        WHEN SEMANA = 'CODSEMANA53' THEN VALOR53
        WHEN SEMANA = 'CODSEMANA54' THEN VALOR54
        WHEN SEMANA = 'CODSEMANA55' THEN VALOR55
        WHEN SEMANA = 'CODSEMANA56' THEN VALOR56
        WHEN SEMANA = 'CODSEMANA57' THEN VALOR57
        WHEN SEMANA = 'CODSEMANA58' THEN VALOR58
        WHEN SEMANA = 'CODSEMANA59' THEN VALOR59
        WHEN SEMANA = 'CODSEMANA60' THEN VALOR60
        WHEN SEMANA = 'CODSEMANA61' THEN VALOR61
        WHEN SEMANA = 'CODSEMANA62' THEN VALOR62
        WHEN SEMANA = 'CODSEMANA63' THEN VALOR63
        WHEN SEMANA = 'CODSEMANA64' THEN VALOR64
        WHEN SEMANA = 'CODSEMANA65' THEN VALOR65
        WHEN SEMANA = 'CODSEMANA66' THEN VALOR66
        WHEN SEMANA = 'CODSEMANA67' THEN VALOR67
        WHEN SEMANA = 'CODSEMANA68' THEN VALOR68
        WHEN SEMANA = 'CODSEMANA69' THEN VALOR69
        WHEN SEMANA = 'CODSEMANA70' THEN VALOR70
        WHEN SEMANA = 'CODSEMANA71' THEN VALOR71
        WHEN SEMANA = 'CODSEMANA72' THEN VALOR72
        ELSE NULL -- CASO NÃO HAJA CORRESPONDÊNCIA
     END AS VALOR
	  FROM
	  (SELECT * FROM [FLUIG].[DBO].[Z_CRM_CMPINTURA] WHERE STATUS='CARGA' AND ATIVA=1
  ) CM
  UNPIVOT
    ( CODSEMANA FOR [SEMANA] IN 
        ([CODSEMANA1],[CODSEMANA2],[CODSEMANA3],[CODSEMANA4],[CODSEMANA5],[CODSEMANA6],[CODSEMANA7],[CODSEMANA8],
        [CODSEMANA9],[CODSEMANA10],[CODSEMANA11],[CODSEMANA12],[CODSEMANA13],[CODSEMANA14],[CODSEMANA15],[CODSEMANA16],
        [CODSEMANA17],[CODSEMANA18],[CODSEMANA19],[CODSEMANA20],[CODSEMANA21],[CODSEMANA22],[CODSEMANA23],[CODSEMANA24],
        [CODSEMANA25],[CODSEMANA26],[CODSEMANA27],[CODSEMANA28],[CODSEMANA29],[CODSEMANA30],[CODSEMANA31],[CODSEMANA32],
        [CODSEMANA33],[CODSEMANA34],[CODSEMANA35],[CODSEMANA36],[CODSEMANA37],[CODSEMANA38],[CODSEMANA39],[CODSEMANA40],
        [CODSEMANA41],[CODSEMANA42],[CODSEMANA43],[CODSEMANA44],[CODSEMANA45],[CODSEMANA46],[CODSEMANA47],[CODSEMANA48],
        [CODSEMANA49],[CODSEMANA50],[CODSEMANA51],[CODSEMANA52],[CODSEMANA53],[CODSEMANA54],[CODSEMANA55],[CODSEMANA56],
        [CODSEMANA57],[CODSEMANA58],[CODSEMANA59],[CODSEMANA60],[CODSEMANA61],[CODSEMANA62],[CODSEMANA63],[CODSEMANA64],
        [CODSEMANA65],[CODSEMANA66],[CODSEMANA67],[CODSEMANA68],[CODSEMANA69],[CODSEMANA70],[CODSEMANA71],[CODSEMANA72])
) AS CM2 ) CM3 WHERE VALOR!=0
UNION ALL
SELECT * FROM ( SELECT [IDCM]
      ,[CODSEMANAHIST]
      ,[OS]
      ,[DESCRICAO]
      ,[DESENHO]
      ,[ORDEM]
      ,[OPERACAO]
      ,[ATIVIDADE]
      ,CODSEMANA,CASE 
        WHEN SEMANA = 'CODSEMANA1' THEN VALOR1
        WHEN SEMANA = 'CODSEMANA2' THEN VALOR2
        WHEN SEMANA = 'CODSEMANA3' THEN VALOR3
        WHEN SEMANA = 'CODSEMANA4' THEN VALOR4
        WHEN SEMANA = 'CODSEMANA5' THEN VALOR5
        WHEN SEMANA = 'CODSEMANA6' THEN VALOR6
        WHEN SEMANA = 'CODSEMANA7' THEN VALOR7
        WHEN SEMANA = 'CODSEMANA8' THEN VALOR8
        WHEN SEMANA = 'CODSEMANA9' THEN VALOR9
        WHEN SEMANA = 'CODSEMANA10' THEN VALOR10
        WHEN SEMANA = 'CODSEMANA11' THEN VALOR11
        WHEN SEMANA = 'CODSEMANA12' THEN VALOR12
        WHEN SEMANA = 'CODSEMANA13' THEN VALOR13
        WHEN SEMANA = 'CODSEMANA14' THEN VALOR14
        WHEN SEMANA = 'CODSEMANA15' THEN VALOR15
        WHEN SEMANA = 'CODSEMANA16' THEN VALOR16
        WHEN SEMANA = 'CODSEMANA17' THEN VALOR17
        WHEN SEMANA = 'CODSEMANA18' THEN VALOR18
        WHEN SEMANA = 'CODSEMANA19' THEN VALOR19
        WHEN SEMANA = 'CODSEMANA20' THEN VALOR20
        WHEN SEMANA = 'CODSEMANA21' THEN VALOR21
        WHEN SEMANA = 'CODSEMANA22' THEN VALOR22
        WHEN SEMANA = 'CODSEMANA23' THEN VALOR23
        WHEN SEMANA = 'CODSEMANA24' THEN VALOR24
        WHEN SEMANA = 'CODSEMANA25' THEN VALOR25
        WHEN SEMANA = 'CODSEMANA26' THEN VALOR26
        WHEN SEMANA = 'CODSEMANA27' THEN VALOR27
        WHEN SEMANA = 'CODSEMANA28' THEN VALOR28
        WHEN SEMANA = 'CODSEMANA29' THEN VALOR29
        WHEN SEMANA = 'CODSEMANA30' THEN VALOR30
        WHEN SEMANA = 'CODSEMANA31' THEN VALOR31
        WHEN SEMANA = 'CODSEMANA32' THEN VALOR32
        WHEN SEMANA = 'CODSEMANA33' THEN VALOR33
        WHEN SEMANA = 'CODSEMANA34' THEN VALOR34
        WHEN SEMANA = 'CODSEMANA35' THEN VALOR35
        WHEN SEMANA = 'CODSEMANA36' THEN VALOR36
        WHEN SEMANA = 'CODSEMANA37' THEN VALOR37
        WHEN SEMANA = 'CODSEMANA38' THEN VALOR38
        WHEN SEMANA = 'CODSEMANA39' THEN VALOR39
        WHEN SEMANA = 'CODSEMANA40' THEN VALOR40
        WHEN SEMANA = 'CODSEMANA41' THEN VALOR41
        WHEN SEMANA = 'CODSEMANA42' THEN VALOR42
        WHEN SEMANA = 'CODSEMANA43' THEN VALOR43
        WHEN SEMANA = 'CODSEMANA44' THEN VALOR44
        WHEN SEMANA = 'CODSEMANA45' THEN VALOR45
        WHEN SEMANA = 'CODSEMANA46' THEN VALOR46
        WHEN SEMANA = 'CODSEMANA47' THEN VALOR47
        WHEN SEMANA = 'CODSEMANA48' THEN VALOR48
        WHEN SEMANA = 'CODSEMANA49' THEN VALOR49
        WHEN SEMANA = 'CODSEMANA50' THEN VALOR50
        WHEN SEMANA = 'CODSEMANA51' THEN VALOR51
        WHEN SEMANA = 'CODSEMANA52' THEN VALOR52
        WHEN SEMANA = 'CODSEMANA53' THEN VALOR53
        WHEN SEMANA = 'CODSEMANA54' THEN VALOR54
        WHEN SEMANA = 'CODSEMANA55' THEN VALOR55
        WHEN SEMANA = 'CODSEMANA56' THEN VALOR56
        WHEN SEMANA = 'CODSEMANA57' THEN VALOR57
        WHEN SEMANA = 'CODSEMANA58' THEN VALOR58
        WHEN SEMANA = 'CODSEMANA59' THEN VALOR59
        WHEN SEMANA = 'CODSEMANA60' THEN VALOR60
        WHEN SEMANA = 'CODSEMANA61' THEN VALOR61
        WHEN SEMANA = 'CODSEMANA62' THEN VALOR62
        WHEN SEMANA = 'CODSEMANA63' THEN VALOR63
        WHEN SEMANA = 'CODSEMANA64' THEN VALOR64
        WHEN SEMANA = 'CODSEMANA65' THEN VALOR65
        WHEN SEMANA = 'CODSEMANA66' THEN VALOR66
        WHEN SEMANA = 'CODSEMANA67' THEN VALOR67
        WHEN SEMANA = 'CODSEMANA68' THEN VALOR68
        WHEN SEMANA = 'CODSEMANA69' THEN VALOR69
        WHEN SEMANA = 'CODSEMANA70' THEN VALOR70
        WHEN SEMANA = 'CODSEMANA71' THEN VALOR71
        WHEN SEMANA = 'CODSEMANA72' THEN VALOR72
        ELSE NULL -- CASO NÃO HAJA CORRESPONDÊNCIA
     END AS VALOR
	  FROM
	  (SELECT * FROM [FLUIG].[DBO].[Z_CRM_CMPREPARACAO] WHERE STATUS='CARGA' AND ATIVA=1
  ) CM
  UNPIVOT
    ( CODSEMANA FOR [SEMANA] IN 
        ([CODSEMANA1],[CODSEMANA2],[CODSEMANA3],[CODSEMANA4],[CODSEMANA5],[CODSEMANA6],[CODSEMANA7],[CODSEMANA8],
        [CODSEMANA9],[CODSEMANA10],[CODSEMANA11],[CODSEMANA12],[CODSEMANA13],[CODSEMANA14],[CODSEMANA15],[CODSEMANA16],
        [CODSEMANA17],[CODSEMANA18],[CODSEMANA19],[CODSEMANA20],[CODSEMANA21],[CODSEMANA22],[CODSEMANA23],[CODSEMANA24],
        [CODSEMANA25],[CODSEMANA26],[CODSEMANA27],[CODSEMANA28],[CODSEMANA29],[CODSEMANA30],[CODSEMANA31],[CODSEMANA32],
        [CODSEMANA33],[CODSEMANA34],[CODSEMANA35],[CODSEMANA36],[CODSEMANA37],[CODSEMANA38],[CODSEMANA39],[CODSEMANA40],
        [CODSEMANA41],[CODSEMANA42],[CODSEMANA43],[CODSEMANA44],[CODSEMANA45],[CODSEMANA46],[CODSEMANA47],[CODSEMANA48],
        [CODSEMANA49],[CODSEMANA50],[CODSEMANA51],[CODSEMANA52],[CODSEMANA53],[CODSEMANA54],[CODSEMANA55],[CODSEMANA56],
        [CODSEMANA57],[CODSEMANA58],[CODSEMANA59],[CODSEMANA60],[CODSEMANA61],[CODSEMANA62],[CODSEMANA63],[CODSEMANA64],
        [CODSEMANA65],[CODSEMANA66],[CODSEMANA67],[CODSEMANA68],[CODSEMANA69],[CODSEMANA70],[CODSEMANA71],[CODSEMANA72])
) AS CM2 ) CM3 WHERE VALOR!=0
UNION ALL
SELECT * FROM ( SELECT [IDCM]
      ,[CODSEMANAHIST]
      ,[OS]
      ,[DESCRICAO]
      ,[DESENHO]
      ,[ORDEM]
      ,[OPERACAO]
      ,[ATIVIDADE]
      ,CODSEMANA,CASE 
        WHEN SEMANA = 'CODSEMANA1' THEN VALOR1
        WHEN SEMANA = 'CODSEMANA2' THEN VALOR2
        WHEN SEMANA = 'CODSEMANA3' THEN VALOR3
        WHEN SEMANA = 'CODSEMANA4' THEN VALOR4
        WHEN SEMANA = 'CODSEMANA5' THEN VALOR5
        WHEN SEMANA = 'CODSEMANA6' THEN VALOR6
        WHEN SEMANA = 'CODSEMANA7' THEN VALOR7
        WHEN SEMANA = 'CODSEMANA8' THEN VALOR8
        WHEN SEMANA = 'CODSEMANA9' THEN VALOR9
        WHEN SEMANA = 'CODSEMANA10' THEN VALOR10
        WHEN SEMANA = 'CODSEMANA11' THEN VALOR11
        WHEN SEMANA = 'CODSEMANA12' THEN VALOR12
        WHEN SEMANA = 'CODSEMANA13' THEN VALOR13
        WHEN SEMANA = 'CODSEMANA14' THEN VALOR14
        WHEN SEMANA = 'CODSEMANA15' THEN VALOR15
        WHEN SEMANA = 'CODSEMANA16' THEN VALOR16
        WHEN SEMANA = 'CODSEMANA17' THEN VALOR17
        WHEN SEMANA = 'CODSEMANA18' THEN VALOR18
        WHEN SEMANA = 'CODSEMANA19' THEN VALOR19
        WHEN SEMANA = 'CODSEMANA20' THEN VALOR20
        WHEN SEMANA = 'CODSEMANA21' THEN VALOR21
        WHEN SEMANA = 'CODSEMANA22' THEN VALOR22
        WHEN SEMANA = 'CODSEMANA23' THEN VALOR23
        WHEN SEMANA = 'CODSEMANA24' THEN VALOR24
        WHEN SEMANA = 'CODSEMANA25' THEN VALOR25
        WHEN SEMANA = 'CODSEMANA26' THEN VALOR26
        WHEN SEMANA = 'CODSEMANA27' THEN VALOR27
        WHEN SEMANA = 'CODSEMANA28' THEN VALOR28
        WHEN SEMANA = 'CODSEMANA29' THEN VALOR29
        WHEN SEMANA = 'CODSEMANA30' THEN VALOR30
        WHEN SEMANA = 'CODSEMANA31' THEN VALOR31
        WHEN SEMANA = 'CODSEMANA32' THEN VALOR32
        WHEN SEMANA = 'CODSEMANA33' THEN VALOR33
        WHEN SEMANA = 'CODSEMANA34' THEN VALOR34
        WHEN SEMANA = 'CODSEMANA35' THEN VALOR35
        WHEN SEMANA = 'CODSEMANA36' THEN VALOR36
        WHEN SEMANA = 'CODSEMANA37' THEN VALOR37
        WHEN SEMANA = 'CODSEMANA38' THEN VALOR38
        WHEN SEMANA = 'CODSEMANA39' THEN VALOR39
        WHEN SEMANA = 'CODSEMANA40' THEN VALOR40
        WHEN SEMANA = 'CODSEMANA41' THEN VALOR41
        WHEN SEMANA = 'CODSEMANA42' THEN VALOR42
        WHEN SEMANA = 'CODSEMANA43' THEN VALOR43
        WHEN SEMANA = 'CODSEMANA44' THEN VALOR44
        WHEN SEMANA = 'CODSEMANA45' THEN VALOR45
        WHEN SEMANA = 'CODSEMANA46' THEN VALOR46
        WHEN SEMANA = 'CODSEMANA47' THEN VALOR47
        WHEN SEMANA = 'CODSEMANA48' THEN VALOR48
        WHEN SEMANA = 'CODSEMANA49' THEN VALOR49
        WHEN SEMANA = 'CODSEMANA50' THEN VALOR50
        WHEN SEMANA = 'CODSEMANA51' THEN VALOR51
        WHEN SEMANA = 'CODSEMANA52' THEN VALOR52
        WHEN SEMANA = 'CODSEMANA53' THEN VALOR53
        WHEN SEMANA = 'CODSEMANA54' THEN VALOR54
        WHEN SEMANA = 'CODSEMANA55' THEN VALOR55
        WHEN SEMANA = 'CODSEMANA56' THEN VALOR56
        WHEN SEMANA = 'CODSEMANA57' THEN VALOR57
        WHEN SEMANA = 'CODSEMANA58' THEN VALOR58
        WHEN SEMANA = 'CODSEMANA59' THEN VALOR59
        WHEN SEMANA = 'CODSEMANA60' THEN VALOR60
        WHEN SEMANA = 'CODSEMANA61' THEN VALOR61
        WHEN SEMANA = 'CODSEMANA62' THEN VALOR62
        WHEN SEMANA = 'CODSEMANA63' THEN VALOR63
        WHEN SEMANA = 'CODSEMANA64' THEN VALOR64
        WHEN SEMANA = 'CODSEMANA65' THEN VALOR65
        WHEN SEMANA = 'CODSEMANA66' THEN VALOR66
        WHEN SEMANA = 'CODSEMANA67' THEN VALOR67
        WHEN SEMANA = 'CODSEMANA68' THEN VALOR68
        WHEN SEMANA = 'CODSEMANA69' THEN VALOR69
        WHEN SEMANA = 'CODSEMANA70' THEN VALOR70
        WHEN SEMANA = 'CODSEMANA71' THEN VALOR71
        WHEN SEMANA = 'CODSEMANA72' THEN VALOR72
        ELSE NULL -- CASO NÃO HAJA CORRESPONDÊNCIA
     END AS VALOR
	  FROM
	  (SELECT * FROM [FLUIG].[DBO].[Z_CRM_CMUSINAGEM] WHERE STATUS='CARGA' AND ATIVA=1
  ) CM
  UNPIVOT
    ( CODSEMANA FOR [SEMANA] IN 
                ([CODSEMANA1],[CODSEMANA2],[CODSEMANA3],[CODSEMANA4],[CODSEMANA5],[CODSEMANA6],[CODSEMANA7],[CODSEMANA8],
        [CODSEMANA9],[CODSEMANA10],[CODSEMANA11],[CODSEMANA12],[CODSEMANA13],[CODSEMANA14],[CODSEMANA15],[CODSEMANA16],
        [CODSEMANA17],[CODSEMANA18],[CODSEMANA19],[CODSEMANA20],[CODSEMANA21],[CODSEMANA22],[CODSEMANA23],[CODSEMANA24],
        [CODSEMANA25],[CODSEMANA26],[CODSEMANA27],[CODSEMANA28],[CODSEMANA29],[CODSEMANA30],[CODSEMANA31],[CODSEMANA32],
        [CODSEMANA33],[CODSEMANA34],[CODSEMANA35],[CODSEMANA36],[CODSEMANA37],[CODSEMANA38],[CODSEMANA39],[CODSEMANA40],
        [CODSEMANA41],[CODSEMANA42],[CODSEMANA43],[CODSEMANA44],[CODSEMANA45],[CODSEMANA46],[CODSEMANA47],[CODSEMANA48],
        [CODSEMANA49],[CODSEMANA50],[CODSEMANA51],[CODSEMANA52],[CODSEMANA53],[CODSEMANA54],[CODSEMANA55],[CODSEMANA56],
        [CODSEMANA57],[CODSEMANA58],[CODSEMANA59],[CODSEMANA60],[CODSEMANA61],[CODSEMANA62],[CODSEMANA63],[CODSEMANA64],
        [CODSEMANA65],[CODSEMANA66],[CODSEMANA67],[CODSEMANA68],[CODSEMANA69],[CODSEMANA70],[CODSEMANA71],[CODSEMANA72])
) AS CM2 ) CM3 WHERE VALOR!=0
UNION ALL
SELECT * FROM ( SELECT [IDCM]
      ,[CODSEMANAHIST]
      ,[OS]
      ,[DESCRICAO]
      ,[DESENHO]
      ,[ORDEM]
      ,[OPERACAO]
      ,[ATIVIDADE]
      ,CODSEMANA,CASE 
        WHEN SEMANA = 'CODSEMANA1' THEN VALOR1
        WHEN SEMANA = 'CODSEMANA2' THEN VALOR2
        WHEN SEMANA = 'CODSEMANA3' THEN VALOR3
        WHEN SEMANA = 'CODSEMANA4' THEN VALOR4
        WHEN SEMANA = 'CODSEMANA5' THEN VALOR5
        WHEN SEMANA = 'CODSEMANA6' THEN VALOR6
        WHEN SEMANA = 'CODSEMANA7' THEN VALOR7
        WHEN SEMANA = 'CODSEMANA8' THEN VALOR8
        WHEN SEMANA = 'CODSEMANA9' THEN VALOR9
        WHEN SEMANA = 'CODSEMANA10' THEN VALOR10
        WHEN SEMANA = 'CODSEMANA11' THEN VALOR11
        WHEN SEMANA = 'CODSEMANA12' THEN VALOR12
        WHEN SEMANA = 'CODSEMANA13' THEN VALOR13
        WHEN SEMANA = 'CODSEMANA14' THEN VALOR14
        WHEN SEMANA = 'CODSEMANA15' THEN VALOR15
        WHEN SEMANA = 'CODSEMANA16' THEN VALOR16
        WHEN SEMANA = 'CODSEMANA17' THEN VALOR17
        WHEN SEMANA = 'CODSEMANA18' THEN VALOR18
        WHEN SEMANA = 'CODSEMANA19' THEN VALOR19
        WHEN SEMANA = 'CODSEMANA20' THEN VALOR20
        WHEN SEMANA = 'CODSEMANA21' THEN VALOR21
        WHEN SEMANA = 'CODSEMANA22' THEN VALOR22
        WHEN SEMANA = 'CODSEMANA23' THEN VALOR23
        WHEN SEMANA = 'CODSEMANA24' THEN VALOR24
        WHEN SEMANA = 'CODSEMANA25' THEN VALOR25
        WHEN SEMANA = 'CODSEMANA26' THEN VALOR26
        WHEN SEMANA = 'CODSEMANA27' THEN VALOR27
        WHEN SEMANA = 'CODSEMANA28' THEN VALOR28
        WHEN SEMANA = 'CODSEMANA29' THEN VALOR29
        WHEN SEMANA = 'CODSEMANA30' THEN VALOR30
        WHEN SEMANA = 'CODSEMANA31' THEN VALOR31
        WHEN SEMANA = 'CODSEMANA32' THEN VALOR32
        WHEN SEMANA = 'CODSEMANA33' THEN VALOR33
        WHEN SEMANA = 'CODSEMANA34' THEN VALOR34
        WHEN SEMANA = 'CODSEMANA35' THEN VALOR35
        WHEN SEMANA = 'CODSEMANA36' THEN VALOR36
        WHEN SEMANA = 'CODSEMANA37' THEN VALOR37
        WHEN SEMANA = 'CODSEMANA38' THEN VALOR38
        WHEN SEMANA = 'CODSEMANA39' THEN VALOR39
        WHEN SEMANA = 'CODSEMANA40' THEN VALOR40
        WHEN SEMANA = 'CODSEMANA41' THEN VALOR41
        WHEN SEMANA = 'CODSEMANA42' THEN VALOR42
        WHEN SEMANA = 'CODSEMANA43' THEN VALOR43
        WHEN SEMANA = 'CODSEMANA44' THEN VALOR44
        WHEN SEMANA = 'CODSEMANA45' THEN VALOR45
        WHEN SEMANA = 'CODSEMANA46' THEN VALOR46
        WHEN SEMANA = 'CODSEMANA47' THEN VALOR47
        WHEN SEMANA = 'CODSEMANA48' THEN VALOR48
        WHEN SEMANA = 'CODSEMANA49' THEN VALOR49
        WHEN SEMANA = 'CODSEMANA50' THEN VALOR50
        WHEN SEMANA = 'CODSEMANA51' THEN VALOR51
        WHEN SEMANA = 'CODSEMANA52' THEN VALOR52
        WHEN SEMANA = 'CODSEMANA53' THEN VALOR53
        WHEN SEMANA = 'CODSEMANA54' THEN VALOR54
        WHEN SEMANA = 'CODSEMANA55' THEN VALOR55
        WHEN SEMANA = 'CODSEMANA56' THEN VALOR56
        WHEN SEMANA = 'CODSEMANA57' THEN VALOR57
        WHEN SEMANA = 'CODSEMANA58' THEN VALOR58
        WHEN SEMANA = 'CODSEMANA59' THEN VALOR59
        WHEN SEMANA = 'CODSEMANA60' THEN VALOR60
        WHEN SEMANA = 'CODSEMANA61' THEN VALOR61
        WHEN SEMANA = 'CODSEMANA62' THEN VALOR62
        WHEN SEMANA = 'CODSEMANA63' THEN VALOR63
        WHEN SEMANA = 'CODSEMANA64' THEN VALOR64
        WHEN SEMANA = 'CODSEMANA65' THEN VALOR65
        WHEN SEMANA = 'CODSEMANA66' THEN VALOR66
        WHEN SEMANA = 'CODSEMANA67' THEN VALOR67
        WHEN SEMANA = 'CODSEMANA68' THEN VALOR68
        WHEN SEMANA = 'CODSEMANA69' THEN VALOR69
        WHEN SEMANA = 'CODSEMANA70' THEN VALOR70
        WHEN SEMANA = 'CODSEMANA71' THEN VALOR71
        WHEN SEMANA = 'CODSEMANA72' THEN VALOR72
        ELSE NULL -- CASO NÃO HAJA CORRESPONDÊNCIA
     END AS VALOR
	  FROM
	  (SELECT * FROM [FLUIG].[DBO].[Z_CRM_CMSOLDAROBO] WHERE STATUS='CARGA' AND ATIVA=1
  ) CM
  UNPIVOT
    ( CODSEMANA FOR [SEMANA] IN 
                ([CODSEMANA1],[CODSEMANA2],[CODSEMANA3],[CODSEMANA4],[CODSEMANA5],[CODSEMANA6],[CODSEMANA7],[CODSEMANA8],
        [CODSEMANA9],[CODSEMANA10],[CODSEMANA11],[CODSEMANA12],[CODSEMANA13],[CODSEMANA14],[CODSEMANA15],[CODSEMANA16],
        [CODSEMANA17],[CODSEMANA18],[CODSEMANA19],[CODSEMANA20],[CODSEMANA21],[CODSEMANA22],[CODSEMANA23],[CODSEMANA24],
        [CODSEMANA25],[CODSEMANA26],[CODSEMANA27],[CODSEMANA28],[CODSEMANA29],[CODSEMANA30],[CODSEMANA31],[CODSEMANA32],
        [CODSEMANA33],[CODSEMANA34],[CODSEMANA35],[CODSEMANA36],[CODSEMANA37],[CODSEMANA38],[CODSEMANA39],[CODSEMANA40],
        [CODSEMANA41],[CODSEMANA42],[CODSEMANA43],[CODSEMANA44],[CODSEMANA45],[CODSEMANA46],[CODSEMANA47],[CODSEMANA48],
        [CODSEMANA49],[CODSEMANA50],[CODSEMANA51],[CODSEMANA52],[CODSEMANA53],[CODSEMANA54],[CODSEMANA55],[CODSEMANA56],
        [CODSEMANA57],[CODSEMANA58],[CODSEMANA59],[CODSEMANA60],[CODSEMANA61],[CODSEMANA62],[CODSEMANA63],[CODSEMANA64],
        [CODSEMANA65],[CODSEMANA66],[CODSEMANA67],[CODSEMANA68],[CODSEMANA69],[CODSEMANA70],[CODSEMANA71],[CODSEMANA72])
) AS CM2 ) CM3 
WHERE VALOR!=0
) CM INNER JOIN KATVORDEM KA ON
KA.CODORDEM=CM.ORDEM COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
AND KA.CODATIVIDADE=CM.ATIVIDADE COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI
INNER JOIN KPOSTO KP ON
KP.CODCOLIGADA=KA.CODCOLIGADA
AND KP.CODFILIAL=KA.CODFILIAL
AND KP.CODPOSTO=KA.CODPOSTO
INNER JOIN GCCUSTO G ON 
G.CODCOLIGADA=KP.CODCOLIGADA 
AND G.CODCCUSTO=KP.CODCCUSTO
WHERE KA.CODCOLIGADA=1
GROUP BY OS,CODSEMANA,NOME
ORDER BY OS,CAST(REPLACE(CODSEMANA,'/','') AS INT)