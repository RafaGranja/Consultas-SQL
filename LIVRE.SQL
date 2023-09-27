" SELECT Z.*, "+
"        AVG(CASE "+
"                      WHEN K.QTDEEFETIVADA >= K.QTDEPLANEJADA THEN 5.0 "+
"                      ELSE CAST(K.STATUS AS FLOAT) END) "+
"                     AS STATUS_FABRI  "+
"  "+
" FROM   FLUIG.DBO.Z_CRM_EX001005 Z "+
" LEFT JOIN KITEMORDEM K "+
"        ON K.CODESTRUTURA = Z.CODIGOPRD COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
"        AND K.CODCOLIGADA = Z.CODCOLIGADA "+
"        AND K.CODFILIAL = Z.CODFILIAL "+
"        AND K.CODCCUSTO = Z.OS COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
"        AND K.STATUS != 6 "+
" LEFT JOIN KORDEM KO "+
"         ON KO.CODCOLIGADA = K.CODCOLIGADA "+
"             AND KO.CODFILIAL = K.CODFILIAL "+
"             AND KO.CODORDEM = K.CODORDEM "+
" 			AND ( KO.REPROCESSAMENTO = 0 OR KO.REPROCESSAMENTO IS NULL OR KO.REPROCESSAMENTO = '') "+
" INNER JOIN KORDEMCOMPL KL "+
"         ON KL.CODCOLIGADA = K.CODCOLIGADA "+
"             AND KL.CODFILIAL = K.CODFILIAL "+
"             AND KL.CODORDEM = K.CODORDEM "+
"             AND KL.NUMEXEC = Z.EXECUCAO "+
" WHERE OS='"+numOS+"' AND EXECUCAO='"+execucao+"' AND ITEMEXCLUSIVO<>2 "+codTrfEx+"  "+
" AND ( KL.NUMEXEC = Z.EXECUCAO  OR K.CODORDEM IS NULL ) "
" GROUP BY [ID] "+
"       ,[companyid] "+
"       ,[cardid] "+
"       ,[documentid] "+
"       ,[version] "+
"       ,[tableid] "+
"       ,[TITULOITEM] "+
"       ,[NIVEL] "+
"       ,[POSICAO] "+
"       ,[DESCRICAO] "+
"       ,[NUMDESENHO] "+
"       ,[REVISAODESENHO] "+
"       ,[NUMDBI] "+
"       ,[REVISAODBI] "+
"       ,[DESQTDE] "+
"       ,[TOTALQTDE] "+
"       ,[BITOLAESP] "+
"       ,[LARGURAMASSA] "+
"       ,[ESPLARGURAROSCA] "+
"       ,[COMPRIMENTO] "+
"       ,[MATERIAL] "+
"       ,[PESO] "+
"       ,[OBSERVACOES] "+
"       ,[TIPO] "+
"       ,[SEQ] "+
"       ,[masterid] "+
"       ,[PESOMATERIAL] "+
"       ,[OS] "+
"       ,[DATAREVISAO] "+
"       ,[PESOBRUTO] "+
"       ,[PESOLIQUIDO] "+
"       ,[OBSERVACOESDESENHO] "+
"       ,[PERIMETROCORTE] "+
"       ,[AREAPINTURA] "+
"       ,[OBSPROCESSO] "+
"       ,[OBSGERAL] "+
"       ,[QUANTIDADEMATERIAL] "+
"       ,[TIPODESENHO] "+
"       ,[POSICAOCOMPLETA] "+
"       ,[ESPESSURA] "+
"       ,[BITOLA] "+
"       ,[LARGURA] "+
"       ,[MASSALINEAR] "+
"       ,[DIAMETROEXTERNO] "+
"       ,[DIAMETROINTERNO] "+
"       ,[ESPROSCA] "+
"       ,[POSICAOCOMPLETAANTIGA] "+
"       ,[NOVOMATERIAL] "+
"       ,[MATERIAL_ZOOM] "+
"       ,[IDMATERIAL] "+
"       ,[CODIGOPRD] "+
"       ,[INDICE] "+
"       ,[POSICAOESTRUTURA] "+
"       ,[POSICAODESENHO] "+
"       ,[AREASECAO] "+
"       ,[ALTURA] "+
"       ,[LARGURAMESA] "+
"       ,[ESPALMA] "+
"       ,[ESPMESA] "+
"       ,[POSICAOINDICE] "+
"       ,[INDICEANTIGO] "+
"       ,[IDCRIACAO] "+
"       ,[PRODUTORM] "+
"       ,[IDPRD] "+
"       ,[EXPANSOR] "+
"       ,[CODIGOPRDMATERIAL] "+
"       ,[CODTRFOS] "+
"       ,[UNDMEDIDA] "+
"       ,[ORDEM] "+
"       ,[LARGURAABA] "+
"       ,[ESPABA] "+
"       ,[INTEGRADO] "+
"       ,[DSCTRFOS] "+
"       ,[SOLICITACAO] "+
"       ,[PAIDETALHADO] "+
"       ,[COMPORLISTA] "+
"       ,[CODTRFITEM] "+
"       ,[IDTRFITEM] "+
"       ,[NOMETRFITEM] "+
"       ,[CODIGOTAREFADESC] "+
"       ,[IDPRJOS] "+
"       ,[DESCOS] "+
"       ,[DETALHADO] "+
"       ,[NUMDOCDELP] "+
"       ,[REVISAODOCDELP] "+
"       ,Z.CODCOLIGADA "+
"       ,[DIAMETROEXTERNODISCO] "+
"       ,[DIAMETROINTERNODISCO] "+
"       ,[CHECKUSINAGEM] "+
"       ,[CHECKCALDERARIA] "+
"       ,Z.CODFILIAL "+
"       ,[CODTRFPAI] "+
"       ,[EXECINTEGRADAS] "+
"       ,[IDTRFPAI] "+
"       ,[NOMETRFPAI] "+
"       ,[EXECUCOES] "+
"       ,[PESOUNITARIO] "+
"       ,[OPSUNITARIAS] "+
"       ,[QTDEUNCOMP] "+
"       ,[OP] "+
"       ,[EXECUCAO] "+
"       ,[CODTRFEX] "+
"       ,[IDCRIACAOPAI] "+
"       ,[ITEMEXCLUSIVO] "+
"       ,Z.RECCREATEDBY "+
"       ,Z.RECCREATEDON "+
"       ,Z.RECMODIFIEDBY "+
"       ,Z.RECMODIFIEDON "+
"       ,[PESOUNLIQUIDO] "+
"       ,[ITEMDERETORNO] "+
" ORDER  BY CAST('/' + REPLACE(INDICE, '.', '/') + '/' AS HIERARCHYID)  "