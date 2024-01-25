// BUSCA TODAS AS INFORMAÇÕES DE UMA DETERMINADA ATIVIDADE DA OP PARA APONTAMENTO SEM PROGRAMAÇÃO
function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var codOrdem = ""
	var celula = ""
	var idAtvOrdem = ""
	var numOS = ""
	var codFilial = ""
    	
	if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "CODFILIAL") {
            	
        		codFilial = constraints[i].initialValue
            	
            }
        	if (constraints[i].fieldName == "CELULA") {
            	
        		celula = constraints[i].initialValue
            	
            }
        	if (constraints[i].fieldName == "NUMOS") {
            	
        		numOS = constraints[i].initialValue
            	
            }
        	if (constraints[i].fieldName == "CODORDEM") {
            	
        		codOrdem = constraints[i].initialValue
            	
            }
        	if (constraints[i].fieldName == "IDATVORDEM") {
            	
        		idAtvOrdem = constraints[i].initialValue
            	
            }
        	
        }
        
	}
    
    var myQuery = 	"SELECT	"+
					"	ATV.CODCOLIGADA, ATV.CODFILIAL, "+
					"		ISNULL((	SELECT TOP 1 AATVD.DSCATIVIDADE "+
					"			FROM  KATVESTRUTURA AATVE "+
					"				INNER JOIN KATIVIDADE AATVD ON AATVE.CODCOLIGADA = AATVD.CODCOLIGADA AND AATVE.CODFILIAL = AATVD.CODFILIAL AND AATVE.CODATIVIDADE = AATVD.CODATIVIDADE "+
					"			WHERE	AATVE.CODCOLIGADA = ATV.CODCOLIGADA  "+
					"			AND		AATVE.CODFILIAL = ATV.CODFILIAL "+
					"			AND		AATVE.CODESTRUTURA = ATV.CODESTRUTURA "+ 
					"			AND		AATVE.PRIORIDADE < ATVE.PRIORIDADE "+
					"			ORDER BY AATVE.PRIORIDADE DESC),'-') ATV_ANTERIOR, "+
					"		P.CODIGOPRD, P.IDPRD, P.DESCRICAO DSCITEM,  "+
					"		EQ.CODEQUIPAMENTO, "+
					"		ATV.QTPREVISTA QTDEPLANEJADA, ATV.QUANTIDADE APONTADO_ATV, ATV.QTPREVISTA-ATV.QUANTIDADE SALDO_ATV, "+
					"		ATV.CODPOSTO, P.CODUNDCONTROLE, ATV.CODORDEM OP,ATV.IDATVORDEM, ATVE.PRIORIDADE, 	"+
					"		(SELECT  "+
					"			CUSTO  "+
					"			FROM  "+
					"			KCUSTOPOSTO (NOLOCK) X "+ 
					"			WHERE  "+
					"			X.CODCOLIGADA = ATV.CODCOLIGADA AND X.CODFILIAL = ATV.CODFILIAL  "+
					"			AND X.CODPOSTO = ATV.CODPOSTO AND DTINICIAL <= CAST(GETDATE() AS DATE) AND DTFINAL >= CAST(GETDATE() AS DATE) "+
					"		) CUSTO_POSTO, "+
					"		ATVD.DSCATIVIDADE, ATVD.CODATIVIDADE, TRF.CODCELULA, CONCAT(TRF.CODCELULA,'-',TRF.DESCCELULA) CELULAR, "+
					"		ATV.CODESTRUTURA, ATVE.CODATIVIDADE,PRJ.IDPRJ,M5.OS, "+
					"		M21.FORNPARA, "+
					"		null FOLGACALC, "+
					"		KC.NUMEXEC EXECUCAO, M5.INDICE, (CASE WHEN PRJ.CODPRJ = '3.07840.32.001' AND M5.INDICE LIKE '1.%' THEN 'S' ELSE 'N' END) INDICEOS7840, "+
					"		M26.F_ITEMAPONTADO ITEMAPONTADO, "+
					"		M28.VIEWPRIORIDADE, "+
					"		ISNULL(KI.QTDEEFETIVADA,0) APONTADO, KI.QTDEPLANEJADA, (KI.QTDEPLANEJADA - ISNULL(KI.QTDEEFETIVADA,0)) SALDO, "+
					"		KI.DTHRINICIALPREV INICIO_PLANEJADO, KI.DTHRFINALPREV FINAL_PLANEJADO, "+
					"		KO.REPROCESSAMENTO RETRABALHO, KO.STATUS STATUS_OP, "+
					"		ATV.PERCENTUAL AVANCO_REALIZADO, ATV.STATUS STATUS_ATV, " +
					"		(CASE WHEN ISNULL((	SELECT TOP 1 AATVD.DSCATIVIDADE "+
					"		FROM  KATVORDEMCOMPL AATVE "+
					"			INNER JOIN KATVORDEM ATVOD ON AATVE.CODCOLIGADA = ATVOD.CODCOLIGADA AND AATVE.CODFILIAL = ATVOD.CODFILIAL AND AATVE.IDATVORDEM = ATVOD.IDATVORDEM AND AATVE.CODORDEM = ATVOD.CODORDEM"+
					"			INNER JOIN KATIVIDADE AATVD ON AATVE.CODCOLIGADA = AATVD.CODCOLIGADA AND AATVE.CODFILIAL = AATVD.CODFILIAL AND AATVD.CODATIVIDADE = ATVOD.CODATIVIDADE "+
					"		WHERE	AATVE.CODCOLIGADA = ATV.CODCOLIGADA "+
					"		AND		AATVE.CODFILIAL = ATV.CODFILIAL "+
					"		AND		AATVE.CODESTRUTURA = ATV.CODESTRUTURA  "+
					"		AND		AATVE.CODORDEM = ATV.CODORDEM "+	
					"		AND		AATVE.PRIORIDADE > ATVC.PRIORIDADE "+
					" 		AND 	ATVOD.STATUS <> 6	"+	
					"		ORDER BY AATVE.PRIORIDADE DESC),'ULTIMA') = 'ULTIMA' AND ISNULL((SELECT OS FROM FLUIG.DBO.Z_CRM_EXPROJETOS WHERE CODCOLIGADA=EX.CODCOLIGADA AND CODFILIAL=EX.CODFILIAL AND OS=EX.OS AND EXECUCAO=EX.EXECUCAO AND CODTRFPAI=EX.CODTRFPAI),'SIM') <> 'SIM' THEN 'ULTIMA' ELSE ATVD.DSCATIVIDADE END) ATV_POSTERIOR, "+ 
					"		ISNULL(( "+
					"			SELECT  "+
					"				SUM(ROUND(CAST(DATEDIFF(minute,DTHRINICIAL,DTHRFINAL)AS FLOAT)/60,4)) "+
					"			FROM "+
					"				KMAOOBRAALOC "+
					"			WHERE "+
					"				CODORDEM=ATV.CODORDEM AND CODESTRUTURA=ATV.CODESTRUTURA AND CODCOLIGADA=ATV.CODCOLIGADA "+
					"				AND CODFILIAL=ATV.CODFILIAL AND IDATVORDEM=ATV.IDATVORDEM AND EFETIVADO=1 "+
					"		),0) + "+
					"		ISNULL(( "+
					"			SELECT  "+
					"				SUM(ROUND(CAST(QTDHORAS AS FLOAT),4)) "+
					"			FROM "+
					"				ZMDKATVORDEMPROGRAMACAO "+
					"			WHERE "+
					"				CODORDEM=ATV.CODORDEM AND CODESTRUTURA=ATV.CODESTRUTURA AND CODCOLIGADA=ATV.CODCOLIGADA "+
					"				AND CODFILIAL=ATV.CODFILIAL AND IDATVORDEM=ATV.IDATVORDEM AND STATUS=1 "+
					"		),0)	"+
					"		TEM_APONTAMENTO "+
					"FROM "+
					"	KATVORDEM ATV "+
					"	INNER JOIN KATVORDEMCOMPL ATVC ON ATV.CODCOLIGADA = ATVC.CODCOLIGADA AND ATV.CODFILIAL = ATVC.CODFILIAL AND ATV.CODORDEM = ATVC.CODORDEM AND ATV.IDATVORDEM = ATVC.IDATVORDEM "+
					"	INNER JOIN KITEMORDEM KI ON ATV.CODCOLIGADA = KI.CODCOLIGADA AND ATV.CODFILIAL = KI.CODFILIAL AND ATV.CODORDEM = KI.CODORDEM AND ATV.CODESTRUTURA = KI.CODESTRUTURA "+
					"	INNER JOIN KORDEM KO ON ATV.CODCOLIGADA = KO.CODCOLIGADA AND ATV.CODFILIAL = KO.CODFILIAL AND ATV.CODORDEM = KO.CODORDEM "+
					"	LEFT JOIN KATVESTRUTURA ATVE ON ATV.CODCOLIGADA = ATVE.CODCOLIGADA AND ATV.CODFILIAL = ATVE.CODFILIAL AND ATV.CODESTRUTURA = ATVE.CODESTRUTURA AND ATV.CODATIVIDADE = ATVE.CODATIVIDADE "+
					"	INNER JOIN KATIVIDADE ATVD ON ATV.CODCOLIGADA = ATVD.CODCOLIGADA AND ATV.CODFILIAL = ATVD.CODFILIAL AND ATV.CODATIVIDADE = ATVD.CODATIVIDADE "+
					"	INNER JOIN TPRD P ON ATV.CODCOLIGADA = P.CODCOLIGADA AND ATV.CODESTRUTURA = P.CODIGOPRD "+
					"	INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL TRF ON TRF.CODCOLIGADA = ATV.CODCOLIGADA AND TRF.CODFILIAL = ATV.CODFILIAL AND TRF.CODORDEM = ATV.CODORDEM "+
					"	INNER JOIN MPRJ PRJ ON PRJ.CODCOLIGADA = TRF.CODCOLIGADA AND PRJ.CODPRJ = TRF.OS COLLATE SQL_Latin1_General_CP1_CI_AI AND PRJ.POSICAO IN (1,4) "+
					"	INNER JOIN KORDEMCOMPL KC ON ATV.CODCOLIGADA = KC.CODCOLIGADA AND ATV.CODFILIAL = KC.CODFILIAL AND ATV.CODORDEM = KC.CODORDEM "+
					"	LEFT JOIN FLUIG.dbo.Z_CRM_ML001005 (NOLOCK) M5 ON PRJ.CODPRJ = M5.OS COLLATE Latin1_General_CI_AS AND P.IDPRD = M5.IDPRD AND M5.ITEMEXCLUSIVO!=2 "+
					"	LEFT JOIN FLUIG.dbo.Z_CRM_ML001021 (NOLOCK) M21 ON M5.OS = M21.OSPROCESSO AND M21.IDCRIACAOPROCESSO = M5.IDCRIACAO AND ATV.CODATIVIDADE=M21.CODATIVIDADE COLLATE SQL_Latin1_General_CP1_CI_AI "+
					"	LEFT JOIN FLUIG.dbo.ML001026 (NOLOCK) M26 ON PRJ.CODPRJ = M26.NUM_OS COLLATE SQL_Latin1_General_CP1_CI_AI  AND ATV.CODORDEM = M26.OPGERADA COLLATE SQL_Latin1_General_CP1_CI_AI "+
					"	LEFT JOIN FLUIG.DBO.ML001028 M28 (NOLOCK) ON M26.DOCUMENTID = M28.DOCUMENTID AND M26.VERSION = M28.VERSION AND ATV.CODATIVIDADE = M28.VIEWCODATIVIDADE  COLLATE Latin1_General_CI_AS "+
					"	LEFT JOIN FLUIG.DBO.Z_CRM_EX001005 EX (NOLOCK) ON KI.CODCOLIGADA = EX.CODCOLIGADA AND KI.CODFILIAL = EX.CODFILIAL AND KI.CODESTRUTURA = EX.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI AND KC.NUMEXEC = EX.EXECUCAO "+
					"	LEFT JOIN KEQUIPAMENTO (NOLOCK) EQ ON ATV.CODCOLIGADA = EQ.CODCOLIGADA AND ATV.CODFILIAL = EQ.CODFILIAL AND ATV.CODPOSTO = EQ.CODPOSTO "+
					"	LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE Z ON Z.CODCOLIGADA = ATV.CODCOLIGADA AND Z.CODFILIAL = ATV.CODFILIAL AND Z.CODORDEM = ATV.CODORDEM AND Z.IDATVORDEM = ATV.IDATVORDEM "+
					"WHERE	"+
					"	ATV.CODCOLIGADA = 1 "+
					"	AND		ATV.CODFILIAL = "+codFilial+" "+
					"	AND		TRF.CODCELULA='"+celula+"' "+
					"	AND		PRJ.CODPRJ='"+numOS+"' "+
					"	AND		ATV.CODORDEM='"+codOrdem+"' "+
					"	AND		ATV.IDATVORDEM="+idAtvOrdem+" "+
					"	AND 	EX.ITEMEXCLUSIVO!=2 "+
					//"	AND		Z.NUMPLANOCORTE IS NULL "+ 
					"ORDER BY "+
					"	ATV.CODORDEM, ATVE.PRIORIDADE "
    
    log.info("QUERY dsBuscaAtvSemProg: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsNewDataset.addColumn(rs.getMetaData().getColumnName(i));
                	
                }
                
                created = true;
                
            }
            
            var Arr = new Array();
            
            for (var i = 1; i <= columnCount; i++) {
            	
                var obj = rs.getObject(rs.getMetaData().getColumnName(i));
                
                if (null != obj) {
                	
                    Arr[i - 1] = rs.getObject(rs.getMetaData().getColumnName(i)).toString();
                    
                } else {
                	
                    Arr[i - 1] = "null";
                    
                }
                
            }
            
            dsNewDataset.addRow(Arr);
            
        }
        
    } catch (e) {
        
    	log.error("ERRO==============> " + e.message);
    
    } finally {
    	
        if (stmt != null) {
        	
            stmt.close();
            
        }
        
        if (conn != null) {
        	
            conn.close();
            
        }
        
    }
    
    return dsNewDataset;
	
}