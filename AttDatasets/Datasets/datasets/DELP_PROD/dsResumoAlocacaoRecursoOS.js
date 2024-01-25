// BUSCA TODOS OS RECURSOS (MÃO DE OBRA) QUE FORMA ALOCADOS DE ACORDO COM OS FILTROS PREENCHIDOS NO FORMULÁRIO
function createDataset(fields, constraints, sortFields) {

	var dsResumoAlocacaoRecursoOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var dataDe = ""
    var dataAte = ""
	var ordenarPor = ""
	var recursoPessoa = ""
    var recursoEquip = ""
    	
    log.info("Dataset dsResumoAlocacaoRecursoOS")
		
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "RECURSO_PESSOA") {
            	
        		if(!(constraints[i].initialValue=="")){
        		
        			recursoPessoa = " WHERE M.CODMO = '"+constraints[i].initialValue+"'"
        			
        		}
        		
            }

        	if (constraints[i].fieldName == "RECURSO_EQUIP") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			recursoEquip = " WHERE EQ.CODEQUIPAMENTO = '"+constraints[i].initialValue+"'"
        			
        		}
        		
            }
        	if (constraints[i].fieldName == "DATA_DE") {
            	
        		//dataDe = " AND CAST(I.DTHRINICIALPREV AS DATE)>= '"+constraints[i].initialValue+"'"
        		dataDe = " AND CAST(KM.DTHRINICIAL AS DATE)>= '"+constraints[i].initialValue+"'"
        		
            }
			if (constraints[i].fieldName == "DATA_ATE") {
				
				//dataAte = " AND CAST(I.DTHRFINALPREV AS DATE)<= '"+constraints[i].initialValue+"'"
				dataAte = " AND CAST(KM.DTHRFINAL AS DATE)<= '"+constraints[i].initialValue+"'"
			}
        	
        	/*if (constraints[i].fieldName == "ORDENAR_POR") {
            	
        		if(!(constraints[i].initialValue=="")){
        			
        			ordenarPor = " ORDER BY "+constraints[i].initialValue+""	
        			
        		} else {
        			
        			ordenarPor = " ORDER BY Z.OS "
        			
        		}
        	
            }*/
        	
        }
        
	}
    
    var myQuery = 	"SELECT * FROM ( "+
    				"SELECT "+
					"	PS.NOME,M.CODMO, "+
					"	EQ.CODEQUIPAMENTO, "+
					"	TR.CODCELULA, PJ.CODPRJ OS, I.CODORDEM OP, I.CODCOLIGADA, I.CODFILIAL, I.CODESTRUTURA, P.CODIGOPRD, P.IDPRD, P.DESCRICAO DSCITEM, DA.CODATIVIDADE, DA.DSCATIVIDADE,  	"+
					"	AE.PRIORIDADE, PE.CODPOSTO, PO.DSCPOSTO,S.CODSTATUS, S.DSCSTATUS, A.TEMPO CARGA_PREV, PC.NUMFLUIG,		"+
					"	PC.INDICE_FLUIG, PC.NIVEL_FLUIG,  		"+
					"	I.DTHRINICIALPREV INICIO_PLANEJADO, I.DTHRFINALPREV FINAL_PLANEJADO, NULL FOLGA, "+
					"	KM.DTHRINICIAL, KM.DTHRFINAL, "+
					"	ISNULL((SELECT 	"+
					"		ROUND(CAST(DATEDIFF(minute,KM.DTHRINICIAL,KM.DTHRFINAL) AS FLOAT)/60,4) HORAS),0)	"+ 
					"		ALOCADO	"+
					"FROM "+
					"	ZMDKATVORDEMPROGRAMACAO KM "+
					"	INNER JOIN KMAOOBRA M ON M.CODCOLIGADA = KM.CODCOLIGADA AND M.CODFILIAL = KM.CODFILIAL AND M.ATIVA='1' AND KM.CODMO = M.CODMO "+
					"	INNER JOIN PPESSOA PS ON M.CODPESSOA = PS.CODIGO  "+
					"	INNER JOIN KITEMORDEM I ON KM.CODCOLIGADA = I.CODCOLIGADA AND KM.CODFILIAL = I.CODFILIAL AND KM.CODORDEM = I.CODORDEM AND KM.CODESTRUTURA = I.CODESTRUTURA "+
					"	INNER JOIN KESTRUTURA E ON I.CODCOLIGADA = E.CODCOLIGADA AND I.CODFILIAL = E.CODFILIAL AND I.CODESTRUTURA = E.CODESTRUTURA  	"+
					"	INNER JOIN TPRD P ON E.CODCOLIGADA = P.CODCOLIGADA AND E.IDPRODUTO = P.IDPRD 		"+
					"	INNER JOIN TPRDCOMPL PC ON I.CODCOLIGADA = PC.CODCOLIGADA AND P.IDPRD = PC.IDPRD  	"+
					"	INNER JOIN KATVORDEM A ON  A.CODCOLIGADA = E.CODCOLIGADA AND A.CODFILIAL = E.CODFILIAL AND A.CODESTRUTURA = E.CODESTRUTURA AND A.CODORDEM = I.CODORDEM AND KM.IDATVORDEM = A.IDATVORDEM "+
					"	INNER JOIN KATVESTRUTURA AE ON AE.CODCOLIGADA = A.CODCOLIGADA AND AE.CODFILIAL = A.CODFILIAL AND AE.CODESTRUTURA = A.CODESTRUTURA AND AE.CODATIVIDADE = A.CODATIVIDADE AND AE.CODATIVIDADE NOT LIKE 'RMP%' "+	 
					"	INNER JOIN KATIVIDADE DA ON DA.CODCOLIGADA = A.CODCOLIGADA AND DA.CODATIVIDADE = A.CODATIVIDADE  "+
					"	INNER JOIN KATVESTPOSTO PE ON PE.CODCOLIGADA = AE.CODCOLIGADA AND PE.CODFILIAL = AE.CODFILIAL AND PE.CODESTRUTURA = AE.CODESTRUTURA AND PE.CODATIVIDADE = AE.CODATIVIDADE  	"+
					"	INNER JOIN KPOSTO PO ON PO.CODCOLIGADA = PE.CODCOLIGADA AND PO.CODFILIAL = PE.CODFILIAL AND PO.CODPOSTO = PE.CODPOSTO 		"+
					"	INNER JOIN KSTATUS S ON S.CODCOLIGADA = I.CODCOLIGADA AND S.CODSTATUS = I.STATUS  		"+
					"	INNER JOIN MPRJ PJ ON PJ.CODCOLIGADA = I.CODCOLIGADA AND PJ.CODPRJ = I.CODCCUSTO AND PJ.POSICAO IN (1,4) 	"+
					"	INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL TR ON I.CODCOLIGADA = TR.CODCOLIGADA AND TR.CODFILIAL = PJ.CODFILIAL AND TR.CODORDEM = KM.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI "+
					"	LEFT JOIN KATVHABILIDADES AH ON A.CODATIVIDADE = AH.CODATIVIDADE AND A.CODCOLIGADA = AH.CODCOLIGADA AND A.CODFILIAL = AH.CODFILIAL "+
					"	LEFT JOIN VHABILIDADES H ON AH.CODHABILIDADE = H.CODHABILIDADE "+
					"	LEFT JOIN VHABILIDADESPESSOAIS HP ON H.CODHABILIDADE = HP.CODHABILIDADE AND M.CODPESSOA = HP.CODPESSOA  "+
					"	LEFT JOIN KEQUIPAMENTO EQ ON I.CODCOLIGADA = EQ.CODCOLIGADA AND I.CODFILIAL = EQ.CODFILIAL AND PO.CODPOSTO = EQ.CODPOSTO "+
					"	LEFT JOIN KEQPALOC EQA ON EQ.CODEQUIPAMENTO = EQA.CODEQUIPAMENTO AND I.CODESTRUTURA = EQA.CODESTRUTURA "+
					"		AND EQ.CODCOLIGADA = EQA.CODCOLIGADA AND EQ.CODFILIAL = EQA.CODFILIAL AND I.CODORDEM = EQA.CODORDEM AND EQA.EFETIVADO = KM.STATUS AND EQA.IDATVORDEM = A.IDATVORDEM 	 "+
					"	"+recursoPessoa+" "+recursoEquip+" "+" AND KM.STATUS=0 "+dataDe+" "+dataAte+" "+ordenarPor+" "+
					" UNION ALL "+
					"SELECT "+
					"    PS.NOME,M.CODMO, "+
					"	EQ.CODEQUIPAMENTO, "+
					"	TR.CODCELULA, PJ.CODPRJ OS, I.CODORDEM OP, I.CODCOLIGADA, I.CODFILIAL, I.CODESTRUTURA, P.CODIGOPRD, P.IDPRD, P.DESCRICAO DSCITEM, DA.CODATIVIDADE, DA.DSCATIVIDADE,  	"+
					"	AE.PRIORIDADE, PE.CODPOSTO, PO.DSCPOSTO,S.CODSTATUS, S.DSCSTATUS, A.TEMPO CARGA_PREV, PC.NUMFLUIG,		"+
					"	PC.INDICE_FLUIG, PC.NIVEL_FLUIG,  		"+
					"	I.DTHRINICIALPREV INICIO_PLANEJADO, I.DTHRFINALPREV FINAL_PLANEJADO, NULL FOLGA, "+
					"	KM.DTHRINICIAL, KM.DTHRFINAL, "+
					"	ISNULL((SELECT 	"+
					"		ROUND(CAST(DATEDIFF(minute,KM.DTHRINICIAL,KM.DTHRFINAL) AS FLOAT)/60,4) HORAS),0)	 "+
					"		ALOCADO	"+
					"FROM "+
					"	KMAOOBRAALOC KM "+
					"	INNER JOIN KMAOOBRA M ON M.CODCOLIGADA = KM.CODCOLIGADA AND M.CODFILIAL = KM.CODFILIAL AND M.ATIVA='1' AND KM.CODMO = M.CODMO "+
					"	INNER JOIN PPESSOA PS ON M.CODPESSOA = PS.CODIGO  "+
					"	INNER JOIN KITEMORDEM I ON KM.CODCOLIGADA = I.CODCOLIGADA AND KM.CODFILIAL = I.CODFILIAL AND KM.CODORDEM = I.CODORDEM AND KM.CODESTRUTURA = I.CODESTRUTURA "+
					"	INNER JOIN KESTRUTURA E ON I.CODCOLIGADA = E.CODCOLIGADA AND I.CODFILIAL = E.CODFILIAL AND I.CODESTRUTURA = E.CODESTRUTURA  	"+
					"	INNER JOIN TPRD P ON E.CODCOLIGADA = P.CODCOLIGADA AND E.IDPRODUTO = P.IDPRD 		"+
					"	INNER JOIN TPRDCOMPL PC ON I.CODCOLIGADA = PC.CODCOLIGADA AND P.IDPRD = PC.IDPRD  	"+
					"	INNER JOIN KATVORDEM A ON  A.CODCOLIGADA = E.CODCOLIGADA AND A.CODFILIAL = E.CODFILIAL AND A.CODESTRUTURA = E.CODESTRUTURA AND A.CODORDEM = I.CODORDEM AND KM.IDATVORDEM = A.IDATVORDEM "+
					"	INNER JOIN KATVESTRUTURA AE ON AE.CODCOLIGADA = A.CODCOLIGADA AND AE.CODFILIAL = A.CODFILIAL AND AE.CODESTRUTURA = A.CODESTRUTURA AND AE.CODATIVIDADE = A.CODATIVIDADE AND AE.CODATIVIDADE NOT LIKE 'RMP%' "+	 
					"	INNER JOIN KATIVIDADE DA ON DA.CODCOLIGADA = A.CODCOLIGADA AND DA.CODATIVIDADE = A.CODATIVIDADE  "+
					"	INNER JOIN KATVESTPOSTO PE ON PE.CODCOLIGADA = AE.CODCOLIGADA AND PE.CODFILIAL = AE.CODFILIAL AND PE.CODESTRUTURA = AE.CODESTRUTURA AND PE.CODATIVIDADE = AE.CODATIVIDADE  	"+
					"	INNER JOIN KPOSTO PO ON PO.CODCOLIGADA = PE.CODCOLIGADA AND PO.CODFILIAL = PE.CODFILIAL AND PO.CODPOSTO = PE.CODPOSTO 		"+
					"	INNER JOIN KSTATUS S ON S.CODCOLIGADA = I.CODCOLIGADA AND S.CODSTATUS = I.STATUS  		"+
					"	INNER JOIN MPRJ PJ ON PJ.CODCOLIGADA = I.CODCOLIGADA AND PJ.CODPRJ = I.CODCCUSTO AND PJ.POSICAO IN (1,4) 	"+
					"	INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL TR ON I.CODCOLIGADA = TR.CODCOLIGADA AND TR.CODFILIAL = PJ.CODFILIAL AND TR.CODORDEM = KM.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI "+
					"	LEFT JOIN KATVHABILIDADES AH ON A.CODATIVIDADE = AH.CODATIVIDADE AND A.CODCOLIGADA = AH.CODCOLIGADA AND A.CODFILIAL = AH.CODFILIAL "+
					"	LEFT JOIN VHABILIDADES H ON AH.CODHABILIDADE = H.CODHABILIDADE "+
					"	LEFT JOIN VHABILIDADESPESSOAIS HP ON H.CODHABILIDADE = HP.CODHABILIDADE AND M.CODPESSOA = HP.CODPESSOA  "+
					"	LEFT JOIN KEQUIPAMENTO EQ ON I.CODCOLIGADA = EQ.CODCOLIGADA AND I.CODFILIAL = EQ.CODFILIAL AND PO.CODPOSTO = EQ.CODPOSTO "+
					"	LEFT JOIN KEQPALOC EQA ON EQ.CODEQUIPAMENTO = EQA.CODEQUIPAMENTO AND I.CODESTRUTURA = EQA.CODESTRUTURA "+
					"		AND EQ.CODCOLIGADA = EQA.CODCOLIGADA AND EQ.CODFILIAL = EQA.CODFILIAL AND I.CODORDEM = EQA.CODORDEM AND EQA.EFETIVADO = KM.EFETIVADO AND EQA.IDATVORDEM = A.IDATVORDEM "+
					"	"+recursoPessoa+" "+recursoEquip+" "+" AND KM.EFETIVADO=0 "+dataDe+" "+dataAte+" "+
					") Z "+
					"ORDER BY "+
					"	Z.OS "
					    

				
    log.info("QUERY dsResumoAlocacaoRecursoOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsResumoAlocacaoRecursoOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsResumoAlocacaoRecursoOS.addRow(Arr);
            
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
    
    return dsResumoAlocacaoRecursoOS;
	
}