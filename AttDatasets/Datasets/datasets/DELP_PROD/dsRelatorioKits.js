// BUSCA INFORMAÇÕES PARA MONTAR RELATORIO DO SEPARADOR DE KITS
function createDataset(fields, constraints, sortFields) {

	var dsRelatorioKits = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var filial = ""
	var os = ""
	var codtrf = ""
	var exec=""
	var op=""
    	
	if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "FILIAL") {
            	
        		filial = " AND A.CODFILIAL = "+constraints[i].initialValue+" "
            	
            }
        	if (constraints[i].fieldName == "OS") {
            	
        		os = " AND A.OS = '"+constraints[i].initialValue+"' "
            	
            }
        	if (constraints[i].fieldName == "CODTRF") {
            	
        		codtrf = " AND A.CODTRFPAI = '"+constraints[i].initialValue+"' "
            	
            }
        	if (constraints[i].fieldName == "EXEC") {
            	
        		exec = " AND A.EXECUCAO = "+constraints[i].initialValue+" "
            	
            }
        	if (constraints[i].fieldName == "OP") {
            	
        		op = " AND K.CODORDEM = '"+constraints[i].initialValue+"' "
            	
            }
        	
        }
        
	}
        
    var myQuery = 	" SELECT * FROM ( "+
		    		" SELECT DISTINCT ATV.ID,A.OS,A.INDICE INDICEK ,B.INDICE,A.IDCRIACAO,A.DESCRICAO NOMEFANTASIA,A.NUMDESENHO,B.POSICAODESENHO,B.TOTALQTDE,B.PESOUNITARIO,B.BITOLA,B.LARGURA,B.COMPRIMENTO,ATV.IDCRIACAOPROCESSO,ATV.DESCATIVIDADE ATIVIDADE, "+
		    		" CC.FIM,ISNULL(C.DESCCELULA,'   ') DESCRICAO,ATV.PRIORIDADE "+
		    		" FROM Z_CRM_EX001005 A "+
		    		" INNER JOIN Z_CRM_EX001005 B "+
		    		" ON A.OS=B.OS "+
		    		" AND A.INDICE=B.NIVEL "+
		    		" AND B.TIPODESENHO!='NAOMANUFATURADO' "+
		    		" AND A.ITEMEXCLUSIVO=B.ITEMEXCLUSIVO "+
		    		" AND A.EXECUCAO=B.EXECUCAO "+
		    		" INNER JOIN Z_CRM_EX001021 ATV  "+
		    		" ON ATV.OSPROCESSO=B.OS "+
		    		" AND ATV.IDCRIACAOPROCESSO=B.IDCRIACAO "+
		    		" AND ATV.EXECUCAO=B.EXECUCAO "+
		    		" AND B.POSICAODESENHO != '0' "+
		    		" LEFT JOIN CORPORE_DEV.DBO.KORDEM K "+
		    		" ON K.CODCOLIGADA=A.CODCOLIGADA "+
		    		" AND K.CODFILIAL=A.CODFILIAL "+
		    		" AND SUBSTRING(K.RESPONSAVEL,0,15)=A.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI "+
		    		" LEFT JOIN CORPORE_DEV.DBO.KORDEMCOMPL KL "+
		    		" ON KL.CODCOLIGADA=K.CODCOLIGADA "+
		    		" AND KL.CODFILIAL=K.CODFILIAL "+
		    		" AND KL.CODORDEM=K.CODORDEM "+
		    		" AND KL.NUMEXEC=A.EXECUCAO "+
		    		" LEFT JOIN CORPORE_DEV.DBO.MPRJ PRJ  "+
		    		" ON PRJ.CODCOLIGADA = K.CODCOLIGADA  "+
		    		" AND PRJ.CODPRJ =K.CODCCUSTO  "+
		    		" AND PRJ.POSICAO IN (1,4)  "+
		    		" LEFT JOIN Z_CRM_EX001005COMPL TRF  "+
		    		" ON TRF.CODCOLIGADA = K.CODCOLIGADA  "+
		    		" AND TRF.OS=PRJ.CODCCUSTO COLLATE SQL_Latin1_General_CP1_CI_AI "+
		    		" AND TRF.CODORDEM = KL.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI  "+
		    		" INNER JOIN ( SELECT OSPROCESSO,IDCRIACAOPROCESSO,EXECUCAO,COUNT(*) FIM FROM Z_CRM_EX001021 "+
                    " GROUP BY  OSPROCESSO,IDCRIACAOPROCESSO,EXECUCAO ) CC "+
                    " ON CC.OSPROCESSO=B.OS  "+
                    " AND CC.IDCRIACAOPROCESSO=B.IDCRIACAO  "+
                    " AND CC.EXECUCAO=B.EXECUCAO "+
		    		" WHERE  "+
		    		" A.ITEMEXCLUSIVO<>2 "+
		    		filial+os+codtrf+exec+op+
		    		" ) Z "+
		    		" ORDER BY CAST('/' + REPLACE(Z.INDICEK, '.', '/') + '/' AS HIERARCHYID),Z.POSICAODESENHO,Z.PRIORIDADE "
    
    log.info("QUERY dsRelatorioKits: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsRelatorioKits.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsRelatorioKits.addRow(Arr);
            
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
    
    return dsRelatorioKits;
	
}