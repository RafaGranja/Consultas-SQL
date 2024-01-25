// BUSCA TODAS AS ORDEM ABERTAS PARA O PROJETO
function createDataset(fields, constraints, sortFields) {

	var dsOrdemProducaoOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var codOrdem = ""
	var codCCusto = ""
	var celula = " WHERE  ( KO.REPROCESSAMENTO=1 OR ( Z.ITEMEXCLUSIVO!=2 AND Z.INDICE IS NOT NULL ) ) "
	var execucao=""
	var desenho=""
	var mprj = " INNER JOIN MPRJ PJ ON PJ.CODCOLIGADA = I.CODCOLIGADA AND PJ.CODPRJ = I.CODCCUSTO AND PJ.POSICAO IN (1,4) "+ 		
	"	INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL T ON PJ.CODCOLIGADA = T.CODCOLIGADA AND PJ.CODCCUSTO = T.OS COLLATE SQL_Latin1_General_CP1_CI_AI  AND I.CODORDEM = T.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI 	"	
	
	var mprj2 = " ,T.CODTRFPAI "
    	
	if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "CELULA") {
            	
        		celula = " WHERE  ( KO.REPROCESSAMENTO=1 OR ( Z.ITEMEXCLUSIVO!=2 AND Z.INDICE IS NOT NULL ) ) AND  T.CODCELULA = '"+constraints[i].initialValue+"'"
        		
        		mprj = " INNER JOIN MPRJ PJ ON PJ.CODCOLIGADA = I.CODCOLIGADA AND PJ.CODPRJ = I.CODCCUSTO AND PJ.POSICAO IN (1,4) "+ 		
				"	INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL T ON PJ.CODCOLIGADA = T.CODCOLIGADA AND PJ.CODCCUSTO = T.OS COLLATE SQL_Latin1_General_CP1_CI_AI  AND I.CODORDEM = T.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI 	"	
				
				mprj2 = " ,T.CODTRFPAI "
				            	
            }
        	if (constraints[i].fieldName == "CODCCUSTO") {
            	
        		codCCusto = " I.CODCCUSTO = '"+constraints[i].initialValue+"'"
            	
            }
        	if (constraints[i].fieldName == "CODORDEM") {
            	
        		codOrdem = " AND I.CODORDEM LIKE '%"+constraints[i].initialValue+"%'"
            	
            }
        	if (constraints[i].fieldName == "EXECUCAO") {
            	
        		execucao = " AND KL.NUMEXEC LIKE '"+constraints[i].initialValue+"' "
            	
            }
        	if (constraints[i].fieldName == "DESENHO") {
            	
        		desenho = " Z.NUMDESENHO='"+constraints[i].initialValue+"' "
            	
            }
        	if (constraints[i].fieldName == "TIRARMPRJ") {
            		
        		if(constraints[i].initialValue==1){
        			
            		mprj = "  "
                	mprj2 = " "
                		
        		}
            	
            }
        	
        	
        }
        
	}
    
    if(!(codCCusto=="")){
    	
    	if(celula==""){
        	
        	codCCusto = " WHERE ( KO.REPROCESSAMENTO=1 OR ( Z.ITEMEXCLUSIVO!=2 AND Z.INDICE IS NOT NULL ) ) AND "+codCCusto
        	
        } else {

        	codCCusto = " AND "+codCCusto
        	
        }

    }
    
    if(!(desenho=="")){
    	
    	if(celula=="" && codCCusto==""){
        	
    		desenho = "  WHERE ( KO.REPROCESSAMENTO=1 OR ( Z.ITEMEXCLUSIVO!=2 AND Z.INDICE IS NOT NULL ) ) AND "+desenho
        	
        } else {

        	desenho = " AND  "+desenho
        	
        }
    	
    }
        
    var myQuery = 	"SELECT TOP 100 "+
					"	I.CODORDEM, KO.DSCORDEM, I.CODESTRUTURA,RIGHT(KO.RESPONSAVEL,LEN(KO.RESPONSAVEL)-CHARINDEX('/',KO.RESPONSAVEL)) CODTRFPAI,KO.STATUS STATUS_OP,KL.NUMEXEC"+mprj2+
					" FROM KITEMORDEM I "+
					"	INNER JOIN KESTRUTURA E ON I.CODCOLIGADA = E.CODCOLIGADA AND I.CODFILIAL = E.CODFILIAL AND I.CODESTRUTURA = E.CODESTRUTURA "+
					//"	INNER JOIN TPRDCOMPL PR ON I.CODCOLIGADA = PR.CODCOLIGADA AND E.IDPRODUTO = PR.IDPRD "+
					mprj+
					"	INNER JOIN KORDEM KO ON I.CODCOLIGADA = KO.CODCOLIGADA AND I.CODFILIAL = KO.CODFILIAL AND I.CODORDEM = KO.CODORDEM "+
					"	INNER JOIN KORDEMCOMPL KL ON KL.CODCOLIGADA=I.CODCOLIGADA AND KL.CODFILIAL=I.CODFILIAL AND KL.CODORDEM=I.CODORDEM "+	
					"	LEFT JOIN FLUIG.DBO.Z_CRM_EX001005 Z ON Z.CODCOLIGADA=I.CODCOLIGADA AND Z.CODFILIAL=I.CODFILIAL AND I.CODCCUSTO=Z.OS COLLATE SQL_Latin1_General_CP1_CI_AI "+
					"	AND I.CODESTRUTURA=Z.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI AND Z.EXECUCAO=KL.NUMEXEC AND Z.ITEMEXCLUSIVO!=2"+	
					" "+celula+" "+codCCusto+" "+codOrdem+" "+execucao+" "+desenho
    
    log.info("QUERY dsOrdemProducaoOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsOrdemProducaoOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsOrdemProducaoOS.addRow(Arr);
            
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
    
    return dsOrdemProducaoOS;
	
}