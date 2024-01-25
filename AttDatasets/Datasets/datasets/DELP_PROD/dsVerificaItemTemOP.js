// VERIFICA SE UM DETERMINADO ITEM DA ESTRUTURA TEM OP
function createDataset(fields, constraints, sortFields) {

	var dsVerificaItemTemOP = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var idCriacao = ""
    var exec = ""
	
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        	
        	}
        	
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = constraints[i].initialValue;
        	
        	}
        	if(constraints[i].fieldName == "EXECUCAO"){
        		
        		exec = constraints[i].initialValue;
        	
        	}

        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    if(exec==""){
    	var myQuery =   "SELECT "+
					"	M5.NIVEL, M5.INDICE, M5.IDPRD, M5.IDCRIACAO "+
					"FROM "+
					"	KITEMORDEM K "+
					"	INNER JOIN FLUIG.dbo.Z_CRM_ML001005 (NOLOCK) M5 ON  K.CODESTRUTURA = '03.023.0'+M5.IDPRD COLLATE Latin1_General_CI_AS OR K.CODESTRUTURA = '04.024.0'+M5.IDPRD COLLATE Latin1_General_CI_AS "+
					" WHERE "+
					"	M5.IDCRIACAO='"+idCriacao+"' AND M5.ITEMEXCLUSIVO!=2 AND M5.OS='"+numOS+"' "
    
    }
    else{
    	 var myQuery =   "SELECT "+
			"	M5.NIVEL, M5.INDICE, M5.IDPRD, M5.IDCRIACAO "+
			"FROM "+
			"	KITEMORDEM K "+
			"	INNER JOIN FLUIG.dbo.Z_CRM_EX001005 (NOLOCK) M5 ON  K.CODESTRUTURA = '03.023.0'+M5.IDPRD COLLATE Latin1_General_CI_AS OR K.CODESTRUTURA = '04.024.0'+M5.IDPRD COLLATE Latin1_General_CI_AS "+
			"	INNER JOIN KORDEMCOMPL KL ON K.CODCOLIGADA=KL.CODCOLIGADA AND K.CODFILIAL=KL.CODFILIAL AND K.CODORDEM=KL.CODORDEM AND KL.NUMEXEC=M5.EXECUCAO "+
			" WHERE "+
			"	M5.IDCRIACAO='"+idCriacao+"' AND M5.ITEMEXCLUSIVO!=2 AND M5.OS='"+numOS+"' AND M5.EXECUCAO="+exec

		  
    }
    
    
				  
    log.info("QUERY dsVerificaItemTemOP: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsVerificaItemTemOP.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsVerificaItemTemOP.addRow(Arr);
            
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
    
    return dsVerificaItemTemOP;
	
}