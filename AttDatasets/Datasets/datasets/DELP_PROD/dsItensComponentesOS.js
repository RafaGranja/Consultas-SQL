//  BUSCA TODOS OS COMPONENTES CADASTRADOS DE UM DETERMINADO ITEM DA TABELA DE COMPONENTES
function createDataset(fields, constraints, sortFields) {

	var dsItensComponentesOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
   
    var numOS = ""
    var idCriacao = ""
	var execucao = ""
		
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OSCOMPONENTES"){
        		
        		numOS = constraints[i].initialValue;
        	
        	}

        	if(constraints[i].fieldName == "IDCRIACAOCOMPONENTES"){
        		
        		idCriacao = constraints[i].initialValue;
        	
        	}
        	if(constraints[i].fieldName == "EXECUCAO"){
        		
        		execucao = constraints[i].initialValue;
        	
        	}

        }
        
    }  

    log.info("VOU MONTAR A QUERY")
    
    var myQuery = ""
    	
    if(execucao!="" && execucao!=undefined && execucao!=null){
    	
    	  var myQuery = "SELECT C.*,T.CODUNDCONTROLE FROM FLUIG.DBO.Z_CRM_EX001019 C " +
  		" INNER JOIN FLUIG.DBO.Z_CRM_EX001005 M ON M.OS = C.OSCOMPONENTES AND M.IDCRIACAO = C.IDCRIACAOCOMPONENTES AND M.EXECUCAO=C.EXECUCAO  "+
		" INNER JOIN TPRD T ON T.CODCOLIGADA=M.CODCOLIGADA AND T.IDPRD=C.IDPRDCOMPONENTES " +
  		" WHERE C.OSCOMPONENTES='"+numOS+"' AND  C.IDCRIACAOCOMPONENTES='"+idCriacao+"' AND C.EXECUCAO='"+execucao+"' ORDER BY C.PRIORIDADEATVCOMPONENTES,C.SUBSTITUTOCOMPONENTES ASC "
  
    }
    else{
    	
    	myQuery = "SELECT C.*,T.CODUNDCONTROLE FROM FLUIG.DBO.Z_CRM_ML001019 C " +
  		" INNER JOIN FLUIG.DBO.Z_CRM_ML001005 M ON M.OS = C.OSCOMPONENTES AND M.IDCRIACAO = C.IDCRIACAOCOMPONENTES  "+
		" INNER JOIN TPRD T ON T.CODCOLIGADA=M.CODCOLIGADA AND T.IDPRD=C.IDPRDCOMPONENTES " +
		" WHERE C.OSCOMPONENTES='"+numOS+"' AND  C.IDCRIACAOCOMPONENTES='"+idCriacao+"' ORDER BY C.PRIORIDADEATVCOMPONENTES,C.SUBSTITUTOCOMPONENTES ASC "
        
    }

    log.info("QUERY dsItensComponentesOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsItensComponentesOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsItensComponentesOS.addRow(Arr);
            
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
    
    return dsItensComponentesOS;

}