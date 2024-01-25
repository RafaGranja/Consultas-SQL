// VERIFICA SE TODOS OS ITENS DA ESTRUTURA QUE NÃO SÃO MANUFATURADOS TEM ATIVIDADES CADASTRADAS NA TABELA DE PROCESSO
function createDataset(fields, constraints, sortFields) {

	var dsVerificaProcessoItemOS = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = ""
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OSPROCESSO"){
        		
        		numOS = constraints[i].initialValue;
        		
        	} 
        	
        }
        
    }  
        
    var myQuery = "SELECT E.OS,E.IDCRIACAO,E.INDICE,P.IDCRIACAOPROCESSO "+
				  "	FROM "+
				  "		Z_CRM_ML001005 E "+
				  "		LEFT JOIN Z_CRM_ML001021 P ON E.OS = P.OSPROCESSO AND E.IDCRIACAO = P.IDCRIACAOPROCESSO "+
				  "	WHERE "+
				  "		E.OS='"+numOS+"' "+
				  "		AND E.TIPODESENHO<>'NAOMANUFATURADO' AND E.TIPODESENHO<>'INDUSTRIALIZACAO' AND E.ITEMEXCLUSIVO!=2 "
            
    log.info("QUERY dsVerificaProcessoItemOS: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsVerificaProcessoItemOS.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsVerificaProcessoItemOS.addRow(Arr);
            
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
    
    return dsVerificaProcessoItemOS;

	
}