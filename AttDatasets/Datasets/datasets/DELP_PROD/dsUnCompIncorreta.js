// VERIFICA SE EXISTEM ITENS DA LISTA DE MATERIAIS QUE TEM UNCOMP INFORMADA INCORRETA
function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = ""
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue; ;
        		
        	} 
        	
        }
        
    }  
    
    var myQuery =   "SELECT	"+
					"	M.INDICE, M.IDCRIACAO, M.QTDEUNCOMP, L.IDCRIACAOSALVOS, T.CODUNDCONTROLE,M.DESCRICAO "+
					"FROM "+
					"	FLUIG.DBO.Z_CRM_ML001005 M "+
					"	INNER JOIN FLUIG.DBO.Z_CRM_LISTAMATERIAIS L ON M.OS = L.NUMOSSALVOS AND M.IDCRIACAO = L.IDCRIACAOSALVOS "+
					"	INNER JOIN TPRD	T ON T.CODCOLIGADA=M.CODCOLIGADA AND T.IDPRD=L.IDPRD1SALVOS "+
					"WHERE "+
					"	OS='"+numOS+"' AND M.ITEMEXCLUSIVO!=2 AND M.COMPORLISTA='SIM' AND (QTDEUNCOMP <> (CASE WHEN T.CODUNDCONTROLE='KG' THEN 'PESOUNITARIO' ELSE 'DESENHO' END)) AND QTDEUNCOMP IS NOT NULL " 

    log.info("QUERY dsUnCompIncorreta: " + myQuery);
    
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