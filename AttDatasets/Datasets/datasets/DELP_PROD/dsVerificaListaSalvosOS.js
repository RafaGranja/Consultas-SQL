// VERIFICA SE UM DETERMINADO ITEM DA ESTRUTURA ESTÁ SALVO NA LISTA DE MATERIAIS
function createDataset(fields, constraints, sortFields) {

	var dsVerificaListaSalvosOS = DatasetBuilder.newDataset()
    var dataSource = "/jdbc/FluigDS"
    var ic = new javax.naming.InitialContext()
    var ds = ic.lookup(dataSource)
    var created = false
   
    var numOS = ""
	var idCriacao = ""
		
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue
        	
        	}
        	if(constraints[i].fieldName == "IDCRIACAO"){
        		
        		idCriacao = " AND M.IDCRIACAO='"+constraints[i].initialValue+"' "
        	
        	}
        	
        }
        
    }  

    log.info("VOU MONTAR A QUERY")

    
					
	// ALTERAÇÃO
	var myQuery = 	"SELECT "+
					"	M.IDCRIACAO,M.TIPODESENHO,M.COMPORLISTA,P.IDCRIACAOPROCESSO,MM.IDCRIACAOSALVOS  "+
					"FROM "+
					"	Z_CRM_ML001005 M "+
					"	LEFT JOIN Z_CRM_ML001021 P ON M.OS=P.OSPROCESSO AND M.IDCRIACAO=P.IDCRIACAOPROCESSO "+
					"	LEFT JOIN ML001015 MM ON M.OS=MM.NUMOSSALVOS AND M.IDCRIACAO=MM.IDCRIACAOSALVOS AND MM.masterid = (SELECT MAX(masterid) FROM ML001015 WHERE NUMOSSALVOS='"+numOS+"') "+
					"WHERE "+
					"	M.OS='"+numOS+"' AND M.ITEMEXCLUSIVO!=2  "+
					"	AND (M.COMPORLISTA='SIM' OR M.TIPODESENHO='INDUSTRIALIZACAO') " +idCriacao+ " "
				  
    log.info("QUERY dsVerificaListaSalvosOS: " + myQuery)
    
    try {
    	
        var conn = ds.getConnection()
        var stmt = conn.createStatement()
        var rs = stmt.executeQuery(myQuery)
        var columnCount = rs.getMetaData().getColumnCount()
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsVerificaListaSalvosOS.addColumn(rs.getMetaData().getColumnName(i))
                	
                }
                
                created = true
                
            }
            
            var Arr = new Array()
            
            for (var i = 1; i <= columnCount; i++) {
            	
                var obj = rs.getObject(rs.getMetaData().getColumnName(i))
                
                if (null != obj) {
                	
                    Arr[i - 1] = rs.getObject(rs.getMetaData().getColumnName(i)).toString()
                    
                } else {
                	
                    Arr[i - 1] = "null"
                    
                }
                
            }
            
            dsVerificaListaSalvosOS.addRow(Arr)
            
        }
        
    } catch (e) {
        
    	log.error("ERRO==============> " + e.message)
    
    } finally {
    	
        if (stmt != null) {
        	
            stmt.close()
            
        }
        
        if (conn != null) {
        	
            conn.close()
            
        }
        
    }
    
    return dsVerificaListaSalvosOS
	
}