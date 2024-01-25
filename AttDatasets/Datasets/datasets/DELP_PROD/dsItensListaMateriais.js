function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var numOS = "";
    
    if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if(constraints[i].fieldName == "OS"){
        		
        		numOS = constraints[i].initialValue;
        		
        	} 
        	
        }
        
    }  
    
    var myQuery = " SELECT L.ID, L.companyid, L.cardid, L.documentid, L.version, L.tableid, L.anonymization_date, L.anonymization_user_id, LINHASALVOS, EXCLUIRITEM,'' EXCLUIRSALVOS,  "+
                  "  P.NUMDESENHO NUMDESENHOSALVOS, P.IDCRIACAO IDCRIACAOSALVOS,P.OS NUMOSSALVOS,P.POSICAODESENHO POSICAOSALVOS,P.TOTALQTDE QUANTIDADESALVOS,P.DESCRICAO DESCRICAOSALVOS,  "+
                  "  P.BITOLA BITOLASALVOS,P.LARGURA LARGURASALVOS,P.COMPRIMENTO COMPRIMENTOSALVOS,P.ESPROSCA ESPROSCASALVOS,P.MATERIAL MATERIALSALVOS,P.PESOBRUTO PESOBRUTOSALVOS,   "+
                  "  PRODUTORM1SALVOS, IDPRD1SALVOS, CODIGOPRD1SALVOS, IDML1SALVOS, INDICE1SALVOS, UNDPRD1SALVOS, PRODUTORM2SALVOS, IDPRD2SALVOS, CODIGOPRD2SALVOS, IDML2SALVOS,   "+
                  "  INDICE2SALVOS, UNDPRD2SALVOS, PRODUTORM3SALVOS, IDPRD3SALVOS, CODIGOPRD3SALVOS, IDML3SALVOS, INDICE3SALVOS, UNDPRD3SALVOS, PRODUTORM4SALVOS, IDPRD4SALVOS,   "+
                  "  CODIGOPRD4SALVOS, IDML4SALVOS, INDICE4SALVOS, UNDPRD4SALVOS, PRODUTORM5SALVOS, IDPRD5SALVOS, CODIGOPRD5SALVOS, IDML5SALVOS, INDICE5SALVOS, UNDPRD5SALVOS,   "+
                  "  PRODUTORM6SALVOS, IDPRD6SALVOS, CODIGOPRD6SALVOS, IDML6SALVOS, INDICE6SALVOS, UNDPRD6SALVOS, L.masterid, PEDIDOCOMPRASALVOS, LOTESALVOS,P.DIAMETROEXTERNODISCO DIAMEXTSALVOS,  "+
                  "  P.DIAMETROINTERNODISCO DIAMINTSALVOS, OBSGERAISSALVOS, ORIGEMMP1SALVOS, ORIGEMMP2SALVOS, QTDORIGEM1SALVOS, QTDORIGEM2SALVOS, "+
                  "  isnull(REVISAOSALVOS,0) REVISAOSALVOS,isnull(P.PESOUNLIQUIDO,0) PESOUNITARIO, isnull(P.PESOLIQUIDO,0)PESOLIQUIDO, isnull(P.MASSALINEAR,0)  MASSALINEAR, "+
                  "  CASE WHEN P.NIVEL = '' OR P.NIVEL= NULL THEN P.EXECUCOES  "+
                  "                      ELSE R.EXECUCOES END AS EXECUCOES "+
                  "  FROM Z_CRM_LISTAMATERIAIS L  "+
                  "  RIGHT JOIN Z_CRM_ML001005 P ON  "+
                  "  L.NUMOSSALVOS=P.OS  "+
                  "  AND L.IDCRIACAOSALVOS=P.IDCRIACAO  "+
                  "  LEFT JOIN (SELECT  "+
                  "                                  M.EXECUCOES,ML.IDCRIACAO "+
                  "                              FROM  "+
                  "                                  Z_CRM_ML001005 M  "+
                  "                                  RIGHT JOIN Z_CRM_ML001005 ML ON M.OS = ML.OS  "+
                  "                              WHERE  "+
                  "                                  (ML.INDICE LIKE M.INDICE+'.%' OR ML.INDICE LIKE M.NIVEL)  "+
                  "                                  AND (M.NIVEL IS NULL OR M.NIVEL LIKE '')  "+
                  "                                  AND M.OS = '"+numOS+"' AND M.ITEMEXCLUSIVO!=2 AND ML.ITEMEXCLUSIVO!=2 ) R ON  "+
                  "                                  R.IDCRIACAO=P.IDCRIACAO "+
                  "  WHERE P.OS='"+numOS+"' AND P.COMPORLISTA='SIM' AND P.ITEMEXCLUSIVO!=2 "+
                  "  ORDER BY P.OS,P.NUMDESENHO,P.POSICAODESENHO "
    
  
    log.info("QUERY dsItensListaMateriais: " + myQuery);
    
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