// VERIFICA SE TEM ATIVIDADES DAS OPS FILHAS COM APONTAMENTO PENDENTE 
function createDataset(fields, constraints, sortFields) {

	var dsNewDataset = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDSRM";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;
    
    var codOrdem = ""
	var qtdeEfet = ""
    	
	if (constraints != null) {
    	
        for (var i = 0; i < constraints.length; i++) {
        	
        	if (constraints[i].fieldName == "CODORDEM") {
            	
        		codOrdem = constraints[i].initialValue
            	
            }
        	if (constraints[i].fieldName == "QTDEFETIVADA") {
            	
        		qtdeEfet = " AND COALESCE(I.QTDEPLANEJADA,0) - COALESCE(I.QTDEEFETIVADA,0) <> 0 "
            	
            }
        	
        }
        
	}
    
    var myQuery = 	"SELECT "+
					"	O.CODCOLIGADA, O.CODFILIAL, O.CODCCUSTO, O.CODORDEM, OM.CODORDEM CODORDEM_MARCO, OM.NUMEXEC, I.CODESTRUTURA, I.QTDEPLANEJADA, COALESCE(I.QTDEEFETIVADA,0) QTDEFETIVADA, COALESCE(I.QTDEPLANEJADA,0) - COALESCE(I.QTDEEFETIVADA,0) SALDO , "+
					"	A. CODATIVIDADE, A.STATUS, A.IDATVORDEM, "+
					"	CASE WHEN F.INDICE LIKE OM.INDICE+'.%' THEN 'DENTRO' ELSE 'FORA' END TIPO, F.* "+
					"FROM "+
					"	KORDEM O "+
					"	INNER JOIN KORDEMCOMPL C ON C.CODCOLIGADA = O.CODCOLIGADA AND C.CODFILIAL = O.CODFILIAL AND C.CODORDEM = O.CODORDEM "+
					"	INNER JOIN KATVORDEM A ON A.CODCOLIGADA = O.CODCOLIGADA AND A.CODFILIAL = O.CODFILIAL AND A.CODORDEM = O.CODORDEM AND A.STATUS <> 6 "+
					"	INNER JOIN KITEMORDEM I ON I.CODCOLIGADA = O.CODCOLIGADA AND I.CODFILIAL = O.CODFILIAL AND I.CODORDEM = O.CODORDEM "+
					"	INNER JOIN FLUIG.DBO.Z_CRM_ML001005 F ON I.CODESTRUTURA = F.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI AND O.CODCCUSTO = F.OS COLLATE SQL_Latin1_General_CP1_CI_AI "+
					"	INNER JOIN (SELECT "+
					"					OD.CODCOLIGADA, OD.CODFILIAL, OD.CODCCUSTO, OD.CODORDEM, PL.NUMEXEC, NULL, FD.INDICE  COLLATE SQL_Latin1_General_CP1_CI_AI INDICE "+
					"				FROM "+
					"					KORDEM OD "+
					"					INNER JOIN KORDEMCOMPL PL ON PL.CODCOLIGADA = OD.CODCOLIGADA AND PL.CODFILIAL = OD.CODFILIAL AND PL.CODORDEM = OD.CODORDEM "+
					"					INNER JOIN KITEMORDEM ID ON ID.CODCOLIGADA = OD.CODCOLIGADA AND ID.CODFILIAL = OD.CODFILIAL AND ID.CODORDEM = OD.CODORDEM "+
					"					LEFT JOIN FLUIG.DBO.Z_CRM_ML001005 FD ON ID.CODESTRUTURA = FD.CODIGOPRD COLLATE SQL_Latin1_General_CP1_CI_AI AND OD.CODCCUSTO = FD.OS COLLATE SQL_Latin1_General_CP1_CI_AI "+
					"					INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL T ON OD.CODCOLIGADA = T.CODCOLIGADA AND OD.CODORDEM = T.CODORDEM "+
					"					INNER JOIN MPRJ P ON P.CODCOLIGADA = T.CODCOLIGADA AND P.IDPRJ = T.IDPRJ "+
					"				WHERE  "+
					"					P.POSICAO IN (1,4) AND FD.ITEMEXCLUSIVO!=2 AND od.codordem = '"+codOrdem+"') OM ON OM.CODCOLIGADA = O.CODCOLIGADA AND OM.CODCCUSTO = O.CODCCUSTO AND C.NUMEXEC = OM.NUMEXEC "+
					"WHERE "+
					"	O.STATUS NOT IN(5,6) AND coalesce(O.REPROCESSAMENTO,0)!= 1 "+
					"	AND CASE WHEN F.INDICE LIKE OM.INDICE+'.%' THEN 'DENTRO' ELSE 'FORA' END  = 'DENTRO' "+
					"	AND A.STATUS NOT IN (5,6) AND F.ITEMEXCLUSIVO!=2 "
    
    log.info("QUERY dsOPAtvsApontPendentes: " + myQuery);
    
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