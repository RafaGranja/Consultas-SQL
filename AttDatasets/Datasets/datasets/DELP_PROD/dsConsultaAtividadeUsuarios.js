function createDataset(fields, constraints, sortFields) {

	var dsRelatorioUsuarios = DatasetBuilder.newDataset();
    var dataSource = "/jdbc/FluigDS";
    var ic = new javax.naming.InitialContext();
    var ds = ic.lookup(dataSource);
    var created = false;	 
    
var myQuery = 	" SELECT LOWER(R2.LOGIN) AS USUARIO ,U.NAME COLLATE SQL_Latin1_General_CP1_CI_AI AS NOME," +
				"	QTDACESSOS,R2.DIAS,CONCAT(ROUND(CONVERT(FLOAT,QTDACESSOS)/CONVERT(FLOAT,DIAS),4)*100,'%') AS PORCENTAGEM,G.STATUS STATUSRM, " +
				"	CAST (U.CREATIONDATE AS DATE) DATA_CRIACAO," +
				"   (SELECT CAST(MAX(ACCESS_DATE) AS DATE) FROM FDN_ACCESSLOG WHERE LOGIN=R2.LOGIN) ULTIMO_LOGIN FROM( " +
				"		SELECT LOGIN,COUNT(LOGIN) QTDACESSOS,(DATEDIFF(DAY,(SELECT CREATIONDATE FROM SOCIAL WHERE LOWER(ALIAS)=LOWER(R.LOGIN)),GETDATE()+1)) DIAS " +
				"			FROM( " +
				"				SELECT DISTINCT LEFT(ACCESS_DATE, 10) AS DATA,LOGIN " +
				"				FROM FDN_ACCESSLOG) R " +
				"				INNER JOIN SOCIAL U1 ON " +
				"				U1.ALIAS=LOWER(R.LOGIN) " +
				"			GROUP BY LOGIN) R2 " +
				"			LEFT JOIN SOCIAL U ON " +
				"			LOWER(U.ALIAS)=LOWER(R2.LOGIN) " +
				"			LEFT JOIN CORPORE.DBO.GUSUARIO G ON " +
				"			G.CODUSUARIO=R2.LOGIN COLLATE SQL_Latin1_General_CP1_CI_AI " +
				"			INNER JOIN FDN_GROUPUSERROLE GF ON " +
				"			R2.LOGIN=GF.LOGIN " +
				"			INNER JOIN FDN_USERROLE ROL ON " +
				"			ROL.LOGIN=R2.LOGIN " +
				"	UNION  " +
				"	SELECT LOWER(U.ALIAS) AS USUARIO ,G.NOME AS NOME,0 ACESSOS,R2.DIAS, " +
				"	CONCAT(ROUND(CONVERT(FLOAT,0)/CONVERT(FLOAT,DIAS),4)*100,'%') AS PORCENTAGEM, " +
				"	G.STATUS STATUSRM,CAST (U.CREATIONDATE AS DATE) DATA_CRIACAO, " +
				"	(SELECT CAST(MAX(ACCESS_DATE) AS DATE) FROM FDN_ACCESSLOG WHERE LOGIN=U.ALIAS) ULTIMO_LOGIN FROM( " +
				"		SELECT ALIAS,(DATEDIFF(DAY,(CREATIONDATE),GETDATE()+1)) DIAS " +
				"			FROM SOCIAL U " +
				"			LEFT JOIN FDN_ACCESSLOG R ON " +
				"			LOWER(R.LOGIN)=LOWER(U.ALIAS) " +
				"			WHERE R.LOGIN IS NULL " +
				"	) R2 " +
				"			LEFT JOIN SOCIAL U ON " +
				"			LOWER(U.ALIAS)=LOWER(R2.ALIAS) " +
				"			LEFT JOIN CORPORE.DBO.GUSUARIO G ON " +
				"			G.CODUSUARIO=R2.ALIAS COLLATE SQL_Latin1_General_CP1_CI_AI " +
				"			INNER JOIN FDN_GROUPUSERROLE GF ON " +
				"			R2.ALIAS=GF.LOGIN " +
				"			INNER JOIN FDN_USERROLE ROL ON " +
				"			ROL.LOGIN=R2.ALIAS " +
				"	ORDER BY QTDACESSOS DESC ,DIAS ASC " 

   
   
    log.info("QUERY dsRelatorioUsuarios: " + myQuery);
    
    try {
    	
        var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(myQuery);
        var columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsRelatorioUsuarios.addColumn(rs.getMetaData().getColumnName(i));
                	
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
            
            dsRelatorioUsuarios.addRow(Arr);
            
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
    
    return dsRelatorioUsuarios;
}