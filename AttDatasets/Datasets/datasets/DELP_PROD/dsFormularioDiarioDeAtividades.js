// BUSCA TODOS OS DADOS DAS RA'S GERADAS
function createDataset(fields, constraints, sortFields) {

	var dsFormularioDiarioDeAtividades = DatasetBuilder.newDataset()
    var dataSource = "/jdbc/FluigDSRM"
    var ic = new javax.naming.InitialContext()
    var ds = ic.lookup(dataSource)
    var created = false
    
    log.info("dsFormularioDiarioDeAtividades")

    var filial = " = 7 "
    var coligada = ""
    var projeto = ""
    var celula = ""
    var turno = ""
    var maodeobra = ""
    var numplano = ""
    var dataprogramacaoinicial = ""
    var dataprogramacaofinal = ""
    var filialpapc = "  = 7  "
    var coligadapapc = ""
    var projetopapc = ""
    var maodeobrapapc = ""
    var numplanopapc = ""
    var dataprogramacaoinicialpapc = ""
    var dataprogramacaofinalpapc = ""
    var controller = ""
    var myQuery = ""

    	
	if (constraints != null) {
		
	    for (var i = 0; i < constraints.length; i++) {
	
			if (constraints[i].fieldName == "FILIAL") {
			        	
				filial = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "COLIGADA") {
	        	
				coligada = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "PROJETO") {
	        	
	        	projeto = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "CELULA") {
	        	
	        	celula = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "TURNO") {
	        	
	        	turno = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "MAODEOBRA") {
	        	
	        	maodeobra = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "NUMPLANO") {
	        	
	        	numplano = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "DATAPROGRAMACAOINICIAL") {
	        	
	        	dataprogramacaoinicial = constraints[i].initialValue
			        	
	        }
            if (constraints[i].fieldName == "DATAPROGRAMACAOFINAL") {
	        	
	        	dataprogramacaofinal = constraints[i].initialValue
			        	
	        }
            if (constraints[i].fieldName == "FILIALPAPC") {
			        	
				filialpapc = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "COLIGADAPAPC") {
	        	
				coligadapapc = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "PROJETOPAPC") {
	        	
	        	projetopapc = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "CELULAPAPC") {
	        	
	        	celulapapc = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "TURNOPAPC") {
	        	
	        	turnopapc = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "MAODEOBRAPAPC") {
	        	
	        	maodeobrapapc = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "NUMPLANOPAPC") {
	        	
	        	numplanopapc = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "DATAPROGRAMACAOINICIALPAPC") {
	        	
	        	dataprogramacaoinicialpapc = constraints[i].initialValue
			        	
	        }
            if (constraints[i].fieldName == "DATAPROGRAMACAOFINALPAPC") {
	        	
	        	dataprogramacaofinalpapc = constraints[i].initialValue
			        	
	        }
            if (constraints[i].fieldName == "CONTROLLER") {
	        	
	        	controller = constraints[i].initialValue
			        	
	        }
            if (constraints[i].fieldName == "CODORDEM") {
	        	
	        	codordem = constraints[i].initialValue
			        	
	        }
			if (constraints[i].fieldName == "IDATVORDEM") {
	        	
	        	idatvordem = constraints[i].initialValue
			        	
	        }
	    }
	    
	}
    if(maodeobra!="" && ( controller=="CONSULTAATIVIDADES" || controller=="CONSULTACOMPONENTES")) { 

        maodeobra = maodeobra.replace(";",",")
        if(maodeobra.indexOf(",")!=-1){
            
            maodeobra = maodeobra.split(",")
            
            var sent = ""
                
            for(var k=0; k<maodeobra.length;k++){
                
                if(sent==""){
                    
                    sent = "'"+maodeobra[k]+"'" 
                    
                } else {
                    
                    sent = sent +",'"+maodeobra[k]+"'"
                    
                }
                
            }
            
            log.info("sent: "+sent)
            
            maodeobra = " IN ("+sent+") "
            
        } else {

            maodeobra = " ='"+maodeobra+"' "
            
        }

    }

    if(celula!="" && ( controller=="CONSULTAATIVIDADES" || controller=="CONSULTACOMPONENTES")) { 

        celula = celula.replace(";",",")
        if(celula.indexOf(",")!=-1){
            
            celula = celula.split(",")
            
            var sent = ""
                
            for(var k=0; k<celula.length;k++){
                
                if(sent==""){
                    
                    sent = "'"+celula[k]+"'" 
                    
                } else {
                    
                    sent = sent +",'"+celula[k]+"'"
                    
                }
                
            }
            
            log.info("sent: "+sent)
            
            celula = " IN ("+sent+") "
            
        } else {
            
            celula = " ='"+celula+"' "
        }

    }
    if(projeto!="" && ( controller=="CONSULTAATIVIDADES" || controller=="CONSULTACOMPONENTES")) { 

        projeto = projeto.replace(";",",")
        if(projeto.indexOf(",")!=-1){
            
            projeto = projeto.split(",")
            
            var sent = ""
                
            for(var k=0; k<projeto.length;k++){
                
                if(sent==""){
                    
                    sent = "'"+projeto[k]+"'" 
                    
                } else {
                    
                    sent = sent +",'"+projeto[k]+"'"
                    
                }
                
            }
            
            log.info("sent: "+sent)
            
            projeto = " IN ("+sent+") "
            
        } else {
            
            projeto = " ='"+projeto+"' "
        }

    }
    if(projetopapc!="" && ( controller=="CONSULTAPAPC" || controller=="CONSULTAATIVIDADESPAPC")) { 

        projetopapc = projetopapc.replace(";",",")
        if(projetopapc.indexOf(",")!=-1){
            
            projetopapc = projetopapc.split(",")
            
            var sent = ""
                
            for(var k=0; k<projetopapc.length;k++){
                
                if(sent==""){
                    
                    sent = "'"+projetopapc[k]+"'" 
                    
                } else {
                    
                    sent = sent +",'"+projetopapc[k]+"'"
                    
                }
                
            }
            
            log.info("sent: "+sent)
            
            projetopapc = " IN ("+sent+") "
            
        } else {
            
            projetopapc = " ='"+projetopapc+"' "

        }

    }
    if(numplanopapc!="" && ( controller=="CONSULTAPAPC" || controller=="CONSULTAATIVIDADESPAPC" )) { 

        numplanopapc = numplanopapc.replace(";",",")
        if(numplanopapc.indexOf(",")!=-1){
            
            numplanopapc = numplanopapc.split(",")
            
            var sent = ""
                
            for(var k=0; k<numplanopapc.length;k++){
                
                if(sent==""){
                    
                    sent = "'"+numplanopapc[k]+"'" 
                    
                } else {
                    
                    sent = sent +",'"+numplanopapc[k]+"'"
                    
                }
                
            }
            
            log.info("sent: "+sent)
            
            numplanopapc = " IN ("+sent+") "
            
        } else {
            
            numplanopapc = " ='"+numplanopapc+"' "
        }

    }
    if(maodeobrapapc!="" && ( controller=="CONSULTAPAPC" || controller=="CONSULTAATIVIDADESPAPC")) { 

        maodeobrapapc = maodeobrapapc.replace(";",",")
        if(maodeobrapapc.indexOf(",")!=-1){
            
            maodeobrapapc = maodeobrapapc.split(",")
            
            var sent = ""
                
            for(var k=0; k<maodeobrapapc.length;k++){
                
                if(sent==""){
                    
                    sent = "'"+maodeobrapapc[k]+"'" 
                    
                } else {
                    
                    sent = sent +",'"+maodeobrapapc[k]+"'"
                    
                }
                
            }
            
            log.info("sent: "+sent)
            
            maodeobrapapc = " IN ("+sent+") "
            
        } else {
            
            maodeobrapapc = " ='"+maodeobrapapc+"' "
        }

    }

    log.info(controller+"CONTROLLER MY QUERY: " + controller)

	if(controller=="COLIGADA" || controller=="COLIGADAPAPC"){

        myQuery = "SELECT DISTINCT CONCAT(CODCOLIGADA,' -- ',NOMEFANTASIA) "+controller+" FROM GCOLIGADA (NOLOCK) "+
        " ORDER BY "+controller

    }
    if(controller=="FILIAL"){

        myQuery = "SELECT DISTINCT CONCAT(CODFILIAL,' -- ',NOMEFANTASIA) "+controller+" FROM GFILIAL (NOLOCK) WHERE NOMEFANTASIA NOT LIKE '%NÃO UTILIZAR%'  AND CODCOLIGADA=1 "
        " ORDER BY "+controller

    }
    if(controller=="FILIALPAPC"){

        myQuery = "SELECT DISTINCT CONCAT(CODFILIAL,' -- ',NOMEFANTASIA) "+controller+" FROM GFILIAL (NOLOCK) WHERE NOMEFANTASIA NOT LIKE '%NÃO UTILIZAR%' AND CODCOLIGADA=1 "+
        " ORDER BY "+controller

    }
    if(controller=="PROJETO"){

        myQuery = "SELECT DISTINCT CONCAT(CODCCUSTO,' -- ',NOME) "+controller+" FROM GCCUSTO (NOLOCK) WHERE  CODCONTAGER IS NOT NULL AND PERMITELANC='T' AND CODCOLIGADA=1  AND ATIVO = 'T' AND CODCOLCONTAGER=1 " 
        " ORDER BY "+controller

    }
    if(controller=="PROJETOPAPC"){

        myQuery = "SELECT DISTINCT CONCAT(CODCCUSTO,' -- ',NOME) "+controller+" FROM GCCUSTO (NOLOCK) WHERE  CODCONTAGER IS NOT NULL AND PERMITELANC='T' AND CODCOLIGADA=1  AND ATIVO = 'T'  AND CODCOLCONTAGER=1 " 
        " ORDER BY "+controller

    }
    if(controller=="CELULA"){

        myQuery = "SELECT DISTINCT CONCAT(CODINTERNO,' -- ',DESCRICAO) "+controller+" FROM GCONSIST (NOLOCK) WHERE CODTABELA='CELULA' AND APLICACAO='M'"+
        " ORDER BY "+controller

    }
    if(controller=="TURNO"){
        myQuery = "SELECT DISTINCT CONCAT(AH.CODIGO,' -- ',AH.DESCRICAO)  "+controller+
                    "  FROM  AHORARIO (NOLOCK) AH "+
                    " WHERE AH.INATIVO=0  AND DATEPART(YEAR,RECCREATEDON)=DATEPART(YEAR,GETDATE())  AND AH.CODCOLIGADA=1 "+
                    " ORDER BY "+controller
    }
    if(controller=="NUMPLANOPAPC"){

        myQuery = " SELECT DISTINCT NUMPLANOCORTE "+controller+" FROM ZMDPLANOAPROVEITAMENTOCORTE (NOLOCK) Z "+
                    " INNER JOIN KITEMORDEM (NOLOCK) K ON "+
                    " Z.CODCOLIGADA=K.CODCOLIGADA "+
                    " AND Z.CODFILIAL=K.CODFILIAL "+
                    " AND Z.CODESTRUTURA=K.CODESTRUTURA "+
                    " WHERE Z.NSEQPLANOCORTE IS NOT NULL  AND Z.CODCOLIGADA=1 "+(filialpapc!=""? " AND Z.CODFILIAL "+filialpapc : "" )+(projetopapc!=""? " AND K.CODCCUSTO "+projetopapc : "" )+
                    " ORDER BY "+controller

    }
    if(controller=="MAODEOBRA"){

        myQuery = "SELECT DISTINCT CONCAT(M.CODMO,' -- ',P.NOME) "+controller+" FROM ZMDKATVORDEMPROGRAMACAO (NOLOCK) A "+
        " INNER JOIN KMAOOBRA (NOLOCK) M ON A.CODCOLIGADA = M.CODCOLIGADA AND A.CODMO = M.CODMO AND A.CODFILIAL = M.CODFILIAL  "+
        " LEFT JOIN PFUNC (NOLOCK) F ON A.CODCOLIGADA = F.CODCOLIGADA AND F.CODPESSOA = M.CODPESSOA AND F.CODSITUACAO IN ('A','F')  "+
        " LEFT JOIN PPESSOA (NOLOCK) P ON P.CODIGO = M.CODPESSOA  "+
        " LEFT JOIN AHORARIO (NOLOCK) T ON F.CODCOLIGADA = T.CODCOLIGADA AND F.CODHORARIO = T.CODIGO "+
        " INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL (NOLOCK) TRF ON TRF.CODCOLIGADA = A.CODCOLIGADA AND TRF.CODFILIAL=A.CODFILIAL AND TRF.CODORDEM = A.CODORDEM "+
        " INNER JOIN MPRJ (NOLOCK) PRJ ON PRJ.CODCOLIGADA = TRF.CODCOLIGADA AND PRJ.CODPRJ = TRF.OS collate SQL_Latin1_General_CP1_CI_AI AND PRJ.POSICAO IN (1,4) "+
        " WHERE A.STATUS=0  AND A.CODCOLIGADA=1 "+(projeto!=""? " AND PRJ.CODPRJ "+projeto : "" )+(celula!=""? " AND TRF.CODCELULA "+celula : "" )+
        (turno!=""? "AND (T.CODIGO "+turno+" OR T.CODIGO IS NULL)" : "" )+" AND A.CODFILIAL "+filial+" AND M.ATIVA=1 AND A.NUMPLANOCORTE='' "+" AND A.DTHRINICIAL BETWEEN "+(dataprogramacaoinicial==""?"'1-1-1'":"'"+dataprogramacaoinicial+"'")+" AND "+(dataprogramacaofinal==""?" GETDATE() ":"'"+dataprogramacaofinal+"'")+" "+
        " ORDER BY "+controller

    }
    if(controller=="MAODEOBRAPAPC"){

        myQuery = "SELECT DISTINCT CONCAT(M.CODMO,' -- ',P.NOME) "+controller+" FROM ZMDKATVORDEMPROGRAMACAO (NOLOCK) A "+
        " INNER JOIN KMAOOBRA (NOLOCK) M ON A.CODCOLIGADA = M.CODCOLIGADA AND A.CODMO = M.CODMO AND A.CODFILIAL = M.CODFILIAL  "+
        " LEFT JOIN PFUNC (NOLOCK) F ON A.CODCOLIGADA = F.CODCOLIGADA AND F.CODPESSOA = M.CODPESSOA AND F.CODSITUACAO IN ('A','F')  "+
        " LEFT JOIN PPESSOA (NOLOCK) P ON P.CODIGO = M.CODPESSOA  "+
        " LEFT JOIN AHORARIO (NOLOCK) T ON F.CODCOLIGADA = T.CODCOLIGADA AND F.CODHORARIO = T.CODIGO "+
        " INNER JOIN KORDEM (NOLOCK) TRF ON TRF.CODCOLIGADA = A.CODCOLIGADA AND TRF.CODFILIAL=A.CODFILIAL AND TRF.CODORDEM = A.CODORDEM "+
        " INNER JOIN MPRJ (NOLOCK) PRJ ON PRJ.CODCOLIGADA = TRF.CODCOLIGADA AND PRJ.CODPRJ = TRF.CODCCUSTO collate SQL_Latin1_General_CP1_CI_AI AND PRJ.POSICAO IN (1,4) "+
        " WHERE A.STATUS=0  AND A.CODCOLIGADA=1 "+(projetopapc!=""? " AND PRJ.CODPRJ "+projetopapc : "" )+" AND A.CODFILIAL "+filialpapc+
        " AND M.ATIVA=1 "+(numplanopapc!=""? " AND A.NUMPLANOCORTE "+numplanopapc : "" )+" AND A.DTHRINICIAL BETWEEN "+(dataprogramacaoinicialpapc==""?"'1-1-1'":"'"+dataprogramacaoinicialpapc+"'")+" AND "+(dataprogramacaofinalpapc==""?" GETDATE() ":"'"+dataprogramacaofinalpapc+"'")+" "+
        " ORDER BY "+controller

    }
    if(controller=="CONSULTAATIVIDADES"){
        myQuery = ";WITH CTE AS ( "+
        "      "+
        "     SELECT PRJ.CODCOLIGADA,PRJ.CODFILIAL,TRF.CODORDEM CODTRFAUX,ISNULL(TAG,'SEM TAG') TAG,CAST(ISNULL(CAST(CODCELULA AS VARCHAR(20)),'SEM CELULA') AS VARCHAR(20)) CELULA,ISNULL(DESCCELULA,'SEM CELULA') DESCRICAO"+
        "     FROM MPRJ (NOLOCK)  PRJ "+
        "     INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL (NOLOCK) TRF ON TRF.CODCOLIGADA = PRJ.CODCOLIGADA AND TRF.CODFILIAL=PRJ.CODFILIAL AND PRJ.CODCCUSTO=TRF.OS COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
        "     WHERE PRJ.POSICAO IN(1,4) AND TRF.CODCOLIGADA=1 "+(projeto!="" ? " AND PRJ.CODPRJ "+projeto : "" )+
        "  "+
        " ), CTE_T AS ( "+
        "    SELECT CODCOLIGADA,CODFILIAL,CODORDEM,IDATVORDEM,LEFT(FORMAT(CAST(TEMPO/60 AS INT),'00')+':'+FORMAT(CAST(TEMPO%60 AS  FLOAT),'00'),6) APONTADO FROM ( "+
        "        SELECT K.CODCOLIGADA,K.CODFILIAL,K.CODORDEM,K.IDATVORDEM,SUM(CAST(DATEDIFF(SECOND,K.DTHRINICIAL,K.DTHRFINAL) AS bigint))/60.0 TEMPO "+
        "        FROM ( SELECT DISTINCT CODCOLIGADA,CODFILIAL,CODORDEM,IDATVORDEM FROM ZMDKATVORDEMPROGRAMACAO (NOLOCK) ) Z "+
        "        INNER JOIN KMAOOBRAALOC (NOLOCK) K  ON "+
        "        K.CODCOLIGADA=Z.CODCOLIGADA "+
        "        AND K.CODFILIAL=Z.CODFILIAL "+
        "        AND K.CODORDEM=Z.CODORDEM "+
        "        AND K.IDATVORDEM=Z.IDATVORDEM "+
        "        WHERE K.EFETIVADO=1 "+
        "        GROUP BY K.CODCOLIGADA,K.CODFILIAL,K.CODORDEM,K.IDATVORDEM ) R )"+
        "   "+
        " ,CTE_Z AS( "+
        "  "+
        "     SELECT K.CODCOLIGADA,K.CODFILIAL,KI.CODORDEM,KA.CODATIVIDADE,KA.FORNPARA,KAA.IDATVORDEM,KI.QTDEPLANEJADA,KA.OSPROCESSO,K.OBSPROCESSO,KA.DESCPROCESSO,KAA.CODPOSTO, "+
        "     CONCAT(KA.DOCAPOIOATV1 COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI,(CASE WHEN KA.DOCAPOIOATV2 = '' THEN '' ELSE '\n • ' END), "+
        "     KA.DOCAPOIOATV2 COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI,(CASE WHEN KA.DOCAPOIOATV3 = '' THEN '' ELSE '\n • ' END), "+
        "     KA.DOCAPOIOATV3 COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI,(CASE WHEN KA.DOCAPOIOATV4 = '' THEN '' ELSE '\n • ' END),KA.DOCAPOIOATV4 COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI) DOCAPOIOATV1,  "+
        "     'NORMAL' TIPO,'' NUMRNC,KA.DESCATIVIDADE,KA.PRIORIDADE,K.UNDMEDIDA,  "+
        "     LEFT(FORMAT(CAST(KAA.TEMPO/60 AS INT),'00')+':'+FORMAT(CAST(KAA.TEMPO%60 AS  FLOAT),'00'),6) TEMPOPREVISTO,  "+
        "     CONCAT(K.DESCRICAO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI ,' - ',K.POSICAODESENHO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI ,' - ',K.NUMDESENHO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI) DSCORDEM "+
        "     FROM FLUIG.DBO.Z_CRM_EX001005(NOLOCK)  K "+
        "     INNER JOIN FLUIG.DBO.Z_CRM_EX001021(NOLOCK)  KA ON K.OS=KA.OSPROCESSO AND K.EXECUCAO=KA.EXECUCAO AND K.IDCRIACAO=KA.IDCRIACAOPROCESSO "+
        "     INNER JOIN KITEMORDEM(NOLOCK)  KI ON KI.CODCOLIGADA=K.CODCOLIGADA AND KI.CODFILIAL=K.CODFILIAL AND KI.CODESTRUTURA=K.CODIGOPRD COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
        "     INNER JOIN KORDEMCOMPL(NOLOCK)  KL ON KL.CODCOLIGADA=KI.CODCOLIGADA AND KL.CODFILIAL=KI.CODFILIAL AND KL.CODORDEM=KI.CODORDEM AND KL.NUMEXEC=K.EXECUCAO "+
        "     INNER JOIN KATVORDEM(NOLOCK)  KAA ON KAA.CODCOLIGADA=K.CODCOLIGADA AND KAA.CODFILIAL=K.CODFILIAL AND KAA.CODORDEM=KI.CODORDEM  "+
        "      AND KAA.CODESTRUTURA=KI.CODESTRUTURA COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI AND KAA.CODATIVIDADE=KA.CODATIVIDADE COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
        "     WHERE K.ITEMEXCLUSIVO<>2 AND KI.STATUS NOT IN (1,6) AND KAA.STATUS NOT IN (1,6) "+(projeto!="" ? " AND KA.OSPROCESSO "+projeto : "" )+
        "     UNION "+
        "     SELECT K.CODCOLIGADA,K.CODFILIAL,KI.CODORDEM,VIEWCODATIVIDADE,KA.VIEWFORNPARA,KAA.IDATVORDEM,KI.QTDEPLANEJADA,K.NUM_OS,K.F_OBSPROCESSO,KA.VIEWDESCPROCESSO,KAA.CODPOSTO, "+
        "     CONCAT(KA.VIEWDOCAPOIOATV1 COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI,(CASE WHEN KA.VIEWDOCAPOIOATV2 = '' THEN '' ELSE '\n • ' END), "+
        "     KA.VIEWDOCAPOIOATV2 COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI,(CASE WHEN KA.VIEWDOCAPOIOATV3 = '' THEN '' ELSE '\n • ' END), "+
        "     KA.VIEWDOCAPOIOATV3 COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI,(CASE WHEN KA.VIEWDOCAPOIOATV4 = '' THEN '' ELSE '\n • ' END), "+
        "     KA.VIEWDOCAPOIOATV4 COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI) DOCAPOIOATV1, "+
        "     KA.DESCRNCRR TIPO,KA.NUMRNC,KA.VIEWDESCATIVIDADE,KA.VIEWPRIORIDADE,K.F_UNDMEDIDA, "+
        "     LEFT(FORMAT(CAST(KAA.TEMPO/60 AS INT),'00')+':'+FORMAT(CAST(KAA.TEMPO%60 AS  FLOAT),'00'),6) TEMPOPREVISTO,  "+
        "     CONCAT(K.F_DESCRICAO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI ,' - ',K.F_POSICAODESENHO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI ,' - ',K.F_NUMDESENHO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI) DSCORDEM "+
        "     FROM FLUIG.DBO.ML001026(NOLOCK)  K "+
        "     INNER JOIN FLUIG.DBO.ML001028(NOLOCK)  KA ON K.NUM_OS=KA.VIEWOSPROCESSO AND K.F_IDCRIACAO=KA.VIEWIDCRIACAOPROCESSO "+
        "     INNER JOIN KITEMORDEM(NOLOCK)  KI ON KI.CODCOLIGADA=K.CODCOLIGADA AND KI.CODFILIAL=K.CODFILIAL AND KI.CODORDEM=K.OPGERADA COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
        "     INNER JOIN KATVORDEM(NOLOCK)  KAA ON KAA.CODCOLIGADA=K.CODCOLIGADA AND KAA.CODFILIAL=K.CODFILIAL AND KAA.CODORDEM=KI.CODORDEM   "+
        "     AND KAA.CODESTRUTURA=KI.CODESTRUTURA COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI AND KAA.CODATIVIDADE=KA.VIEWCODATIVIDADE COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI  "+
        "     WHERE KI.STATUS NOT IN (1,6)  AND KAA.STATUS NOT IN (1,6) "+(projeto!="" ? " AND K.NUM_OS "+projeto : "" )+
        "  "+
        " ), CTE_H AS ( "+
        "  "+
        "     SELECT M.CODCOLIGADA,M.CODFILIAL,M.CODMO,T.CODIGO,T.DESCRICAO,P.NOME,F.CHAPA,CODSITUACAO FROM KMAOOBRA(NOLOCK)  M  "+
        "     INNER JOIN PPESSOA(NOLOCK)  P ON P.CODIGO = M.CODPESSOA "+
        "     LEFT JOIN PFUNC(NOLOCK)  F ON M.CODCOLIGADA = F.CODCOLIGADA AND F.CODPESSOA = P.CODIGO  AND M.CODMO=F.CHAPA "+
        "     LEFT JOIN AHORARIO(NOLOCK)  T ON F.CODCOLIGADA = T.CODCOLIGADA AND F.CODHORARIO = T.CODIGO "+
        "     WHERE ( T.INATIVO=0 OR T.INATIVO IS NULL ) AND M.ATIVA=1 AND ( F.CODSITUACAO IN ('A','F') OR ( F.CODSITUACAO IS NULL AND P.CODIGO IS NOT NULL) ) "+(maodeobra!=""? " AND M.CODMO "+maodeobra : "" )+(turno!=""? " AND T.CODIGO "+turno : "" )+
        "  "+
        " ) "+
        "  "+
        " SELECT A.CODORDEM OP,A.IDATVORDEM,A.CODMO,H.NOME,CONVERT(VARCHAR(10),A.DTHRINICIAL,103) DATAPROGRAMADA,KA.CODPOSTO,KA.TEMPOPREVISTO,ISNULL(T.APONTADO,'0:00') APONTADO,KA.DOCAPOIOATV1,KA.TIPO,KA.NUMRNC,KA.DESCATIVIDADE,KA.DESCPROCESSO, "+
        " KA.OSPROCESSO,LEFT(FORMAT(CAST(A.QTDHORAS AS INT),'00')+':'+FORMAT(CAST((A.QTDHORAS)-CAST(A.QTDHORAS AS INT) AS FLOAT)*60,'00'),6) QTDEMHORAS "+
        " ,ISNULL(H.DESCRICAO,'SEM TURNO') TURNO,H.CODIGO,KA.DSCORDEM,REPLACE(FORMAT(KA.QTDEPLANEJADA,'0.00'),'.',',') QTDEPLANEJADA,ISNULL(C.TAG,'SEM TAG') TAG,ISNULL(C.DESCRICAO,'SEM CELULA') DESCCELULA,KA.FORNPARA,KA.PRIORIDADE,KA.UNDMEDIDA "+
        " FROM ZMDKATVORDEMPROGRAMACAO(NOLOCK)  A "+
        " INNER JOIN CTE_H H ON A.CODCOLIGADA = H.CODCOLIGADA AND A.CODMO = H.CODMO AND A.CODFILIAL = H.CODFILIAL "+
        " INNER JOIN CTE_Z KA ON A.CODCOLIGADA=A.CODCOLIGADA AND KA.CODFILIAL=A.CODFILIAL AND KA.CODORDEM=A.CODORDEM COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI AND KA.IDATVORDEM=A.IDATVORDEM  "+
        " LEFT JOIN CTE_T T ON T.CODCOLIGADA=A.CODCOLIGADA AND T.CODFILIAL=A.CODFILIAL AND T.CODORDEM=A.CODORDEM AND T.IDATVORDEM=A.IDATVORDEM"+
        " LEFT JOIN CTE C ON C.CODCOLIGADA=A.CODCOLIGADA AND C.CODFILIAL=A.CODFILIAL AND C.CODTRFAUX=A.CODORDEM "+
        " WHERE A.STATUS=0 AND A.NUMPLANOCORTE='' AND A.CODCOLIGADA=1 AND A.CODFILIAL = "+filial+
        " AND A.DTHRINICIAL BETWEEN '"+dataprogramacaoinicial+"' AND '"+dataprogramacaofinal+"' "+(celula!="" ? " AND C.CELULA "+celula : "" )
    }
    if(controller=="CONSULTACOMPONENTES"){ 

        myQuery = ";WITH CTE AS (SELECT DISTINCT KA.CODCOLIGADA,KA.IDATVORDEM,KA.CODORDEM,P.NOMEFANTASIA,KA.CODESTRUTURA,KMA.IDPRODUTO,P.CODIGOPRD,P.CODUNDCONTROLE,KA.CODATIVIDADE,KL.NUMEXEC, "+
                 "   SUM(CASE WHEN KMA.EFETIVADO=0 THEN KMA.QUANTIDADE ELSE 0 END) AS PREVISTO, "+
                 "   SUM(CASE WHEN KMA.EFETIVADO=1 THEN KMA.QUANTIDADE ELSE 0 END) AS APONTADO, "+
                 "   CASE WHEN SUM(CASE WHEN KMA.EFETIVADO=0 THEN KMA.QUANTIDADE ELSE 0 END) = 0 THEN ISNULL(KSUB.IDPRDORIGEM,KMA.IDPRODUTO) ELSE KMA.IDPRODUTO END AS PRINCIPAL "+
                 "   FROM KATVORDEM (NOLOCK) KA  "+
                 "       INNER JOIN ZMDKATVORDEMPROGRAMACAO A ON "+
                 "       A.CODCOLIGADA=KA.CODCOLIGADA "+
                 "       AND A.CODFILIAL=KA.CODFILIAL "+
                 "       AND A.CODORDEM=KA.CODORDEM "+
                 "       AND A.IDATVORDEM=KA.IDATVORDEM "+
                 "       INNER JOIN KORDEM (NOLOCK) KK ON "+
                 "       KK.CODCOLIGADA=KA.CODCOLIGADA "+
                 "       AND KK.CODFILIAL=KA.CODFILIAL "+
                 "       AND KK.CODORDEM=KA.CODORDEM "+
                 "       INNER JOIN KORDEMCOMPL (NOLOCK) KL ON "+
                 "       KL.CODCOLIGADA=KA.CODCOLIGADA "+
                 "       AND KL.CODFILIAL=KA.CODFILIAL "+
                 "       AND KL.CODORDEM=KA.CODORDEM "+
                 "       INNER JOIN KATVORDEMMP (NOLOCK) KMA ON "+
                 "       KMA.CODCOLIGADA=KA.CODCOLIGADA "+
                 "       AND KMA.CODFILIAL=KA.CODFILIAL "+
                 "       AND KMA.CODORDEM=KA.CODORDEM "+
                 "       AND KMA.IDATIVIDADE=KA.IDATVORDEM "+
                 "       INNER JOIN TPRD (NOLOCK) P ON          "+
                 "       KMA.CODCOLIGADA = P.CODCOLIGADA  "+
                 "       AND KMA.IDPRODUTO = P.IDPRD "+
                 "       LEFT JOIN ZMDPLANOAPROVEITAMENTOCORTE (NOLOCK) PAPC ON "+
                 "       PAPC.CODCOLIGADA=KA.CODCOLIGADA "+
                 "       AND PAPC.CODFILIAL=KA.CODFILIAL "+
                 "       AND PAPC.CODORDEM=KA.CODORDEM "+
                 "       AND PAPC.CODATIVIDADE=KA.CODATIVIDADE "+
                 "       LEFT JOIN KCOMPSUBSTITUTO(NOLOCK) KSUB ON "+
                 "       KSUB.IDPRD=KMA.IDPRODUTO  "+
                 "       AND KSUB.CODESTRUTURA=KMA.CODESTRUTURA  "+
                 "       AND KSUB.CODATIVIDADE=KA.CODATIVIDADE "+
                 "   WHERE A.STATUS=0 AND KA.STATUS NOT IN (1,6) AND PAPC.NSEQPLANOCORTE IS NULL "+(maodeobra!=""? " AND A.CODMO "+maodeobra : "" )+" AND KA.CODCOLIGADA=1 AND KA.CODFILIAL = "+filial+" AND A.DTHRINICIAL BETWEEN '"+dataprogramacaoinicial+"' AND '"+dataprogramacaofinal+"' "+(projeto!="" ? " AND KK.CODCCUSTO "+projeto : "" )+
                 "   GROUP BY KA.CODCOLIGADA,P.NOMEFANTASIA,KA.CODESTRUTURA,KMA.IDPRODUTO,P.CODIGOPRD,P.CODUNDCONTROLE,KL.NUMEXEC,KA.CODATIVIDADE,KSUB.IDPRD,KA.CODORDEM,KA.IDATVORDEM,KSUB.IDPRDORIGEM "+
                 "   ) "+
                 "       ,CTE2 AS ( "+
                 "       SELECT C.CODORDEM,C.CODATIVIDADE,C.IDATVORDEM,C.CODCOLIGADA,NOMEFANTASIA,CODIGOPRD,REPLACE(FORMAT(PREVISTO-APONTADO,'0.00'),'.',',') PREVISTO,CODUNDCONTROLE,'' QTD,C.IDPRODUTO PRINCIPAL,C.IDPRODUTO,C.NUMEXEC,1 ORD "+
                 "       FROM CTE C "+
                 "       WHERE APONTADO<PREVISTO "+
                 "       UNION  "+
                 "       SELECT C.CODORDEM,C.CODATIVIDADE,C.IDATVORDEM,C.CODCOLIGADA,P.NOMEFANTASIA,P.CODIGOPRD,REPLACE(FORMAT(0,'0.00'),'.',',') PREVISTO,P.CODUNDCONTROLE,'' QTD,KSUB.IDPRDORIGEM PRINCIPAL,P.IDPRD,C.NUMEXEC,2 ORD "+
                 "       FROM CTE C "+
                 "       INNER JOIN KCOMPONENTE (NOLOCK) KC ON "+
                 "       KC.CODCOLIGADA=C.CODCOLIGADA "+
                 "       AND KC.IDPRODUTO=C.PRINCIPAL  "+
                 "       AND KC.CODESTRUTURA=C.CODESTRUTURA  "+
                 "       AND KC.CODATIVIDADE=C.CODATIVIDADE "+
                 "       INNER JOIN KCOMPSUBSTITUTO(NOLOCK) KSUB ON "+
                 "       KSUB.CODCOLIGADA=C.CODCOLIGADA "+
                 "       AND KSUB.CODESTRUTURA=C.CODESTRUTURA  "+
                 "       AND KSUB.IDPRDORIGEM=KC.IDPRODUTO "+
                 "       AND KSUB.IDPRD!=KC.IDPRODUTO "+
                 "       INNER JOIN TPRD (NOLOCK) P ON          "+
                 "       KSUB.CODCOLIGADA = P.CODCOLIGADA  "+
                 "       AND KSUB.IDPRD = P.IDPRD "+
                 "       WHERE APONTADO<PREVISTO "+
                 "       ) "+
                 "       SELECT CODORDEM,CODATIVIDADE,IDATVORDEM,NOMEFANTASIA,CODIGOPRD,PREVISTO,CODUNDCONTROLE,QTD,ISNULL(NUMLOTE,'') NUMLOTE,PRINCIPAL,ORD "+
                 "       FROM CTE2 C "+
                 "       LEFT JOIN (  "+
                 "       SELECT DISTINCT T.CODCOLIGADA,T.IDPRD,T.NUMLOTE,NULL NUMEXEC FROM TLOTEPRD (NOLOCK) T  "+
                 "       INNER JOIN TLOTEPRDLOC (NOLOCK) TC ON "+
                 "       T.CODCOLIGADA=TC.CODCOLIGADA "+
                 "       AND T.IDLOTE=T.IDLOTE "+
                 "       WHERE TC.CODLOC IN ('23','25','27') AND TC.SALDOFISICO2>0 AND T.NUMLOTE NOT LIKE '%/%' AND T.IDPRD=141142 "+
                 "       UNION "+
                 "       SELECT DISTINCT KL.CODCOLIGADA,P.IDPRD,KL.CODORDEM NUMLOTE,KL.NUMEXEC FROM KORDEMCOMPL (NOLOCK) KL  "+
                 "       INNER JOIN KITEMORDEM (NOLOCK) KI ON "+
                 "       KI.CODCOLIGADA=KL.CODCOLIGADA "+
                 "       AND KI.CODFILIAL=KL.CODFILIAL "+
                 "       AND KI.CODORDEM=KL.CODORDEM "+
                 "       INNER JOIN KORDEM (NOLOCK) KO ON "+
                 "       KO.CODCOLIGADA=KL.CODCOLIGADA "+
                 "       AND KO.CODFILIAL=KL.CODFILIAL "+
                 "       AND KO.CODORDEM=KL.CODORDEM "+
                 "       INNER JOIN TPRD (NOLOCK) P  ON "+
                 "       KL.CODCOLIGADA=P.CODCOLIGADA "+
                 "       AND KI.CODESTRUTURA=P.CODIGOPRD "+
                 "       WHERE ISNULL(KO.REPROCESSAMENTO,0)=0 "+
                 "       ) L ON  L.CODCOLIGADA=C.CODCOLIGADA AND L.IDPRD=C.PRINCIPAL AND L.NUMEXEC=C.NUMEXEC  "+
                 "       ORDER BY PRINCIPAL,ORD,CODIGOPRD "


    }
    if(controller=="CONSULTAPAPC"){

        myQuery = ";WITH  CTE_H AS ( "+
        "     SELECT M.CODCOLIGADA,M.CODFILIAL,M.CODMO,T.CODIGO,T.DESCRICAO,P.NOME,F.CHAPA,CODSITUACAO FROM KMAOOBRA(NOLOCK)  M   "+
        "    INNER JOIN PPESSOA(NOLOCK)  P ON P.CODIGO = M.CODPESSOA "+
        "     LEFT JOIN PFUNC(NOLOCK)  F ON M.CODCOLIGADA = F.CODCOLIGADA AND F.CODPESSOA = P.CODIGO  AND M.CODMO=F.CHAPA  "+
        "     LEFT JOIN AHORARIO(NOLOCK)  T ON F.CODCOLIGADA = T.CODCOLIGADA AND F.CODHORARIO = T.CODIGO  "+
        "     WHERE ( T.INATIVO=0 OR T.INATIVO IS NULL ) AND M.ATIVA=1 AND ( F.CODSITUACAO IN ('A','F') OR ( F.CODSITUACAO IS NULL AND P.CODIGO IS NOT NULL) ) "+(maodeobrapapc!=""? " AND M.CODMO "+maodeobrapapc : "" )+
        " ) "+
        " SELECT A.NUMPLANOCORTE,A.CODMO,H.NOME,CONVERT(VARCHAR(10),A.DTHRINICIAL,103) DATAPROGRAMADA,  "+
        " SUM(A.QTDHORAS) QTDHORAS,LEFT(FORMAT(CAST(SUM(KA.TEMPO)/60 AS INT),'00')+':'+FORMAT(CAST(SUM(KA.TEMPO)%60 AS  FLOAT),'00'),6) QTDEMHORAS  "+
        " ,ISNULL(H.DESCRICAO,'SEM TURNO') TURNO,H.CODIGO, "+
        " T.NOMEFANTASIA NOMEPRODUTO,T.CODIGOPRD, REPLACE(FORMAT(Z2.QTDEMP,'0.00'),'.',',') QTDEMP,T.CODUNDCONTROLE,Z2.NUMLOTE,K.CODCCUSTO OSPROCESSO "+
        " FROM ZMDKATVORDEMPROGRAMACAO (NOLOCK) A "+
        " INNER JOIN CTE_H H ON  "+
        " A.CODCOLIGADA = H.CODCOLIGADA  "+
        " AND A.CODMO = H.CODMO  "+
        " AND A.CODFILIAL = H.CODFILIAL  "+
        " INNER JOIN KORDEM K ON  "+
        " K.CODCOLIGADA=A.CODCOLIGADA "+
        " AND K.CODFILIAL = A.CODFILIAL "+
        " AND K.CODORDEM = A.CODORDEM "+
        " INNER JOIN KATVORDEM KA ON  "+
        " KA.CODCOLIGADA=A.CODCOLIGADA "+
        " AND KA.CODFILIAL = A.CODFILIAL "+
        " AND KA.CODORDEM = A.CODORDEM "+
        " AND KA.IDATVORDEM = A.IDATVORDEM "+
        " INNER JOIN ZMDPLANOAPROVEITAMENTOCORTE Z2 ON  "+
        " A.CODCOLIGADA=Z2.CODCOLIGADA "+
        " AND A.CODFILIAL=Z2.CODFILIAL "+
        " AND A.NUMPLANOCORTE=Z2.NUMPLANOCORTE COLLATE SQL_Latin1_General_CP1_CI_AI "+
        " AND A.CODORDEM=Z2.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI "+
        " AND Z2.CODATIVIDADE=KA.CODATIVIDADE "+
        " INNER JOIN TPRD T ON "+
        " T.CODCOLIGADA=Z2.CODCOLIGADA "+
        " AND T.CODIGOPRD=Z2.CODIGOMP "+
        " WHERE A.STATUS=0 AND KA.STATUS NOT IN (1,6) "+(numplanopapc!=""? "AND Z2.NUMPLANOCORTE "+numplanopapc : "" )+" AND A.CODCOLIGADA=1 AND A.CODFILIAL = "+filialpapc+" AND A.DTHRINICIAL BETWEEN '"+dataprogramacaoinicialpapc+"' AND '"+dataprogramacaofinalpapc+"' "+(projetopapc!=""? " AND K.CODCCUSTO "+projetopapc : "" )+
        " GROUP BY  H.DESCRICAO,A.DTHRINICIAL,A.NUMPLANOCORTE,A.CODMO,H.NOME,H.CODIGO,T.NOMEFANTASIA, "+
        " T.CODIGOPRD,Z2.QTDEMP,T.CODUNDCONTROLE,Z2.NUMLOTE,K.CODCCUSTO"+
        " ORDER BY A.CODMO,A.NUMPLANOCORTE "



    }
    if(controller=="CONSULTAATIVIDADESPAPC"){

        myQuery = "WITH CTE_Z AS(  "+
        "     SELECT K.CODCOLIGADA,K.CODFILIAL,KI.CODORDEM,KA.CODATIVIDADE,KA.FORNPARA,KAA.IDATVORDEM,KI.QTDEPLANEJADA,KA.OSPROCESSO,K.OBSPROCESSO,KA.DESCPROCESSO,KAA.CODPOSTO,KA.DOCAPOIOATV1, "+
        "     'NORMAL' TIPO,'' NUMRNC,KA.DESCATIVIDADE,KA.PRIORIDADE,K.UNDMEDIDA,  "+
        "     LEFT(FORMAT(CAST(KAA.TEMPO/60 AS INT),'00')+':'+FORMAT(CAST(KAA.TEMPO%60 AS  FLOAT),'00'),6) TEMPOPREVISTO,  "+
        "     CONCAT(K.DESCRICAO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI ,' - ',K.POSICAODESENHO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI ,' - ',K.NUMDESENHO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI) DSCORDEM "+
        "     FROM FLUIG.DBO.Z_CRM_EX001005(NOLOCK)  K "+
        "     INNER JOIN FLUIG.DBO.Z_CRM_EX001021(NOLOCK)  KA ON K.OS=KA.OSPROCESSO AND K.EXECUCAO=KA.EXECUCAO AND K.IDCRIACAO=KA.IDCRIACAOPROCESSO "+
        "     INNER JOIN KITEMORDEM(NOLOCK)  KI ON KI.CODCOLIGADA=K.CODCOLIGADA AND KI.CODFILIAL=K.CODFILIAL AND KI.CODESTRUTURA=K.CODIGOPRD COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
        "     INNER JOIN KORDEMCOMPL(NOLOCK)  KL ON KL.CODCOLIGADA=KI.CODCOLIGADA AND KL.CODFILIAL=KI.CODFILIAL AND KL.CODORDEM=KI.CODORDEM AND KL.NUMEXEC=K.EXECUCAO "+
        "     INNER JOIN KATVORDEM(NOLOCK)  KAA ON KAA.CODCOLIGADA=K.CODCOLIGADA AND KAA.CODFILIAL=K.CODFILIAL AND KAA.CODORDEM=KI.CODORDEM  "+
        "      AND KAA.CODESTRUTURA=KI.CODESTRUTURA COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI AND KAA.CODATIVIDADE=KA.CODATIVIDADE COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
        "     WHERE K.ITEMEXCLUSIVO<>2 AND KI.STATUS NOT IN (1,6) AND KAA.STATUS NOT IN (1,6) "+(projetopapc!=""? " AND K.OS "+projetopapc : "" )+
        "     UNION "+
        "     SELECT K.CODCOLIGADA,K.CODFILIAL,KI.CODORDEM,VIEWCODATIVIDADE,KA.VIEWFORNPARA,KAA.IDATVORDEM,KI.QTDEPLANEJADA,K.NUM_OS,K.F_OBSPROCESSO,KA.VIEWDESCPROCESSO,KAA.CODPOSTO,KA.VIEWDOCAPOIOATV1 , "+
        "     KA.DESCRNCRR TIPO,KA.NUMRNC,KA.VIEWDESCATIVIDADE,KA.VIEWPRIORIDADE,K.F_UNDMEDIDA, "+
        "     LEFT(FORMAT(CAST(KAA.TEMPO/60 AS INT),'00')+':'+FORMAT(CAST(KAA.TEMPO%60 AS  FLOAT),'00'),6) TEMPOPREVISTO,  "+
        "     CONCAT(K.F_DESCRICAO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI ,' - ',K.F_POSICAODESENHO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI ,' - ',K.F_NUMDESENHO COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI) DSCORDEM "+
        "     FROM FLUIG.DBO.ML001026(NOLOCK)  K "+
        "     INNER JOIN FLUIG.DBO.ML001028(NOLOCK)  KA ON K.NUM_OS=KA.VIEWOSPROCESSO AND K.F_IDCRIACAO=KA.VIEWIDCRIACAOPROCESSO "+
        "     INNER JOIN KITEMORDEM(NOLOCK)  KI ON KI.CODCOLIGADA=K.CODCOLIGADA AND KI.CODFILIAL=K.CODFILIAL AND KI.CODORDEM=K.OPGERADA COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI "+
        "     INNER JOIN KATVORDEM(NOLOCK)  KAA ON KAA.CODCOLIGADA=K.CODCOLIGADA AND KAA.CODFILIAL=K.CODFILIAL AND KAA.CODORDEM=KI.CODORDEM   "+
        "     AND KAA.CODESTRUTURA=KI.CODESTRUTURA COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI AND KAA.CODATIVIDADE=KA.VIEWCODATIVIDADE COLLATE SQL_LATIN1_GENERAL_CP1_CI_AI  "+
        "     WHERE KI.STATUS NOT IN (1,6)  AND KAA.STATUS NOT IN (1,6) "+(projetopapc!=""? " AND K.NUM_OS "+projetopapc : "" )+
        "  "+
        " )"+
        " SELECT A.NUMPLANOCORTE,A.CODORDEM OP,A.IDATVORDEM,A.CODMO,KA.DOCAPOIOATV1,KA.TIPO,KA.NUMRNC,KA.DESCATIVIDADE,KA.DESCPROCESSO, "+
        "     A.QTDHORAS,LEFT(FORMAT(CAST(A.QTDHORAS AS INT),'00')+':'+FORMAT(CAST((A.QTDHORAS)-CAST(A.QTDHORAS AS INT) AS FLOAT)*60,'00'),6) QTDEMHORAS "+
        "     ,KA.DSCORDEM,REPLACE(FORMAT(Z2.QUANTIDADE,'0.00'),'.',',') QTDEPLANEJADA,KA.FORNPARA,KA.PRIORIDADE,KA.UNDMEDIDA,ISNULL(CAST(Z2.QTDEAPONTADA AS VARCHAR(10)),'') QTDEAPONTADA"+
        "     FROM ZMDKATVORDEMPROGRAMACAO (NOLOCK) A"+
        "     INNER JOIN CTE_Z KA ON"+
        "     KA.CODCOLIGADA=A.CODCOLIGADA"+
        "     AND KA.CODFILIAL=A.CODFILIAL"+
        "     AND KA.CODORDEM=A.CODORDEM"+
        "     AND KA.IDATVORDEM=A.IDATVORDEM"+
        "     INNER JOIN ZMDPLANOAPROVEITAMENTOCORTE (NOLOCK) Z2 ON "+
        "     A.CODCOLIGADA=Z2.CODCOLIGADA"+
        "     AND A.CODFILIAL=Z2.CODFILIAL"+
        "     AND A.NUMPLANOCORTE=Z2.NUMPLANOCORTE COLLATE SQL_Latin1_General_CP1_CI_AI"+
        "     AND A.CODORDEM=Z2.CODORDEM COLLATE SQL_Latin1_General_CP1_CI_AI"+
        "     AND KA.CODATIVIDADE=Z2.CODATIVIDADE COLLATE SQL_Latin1_General_CP1_CI_AI"+
        "     WHERE A.STATUS=0 "+(maodeobrapapc!=""? "AND A.CODMO "+maodeobrapapc : "" )+" "+(numplanopapc!=""? " AND Z2.NUMPLANOCORTE "+numplanopapc : "" )+" AND A.CODCOLIGADA=1 AND A.CODFILIAL = "+filialpapc+" AND A.DTHRINICIAL BETWEEN '"+dataprogramacaoinicialpapc+"' AND '"+dataprogramacaofinalpapc+"' "
    
    }
	
					  
				
    log.info("dsFormularioDiarioDeAtividades MY QUERY: " + myQuery)
    
    try {
    	
        var conn = ds.getConnection()
        var stmt = conn.createStatement()
        var rs = stmt.executeQuery(myQuery)
        var columnCount = rs.getMetaData().getColumnCount()
        
        while (rs.next()) {
        	
            if (!created) {
            	
                for (var i = 1; i <= columnCount; i++) {
                	
                	dsFormularioDiarioDeAtividades.addColumn(rs.getMetaData().getColumnName(i))
                    
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
            
            dsFormularioDiarioDeAtividades.addRow(Arr)
            
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
    
    return dsFormularioDiarioDeAtividades;
	
}