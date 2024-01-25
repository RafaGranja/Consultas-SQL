// DATASET PARA ATUALIZAR OS DADOS DA ATIVIDADE APÓS A ALOCAÇÃO/DESPROGRAMAÇÃO
function createDataset(fields, constraints, sortFields) {
  var dsAtualizaDadosAtvOS = DatasetBuilder.newDataset();
  var dataSource = "/jdbc/FluigDSRM";
  var ic = new javax.naming.InitialContext();
  var ds = ic.lookup(dataSource);
  var created = false;
  var codcoligada = "";
  var codfilial = "";
  var dataDe = "";
  var dataAte = "";
  var codOrdem = "";
  var celula = "";
  var idprd = "";
  var codigoPrd = "";
  var codAtividade = "";
  var codPosto = "";
  var codPrj = "";
  if (constraints != null) {
    for (var i = 0; i < constraints.length; i++) {
      if (constraints[i].fieldName == "CODCOLIGADA") {
        codcoligada = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "CODFILIAL") {
        codfilial = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "DATA_DE") {
        dataDe = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "DATA_ATE") {
        dataAte = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "CODORDEM") {
        codOrdem = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "CELULA") {
        celula = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "IDPRD") {
        idprd = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "CODIGOPRD") {
        codigoPrd = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "CODATIVIDADE") {
        codAtividade = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "CODPOSTO") {
        codPosto = constraints[i].initialValue;
      }
      if (constraints[i].fieldName == "CODPRJ") {
        codPrj = constraints[i].initialValue;
      }
    }
  }
  var myQuery =
    "SELECT   " +
    " CPL.CODCELULA, PJ.CODPRJ OS, I.CODORDEM OP, I.CODCOLIGADA, I.CODFILIAL, I.CODESTRUTURA,  " +
    " P.CODIGOPRD, P.IDPRD,  P.DESCRICAO DSCITEM, DA.CODATIVIDADE, DA.DSCATIVIDADE,   " +
    " A.STATUS, (SELECT S.DSCSTATUS " +
    "  FROM KSTATUS S " +
    "  WHERE A.CODCOLIGADA = S.CODCOLIGADA AND S.CODSTATUS = A.STATUS) STATUS_ATV, " +
    " AE.PRIORIDADE, PE.CODPOSTO, PO.DSCPOSTO,S.CODSTATUS, S.DSCSTATUS,  A.TEMPO AS CARGA_PREV, PC.NUMFLUIG,    " +
    " PC.INDICE_FLUIG, PC.NIVEL_FLUIG,I.CODCCUSTO CODPRJ,    " +
    " I.DTHRINICIALPREV INICIO_PLANEJADO, I.DTHRFINALPREV FINAL_PLANEJADO, NULL FOLGA,  " +
    " ISNULL( " +
    "  ISNULL((SELECT " +
    "   SUM(ROUND(QTDHORAS,4)) HORAS " +
    "  FROM " +
    "   ZMDKATVORDEMPROGRAMACAO " +
    "  WHERE " +
    "   STATUS = 0 " +
    "   AND IDATVORDEM = A.IDATVORDEM " +
    "   AND CODCOLIGADA = I.CODCOLIGADA " +
    "   AND CODFILIAL = I.CODFILIAL " +
    "   AND CODORDEM = I.CODORDEM " +
    "  ),0) " +
    "  + " +
    "  ISNULL((SELECT  " +
    "    SUM(ROUND(CAST(DATEDIFF(minute,X.DTHRINICIAL,X.DTHRFINAL)AS FLOAT)/60.0000,4))   " +
    "   FROM " +
    "    KMAOOBRAALOC (NOLOCK) X  " +
    "   WHERE " +
    "    X.CODORDEM = KATVORDEM.CODORDEM AND X.CODCOLIGADA = KATVORDEM.CODCOLIGADA AND X.CODFILIAL = KATVORDEM.CODFILIAL AND X.EFETIVADO = 0 AND X.IDATVORDEM = KATVORDEM.IDATVORDEM " +
    "  ),0) " +
    "  ,0) ALOCADO, " +
    "  ISNULL( " +
    "  ISNULL((SELECT " +
    "   SUM(ROUND(QTDHORAS,4)) HORAS   " +
    "  FROM  " +
    "   ZMDKATVORDEMPROGRAMACAO " +
    "  WHERE  " +
    "   STATUS = 1 " +
    "   AND IDATVORDEM = A.IDATVORDEM    " +
    "   AND CODCOLIGADA = I.CODCOLIGADA   " +
    "   AND CODFILIAL = I.CODFILIAL    " +
    "   AND CODORDEM = I.CODORDEM    " +
    "  ),0) " +
    "  + " +
    "  ISNULL((SELECT  " +
    "    SUM(ROUND(CAST(DATEDIFF(minute,X.DTHRINICIAL,X.DTHRFINAL)AS FLOAT)/60.0000,4))   " +
    "   FROM " +
    "    KMAOOBRAALOC (NOLOCK) X  " +
    "   WHERE " +
    "    X.CODORDEM = KATVORDEM.CODORDEM AND X.CODCOLIGADA = KATVORDEM.CODCOLIGADA AND X.CODFILIAL = KATVORDEM.CODFILIAL AND X.EFETIVADO = 1 AND X.IDATVORDEM = KATVORDEM.IDATVORDEM " +
    "  ),0) " +
    "  ,0) APONTADO " +
    " FROM " +
    "  KITEMORDEM I    " +
    "  INNER JOIN KESTRUTURA E ON I.CODCOLIGADA = E.CODCOLIGADA AND I.CODFILIAL = E.CODFILIAL AND I.CODESTRUTURA = E.CODESTRUTURA  " +
    "  INNER JOIN TPRD P ON E.CODCOLIGADA = P.CODCOLIGADA AND E.IDPRODUTO = P.IDPRD    " +
    "  INNER JOIN TPRDCOMPL PC ON I.CODCOLIGADA = PC.CODCOLIGADA AND P.IDPRD = PC.IDPRD     " +
    "  INNER JOIN KATVORDEM A ON  A.CODCOLIGADA = E.CODCOLIGADA AND A.CODFILIAL = E.CODFILIAL AND A.CODESTRUTURA = E.CODESTRUTURA AND A.CODORDEM = I.CODORDEM  " +
    "  INNER JOIN KATVESTRUTURA AE ON AE.CODCOLIGADA = A.CODCOLIGADA AND AE.CODFILIAL = A.CODFILIAL AND AE.CODESTRUTURA = A.CODESTRUTURA AND AE.CODATIVIDADE = A.CODATIVIDADE AND AE.CODATIVIDADE NOT LIKE 'RMP%'   " +
    "  INNER JOIN KATIVIDADE DA ON DA.CODCOLIGADA = A.CODCOLIGADA AND DA.CODATIVIDADE = A.CODATIVIDADE     " +
    "  INNER JOIN KATVESTPOSTO PE ON PE.CODCOLIGADA = AE.CODCOLIGADA AND PE.CODFILIAL = AE.CODFILIAL AND PE.CODESTRUTURA = AE.CODESTRUTURA AND PE.CODATIVIDADE = AE.CODATIVIDADE    " +
    "  INNER JOIN KPOSTO PO ON PO.CODCOLIGADA = PE.CODCOLIGADA AND PO.CODFILIAL = PE.CODFILIAL AND PO.CODPOSTO = PE.CODPOSTO    " +
    "  INNER JOIN KSTATUS S ON S.CODCOLIGADA = I.CODCOLIGADA AND S.CODSTATUS = I.STATUS     " +
    "  INNER JOIN FLUIG.DBO.Z_CRM_EX001005COMPL CPL ON CPL.CODCOLIGADA=I.CODCOLIGADA AND CPL.CODFILIAL=I.CODFILIAL AND CPL.CODORDEM=I.CODORDEM "
    " WHERE " +
    " I.CODCOLIGADA = " +
    codcoligada +
    " " +
    " AND I.CODFILIAL = " +
    codfilial +
    " " +
    " AND ( (CAST(I.DTHRINICIALPREV AS DATE) BETWEEN '" +
    dataDe +
    "' AND '" +
    dataAte +
    "') " +
    "  OR (CAST(I.DTHRFINALPREV AS DATE) BETWEEN '" +
    dataDe +
    "' AND '" +
    dataAte +
    "')) " +
    /*" AND ( ('"+dataDe+"' BETWEEN CAST(I.DTHRINICIALPREV AS DATE) AND CAST(I.DTHRFINALPREV AS DATE)) OR  "+        " ('"+dataAte+"' BETWEEN CAST(I.DTHRINICIALPREV AS DATE) AND CAST(I.DTHRFINALPREV AS DATE))) "+*/
    " AND I.CODORDEM = '" +
    codOrdem +
    "' " +
    " AND CPL.CODCELULA='" +
    celula +
    "' " +
    " AND P.IDPRD = '" +
    idprd +
    "' " +
    " AND P.CODIGOPRD = '" +
    codigoPrd +
    "' " +
    " AND A.CODATIVIDADE = '" +
    codAtividade +
    "' " +
    " AND PO.CODPOSTO = '" +
    codPosto +
    "' " +
    " AND I.CODCCUSTO = '" +
    codPrj +
    "' "
  log.info("QUERY dsAtualizaDadosAtvOS: " + myQuery);
  try {
    var conn = ds.getConnection();
    var stmt = conn.createStatement();
    var rs = stmt.executeQuery(myQuery);
    var columnCount = rs.getMetaData().getColumnCount();
    while (rs.next()) {
      if (!created) {
        for (var i = 1; i <= columnCount; i++) {
          dsAtualizaDadosAtvOS.addColumn(rs.getMetaData().getColumnName(i));
        }
        created = true;
      }
      var Arr = new Array();
      for (var i = 1; i <= columnCount; i++) {
        var obj = rs.getObject(rs.getMetaData().getColumnName(i));
        if (null != obj) {
          Arr[i - 1] = rs
            .getObject(rs.getMetaData().getColumnName(i))
            .toString();
        } else {
          Arr[i - 1] = "null";
        }
      }
      dsAtualizaDadosAtvOS.addRow(Arr);
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
  return dsAtualizaDadosAtvOS;
}
