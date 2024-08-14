SELECT *
FROM   TUSRTMV (NOLOCK)
WHERE  CODUSUARIO IN (SELECT GP.CODUSUARIO
                      FROM   GUSRPERFIL GP (NOLOCK)
                      WHERE  GP.CODCOLIGADA = 1
                             AND CODPERFIL IN (SELECT CODPERFIL
                                               FROM   GPERFIL (NOLOCK)
                                               WHERE  STATUS = 1)
                             AND CODUSUARIO LIKE '%')
       AND ( INCLUIR = 1
              OR ALTERAR = 1
              OR EXCLUIR = 1
              OR CANCELAR = 1
              OR LANCAR = 1
              OR CONSULTAR = 1
              OR FATURAR = 1
              OR ATIVAR = 1
              OR ALTERARDEPOISIMP = 1
              OR REABRIR = 1
              OR ESTORNARCONTAB = 1
              OR INCLUIRPORFAT = 1
              OR ALTERARITEMINTEGRADO = 1
              OR ALTERARDEPOISEMAIL = 1
              OR ALTERARDEPOISIMP = 1
              OR COTAR = 1
              OR COPIAR = 1
              OR IMPRIMIR = 1
              OR REIMPRIMIR = 1
              OR GERACONTRATO = 1
              OR GERARCOTACAOAUTOMATICA = 1 )
       AND CODCOLIGADA = 1 
	   and CODTMV='3.1.13'