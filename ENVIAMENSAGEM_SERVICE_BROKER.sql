USE CORPORE
GO

Begin Transaction
 
Declare @MyConversationHandle Uniqueidentifier
  DECLARE @RecvReqDlgHandle UNIQUEIDENTIFIER;
  DECLARE @XMLRecvReqMsg XML;
  DECLARE @RecvReqMsg NVARCHAR(MAX);
  DECLARE @RecvReqMsgName sysname;

 
/* Inicia um diálogo entre os serviços da origem e destino */
BEGIN DIALOG @MyConversationHandle
FROM SERVICE [ServiceOrigem]
TO SERVICE 'ServiceDestino'
ON CONTRACT [ContratoProcessaMensagens]
WITH ENCRYPTION=OFF,
LIFETIME= 5000;
 
/* Declarando a Estrutura e Conteúdo da Mensagem */
Declare @MyMensagemServiceBroker XML(XMLIntegracao)
SET @MyMensagemServiceBroker = N'
	<Parametros> 
			<Parameter Type="OS">3.07872.32.001</Parameter>
			<Parameter Type="IDCRIACAO">123</Parameter>
			<Function>Principal</Function>
	</Parametros>
';
 
/* Enviando uma mensagem no Diálogo */
SEND ON CONVERSATION @MyConversationHandle
MESSAGE TYPE [MensagemEnvio](@MyMensagemServiceBroker)

WAITFOR
( RECEIVE TOP(1)
	@RecvReqDlgHandle = conversation_handle,
	@RecvReqMsg = message_body,
	@RecvReqMsgName = message_type_name
FROM QueueOrigem
), TIMEOUT 5000;

set @XMLRecvReqMsg =  CAST(@RecvReqMsg as XML)

select Rec.rat.value('(../Result)[1]','varchar(100)') as Resultado from @XMLRecvReqMsg.nodes('Result') as Rec(rat);

END CONVERSATION @RecvReqDlgHandle

Commit Transaction

set transaction isolation level READ UNCOMMITTED

select CASE message_type_name WHEN 'X' 
  THEN CAST(message_body AS NVARCHAR(MAX)) 
  ELSE message_body 
END ,* from QueueDestino

select CASE message_type_name WHEN 'X' 
  THEN CAST(message_body AS NVARCHAR(MAX)) 
  ELSE message_body 
END,* from QueueOrigem