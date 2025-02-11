USE [CORPORE]
GO
/****** Object:  StoredProcedure [dbo].[SP_DELP_LEITURA_QUEUE_DESTINO]    Script Date: 24/01/2025 14:21:32 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[SP_DELP_LEITURA_QUEUE_DESTINO]
AS
  DECLARE @RecvReqDlgHandle UNIQUEIDENTIFIER;
  DECLARE @RecvReqMsg NVARCHAR(MAX);
  DECLARE @Function NVARCHAR(MAX),@NumProcesso NVARCHAR(MAX),@Os NVARCHAR(MAX),@CodTrfPai NVARCHAR(MAX),@Execucao NVARCHAR(MAX),@IdCriacao NVARCHAR(MAX);
  DECLARE @XMLRecvReqMsg XML;
  DECLARE @RecvReqMsgName sysname;
  DECLARE @ReplyMsg xml(XMLIntegracaoResposta);

  WHILE (1=1)
  BEGIN

	BEGIN TRY

		BEGIN TRANSACTION;

		WAITFOR
		( RECEIVE TOP(1)
			@RecvReqDlgHandle = conversation_handle,
			@RecvReqMsg = message_body,
			@RecvReqMsgName = message_type_name
		  FROM QueueDestino
		), TIMEOUT 5000;

		IF (@@ROWCOUNT = 0)
		BEGIN
		  ROLLBACK;
		  BREAK;
		END

		IF @RecvReqMsgName =
		   N'MensagemEnvio'
		BEGIN

			SET @XMLRecvReqMsg = CAST(@RecvReqMsg AS XML)



			select 
			@Function= case when @XMLRecvReqMsg.exist('Parametros/Function')=1 then rat.value('(Function)[1]','NVARCHAR(MAX)') else null end,
			@NumProcesso= case when @XMLRecvReqMsg.exist('Parametros/Parameter[@Type="NUMPROCESSO"]')=1 then rat.query('Parameter[@Type="NUMPROCESSO"]').value('.','NVARCHAR(MAX)') else null end,
			@Os= case when @XMLRecvReqMsg.exist('Parametros/Parameter[@Type="OS"]')=1 then rat.query('Parameter[@Type="OS"]').value('.','NVARCHAR(MAX)') else null end,
			@CodTrfPai= case when @XMLRecvReqMsg.exist('Parametros/Parameter[@Type="CODTRFPAI"]')=1 then rat.query('Parameter[@Type="CODTRFPAI"]').value('.','NVARCHAR(MAX)') else null end,
			@Execucao= case when @XMLRecvReqMsg.exist('Parametros/Parameter[@Type="EXECUCAO"]')=1 then rat.query('Parameter[@Type="EXECUCAO"]').value('.','NVARCHAR(MAX)') else null end,
			@IdCriacao= case when @XMLRecvReqMsg.exist('Parametros/Parameter[@Type="IDCRIACAO"]')=1 then rat.query('Parameter[@Type="IDCRIACAO"]').value('.','NVARCHAR(MAX)') else null end
			from @XMLRecvReqMsg.nodes('Parametros') as Rec(rat);

			if @Function='Principal'
				begin
					exec SP_DELP_EDICAOESTRUTURA @NumProcesso,@Os,@IdCriacao
				end
			else
				begin
					exec SP_DELP_EDICAOEXECUCAO @NumProcesso,@Os,@CodTrfPai,@Execucao,@IdCriacao
				end


			SELECT @ReplyMsg = N'
			<Result>Sucesso no processo</Result>
			';

			SEND ON CONVERSATION @RecvReqDlgHandle
					MESSAGE TYPE 
					[MensagemRecebimento]
					(@ReplyMsg);
		END
		ELSE IF @RecvReqMsgName =
			N'https://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
		BEGIN
		   END CONVERSATION @RecvReqDlgHandle;
		END
		ELSE IF @RecvReqMsgName =
			N'https://schemas.microsoft.com/SQL/ServiceBroker/Error'
		BEGIN
		   END CONVERSATION @RecvReqDlgHandle;
		END

		COMMIT;

	END TRY
	BEGIN CATCH

		   SELECT @ReplyMsg = N'
				<Result>Erro durante o processo</Result>
		   ';

		SEND ON CONVERSATION @RecvReqDlgHandle
				MESSAGE TYPE 
				[MensagemRecebimento]
				(@ReplyMsg);

		WHILE @@TRANCOUNT>0
		BEGIN 

			ROLLBACK

		END

	END CATCH

  END
