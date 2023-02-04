tasks = {
        "t_anuidade_produto":{
            "sql": """SELECT p.ds_produto as ds_produto,
                            p.id_produto as id_produto,
                            p.vl_faturamento_minimo as vl_faturamento_minimo
                        FROM vuon_db1.t_anuidade_produto p"""
        },
        "t_fatura":{
            "sql": """SELECT i.id_fatura as id_fatura,
					i.id_conta as id_conta,
					i.dt_vencimento as dt_vencimento_fatura,
					i.fl_ativa as fl_ativa,
					i.fl_envia_sms_directone as fl_servico_sms,
					i.vl_fatura as vlr_fatura
                FROM vuon_db1.t_fatura i
                WHERE YEAR(i.dt_vencimento) || MONTH (i.dt_vencimento) =
                    YEAR(from_unixtime(unix_timestamp())) || MONTH(from_unixtime(unix_timestamp()))"""
        },
        "t_seguros":{
            "sql":"""select tkr.vl_resposta as CPF,
                            tka.id_produto as id_produto
                        from vuon_db1.t_klok_campo    tkc,
                            vuon_db1.t_klok_resposta tkr,
                            vuon_db1.t_klok_adesao   tka
                        where tkc.ds_codigo = 'contratante.dadosPessoais.cpf'
                        and tkc.id_campo = tkr.id_campo
                        and tkr.id_adesao = tka.id_adesao
                        and tka.ds_status IN ('ATIVA', 'PENDENTE_CONFIRMACAO_PROVEDOR', 'INCONSISTENTE')
                        and tka.id_produto IN  ('1','2','3', '4','5')"""
        },
        "t_venda":{
            "sql": """SELECT tv.id_conta as id_conta,
                            max(tv.dt_venda) as dt_ultima_compra,
                            min(tv.dt_venda) as dt_primeira_compra
                        FROM vuon_db1.t_venda tv
                        WHERE tv.fl_status = 'A'
                        GROUP BY tv.id_conta"""
        },
        "t_pagamento_fatura":{
            "sql": """select --f.id_pagamento_fatura   as id_pagamento_fatura,
                             f.id_fatura               as id_fatura,
                             f.id_conta                as id_conta,
                             f.dt_pagamento            as dt_pagamento_ult_fatura,
                             f.vl_pagamento            as vlr_pago
                            from vuon_db1.t_pagamento_fatura f
                            WHERE
                            YEAR(f.dt_pagamento) || MONTH (f.dt_pagamento) =
                            YEAR(from_unixtime(unix_timestamp())) || MONTH(from_unixtime(unix_timestamp()))"""
        },
        "t_vd_lojas_elt":{
            "sql":"""SELECT vl.COD_LOJA		as	codigo_loja,
							vl.CGC			as		cnpj
							FROM elt.VD_LOJAS vl
							WHERE vl.IMPORTAR_VENDA = 'S'"""
        },
        "t_conta":{
            "sql":"""select co.id_conta                     as  id_conta,
                            co.nu_cpf_cnpj                	as  nu_cpf_cnpj,
                            co.ds_status_conta            	as  tx_status_conta,
                            co.ds_status_cartao           	as  tx_status_cartao,
                            co.ds_motivo_bloqueio_cartao  	as  tx_motivo_bloqueio,
                            co.id_produto                 	as  id_produto,
                            co.nm_produto                 	as  tx_descricao_produto,
                            co.nu_dias_atraso             	as  qtd_dias_atraso,
                            co.vl_limite                  	as  vl_absoluto_limite,
                            co.vl_fatura_ativa            	as  vl_fatura_ativa,
                            co.vl_renda_titular 	       	as  renda,
                            co.nu_cnpj_origem_comercial	as	cnpj,
                            co.nm_origem_comercial 		as	tx_loja_origem
                            from vuon_db1.t_conta co"""
        },
        "t_cliente":{
            "sql":"""select cli.id_conta			as		id_conta,
							cli.fl_status_analise   as		status,
							cli.nu_cpf_cnpj			as		cpfcnpj,
							cli.ds_escolaridade 	as		escolaridade,
							cli.ds_estado_civil		as		estadocivil,
							cli.ds_complemento		as		cmpltologradouro,
							cli.dt_cadastro 		as 		dt_cadastro
                        From vuon_db1.t_cliente cli"""
        },
        "t_parcelamento_fatura":{
            "sql":"""SELECT tp.id_conta		as	id_conta,
                            tp.fl_status	as	fl_status
                        FROM vuon_db1.t_parcelamento_fatura_acid tp
                        where tp.fl_status = 'A'"""
        },
        "t_fat_limite":{
            "sql":"""SELECT lm.id_conta 			as	id_conta,
                            lm.vl_limite 			as 	vlr_absoluto_limite,
                            lm.vl_devedor_contabil 	as 	vlr_limite_utilizado,
                            lm.disponivel_rotativo 	as	vlr_limite_disponivel
                        FROM vuon_db1_gold.t_fat_cliente_limite lm"""
        },
        "t_cliente_gold":{ # Manter as alias de acordo com os nomes reais da tabela de destino
            "sql": """SELECT null               as id_pessoa_fila, 
                            'VUON' 				as Edi_Source,  		
                            f.nm_cliente 		as Nomerazao,			
                            f.nm_cliente 		as Fantasia,
                            f.ds_email 			as Email,
                            'CLIENTE' 			as Palavrachave,
                            'BRASIL' 			as Pais,
                            f.cd_uf 			as Uf,
                            f.nm_bairro 		as Bairro,
                            f.nm_logradouro		as Logradouro,
                            f.nu_cep			as Cep,
                            f.nu_endereco		as Nrologradouro,
                            f.nu_cpf_cnpj 		as Cpf_cnpj,
                            null                as Digcgccpf,
                            f.dt_nascimento 	as Dtanascfund,
                            'VUON' 				as Origem,
                            'HIVE' 				as Usuinclusao,
                            f.fl_sexo 			as Sexo,
                            f.nm_cidade 		as Cidade,
                            f.fl_tipo_pessoa 	as Fisicajuridica,
                            f.nu_rg 			as Inscricaorg,
                            --to_date(from_unixtime(unix_timestamp(CURRENT_DATE, 'yyyyMMdd'),"yyyy-MM-dd")) as Data_Selecao,
                            f.id_conta 			as id_conta,
                            'I'                 as Tipo_Integracao,
                            'F'                 as Status_Integracao
                            FROM vuon_db1_gold.t_dim_cliente f"""
        },
        "t_cliente_cadastro":{ # Manter as alias de acordo com os nomes reais da tabela de destino
            "sql": """SELECT c.dt_cadastro 		as Dtainclusao,
							c.id_conta 			as id_conta
							FROM vuon_db1_gold.t_fat_cliente_cadastro c"""
        },
        "t_telefone":{
            "sql": """SELECT t.id_conta			as id_conta,
				             t.telefone_celular 		as fone_celular
				             --,t.telefone_residencial 	as fone_residencial
				             FROM vuon_db1_gold.t_fat_telefone t
                        """
        },
        "t_gecidade":{
            "sql": """SELECT h.seqcidade, h.Cepinicial, h.Cepfinal, h.Cidade FROM Consinco.Ge_Cidade@c5 h
                        WHERE h.codmunicipio IS NOT null"""
        },
        "sequence":{
            "sql": """select t.id_fila from crm.stg_sequence_vuon_integra t"""
        },
        "t_dependentes_raw":{
            "sql":"""SELECT td.id_dependente		as	id_dependente,
                            td.id_conta 		as	id_conta,
                            td.nm_dependente	as	Nomerazao,
                            td.dt_cadastro		as	Dtainclusao,
                            td.dt_nascimento	as	Dtanascfund,
                            td.fl_status		as	status,
                            td.fl_sexo			as	sexo,
                            td.nu_cpf			as	Cpfcnpj
                        FROM vuon_db1.t_dependente td"""
        }
    }


def config_task(name_task):
    consulta = tasks
    return consulta[name_task]