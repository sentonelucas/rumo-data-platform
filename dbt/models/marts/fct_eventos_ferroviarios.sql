-- fct_eventos_ferroviarios.sql
-- Tabela fato principal: eventos ferroviários para analytics e BI
--
-- Estratégia de materialização:
--   - incremental: só processa dados novos/alterados (merge by evento_id)
--   - lookback de 3 dias para garantir idempotência com dados atrasados
--   - Particionado por data_particao para queries filtradas por data

{{
  config(
    materialized = 'incremental',
    unique_key = 'evento_id',
    incremental_strategy = 'merge',
    dist = 'vagao_sk',
    sort = ['data_particao', 'status_evento'],
    tags = ['marts', 'gold', 'core'],
    on_schema_change = 'append_new_columns'
  )
}}

with

eventos as (
    select * from {{ ref('stg_eventos_ferroviarios') }}
    {% if is_incremental() %}
    -- Em runs incrementais, reprocessa os últimos N dias para capturar late-arriving data
    where data_particao >= dateadd(day, -{{ var('incremental_lookback_days', 3) }}, current_date)
    {% endif %}
),

vagoes as (
    select * from {{ ref('dim_vagoes') }}
),

contratos as (
    select * from {{ ref('dim_contratos') }}
),

dim_data as (
    select * from {{ ref('dim_data') }}
),

-- Gera surrogate keys para joins performáticos no Redshift
joined as (

    select
        -- Surrogate key (hash determinístico para idempotência)
        {{ dbt_utils.generate_surrogate_key(['e.evento_id']) }}    as evento_sk,

        -- Natural key (para debugging e rastreabilidade)
        e.evento_id,

        -- FKs para dimensões
        coalesce(v.vagao_sk, -1)                                    as vagao_sk,
        coalesce(c.contrato_sk, -1)                                 as contrato_sk,
        coalesce(d.data_sk, -1)                                     as data_sk,

        -- Atributos do evento
        e.status_evento,
        e.iniciado_em,
        e.finalizado_em,
        e.duracao_minutos,

        -- Métricas derivadas
        case
            when e.duracao_minutos >= 60 then round(e.duracao_minutos / 60.0, 2)
            else null
        end                                                          as duracao_horas,

        case
            when e.sla_horas_contrato > 0
            then round(e.duracao_minutos / (e.sla_horas_contrato * 60) * 100, 2)
            else null
        end                                                          as pct_sla_utilizado,

        -- Flags de negócio
        e.sla_violado,

        case
            when e.status_evento in ('PARADO', 'MANUTENCAO')
                 and e.duracao_minutos > 120
            then true
            else false
        end                                                          as parada_critica,

        -- Atributos desnormalizados (performance de queries BI)
        e.cliente_nome,
        e.segmento,
        e.tipo_vagao,
        e.capacidade_ton,
        e.sla_horas_contrato,

        -- Partição
        e.data_particao,

        -- Metadados de auditoria
        e.ingerido_em,
        e.processado_em,
        current_timestamp as dbt_updated_at

    from eventos e
    left join vagoes v
        on e.vagao_id = v.vagao_id
    left join contratos c
        on e.contrato_id = c.contrato_id
    left join dim_data d
        on e.data_particao = d.data_completa

)

select * from joined
