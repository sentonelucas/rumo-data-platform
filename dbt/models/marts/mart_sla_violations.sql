-- mart_sla_violations.sql
-- Mart analítico: visão consolidada de violações de SLA por cliente, segmento e período
-- Consumido diretamente pelo QuickSight e relatórios operacionais
-- Atualizado diariamente (D-1)

{{
  config(
    materialized = 'table',
    dist = 'cliente_nome',
    sort = ['data_particao', 'total_violacoes_desc'],
    tags = ['marts', 'gold', 'sla', 'bi']
  )
}}

with fato as (
    select * from {{ ref('fct_eventos_ferroviarios') }}
    where data_particao >= dateadd(day, -{{ var('sla_lookback_days', 90) }}, current_date)
),

-- Agregação por cliente + segmento + data
resumo_diario as (

    select
        data_particao,
        cliente_nome,
        segmento,
        tipo_vagao,

        -- Volume
        count(*)                                                as total_eventos,
        count(case when sla_violado then 1 end)                 as total_violacoes,
        count(case when parada_critica then 1 end)              as paradas_criticas,

        -- Métricas de duração
        round(avg(duracao_minutos), 2)                          as duracao_media_min,
        round(avg(pct_sla_utilizado), 2)                        as pct_sla_medio,
        max(pct_sla_utilizado)                                  as pct_sla_maximo,

        -- Taxa de violação
        round(
            count(case when sla_violado then 1 end) * 100.0 / nullif(count(*), 0),
            2
        )                                                       as taxa_violacao_pct

    from fato
    group by 1, 2, 3, 4

),

-- Ranking de clientes por violações no período
ranking as (
    select
        *,
        sum(total_violacoes) over (partition by cliente_nome)   as total_violacoes_cliente_periodo,
        row_number() over (
            order by sum(total_violacoes) over (partition by cliente_nome) desc
        )                                                       as total_violacoes_desc
    from resumo_diario
)

select * from ranking
