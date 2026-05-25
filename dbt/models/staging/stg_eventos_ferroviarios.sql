-- stg_eventos_ferroviarios.sql
-- Staging: lê direto do silver zone via Redshift Spectrum (Iceberg)
-- Responsabilidade: renomear colunas para snake_case, cast de tipos, remover nulos críticos.
-- NÃO aplica regras de negócio — isso é responsabilidade do mart.

{{
  config(
    materialized = 'view',
    tags = ['staging', 'operacional']
  )
}}

with source as (

    -- Lê da camada silver via Spectrum (S3 + Iceberg)
    -- A view externa já está mapeada no Glue Catalog → Spectrum
    select * from {{ source('rumo_silver', 'fato_eventos_ferroviarios') }}

),

renamed as (

    select
        -- Chaves
        evento_id                                   as evento_id,
        vagao_id                                    as vagao_id,
        contrato_id                                 as contrato_id,

        -- Status normalizado (já veio uppercase do silver)
        status_norm                                 as status_evento,

        -- Timestamps com cast explícito
        cast(data_inicio as timestamp)              as iniciado_em,
        cast(data_fim    as timestamp)              as finalizado_em,

        -- Métricas calculadas
        cast(duracao_minutos as decimal(10, 2))     as duracao_minutos,

        -- Dados do vagão (enriquecidos no silver)
        tipo_vagao,
        cast(capacidade_ton as decimal(10, 2))      as capacidade_ton,
        cast(ano_fabricacao as integer)             as vagao_ano_fabricacao,

        -- Dados do contrato
        cliente_nome,
        cast(sla_horas as decimal(6, 2))            as sla_horas_contrato,
        segmento_cliente                            as segmento,

        -- Flag de violação (boolean limpo)
        cast(sla_violado as boolean)                as sla_violado,

        -- Metadados de pipeline
        _partition_date                             as data_particao,
        _ingested_at                                as ingerido_em,
        _silver_processed_at                        as processado_em

    from source

    where
        -- Remove registros sem chave primária (barrado pela DQ, mas defensivo)
        evento_id is not null
        -- Remove registros com datas inválidas
        and data_inicio is not null
        and (data_fim is null or data_fim >= data_inicio)

)

select * from renamed
