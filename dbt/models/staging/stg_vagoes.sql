-- stg_vagoes.sql
-- Dimensão de vagões: dados cadastrais (SCD Tipo 1 — sobrescreve)

{{
  config(
    materialized = 'view',
    tags = ['staging', 'dimensao']
  )
}}

with source as (
    select * from {{ source('rumo_silver', 'vagoes') }}
),

renamed as (
    select
        vagao_id,
        tipo_vagao,
        cast(capacidade_ton as decimal(10, 2))   as capacidade_ton,
        cast(ano_fabricacao as integer)          as ano_fabricacao,
        fabricante,
        cast(ultima_revisao as date)             as data_ultima_revisao,
        ativo,
        _ingested_at                             as atualizado_em
    from source
    where vagao_id is not null
      and ativo = true
)

select * from renamed
