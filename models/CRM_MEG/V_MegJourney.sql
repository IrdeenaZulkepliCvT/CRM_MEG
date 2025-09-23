{{ config(materialized='view') }}

with megjourney as (
    select _BPF_CONTACTID_VALUE, ACTIVESTAGESTARTEDON, BPF_DURATION 
    from {{ source('CRM_MEG_PRD', 'NTT_MEGCONSUMERJOURNEY') }}
),
caretype as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_caretype' and objecttypecode ='contact' and langid = 1033
)

SELECT
        c.CONTACTID as ContactID,
        care.val as ntt_caretype,
        megj.ACTIVESTAGESTARTEDON as Active_Stage_Started_On,
        megj.BPF_DURATION as Bpf_Duration,
    FROM {{ source('CRM_MEG_PRD', 'contact') }} c
    LEFT JOIN megjourney megj 
    ON c.CONTACTID = megj._BPF_CONTACTID_VALUE
    left join caretype care
    ON c.NTT_CARETYPE = care.ATTRIBUTEVALUE
    WHERE
        c.statecode = 0  