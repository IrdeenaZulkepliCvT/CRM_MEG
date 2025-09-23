{{ config(materialized='view') }}

with megjourney as (
    select _BPF_CONTACTID_VALUE, ACTIVESTAGESTARTEDON, BPF_DURATION 
    from {{ source('CRM_MEG_PRD', 'NTT_MEGCONSUMERJOURNEY') }}
)

SELECT
        c.CONTACTID as ContactID,
        megj.ACTIVESTAGESTARTEDON as Active_Stage_Started_On,
        megj.BPF_DURATION as Bpf_Duration,
    FROM {{ source('CRM_MEG_PRD', 'contact') }} c
    LEFT JOIN megjourney megj 
    ON c.CONTACTID = megj._BPF_CONTACTID_VALUE
    WHERE
        c.statecode = 0  