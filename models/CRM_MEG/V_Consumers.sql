{{ config(materialized='view') }}

with 
caretype as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_caretype' and objecttypecode ='contact' and langid = 1033
)
,enrolsourse as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_meenrolmentsource' and objecttypecode ='contact' and langid = 1033
)
,businessunit as (
    select BUSINESSUNITID, NAME from {{ source('CRM_MEG_PRD', 'BUSINESSUNIT') }}
)
,account as (
    select * from {{ source('CRM_MEG_PRD', 'ACCOUNT') }}
)
,usergeo as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'elogic_contactusergeography' and objecttypecode ='contact' and langid = 1033
)
,consumertype as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_consumertype' and objecttypecode ='contact' and langid = 1033
)
,source_data as (

    SELECT
        c.CONTACTID as ContactID,
        care.val as ntt_caretype,
        contype.val as Consumer_Type,
        NTT_CURRENTBPFSTAGENAME,
        c.FULLNAME,
        ELOGIC_ME_PLUS,
        es.val as NTT_MEENROLMENTSOURCE,
        ELOGIC_ME_REGISTRATION_DATE,
        ELOGIC_DATE_OF_FIRST_SURGERY,
        c.CREATEDON,
        c.MODIFIEDON,
        crt.FULLNAME AS CREATEDBY,
        bu.NAME as OWNINGBUSINESSUNIT,
        c.ELOGIC_AGE,
        acc.NAME as ACCOUNT,
        geo.val as Country
    FROM {{ source('CRM_MEG_PRD', 'contact') }} c
    INNER JOIN {{ source('CRM_MEG_PRD', 'SYSTEMUSER') }} crt
    ON c._CREATEDBY_VALUE = crt.OWNERID
    LEFT join caretype care
    ON c.NTT_CARETYPE = care.ATTRIBUTEVALUE
    LEFT join enrolsourse es
    ON c.NTT_MEENROLMENTSOURCE = es.ATTRIBUTEVALUE
    inner join businessunit bu
    ON c._OWNINGBUSINESSUNIT_VALUE = bu.BUSINESSUNITID
    LEFT JOIN account acc
    ON c._ELOGIC_ACCOUNT_VALUE = acc.accountid
    inner JOIN usergeo geo
    ON c.ELOGIC_CONTACTUSERGEOGRAPHY = geo.ATTRIBUTEVALUE
    inner JOIN consumertype contype
    ON c.NTT_CONSUMERTYPE = contype.ATTRIBUTEVALUE
    WHERE c._fivetran_deleted = 'FALSE'
)
select * from source_data