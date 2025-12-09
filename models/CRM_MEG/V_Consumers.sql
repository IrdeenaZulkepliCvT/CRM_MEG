{{ config(materialized="view") }}

with
    caretype as (
        select attributevalue, value as val
        from {{ source("CRM_MEG_PRD", "stringmap") }}
        where
            attributename = 'ntt_caretype'
            and objecttypecode = 'contact'
            and langid = 1033
    ),
    enrolsourse as (
        select attributevalue, value as val
        from {{ source("CRM_MEG_PRD", "stringmap") }}
        where
            attributename = 'ntt_meenrolmentsource'
            and objecttypecode = 'contact'
            and langid = 1033
    ),
    businessunit as (
        select businessunitid, name from {{ source("CRM_MEG_PRD", "BUSINESSUNIT") }}
    ),
    account as (select * from {{ source("CRM_MEG_PRD", "ACCOUNT") }}),
    usergeo as (
        select attributevalue, value as val
        from {{ source("CRM_MEG_PRD", "stringmap") }}
        where
            attributename = 'elogic_contactusergeography'
            and objecttypecode = 'contact'
            and langid = 1033
    ),
    consumertype as (
        select attributevalue, value as val
        from {{ source("CRM_MEG_PRD", "stringmap") }}
        where
            attributename = 'ntt_consumertype'
            and objecttypecode = 'contact'
            and langid = 1033
    ),
    source_data as (

        select
            c.contactid as contactid,
            care.val as ntt_caretype,
            contype.val as consumer_type,
            ntt_currentbpfstagename,
            c.fullname,
            elogic_me_plus,
            es.val as ntt_meenrolmentsource,
            elogic_me_registration_date,
            elogic_date_of_first_surgery,
            c.createdon,
            c.modifiedon,
            crt.fullname as createdby,
            bu.name as owningbusinessunit,
            c.elogic_age,
            acc.name as account,
            geo.val as country
        from {{ source("CRM_MEG_PRD", "contact") }} c
        left join
            {{ source("CRM_MEG_PRD", "SYSTEMUSER") }} crt
            on c._createdby_value = crt.ownerid
        inner join caretype care on c.ntt_caretype = care.attributevalue
        inner join enrolsourse es on c.ntt_meenrolmentsource = es.attributevalue
        inner join businessunit bu on c._owningbusinessunit_value = bu.businessunitid
        inner join account acc on c._elogic_account_value = acc.accountid
        inner join usergeo geo on c.elogic_contactusergeography = geo.attributevalue
        inner join consumertype contype on c.ntt_consumertype = contype.attributevalue
        where c.statecode = 0
    )
select *
from source_data
