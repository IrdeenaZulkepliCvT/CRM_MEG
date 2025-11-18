{{ config(materialized='view') }}

with 

systemuser as (
    select * from {{ source('CRM_MEG_PRD', 'SYSTEMUSER') }}
)
, product as (
    select * from {{ source('CRM_MEG_PRD', 'product') }}
)

, productusagestatus as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_productusagestatus' and objecttypecode ='elogic_usedproduct' and langid = 1033
)

, outcome as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_outcome' and objecttypecode ='elogic_usedproduct' and langid = 1033
)

, caretype as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_caretype' and objecttypecode ='elogic_usedproduct' and langid = 1033
)
, medicalcondition as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_medicalcondition' and objecttypecode ='elogic_medicalcondition' and langid = 1033
)
, careprofileostomy as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_careprofileostomy' and objecttypecode ='elogic_medicalcondition' and langid = 1033
)
, careprofilecontinencecare as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_careprofilecontinencecare' and objecttypecode ='elogic_medicalcondition' and langid = 1033
)

, elogic_systemtype as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'elogic_system_type' and objecttypecode ='product' and langid = 1033
)
,usergeo as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'elogic_contactusergeography' and objecttypecode ='contact' and langid = 1033
)
,consumertype as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_consumertype' and objecttypecode ='contact' and langid = 1033
)
,changereason as (
    select ATTRIBUTEVALUE, VALUE as val from {{ source('CRM_MEG_PRD', 'stringmap') }}
    where attributename = 'ntt_reasonforchange' and objecttypecode ='elogic_usedproduct' and langid = 1033
)

,source_data as (

    SELECT
        ctc.CONTACTID as ContactID,
        ctc.FULLNAME as elogic_end_user, -- elogic_contact
        ctc.ELOGIC_ME_PLUS as elogic_me_plus,
        geo.val as Country,
        care.val as ntt_caretype,
        p1.NAME as elogic_product, -- elogic_product
        stat.val as ntt_productusagestatus,
        out.val as ntt_outcome,
        man.elogic_name as ntt_manufacturer, -- ntt_manufacturer
        eup.elogic_date_start_using,
        eup.elogic_usedproductid,
        cond.elogic_medicalconditionid as elogic_medicalconditionid,
        cond.ELOGIC_NAME as medicalcondition_elogic_name, -- ntt_condition
        exman.elogic_name as ntt_existingproductmanufacturer,  
        eup.ntt_confirmeddate,
        sys.FULLNAME as ntt_confirmeduser, -- ntt_confirmeduser,
        eup.elogic_date_stop,
        eup.createdon as createdon,
        crt.FULLNAME as createdby,
        p2.NAME AS ntt_currentproductname, -- ntt_currentproductname,
        eup.NTT_ISFIRSTPRODUCTFORCONDITION,
        stype.val as elogic_system_type,
        producttype.ELOGIC_NAME as elogic_product_type, -- product_type,
        cond.ELOGIC_SURGERY_DATE as elogic_surgery_date, -- elogic_surgery_date
        cond.NTT_NEWCATHETERUSERASOF as ntt_newcatheteruserasof, -- ntt_newcatheteruserasof
        nurse.FULLNAME as ntt_menurse, -- ntt_menurse 
        chg.val as Change_Reason,
        cpostomy.val as ntt_careprofileostomy,
        cpcontinencecare.val as ntt_careprofilecontinencecare,
        ctc.NTT_CURRENTBPFSTAGENAME,
        contype.val as Consumer_Type,
        eup.MODIFIEDON as ModifiedOn_Date
        FROM {{ source('CRM_MEG_PRD', 'elogic_usedproduct') }} eup
        LEFT JOIN product p1
        ON p1.PRODUCTID = eup._ELOGIC_PRODUCT_VALUE
        LEFT JOIN product p2
        ON p2.PRODUCTID = eup._NTT_CURRENTPRODUCTNAME_VALUE
        LEFT JOIN {{ source('CRM_MEG_PRD', 'elogic_manufacturers') }} man
        ON eup._NTT_MANUFACTURER_VALUE = man.ELOGIC_MANUFACTURERSID
        LEFT JOIN {{ source('CRM_MEG_PRD', 'elogic_manufacturers') }} exman
        ON eup._NTT_EXISTINGPRODUCTMANUFACTURER_VALUE = exman.ELOGIC_MANUFACTURERSID
        LEFT JOIN {{ source('CRM_MEG_PRD', 'contact') }} ctc
        ON eup._ELOGIC_CONTACT_VALUE = ctc.CONTACTID
        LEFT JOIN {{ source('CRM_MEG_PRD', 'elogic_medicalcondition') }}  cond
        ON ctc.CONTACTID = cond._ELOGIC_END_USER_VALUE
        LEFT JOIN systemuser sys
        ON eup._NTT_CONFIRMEDUSER_VALUE = sys.OWNERID
        LEFT JOIN systemuser crt
        ON eup._CREATEDBY_VALUE = crt.OWNERID
        LEFT JOIN {{ source('CRM_MEG_PRD', 'contact') }} nurse
        ON cond._NTT_NURSENAME_VALUE = nurse.CONTACTID
        LEFT JOIN {{ source('CRM_MEG_PRD', 'ELOGIC_PRODUCTTYPE') }} producttype
        ON p1._ELOGIC_PRODUCT_TYPEID_VALUE = producttype.ELOGIC_PRODUCTTYPEID
        LEFT JOIN productusagestatus stat
        ON eup.ntt_productusagestatus = stat.ATTRIBUTEVALUE
        LEFT JOIN outcome out 
        ON eup.ntt_outcome = out.ATTRIBUTEVALUE
        LEFT JOIN caretype care 
        ON eup.ntt_caretype = care.ATTRIBUTEVALUE
        LEFT JOIN careprofileostomy cpostomy 
        ON cond.NTT_CAREPROFILEOSTOMY = cpostomy.ATTRIBUTEVALUE
        LEFT JOIN careprofilecontinencecare cpcontinencecare
        ON cond.NTT_CAREPROFILECONTINENCECARE = cpcontinencecare.ATTRIBUTEVALUE
        LEFT JOIN elogic_systemtype stype 
        ON p1.ELOGIC_SYSTEM_TYPE = stype.ATTRIBUTEVALUE
        LEFT JOIN usergeo geo
        ON ctc.ELOGIC_CONTACTUSERGEOGRAPHY = geo.ATTRIBUTEVALUE
        LEFT JOIN changereason chg
        ON eup.NTT_REASONFORCHANGE = chg.ATTRIBUTEVALUE
        LEFT JOIN consumertype contype
        ON ctc.NTT_CONSUMERTYPE = contype.ATTRIBUTEVALUE
        WHERE
        eup.statecode = 0    
        and eup.elogic_used_products_user_geography in  ('961080006', '851750006')
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
