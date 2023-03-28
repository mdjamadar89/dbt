{{ config(tags=["every_night"]) }}

SELECT 
    users.object:voffice_id::VARCHAR AS voffice_id,
    users.object:site_id::VARCHAR AS site_id,
    contacts.object:id::INT AS contacts_id,
    contacts.object:third_party_id::VARCHAR AS third_party_id,
    contacts.object:first_name::VARCHAR AS first_name,
    contacts.object:last_name::VARCHAR AS last_name,
    contacts.object:email::VARCHAR AS email,
    contacts.object:sms_phone::VARCHAR AS sms_phone,
    contacts.object:phone::VARCHAR AS phone,
    contacts.object:phone2::VARCHAR AS phone2,
    contacts.object:address::VARCHAR AS "ADDRESS",
    contacts.object:address2::VARCHAR AS address2,
    contacts.object:city::VARCHAR AS city,
    contacts.object:state::VARCHAR AS "STATE",
    contacts.object:zip::VARCHAR AS zip,
    contacts.object:country::VARCHAR AS country,
    contacts.object:notes::VARCHAR AS notes,
    contacts.object:do_not_email_me::VARCHAR AS do_not_email_me,
    contacts.object:do_not_sms::VARCHAR AS do_not_sms,
    contacts.object:created::DATETIME AS contact_created,
    contacts.object:modified::DATETIME AS modified
FROM repsites.mysql_parquet_complete.contacts
    JOIN repsites.mysql_parquet_complete.users 
        ON contacts.object:user_id::INT = users.object:id::INT
       AND contacts.schema_name = users.schema_name
WHERE
    NOT(contacts.object:email::VARCHAR LIKE '%@soundconcepts.com')
    AND NOT(contacts.object:email::VARCHAR LIKE '%@myverb.com')
    AND NOT(contacts.object:email::VARCHAR LIKE '%@verb.tech')
    AND NOT(contacts.object:email::VARCHAR LIKE '%@f3code.com')
    AND NOT(contacts.object:email::VARCHAR LIKE '%test.com')
    AND NOT(users.object:email::VARCHAR LIKE '%@soundconcepts.com')
    AND NOT(users.object:email::VARCHAR LIKE '%myverb.com')
    AND NOT(users.object:email::VARCHAR LIKE '%verb.tech')
    AND NOT(users.object:email::VARCHAR LIKE '%@f3code.com')
    AND NOT(users.object:email::VARCHAR LIKE '%test.com')
ORDER BY users.object:site_id::VARCHAR, contacts.object:id::INT