TRUNCATE users CASCADE ;
TRUNCATE user_role_permissions CASCADE;
TRUNCATE user_roles CASCADE ;

DROP TABLE IF EXISTS schedules;
DROP TABLE IF EXISTS blacklist;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS requests;
DROP TABLE IF EXISTS server_allowed_sources;
DROP TABLE IF EXISTS servers;
DROP TABLE IF EXISTS sync_log;
DROP TABLE IF EXISTS orgunitfailure;
DROP TABLE IF EXISTS orgunitrevision;
DROP TABLE IF EXISTS orgunitgroupmembers;
DROP TABLE IF EXISTS organisationunit ;
DROP TABLE IF EXISTS orgunitgroup;
DROP TABLE IF EXISTS orgunitlevel;
DROP TABLE IF EXISTS attribute;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS user_role_permissions;
DROP TABLE IF EXISTS user_roles;

DROP FUNCTION IF EXISTS body_pprint ( text);
DROP FUNCTION IF EXISTS pp_json ( text, boolean, text);
DROP FUNCTION IF EXISTS is_valid_json ( text) ;
DROP FUNCTION IF EXISTS xml_pretty ( text);
DROP FUNCTION IF EXISTS in_submission_period ( integer);
DROP FUNCTION IF EXISTS get_server_apps (integer);
DROP FUNCTION IF EXISTS is_allowed_source (integer, integer);
DROP FUNCTION IF EXISTS jsonb_diff_val(jsonb, jsonb);
DROP FUNCTION IF EXISTS create_requests_cc_status(integer[]);
DROP FUNCTION IF EXISTS after_request_insert_trigger_function();
DROP FUNCTION IF EXISTS status_of_dependence(bigint);
DROP FUNCTION IF EXISTS failed_cc_servers(integer[], jsonb);
DROP FUNCTION IF EXISTS reset_request_cc_server_retries(bigint, integer[]);


DROP EXTENSION IF EXISTS xml2;
DROP EXTENSION IF EXISTS plpython3u;
DROP EXTENSION IF EXISTS pgcrypto;
DROP EXTENSION IF EXISTS postgis CASCADE;
DROP TABLE IF EXISTS "spatial_ref_sys" CASCADE;
