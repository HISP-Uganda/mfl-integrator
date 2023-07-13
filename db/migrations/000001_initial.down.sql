TRUNCATE users CASCADE ;
TRUNCATE user_role_permissions CASCADE;
TRUNCATE user_roles CASCADE ;

DROP TABLE IF EXISTS schedules;
DROP TABLE IF EXISTS blacklist;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS requests;
DROP TABLE IF EXISTS server_allowed_sources;
DROP TABLE IF EXISTS servers;
DROP TABLE IF EXISTS organisationunit;
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


DROP EXTENSION IF EXISTS xml2;
DROP EXTENSION IF EXISTS plpython3u;
DROP EXTENSION IF EXISTS pgcrypto;
