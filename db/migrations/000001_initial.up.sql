CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS plpython3u;
CREATE EXTENSION xml2;

CREATE TABLE organisationunit(
    id BIGSERIAL NOT NULL PRIMARY KEY,
    uid TEXT NOT NULL DEFAULT '',
    code TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    parentid BIGSERIAL REFERENCES organisationunit(id),
    hierarchylevel INTEGER NOT NULL,
    path TEXT NOT NULL,
    address TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    phonenumber TEXT NOT NULL DEFAULT '',
    extras JSONB NOT NULL DEFAULT '{}'::jsonb,
    attributevalues JSONB NOT NULL DEFAULT '{}'::jsonb,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_roles (
      id BIGSERIAL NOT NULL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      description text DEFAULT '',
      created timestamptz DEFAULT CURRENT_TIMESTAMP,
      updated timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_role_permissions (
     id bigserial NOT NULL PRIMARY KEY,
     user_role BIGINT NOT NULL REFERENCES user_roles ON DELETE CASCADE ON UPDATE CASCADE,
     sys_module TEXT NOT NULL, -- the name of the module - defined above this level
     sys_perms VARCHAR(16) NOT NULL,
     created timestamptz DEFAULT CURRENT_TIMESTAMP,
     updated timestamptz DEFAULT CURRENT_TIMESTAMP,
     UNIQUE(sys_module, user_role)
);

CREATE TABLE IF NOT EXISTS users (
     id bigserial NOT NULL PRIMARY KEY,
     uid TEXT NOT NULL DEFAULT '',
     user_role  BIGINT NOT NULL REFERENCES user_roles ON DELETE RESTRICT ON UPDATE CASCADE,
     username TEXT NOT NULL UNIQUE,
     password TEXT NOT NULL, -- blowfish hash of password
     onetime_password TEXT,
     firstname TEXT NOT NULL,
     lastname TEXT NOT NULL,
     telephone TEXT NOT NULL DEFAULT '',
     email TEXT,
     is_active BOOLEAN NOT NULL DEFAULT 't',
     is_system_user BOOLEAN NOT NULL DEFAULT 'f',
     failed_attempts TEXT DEFAULT '0/'||to_char(NOW(),'YYYYmmdd'),
     transaction_limit TEXT DEFAULT '0/'||to_char(NOW(),'YYYYmmdd'),
     last_login timestamptz,
     created timestamptz DEFAULT CURRENT_TIMESTAMP,
     updated timestamptz DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE servers(
    id serial PRIMARY KEY NOT NULL,
    uid TEXT NOT NULL DEFAULT '',
    name text NOT NULL UNIQUE,
    username text NOT NULL DEFAULT '',
    password text NOT NULL DEFAULT '',
    auth_token text NOT NULL DEFAULT '',
    ipaddress text NOT NULL DEFAULT '',
    url text NOT NULL DEFAULT '', -- endpoint
    callback_url text NOT NULL DEFAULT '', -- url to call with response from endpoint
    cc_urls TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    http_method text NOT NULL DEFAULT 'POST',
    auth_method text NOT NULL DEFAULT '',
    allow_callbacks BOOLEAN NOT NULL DEFAULT 'f', --whether to make callbacks when destination app returns successfully
    allow_copies BOOLEAN NOT NULL DEFAULT 'f', --whether to allow copies to other servers
    use_async BOOLEAN NOT NULL DEFAULT 'f', -- whether to make async calls
    use_ssl BOOLEAN NOT NULL DEFAULT 'f', --whether ssl is enabled for this server/app
    parse_responses BOOLEAN NOT NULL DEFAULT 't', --whether to parse responses from this server/app
    ssl_client_certkey_file TEXT NOT NULL DEFAULT '',
    start_submission_period INTEGER NOT NULL DEFAULT 0, -- starting hour for off peak period
    end_submission_period INTEGER NOT NULL DEFAULT 24, -- ending hour for off peak period
    xml_response_xpath TEXT NOT NULL DEFAULT '',
    json_response_xpath TEXT NOT NULL DEFAULT '',
    suspended BOOLEAN NOT NULL DEFAULT 'f', --whether the app, sever or endpoint is suspended
    created timestamptz DEFAULT current_timestamp,
    updated timestamptz DEFAULT current_timestamp
);

CREATE INDEX servers_name ON servers(name);
CREATE INDEX servers_uid ON servers(uid);

CREATE TABLE server_allowed_sources(
    id serial PRIMARY KEY NOT NULL,
    server_id INTEGER NOT NULL REFERENCES servers(id),
    allowed_sources INTEGER[] NOT NULL DEFAULT ARRAY[]::INTEGER[],
    created timestamptz DEFAULT current_timestamp,
    updated timestamptz DEFAULT current_timestamp,
    UNIQUE(server_id)
);

CREATE TABLE requests(
     id bigserial PRIMARY KEY NOT NULL,
     uid VARCHAR(11) NOT NULL DEFAULT '',
     source INTEGER REFERENCES servers(id), -- source app/server
     destination INTEGER REFERENCES servers(id), -- source app/server
     batchid TEXT NOT NULL DEFAULT '',
     body TEXT NOT NULL DEFAULT '',
     response TEXT NOT NULL DEFAULT '',
     body_is_query_param BOOLEAN NOT NULL DEFAULT 'f',
     url_suffix TEXT DEFAULT '', -- if present, it is added to API url
     ctype TEXT NOT NULL DEFAULT '',
     status VARCHAR(32) NOT NULL DEFAULT 'ready' CHECK( status IN('pending', 'ready', 'inprogress', 'failed', 'error', 'expired', 'completed', 'canceled')),
     statuscode text DEFAULT '',
     retries INTEGER NOT NULL DEFAULT 0,
     errors TEXT DEFAULT '', -- indicative response message
     submissionid INTEGER NOT NULL DEFAULT 0, -- message_id in source app -> helpful when check for already sent submissions
     frequency_type TEXT NOT NULL DEFAULT '',
     period TEXT NOT NULL DEFAULT '', --whether ssl is enabled for this server/app
     week TEXT DEFAULT '', -- reporting week
     month TEXT DEFAULT '', -- reporting month
     year INTEGER, -- year of submission
     msisdn TEXT NOT NULL DEFAULT '', -- can be report sender in source
     raw_msg TEXT NOT NULL DEFAULT '', -- raw message in source system
     facility TEXT NOT NULL DEFAULT '', -- facility owning report
     district TEXT NOT NULL DEFAULT '', -- district
     report_type TEXT NOT NULL DEFAULT '',
     object_type TEXT NOT NULL DEFAULT '',
     extras TEXT NOT NULL DEFAULT '',
     suspended INT NOT NULL DEFAULT 0, --whether to suspend this request 0 = false, 1 = true
     created timestamptz DEFAULT current_timestamp,
     updated timestamptz DEFAULT current_timestamp
);

CREATE INDEX requests_submissionid ON requests(submissionid);
CREATE INDEX requests_status ON requests(status);
CREATE INDEX requests_statuscode ON requests(statuscode);
CREATE INDEX requests_batchid ON requests(batchid);
CREATE INDEX requests_created ON requests(created);
CREATE INDEX requests_updated ON requests(updated);
CREATE INDEX requests_msisdn ON requests(msisdn);
CREATE INDEX requests_facility ON requests(facility);
CREATE INDEX requests_district ON requests(district);
CREATE INDEX requests_ctype ON requests(ctype);
CREATE INDEX requests_uid ON requests(uid);

CREATE TABLE blacklist (
    id bigserial PRIMARY KEY,
    msisdn text NOT NULL,
    created timestamptz  NOT NULL DEFAULT current_timestamp,
    updated timestamptz DEFAULT current_timestamp
);
CREATE INDEX blacklist_msisdn ON blacklist(msisdn);
CREATE INDEX blacklist_created ON blacklist(created);
CREATE INDEX blacklist_updated ON blacklist(updated);

CREATE TABLE audit_log (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    logtype VARCHAR(32) NOT NULL DEFAULT '',
    actor TEXT NOT NULL,
    action text NOT NULL DEFAULT '',
    remote_ip INET,
    detail TEXT NOT NULL,
    created_by INTEGER REFERENCES users(id), -- like actor id
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX audit_log_created ON audit_log(created);
CREATE INDEX audit_log_logtype ON audit_log(logtype);
CREATE INDEX audit_log_action ON audit_log(action);

CREATE TABLE schedules(
    id bigserial NOT NULL PRIMARY KEY,
    sched_type TEXT NOT NULL DEFAULT 'sms' CHECK (sched_type IN ('sms', 'contact_push', 'url', 'command')), -- also 'push_contact'
    params JSON NOT NULL DEFAULT '{}'::json,
    sched_content TEXT, -- body of scheduled url call
    sched_url TEXT DEFAULT '',
    command TEXT DEFAULT '',
    command_args TEXT,
    first_run_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP, -- when to push first.
    repeat varchar(16) NOT NULL DEFAULT 'never' CHECK (repeat IN ('never','daily','weekly','monthly','yearly')),
    last_run_at  TIMESTAMPTZ, -- when last ran
    next_run_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status text NOT NULL DEFAULT 'ready' CHECK(status IN ('ready', 'skipped', 'sent','failed','error', 'completed')),
    is_active BOOLEAN NOT NULL DEFAULT 't',
    created_by INTEGER REFERENCES users(id),
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX schedules_created ON schedules(created);
CREATE INDEX schedules_first_run_at ON schedules(first_run_at);
CREATE INDEX schedules_last_run_at ON schedules(last_run_at);
CREATE INDEX schedules_next_run_at ON schedules(next_run_at);

-- FUNCTIONS
-- Check if source is an allowed 'source' for destination server/app dest
CREATE OR REPLACE FUNCTION is_allowed_source(source integer, dest integer) RETURNS BOOLEAN AS $delim$
DECLARE
    t boolean;
BEGIN
    select source = ANY(allowed_sources) INTO t FROM server_allowed_sources WHERE server_id = dest;
    RETURN t;
END;
$delim$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_server_apps(xid INT) RETURNS TEXT AS
$delim$
DECLARE
    r TEXT;
    p TEXT;
BEGIN
    r := '';
    FOR p IN SELECT name FROM servers WHERE id =
        ANY((select allowed_sources FROM server_allowed_sources WHERE server_id = xid)::INT[]) LOOP
            r := r || p || ',';
        END LOOP;
    RETURN rtrim(r,',');
END;
$delim$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION in_submission_period(server_id integer) RETURNS BOOLEAN AS $delim$
DECLARE
    t boolean;
BEGIN
    SELECT
                to_char(current_timestamp, 'HH24')::int >= start_submission_period
            AND
                to_char(current_timestamp, 'HH24')::int <= end_submission_period INTO t
    FROM servers WHERE id = server_id;
    RETURN t;
END;
$delim$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION xml_pretty(xml text)
    RETURNS xml AS $$
SELECT xslt_process($1,
        '<xsl:stylesheet version=\1.0\ xmlns:xsl=\http://www.w3.org/1999/XSL/Transform\>
        <xsl:strip-space elements=\*\ />
        <xsl:output method=\xml\ indent=\yes\ />
        <xsl:template match=\node() | @*\>
        <xsl:copy>
        <xsl:apply-templates select=\node() | @*\ />
        </xsl:copy>
        </xsl:template>
        </xsl:stylesheet>')::xml
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION is_valid_json(p_json text)
    RETURNS BOOLEAN
AS $$
BEGIN
    return (p_json::json is not null);
EXCEPTION WHEN OTHERS THEN
    return false;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION pp_json(j TEXT, sort_keys BOOLEAN = TRUE, indent TEXT = '    ')
    RETURNS TEXT AS $delim$
  import simplejson as json
  if not j:
      return ''
  return json.dumps(json.loads(j), sort_keys=sort_keys, indent=indent)
$delim$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION body_pprint(body text)
    RETURNS TEXT AS $$
BEGIN
    IF xml_is_well_formed_document(body) THEN
        return xml_pretty(body)::text;
    ELSIF is_valid_json(body) THEN
        return pp_json(body, 't', '    ');
    ELSE
        return body;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Data Follows
INSERT INTO servers (name, username, password, ipaddress, url, auth_method, auth_token)
VALUES
    ('localhost', 'tester', 'foobar', '127.0.0.1', 'http://localhost:8080/test', 'Basic Auth', ''),
    ('mtrack', 'tester', 'foobar', '127.0.0.1', 'http://localhost:8080/test', 'Basic Auth', ''),
    ('mtrackpro', 'tester', 'foobar', '127.0.0.1', 'http://localhost:8080/test', 'Basic Auth', ''),
    ('dhis2', 'admin', 'district', '127.0.0.1', 'http://localhost/api/dataValueSets', 'Token',
     'd2pat_yrpULZwF9iltNDB3SxCTqUxTchRK5Byx0832006526');

INSERT INTO user_roles(name, description)
VALUES('Administrator','For the Administrators'),
      ('SMS User', 'For SMS third party apps');

INSERT INTO user_role_permissions(user_role, sys_module,sys_perms)
VALUES
    ((SELECT id FROM user_roles WHERE name ='Administrator'),'Users','rmad');

INSERT INTO users(firstname,lastname,username,password,email,user_role,is_system_user)
VALUES
    ('Samuel','Sekiwere','admin',crypt('@dm1n',gen_salt('bf')),'sekiskylink@gmail.com',
     (SELECT id FROM user_roles WHERE name ='Administrator'),'t');

INSERT INTO server_allowed_sources (server_id, allowed_sources)
VALUES((SELECT id FROM servers where name = 'dhis2'),
       (SELECT array_agg(id) FROM servers WHERE name in ('localhost', 'mtrack', 'mtrackpro')));