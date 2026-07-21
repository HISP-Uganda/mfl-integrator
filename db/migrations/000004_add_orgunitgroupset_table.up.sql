CREATE TABLE orgunitgroupset
(
    id              BIGSERIAL NOT NULL PRIMARY KEY,
    uid             TEXT      NOT NULL UNIQUE,
    code            VARCHAR(50) UNIQUE,
    name            TEXT      NOT NULL DEFAULT '',
    shortname       TEXT      NOT NULL DEFAULT '',
    description     TEXT      NOT NULL DEFAULT '',
    attributevalues JSONB              DEFAULT '{}'::jsonb,
    created         TIMESTAMPTZ        DEFAULT CURRENT_TIMESTAMP,
    updated         TIMESTAMPTZ        DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orgunitgroupsetmembers
(
    orgunitgroupsetid BIGSERIAL REFERENCES orgunitgroupset (id),
    orgunitgroupid     SERIAL REFERENCES orgunitgroup (id),
    created            TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated            TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (orgunitgroupsetid, orgunitgroupid)

);