CREATE TABLE used_uids (
    id SERIAL PRIMARY KEY NOT NULL,
    uid VARCHAR(11) NOT NULL,
    mfluid TEXT NOT NULL,
    created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (uid, mfluid)
);

CREATE INDEX used_uids_mfluid ON used_uids(mfluid);