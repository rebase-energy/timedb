--
-- PostgreSQL database dump
--

\restrict hfkqOBU6Xv96cifuwF4m3Dyo2zXvBozobnq18t2Mc1XBCg9fEwMgpYf64IZHTU3

-- Dumped from database version 18.1 (Ubuntu 18.1-1.pgdg22.04+2)
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: tsdbadmin
--

-- *not* creating schema, since initdb creates it


ALTER SCHEMA public OWNER TO tsdbadmin;

--
-- Name: timescaledb; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';


--
-- Name: timescale_functions; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA timescale_functions;


ALTER SCHEMA timescale_functions OWNER TO postgres;

--
-- Name: SCHEMA timescale_functions; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA timescale_functions IS 'This schema contains helper functions for Timescale Cloud';


--
-- Name: timescaledb_toolkit; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb_toolkit; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION timescaledb_toolkit IS 'Library of analytical hyperfunctions, time-series pipelining, and other SQL utilities';


--
-- Name: pg_buffercache; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_buffercache WITH SCHEMA public;


--
-- Name: EXTENSION pg_buffercache; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_buffercache IS 'examine the shared buffer cache';


--
-- Name: pg_stat_statements; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public;


--
-- Name: EXTENSION pg_stat_statements; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_stat_statements IS 'track planning and execution statistics of all SQL statements executed';


--
-- Name: postgres_fdw; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;


--
-- Name: EXTENSION postgres_fdw; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgres_fdw IS 'foreign-data wrapper for remote PostgreSQL servers';


--
-- Name: create_playground(regclass, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.create_playground(src_hypertable regclass, compressed boolean DEFAULT false) RETURNS text
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $_$
DECLARE
    _table_name NAME;
    _schema_name NAME;
    _src_relation NAME;
    _playground_table_fqn NAME;
    _chunk_name NAME;
    _chunk_check BOOL;
    _playground_schema_check BOOL;
    _next_id INTEGER;
    _dimension TEXT;
    _interval TEXT;
    _segmentby_cols TEXT;
    _orderby_cols TEXT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM information_schema.schemata
    WHERE schema_name = 'tsdb_playground') INTO _playground_schema_check;

    IF NOT _playground_schema_check THEN
        RAISE EXCEPTION '"tsdb_playground" schema must be created before running this';
    END IF;

    -- get schema and table name
    SELECT n.nspname, c.relname INTO _schema_name, _table_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
    INNER JOIN timescaledb_information.hypertables i ON (i.hypertable_name = c.relname )
    WHERE c.oid = src_hypertable;

    IF _table_name IS NULL THEN
        RAISE EXCEPTION '% is not a hypertable', src_hypertable;
    END IF;

    SELECT EXISTS(SELECT 1 FROM timescaledb_information.chunks WHERE hypertable_name = _table_name AND hypertable_schema = _schema_name) INTO _chunk_check;

    IF NOT _chunk_check THEN
        RAISE EXCEPTION '% has no chunks for playground testing', src_hypertable;
    END IF;

    EXECUTE pg_catalog.format($$ CREATE SEQUENCE IF NOT EXISTS tsdb_playground.%I $$, _table_name||'_seq');
    SELECT pg_catalog.nextval('tsdb_playground.' || pg_catalog.quote_ident(_table_name || '_seq')) INTO _next_id;

    SELECT pg_catalog.format('%I.%I', _schema_name, _table_name) INTO _src_relation;

    SELECT pg_catalog.format('tsdb_playground.%I', _table_name || '_' || _next_id::text) INTO _playground_table_fqn;

    EXECUTE pg_catalog.format(
        $$ CREATE TABLE %s (like %s including comments including constraints including defaults including indexes) $$
        , _playground_table_fqn, _src_relation
        );

    -- get dimension column from src ht for partitioning playground ht
    SELECT column_name, time_interval INTO _dimension, _interval FROM timescaledb_information.dimensions WHERE hypertable_name = _table_name AND hypertable_schema = _schema_name LIMIT 1;

    PERFORM public.create_hypertable(_playground_table_fqn::REGCLASS, _dimension::NAME, chunk_time_interval := _interval::interval);

    -- Ideally, it should pick up the latest complete chunk (second last chunk) from this hypertable.
    -- If num_chunks > 1 then it will get true, converted into 1, taking the second row, otherwise it'll get false converted to 0 and get no offset.
    SELECT
        format('%I.%I',chunk_schema,chunk_name)
    INTO STRICT
        _chunk_name
    FROM
        timescaledb_information.chunks
    WHERE
        hypertable_schema = _schema_name AND
        hypertable_name = _table_name
    ORDER BY
        chunk_creation_time DESC OFFSET (
            SELECT
                (num_chunks > 1)::integer
            FROM timescaledb_information.hypertables
            WHERE
                hypertable_name = _table_name)
    LIMIT 1;
	EXECUTE pg_catalog.format($$ INSERT INTO %s SELECT * FROM %s $$, _playground_table_fqn, _chunk_name);

    IF compressed THEN
        --retrieve compression settings from source hypertable
        SELECT segmentby INTO _segmentby_cols
        FROM timescaledb_information.hypertable_compression_settings
        WHERE hypertable = _src_relation::REGCLASS;

		SELECT orderby INTO _orderby_cols
		FROM timescaledb_information.hypertable_compression_settings
        WHERE hypertable = _src_relation::REGCLASS;

        IF (_segmentby_cols IS NOT NULL) AND (_orderby_cols IS NOT NULL) THEN
            EXECUTE pg_catalog.format(
                $$ ALTER TABLE %s SET(timescaledb.compress, timescaledb.compress_segmentby = %I, timescaledb.compress_orderby = %I) $$
                , _playground_table_fqn, _segmentby_cols, _orderby_cols
                );
        ELSE
            EXECUTE pg_catalog.format(
                $$ ALTER TABLE %s SET(timescaledb.compress) $$
                , _playground_table_fqn
                );
        END IF;
        -- get playground chunk and compress
    PERFORM public.compress_chunk(public.show_chunks(_playground_table_fqn::REGCLASS));
    END IF;

	RETURN _playground_table_fqn;
END
$_$;


ALTER FUNCTION public.create_playground(src_hypertable regclass, compressed boolean) OWNER TO postgres;

--
-- Name: update_users_updated_at(); Type: FUNCTION; Schema: public; Owner: tsdbadmin
--

CREATE FUNCTION public.update_users_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_users_updated_at() OWNER TO tsdbadmin;

--
-- Name: create_bare_readonly_role(text, text); Type: FUNCTION; Schema: timescale_functions; Owner: postgres
--

CREATE FUNCTION timescale_functions.create_bare_readonly_role(role_name text, role_password text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
BEGIN
  -- Sanity checks
  IF pg_catalog.current_setting('server_version_num')::integer < 160000 THEN
    RAISE EXCEPTION 'this function should be called only on pg version 16 and higher, your version is %s', pg_catalog.version();
  END IF;

  IF pg_catalog.to_regrole(role_name) IS NOT NULL THEN
    RAISE EXCEPTION 'role % already exists', role_name;
  END IF;

  IF pg_catalog.to_regrole('bare_roles') IS NULL THEN
    RAISE EXCEPTION 'role bare_roles does not exist';
  END IF;

  -- Create the role with password
  EXECUTE pg_catalog.format('CREATE ROLE %I LOGIN PASSWORD %L', role_name, role_password);

  -- Add to the bare_roles tracking group
  EXECUTE pg_catalog.format('GRANT bare_roles TO %I', role_name);

  -- Set the read-only GUC
  EXECUTE pg_catalog.format('ALTER ROLE %I SET tsdb_admin.read_only_role = true', role_name);
END;
$$;


ALTER FUNCTION timescale_functions.create_bare_readonly_role(role_name text, role_password text) OWNER TO postgres;

--
-- Name: FUNCTION create_bare_readonly_role(role_name text, role_password text); Type: COMMENT; Schema: timescale_functions; Owner: postgres
--

COMMENT ON FUNCTION timescale_functions.create_bare_readonly_role(role_name text, role_password text) IS 'Creates a bare role (not a member of or administered by tsdbadmin) that can be granted tsdbadmin privileges without circular dependency';


--
-- Name: delete_bare_readonly_role(text); Type: FUNCTION; Schema: timescale_functions; Owner: postgres
--

CREATE FUNCTION timescale_functions.delete_bare_readonly_role(role_name text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
DECLARE
  is_member BOOLEAN;
BEGIN
  -- Sanity checks
  IF pg_catalog.current_setting('server_version_num')::integer < 160000 THEN
    RAISE EXCEPTION 'this function should be called only on pg version 16 and higher, your version is %s', pg_catalog.version();
  END IF;

  IF pg_catalog.to_regrole(role_name) IS NULL THEN
    RAISE EXCEPTION 'role % does not exist', role_name;
  END IF;

  IF pg_catalog.to_regrole('bare_roles') IS NULL THEN
    RAISE EXCEPTION 'role bare_roles does not exist';
  END IF;

  -- Verify the role is a member of bare_roles before deletion
  SELECT pg_catalog.pg_has_role(role_name, 'bare_roles', 'USAGE') INTO is_member;

  IF NOT is_member THEN
    RAISE EXCEPTION 'role % is not a bare read-only role (not a member of bare_roles)', role_name;
  END IF;

  -- Drop the role (this will fail if the role owns objects or has dependent privileges)
  EXECUTE pg_catalog.format('DROP ROLE %I', role_name);
END;
$$;


ALTER FUNCTION timescale_functions.delete_bare_readonly_role(role_name text) OWNER TO postgres;

--
-- Name: FUNCTION delete_bare_readonly_role(role_name text); Type: COMMENT; Schema: timescale_functions; Owner: postgres
--

COMMENT ON FUNCTION timescale_functions.delete_bare_readonly_role(role_name text) IS 'Safely deletes a bare role (not a member of or administered by tsdbadmin) by verifying it is a member of the bare_roles tracking group';


--
-- Name: grant_tsdbadmin_to_role(text); Type: FUNCTION; Schema: timescale_functions; Owner: postgres
--

CREATE FUNCTION timescale_functions.grant_tsdbadmin_to_role(rolename text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
BEGIN
  IF pg_catalog.current_setting('server_version_num')::integer < 160000 THEN
    RAISE EXCEPTION 'this function should be called only on pg version 16 and higher, your version is %s', pg_catalog.version();
  END IF;

  IF pg_catalog.to_regrole(rolename) IS NULL THEN
    RAISE EXCEPTION 'role %s does not exist', rolename;
  END IF;

  IF pg_catalog.to_regrole('tsdbadmin') IS NULL THEN
    RAISE EXCEPTION 'role tsdbadmin does not exist';
  END IF;

  EXECUTE pg_catalog.format('GRANT tsdbadmin TO %I WITH INHERIT TRUE, SET TRUE', rolename);
END;
$$;


ALTER FUNCTION timescale_functions.grant_tsdbadmin_to_role(rolename text) OWNER TO postgres;

--
-- Name: FUNCTION grant_tsdbadmin_to_role(rolename text); Type: COMMENT; Schema: timescale_functions; Owner: postgres
--

COMMENT ON FUNCTION timescale_functions.grant_tsdbadmin_to_role(rolename text) IS 'This role grants tsdbadmin (database owner) to another role';


--
-- Name: move_createrole(regrole, regrole); Type: FUNCTION; Schema: timescale_functions; Owner: postgres
--

CREATE FUNCTION timescale_functions.move_createrole(old_role regrole, new_role regrole) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
DECLARE
  role_member RECORD;
BEGIN
  -- sanity checks
  IF pg_catalog.current_setting('server_version_num')::integer < 160000 THEN
    RAISE EXCEPTION 'this function should be called only on pg version 16 and higher, your version is %s', pg_catalog.version();
  END IF;

  IF pg_catalog.to_regrole(old_role::text) IS NULL
  THEN
    RAISE EXCEPTION 'old role % does not exist', old_role;
  END IF;
  IF pg_catalog.to_regrole(new_role::text) IS NULL
  THEN
    RAISE EXCEPTION 'new role % does not exist', new_role;
  END IF;

  IF (SELECT rolcreaterole FROM pg_catalog.pg_roles where rolname = old_role::text) IS FALSE
  THEN
    RAISE EXCEPTION 'role % does is not CREATEROLE', old_role;
  END IF;
  IF (SELECT rolcreaterole FROM pg_catalog.pg_roles where rolname = new_role::text) IS TRUE
  THEN
    RAISE EXCEPTION 'role % is already CREATEROLE', new_role;
  END IF;
  -- for each member role of the old role, move the members to the new role
  -- so that new role will be able to administer roles created by the old role
  FOR role_member IN
    SELECT
      roleid::regrole,
      admin_option,
      inherit_option,
      set_option
  FROM
    pg_catalog.pg_auth_members
  WHERE
    member = old_role AND
    roleid::regrole != old_role AND
    roleid::regrole != new_role
  LOOP
      EXECUTE format(
          'GRANT %I TO %I WITH %s, %s, %s',
          role_member.roleid,
          new_role,
          CASE WHEN role_member.admin_option THEN 'ADMIN OPTION' ELSE 'ADMIN FALSE' END,
          CASE WHEN role_member.inherit_option THEN 'INHERIT TRUE' ELSE 'INHERIT FALSE' END,
          CASE WHEN role_member.set_option THEN 'SET TRUE' ELSE 'SET FALSE' END
              );
      EXECUTE format('REVOKE %I FROM %I', role_member.roleid, old_role
              );
    END LOOP;
    -- without revoking new role from the old one, the roles created by the new role
    -- will still contain (indirectly) the old one, and the old one (tsdbadmin) would
    -- be non-grantable to them.
    EXECUTE format('REVOKE %I FROM %I', new_role, old_role);

    EXECUTE format('ALTER ROLE %I CREATEROLE', new_role);
    EXECUTE format('ALTER ROLE %I NOCREATEROLE', old_role);
    -- make sure new role can transfer its CREATEROLE, but old role not.
    EXECUTE format('GRANT EXECUTE ON FUNCTION timescale_functions.move_createrole TO %I', new_role);
    EXECUTE format('REVOKE EXECUTE ON FUNCTION timescale_functions.move_createrole FROM %I', old_role);
    EXECUTE format('REVOKE EXECUTE ON FUNCTION timescale_functions.grant_tsdbadmin_to_role FROM %I', old_role);
    EXECUTE format('GRANT EXECUTE ON FUNCTION timescale_functions.grant_tsdbadmin_to_role TO %I', new_role);
    EXECUTE format('REVOKE EXECUTE ON FUNCTION timescale_functions.revoke_tsdbadmin_from_role FROM %I', old_role);
    EXECUTE format('GRANT EXECUTE ON FUNCTION timescale_functions.revoke_tsdbadmin_from_role TO %I', new_role);
END;
$$;


ALTER FUNCTION timescale_functions.move_createrole(old_role regrole, new_role regrole) OWNER TO postgres;

--
-- Name: FUNCTION move_createrole(old_role regrole, new_role regrole); Type: COMMENT; Schema: timescale_functions; Owner: postgres
--

COMMENT ON FUNCTION timescale_functions.move_createrole(old_role regrole, new_role regrole) IS 'This function allows moving createrole privileges between existing createrole, and another user';


--
-- Name: revoke_tsdbadmin_from_role(text); Type: FUNCTION; Schema: timescale_functions; Owner: postgres
--

CREATE FUNCTION timescale_functions.revoke_tsdbadmin_from_role(rolename text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
BEGIN
  IF pg_catalog.current_setting('server_version_num')::integer < 160000 THEN
    RAISE EXCEPTION 'this function should be called only on pg version 16 and higher, your version is %s', pg_catalog.version();
  END IF;

  IF pg_catalog.to_regrole(rolename) IS NULL THEN
    RAISE EXCEPTION 'role %s does not exist', rolename;
  END IF;

  IF pg_catalog.to_regrole('tsdbadmin') IS NULL THEN
    RAISE EXCEPTION 'role tsdbadmin does not exist';
  END IF;

  EXECUTE pg_catalog.format('REVOKE tsdbadmin FROM %I', rolename);
END;
$$;


ALTER FUNCTION timescale_functions.revoke_tsdbadmin_from_role(rolename text) OWNER TO postgres;

--
-- Name: FUNCTION revoke_tsdbadmin_from_role(rolename text); Type: COMMENT; Schema: timescale_functions; Owner: postgres
--

COMMENT ON FUNCTION timescale_functions.revoke_tsdbadmin_from_role(rolename text) IS 'This role revokes tsdbadmin (database owner) to another role';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _compressed_hypertable_133; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_133 (
);


ALTER TABLE _timescaledb_internal._compressed_hypertable_133 OWNER TO tsdbadmin;

--
-- Name: _compressed_hypertable_134; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_134 (
);


ALTER TABLE _timescaledb_internal._compressed_hypertable_134 OWNER TO tsdbadmin;

--
-- Name: _compressed_hypertable_135; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_135 (
);


ALTER TABLE _timescaledb_internal._compressed_hypertable_135 OWNER TO tsdbadmin;

--
-- Name: _compressed_hypertable_136; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_136 (
);


ALTER TABLE _timescaledb_internal._compressed_hypertable_136 OWNER TO tsdbadmin;

--
-- Name: projections_long; Type: TABLE; Schema: public; Owner: tsdbadmin
--

CREATE TABLE public.projections_long (
    projection_id bigint NOT NULL,
    batch_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    series_id uuid NOT NULL,
    valid_time timestamp with time zone NOT NULL,
    valid_time_end timestamp with time zone,
    value double precision,
    known_time timestamp with time zone NOT NULL,
    annotation text,
    metadata jsonb,
    tags text[],
    changed_by text,
    change_time timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT annotation_not_empty_long CHECK (((annotation IS NULL) OR (length(btrim(annotation)) > 0))),
    CONSTRAINT tags_not_empty_array_long CHECK (((tags IS NULL) OR (cardinality(tags) > 0))),
    CONSTRAINT valid_time_interval_check_long CHECK (((valid_time_end IS NULL) OR (valid_time_end > valid_time)))
);


ALTER TABLE public.projections_long OWNER TO tsdbadmin;

--
-- Name: _direct_view_137; Type: VIEW; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE VIEW _timescaledb_internal._direct_view_137 AS
 SELECT tenant_id,
    series_id,
    public.time_bucket('00:01:00'::interval, valid_time) AS bucket_time,
    public.last(value, known_time) AS value,
    public.last(batch_id, known_time) AS source_batch_id,
    max(known_time) AS generated_at
   FROM public.projections_long
  GROUP BY tenant_id, series_id, (public.time_bucket('00:01:00'::interval, valid_time));


ALTER VIEW _timescaledb_internal._direct_view_137 OWNER TO tsdbadmin;

--
-- Name: projections_medium; Type: TABLE; Schema: public; Owner: tsdbadmin
--

CREATE TABLE public.projections_medium (
    projection_id bigint NOT NULL,
    batch_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    series_id uuid NOT NULL,
    valid_time timestamp with time zone NOT NULL,
    valid_time_end timestamp with time zone,
    value double precision,
    known_time timestamp with time zone NOT NULL,
    annotation text,
    metadata jsonb,
    tags text[],
    changed_by text,
    change_time timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT annotation_not_empty_med CHECK (((annotation IS NULL) OR (length(btrim(annotation)) > 0))),
    CONSTRAINT tags_not_empty_array_med CHECK (((tags IS NULL) OR (cardinality(tags) > 0))),
    CONSTRAINT valid_time_interval_check_med CHECK (((valid_time_end IS NULL) OR (valid_time_end > valid_time)))
);


ALTER TABLE public.projections_medium OWNER TO tsdbadmin;

--
-- Name: _direct_view_138; Type: VIEW; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE VIEW _timescaledb_internal._direct_view_138 AS
 SELECT tenant_id,
    series_id,
    public.time_bucket('00:01:00'::interval, valid_time) AS bucket_time,
    public.last(value, known_time) AS value,
    public.last(batch_id, known_time) AS source_batch_id,
    max(known_time) AS generated_at
   FROM public.projections_medium
  GROUP BY tenant_id, series_id, (public.time_bucket('00:01:00'::interval, valid_time));


ALTER VIEW _timescaledb_internal._direct_view_138 OWNER TO tsdbadmin;

--
-- Name: projections_short; Type: TABLE; Schema: public; Owner: tsdbadmin
--

CREATE TABLE public.projections_short (
    projection_id bigint NOT NULL,
    batch_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    series_id uuid NOT NULL,
    valid_time timestamp with time zone NOT NULL,
    valid_time_end timestamp with time zone,
    value double precision,
    known_time timestamp with time zone NOT NULL,
    annotation text,
    metadata jsonb,
    tags text[],
    changed_by text,
    change_time timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT annotation_not_empty_short CHECK (((annotation IS NULL) OR (length(btrim(annotation)) > 0))),
    CONSTRAINT tags_not_empty_array_short CHECK (((tags IS NULL) OR (cardinality(tags) > 0))),
    CONSTRAINT valid_time_interval_check_short CHECK (((valid_time_end IS NULL) OR (valid_time_end > valid_time)))
);


ALTER TABLE public.projections_short OWNER TO tsdbadmin;

--
-- Name: _direct_view_139; Type: VIEW; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE VIEW _timescaledb_internal._direct_view_139 AS
 SELECT tenant_id,
    series_id,
    public.time_bucket('00:01:00'::interval, valid_time) AS bucket_time,
    public.last(value, known_time) AS value,
    public.last(batch_id, known_time) AS source_batch_id,
    max(known_time) AS generated_at
   FROM public.projections_short
  GROUP BY tenant_id, series_id, (public.time_bucket('00:01:00'::interval, valid_time));


ALTER VIEW _timescaledb_internal._direct_view_139 OWNER TO tsdbadmin;

--
-- Name: actuals; Type: TABLE; Schema: public; Owner: tsdbadmin
--

CREATE TABLE public.actuals (
    actual_id bigint NOT NULL,
    tenant_id uuid NOT NULL,
    series_id uuid NOT NULL,
    valid_time timestamp with time zone NOT NULL,
    valid_time_end timestamp with time zone,
    value double precision,
    annotation text,
    metadata jsonb,
    tags text[],
    changed_by text,
    change_time timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT annotation_not_empty_actuals CHECK (((annotation IS NULL) OR (length(btrim(annotation)) > 0))),
    CONSTRAINT tags_not_empty_array_actuals CHECK (((tags IS NULL) OR (cardinality(tags) > 0))),
    CONSTRAINT valid_time_interval_check_actuals CHECK (((valid_time_end IS NULL) OR (valid_time_end > valid_time)))
);


ALTER TABLE public.actuals OWNER TO tsdbadmin;

--
-- Name: _hyper_129_42_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal._hyper_129_42_chunk (
    CONSTRAINT constraint_29 CHECK (((valid_time >= '2024-12-26 00:00:00+00'::timestamp with time zone) AND (valid_time < '2025-01-02 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.actuals);


ALTER TABLE _timescaledb_internal._hyper_129_42_chunk OWNER TO tsdbadmin;

--
-- Name: _materialized_hypertable_137; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal._materialized_hypertable_137 (
    tenant_id uuid,
    series_id uuid,
    bucket_time timestamp with time zone,
    value double precision,
    source_batch_id uuid,
    generated_at timestamp with time zone
);


ALTER TABLE _timescaledb_internal._materialized_hypertable_137 OWNER TO tsdbadmin;

--
-- Name: _materialized_hypertable_138; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal._materialized_hypertable_138 (
    tenant_id uuid,
    series_id uuid,
    bucket_time timestamp with time zone,
    value double precision,
    source_batch_id uuid,
    generated_at timestamp with time zone
);


ALTER TABLE _timescaledb_internal._materialized_hypertable_138 OWNER TO tsdbadmin;

--
-- Name: _materialized_hypertable_139; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal._materialized_hypertable_139 (
    tenant_id uuid,
    series_id uuid,
    bucket_time timestamp with time zone,
    value double precision,
    source_batch_id uuid,
    generated_at timestamp with time zone
);


ALTER TABLE _timescaledb_internal._materialized_hypertable_139 OWNER TO tsdbadmin;

--
-- Name: _partial_view_137; Type: VIEW; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE VIEW _timescaledb_internal._partial_view_137 AS
 SELECT tenant_id,
    series_id,
    public.time_bucket('00:01:00'::interval, valid_time) AS bucket_time,
    public.last(value, known_time) AS value,
    public.last(batch_id, known_time) AS source_batch_id,
    max(known_time) AS generated_at
   FROM public.projections_long
  GROUP BY tenant_id, series_id, (public.time_bucket('00:01:00'::interval, valid_time));


ALTER VIEW _timescaledb_internal._partial_view_137 OWNER TO tsdbadmin;

--
-- Name: _partial_view_138; Type: VIEW; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE VIEW _timescaledb_internal._partial_view_138 AS
 SELECT tenant_id,
    series_id,
    public.time_bucket('00:01:00'::interval, valid_time) AS bucket_time,
    public.last(value, known_time) AS value,
    public.last(batch_id, known_time) AS source_batch_id,
    max(known_time) AS generated_at
   FROM public.projections_medium
  GROUP BY tenant_id, series_id, (public.time_bucket('00:01:00'::interval, valid_time));


ALTER VIEW _timescaledb_internal._partial_view_138 OWNER TO tsdbadmin;

--
-- Name: _partial_view_139; Type: VIEW; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE VIEW _timescaledb_internal._partial_view_139 AS
 SELECT tenant_id,
    series_id,
    public.time_bucket('00:01:00'::interval, valid_time) AS bucket_time,
    public.last(value, known_time) AS value,
    public.last(batch_id, known_time) AS source_batch_id,
    max(known_time) AS generated_at
   FROM public.projections_short
  GROUP BY tenant_id, series_id, (public.time_bucket('00:01:00'::interval, valid_time));


ALTER VIEW _timescaledb_internal._partial_view_139 OWNER TO tsdbadmin;

--
-- Name: compress_hyper_133_43_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE TABLE _timescaledb_internal.compress_hyper_133_43_chunk (
    _ts_meta_count integer,
    tenant_id uuid,
    series_id uuid,
    actual_id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    valid_time _timescaledb_internal.compressed_data,
    valid_time_end _timescaledb_internal.compressed_data,
    value _timescaledb_internal.compressed_data,
    annotation _timescaledb_internal.compressed_data,
    metadata _timescaledb_internal.compressed_data,
    tags _timescaledb_internal.compressed_data,
    changed_by _timescaledb_internal.compressed_data,
    change_time _timescaledb_internal.compressed_data,
    inserted_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN tenant_id SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN series_id SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN actual_id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN valid_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN valid_time_end SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN value SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN annotation SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN annotation SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN metadata SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN metadata SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN tags SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN tags SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN changed_by SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN changed_by SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN change_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_133_43_chunk ALTER COLUMN inserted_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_133_43_chunk OWNER TO tsdbadmin;

--
-- Name: actuals_actual_id_seq; Type: SEQUENCE; Schema: public; Owner: tsdbadmin
--

CREATE SEQUENCE public.actuals_actual_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.actuals_actual_id_seq OWNER TO tsdbadmin;

--
-- Name: actuals_actual_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tsdbadmin
--

ALTER SEQUENCE public.actuals_actual_id_seq OWNED BY public.actuals.actual_id;


--
-- Name: all_projections_raw; Type: VIEW; Schema: public; Owner: tsdbadmin
--

CREATE VIEW public.all_projections_raw AS
 SELECT projections_long.projection_id,
    projections_long.batch_id,
    projections_long.tenant_id,
    projections_long.series_id,
    projections_long.valid_time,
    projections_long.valid_time_end,
    projections_long.value,
    projections_long.known_time,
    projections_long.annotation,
    projections_long.metadata,
    projections_long.tags,
    projections_long.changed_by,
    projections_long.change_time,
    'long'::text AS tier
   FROM public.projections_long
UNION ALL
 SELECT projections_medium.projection_id,
    projections_medium.batch_id,
    projections_medium.tenant_id,
    projections_medium.series_id,
    projections_medium.valid_time,
    projections_medium.valid_time_end,
    projections_medium.value,
    projections_medium.known_time,
    projections_medium.annotation,
    projections_medium.metadata,
    projections_medium.tags,
    projections_medium.changed_by,
    projections_medium.change_time,
    'medium'::text AS tier
   FROM public.projections_medium
UNION ALL
 SELECT projections_short.projection_id,
    projections_short.batch_id,
    projections_short.tenant_id,
    projections_short.series_id,
    projections_short.valid_time,
    projections_short.valid_time_end,
    projections_short.value,
    projections_short.known_time,
    projections_short.annotation,
    projections_short.metadata,
    projections_short.tags,
    projections_short.changed_by,
    projections_short.change_time,
    'short'::text AS tier
   FROM public.projections_short;


ALTER VIEW public.all_projections_raw OWNER TO tsdbadmin;

--
-- Name: batches_table; Type: TABLE; Schema: public; Owner: tsdbadmin
--

CREATE TABLE public.batches_table (
    batch_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    workflow_id text,
    batch_start_time timestamp with time zone,
    batch_finish_time timestamp with time zone,
    known_time timestamp with time zone DEFAULT now() NOT NULL,
    batch_params jsonb,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT batch_params_is_object CHECK (((batch_params IS NULL) OR (jsonb_typeof(batch_params) = 'object'::text)))
);


ALTER TABLE public.batches_table OWNER TO tsdbadmin;

--
-- Name: latest_projections_long; Type: VIEW; Schema: public; Owner: tsdbadmin
--

CREATE VIEW public.latest_projections_long AS
 SELECT _materialized_hypertable_137.tenant_id,
    _materialized_hypertable_137.series_id,
    _materialized_hypertable_137.bucket_time,
    _materialized_hypertable_137.value,
    _materialized_hypertable_137.source_batch_id,
    _materialized_hypertable_137.generated_at
   FROM _timescaledb_internal._materialized_hypertable_137
  WHERE (_materialized_hypertable_137.bucket_time < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(137)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT projections_long.tenant_id,
    projections_long.series_id,
    public.time_bucket('00:01:00'::interval, projections_long.valid_time) AS bucket_time,
    public.last(projections_long.value, projections_long.known_time) AS value,
    public.last(projections_long.batch_id, projections_long.known_time) AS source_batch_id,
    max(projections_long.known_time) AS generated_at
   FROM public.projections_long
  WHERE (projections_long.valid_time >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(137)), '-infinity'::timestamp with time zone))
  GROUP BY projections_long.tenant_id, projections_long.series_id, (public.time_bucket('00:01:00'::interval, projections_long.valid_time));


ALTER VIEW public.latest_projections_long OWNER TO tsdbadmin;

--
-- Name: latest_projections_medium; Type: VIEW; Schema: public; Owner: tsdbadmin
--

CREATE VIEW public.latest_projections_medium AS
 SELECT _materialized_hypertable_138.tenant_id,
    _materialized_hypertable_138.series_id,
    _materialized_hypertable_138.bucket_time,
    _materialized_hypertable_138.value,
    _materialized_hypertable_138.source_batch_id,
    _materialized_hypertable_138.generated_at
   FROM _timescaledb_internal._materialized_hypertable_138
  WHERE (_materialized_hypertable_138.bucket_time < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(138)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT projections_medium.tenant_id,
    projections_medium.series_id,
    public.time_bucket('00:01:00'::interval, projections_medium.valid_time) AS bucket_time,
    public.last(projections_medium.value, projections_medium.known_time) AS value,
    public.last(projections_medium.batch_id, projections_medium.known_time) AS source_batch_id,
    max(projections_medium.known_time) AS generated_at
   FROM public.projections_medium
  WHERE (projections_medium.valid_time >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(138)), '-infinity'::timestamp with time zone))
  GROUP BY projections_medium.tenant_id, projections_medium.series_id, (public.time_bucket('00:01:00'::interval, projections_medium.valid_time));


ALTER VIEW public.latest_projections_medium OWNER TO tsdbadmin;

--
-- Name: latest_projections_short; Type: VIEW; Schema: public; Owner: tsdbadmin
--

CREATE VIEW public.latest_projections_short AS
 SELECT _materialized_hypertable_139.tenant_id,
    _materialized_hypertable_139.series_id,
    _materialized_hypertable_139.bucket_time,
    _materialized_hypertable_139.value,
    _materialized_hypertable_139.source_batch_id,
    _materialized_hypertable_139.generated_at
   FROM _timescaledb_internal._materialized_hypertable_139
  WHERE (_materialized_hypertable_139.bucket_time < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(139)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT projections_short.tenant_id,
    projections_short.series_id,
    public.time_bucket('00:01:00'::interval, projections_short.valid_time) AS bucket_time,
    public.last(projections_short.value, projections_short.known_time) AS value,
    public.last(projections_short.batch_id, projections_short.known_time) AS source_batch_id,
    max(projections_short.known_time) AS generated_at
   FROM public.projections_short
  WHERE (projections_short.valid_time >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(139)), '-infinity'::timestamp with time zone))
  GROUP BY projections_short.tenant_id, projections_short.series_id, (public.time_bucket('00:01:00'::interval, projections_short.valid_time));


ALTER VIEW public.latest_projections_short OWNER TO tsdbadmin;

--
-- Name: latest_projection_curve; Type: VIEW; Schema: public; Owner: tsdbadmin
--

CREATE VIEW public.latest_projection_curve AS
 SELECT latest_projections_long.tenant_id,
    latest_projections_long.series_id,
    latest_projections_long.bucket_time,
    latest_projections_long.value,
    latest_projections_long.source_batch_id,
    latest_projections_long.generated_at,
    'long'::text AS tier
   FROM public.latest_projections_long
UNION ALL
 SELECT latest_projections_medium.tenant_id,
    latest_projections_medium.series_id,
    latest_projections_medium.bucket_time,
    latest_projections_medium.value,
    latest_projections_medium.source_batch_id,
    latest_projections_medium.generated_at,
    'medium'::text AS tier
   FROM public.latest_projections_medium
UNION ALL
 SELECT latest_projections_short.tenant_id,
    latest_projections_short.series_id,
    latest_projections_short.bucket_time,
    latest_projections_short.value,
    latest_projections_short.source_batch_id,
    latest_projections_short.generated_at,
    'short'::text AS tier
   FROM public.latest_projections_short;


ALTER VIEW public.latest_projection_curve OWNER TO tsdbadmin;

--
-- Name: projections_long_projection_id_seq; Type: SEQUENCE; Schema: public; Owner: tsdbadmin
--

CREATE SEQUENCE public.projections_long_projection_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.projections_long_projection_id_seq OWNER TO tsdbadmin;

--
-- Name: projections_long_projection_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tsdbadmin
--

ALTER SEQUENCE public.projections_long_projection_id_seq OWNED BY public.projections_long.projection_id;


--
-- Name: projections_medium_projection_id_seq; Type: SEQUENCE; Schema: public; Owner: tsdbadmin
--

CREATE SEQUENCE public.projections_medium_projection_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.projections_medium_projection_id_seq OWNER TO tsdbadmin;

--
-- Name: projections_medium_projection_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tsdbadmin
--

ALTER SEQUENCE public.projections_medium_projection_id_seq OWNED BY public.projections_medium.projection_id;


--
-- Name: projections_short_projection_id_seq; Type: SEQUENCE; Schema: public; Owner: tsdbadmin
--

CREATE SEQUENCE public.projections_short_projection_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.projections_short_projection_id_seq OWNER TO tsdbadmin;

--
-- Name: projections_short_projection_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tsdbadmin
--

ALTER SEQUENCE public.projections_short_projection_id_seq OWNED BY public.projections_short.projection_id;


--
-- Name: series_table; Type: TABLE; Schema: public; Owner: tsdbadmin
--

CREATE TABLE public.series_table (
    series_id uuid NOT NULL,
    name text NOT NULL,
    unit text NOT NULL,
    labels jsonb DEFAULT '{}'::jsonb NOT NULL,
    description text,
    data_class text DEFAULT 'projection'::text NOT NULL,
    storage_tier text DEFAULT 'medium'::text NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT series_name_not_empty CHECK ((length(btrim(name)) > 0)),
    CONSTRAINT valid_data_class CHECK ((data_class = ANY (ARRAY['actual'::text, 'projection'::text]))),
    CONSTRAINT valid_storage_tier CHECK ((storage_tier = ANY (ARRAY['short'::text, 'medium'::text, 'long'::text])))
);


ALTER TABLE public.series_table OWNER TO tsdbadmin;

--
-- Name: users_table; Type: TABLE; Schema: public; Owner: tsdbadmin
--

CREATE TABLE public.users_table (
    user_id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    email text NOT NULL,
    api_key text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT api_key_not_empty CHECK ((length(btrim(api_key)) > 0)),
    CONSTRAINT email_not_empty CHECK ((length(btrim(email)) > 0))
);


ALTER TABLE public.users_table OWNER TO tsdbadmin;

--
-- Name: _hyper_129_42_chunk actual_id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: tsdbadmin
--

ALTER TABLE ONLY _timescaledb_internal._hyper_129_42_chunk ALTER COLUMN actual_id SET DEFAULT nextval('public.actuals_actual_id_seq'::regclass);


--
-- Name: _hyper_129_42_chunk change_time; Type: DEFAULT; Schema: _timescaledb_internal; Owner: tsdbadmin
--

ALTER TABLE ONLY _timescaledb_internal._hyper_129_42_chunk ALTER COLUMN change_time SET DEFAULT now();


--
-- Name: _hyper_129_42_chunk inserted_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: tsdbadmin
--

ALTER TABLE ONLY _timescaledb_internal._hyper_129_42_chunk ALTER COLUMN inserted_at SET DEFAULT now();


--
-- Name: actuals actual_id; Type: DEFAULT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.actuals ALTER COLUMN actual_id SET DEFAULT nextval('public.actuals_actual_id_seq'::regclass);


--
-- Name: projections_long projection_id; Type: DEFAULT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_long ALTER COLUMN projection_id SET DEFAULT nextval('public.projections_long_projection_id_seq'::regclass);


--
-- Name: projections_medium projection_id; Type: DEFAULT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_medium ALTER COLUMN projection_id SET DEFAULT nextval('public.projections_medium_projection_id_seq'::regclass);


--
-- Name: projections_short projection_id; Type: DEFAULT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_short ALTER COLUMN projection_id SET DEFAULT nextval('public.projections_short_projection_id_seq'::regclass);


--
-- Name: _hyper_129_42_chunk 42_46_actuals_tenant_id_series_id_valid_time_key; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: tsdbadmin
--

ALTER TABLE ONLY _timescaledb_internal._hyper_129_42_chunk
    ADD CONSTRAINT "42_46_actuals_tenant_id_series_id_valid_time_key" UNIQUE (tenant_id, series_id, valid_time);


--
-- Name: actuals actuals_tenant_id_series_id_valid_time_key; Type: CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.actuals
    ADD CONSTRAINT actuals_tenant_id_series_id_valid_time_key UNIQUE (tenant_id, series_id, valid_time);


--
-- Name: batches_table batches_table_pkey; Type: CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.batches_table
    ADD CONSTRAINT batches_table_pkey PRIMARY KEY (batch_id);


--
-- Name: series_table series_identity_uniq; Type: CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.series_table
    ADD CONSTRAINT series_identity_uniq UNIQUE (name, labels);


--
-- Name: series_table series_table_pkey; Type: CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.series_table
    ADD CONSTRAINT series_table_pkey PRIMARY KEY (series_id);


--
-- Name: users_table users_email_tenant_unique; Type: CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.users_table
    ADD CONSTRAINT users_email_tenant_unique UNIQUE (tenant_id, email);


--
-- Name: users_table users_table_api_key_key; Type: CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.users_table
    ADD CONSTRAINT users_table_api_key_key UNIQUE (api_key);


--
-- Name: users_table users_table_pkey; Type: CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.users_table
    ADD CONSTRAINT users_table_pkey PRIMARY KEY (user_id);


--
-- Name: _hyper_129_42_chunk_actuals_valid_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _hyper_129_42_chunk_actuals_valid_time_idx ON _timescaledb_internal._hyper_129_42_chunk USING btree (valid_time DESC);


--
-- Name: _materialized_hypertable_137_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_137_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_137 USING btree (bucket_time DESC);


--
-- Name: _materialized_hypertable_137_series_id_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_137_series_id_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_137 USING btree (series_id, bucket_time DESC);


--
-- Name: _materialized_hypertable_137_tenant_id_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_137_tenant_id_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_137 USING btree (tenant_id, bucket_time DESC);


--
-- Name: _materialized_hypertable_138_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_138_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_138 USING btree (bucket_time DESC);


--
-- Name: _materialized_hypertable_138_series_id_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_138_series_id_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_138 USING btree (series_id, bucket_time DESC);


--
-- Name: _materialized_hypertable_138_tenant_id_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_138_tenant_id_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_138 USING btree (tenant_id, bucket_time DESC);


--
-- Name: _materialized_hypertable_139_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_139_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_139 USING btree (bucket_time DESC);


--
-- Name: _materialized_hypertable_139_series_id_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_139_series_id_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_139 USING btree (series_id, bucket_time DESC);


--
-- Name: _materialized_hypertable_139_tenant_id_bucket_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX _materialized_hypertable_139_tenant_id_bucket_time_idx ON _timescaledb_internal._materialized_hypertable_139 USING btree (tenant_id, bucket_time DESC);


--
-- Name: compress_hyper_133_43_chunk_series_id_tenant_id__ts_meta_mi_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: tsdbadmin
--

CREATE INDEX compress_hyper_133_43_chunk_series_id_tenant_id__ts_meta_mi_idx ON _timescaledb_internal.compress_hyper_133_43_chunk USING btree (series_id, tenant_id, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC);


--
-- Name: actuals_valid_time_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX actuals_valid_time_idx ON public.actuals USING btree (valid_time DESC);


--
-- Name: batches_tenant_known_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX batches_tenant_known_idx ON public.batches_table USING btree (tenant_id, known_time DESC);


--
-- Name: long_lookup_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX long_lookup_idx ON public.projections_long USING btree (tenant_id, series_id, valid_time, known_time DESC);


--
-- Name: medium_lookup_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX medium_lookup_idx ON public.projections_medium USING btree (tenant_id, series_id, valid_time, known_time DESC);


--
-- Name: projections_long_valid_time_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX projections_long_valid_time_idx ON public.projections_long USING btree (valid_time DESC);


--
-- Name: projections_medium_valid_time_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX projections_medium_valid_time_idx ON public.projections_medium USING btree (valid_time DESC);


--
-- Name: projections_short_valid_time_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX projections_short_valid_time_idx ON public.projections_short USING btree (valid_time DESC);


--
-- Name: series_labels_gin_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX series_labels_gin_idx ON public.series_table USING gin (labels);


--
-- Name: short_lookup_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX short_lookup_idx ON public.projections_short USING btree (tenant_id, series_id, valid_time, known_time DESC);


--
-- Name: users_api_key_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX users_api_key_idx ON public.users_table USING btree (api_key) WHERE (is_active = true);


--
-- Name: users_email_tenant_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX users_email_tenant_idx ON public.users_table USING btree (tenant_id, email);


--
-- Name: users_tenant_id_idx; Type: INDEX; Schema: public; Owner: tsdbadmin
--

CREATE INDEX users_tenant_id_idx ON public.users_table USING btree (tenant_id);


--
-- Name: users_table users_updated_at_trigger; Type: TRIGGER; Schema: public; Owner: tsdbadmin
--

CREATE TRIGGER users_updated_at_trigger BEFORE UPDATE ON public.users_table FOR EACH ROW EXECUTE FUNCTION public.update_users_updated_at();


--
-- Name: _hyper_129_42_chunk 42_45_actuals_series_id_fkey; Type: FK CONSTRAINT; Schema: _timescaledb_internal; Owner: tsdbadmin
--

ALTER TABLE ONLY _timescaledb_internal._hyper_129_42_chunk
    ADD CONSTRAINT "42_45_actuals_series_id_fkey" FOREIGN KEY (series_id) REFERENCES public.series_table(series_id) ON DELETE CASCADE;


--
-- Name: actuals actuals_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.actuals
    ADD CONSTRAINT actuals_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_table(series_id) ON DELETE CASCADE;


--
-- Name: projections_long projections_long_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_long
    ADD CONSTRAINT projections_long_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.batches_table(batch_id) ON DELETE CASCADE;


--
-- Name: projections_long projections_long_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_long
    ADD CONSTRAINT projections_long_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_table(series_id) ON DELETE CASCADE;


--
-- Name: projections_medium projections_medium_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_medium
    ADD CONSTRAINT projections_medium_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.batches_table(batch_id) ON DELETE CASCADE;


--
-- Name: projections_medium projections_medium_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_medium
    ADD CONSTRAINT projections_medium_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_table(series_id) ON DELETE CASCADE;


--
-- Name: projections_short projections_short_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_short
    ADD CONSTRAINT projections_short_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.batches_table(batch_id) ON DELETE CASCADE;


--
-- Name: projections_short projections_short_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tsdbadmin
--

ALTER TABLE ONLY public.projections_short
    ADD CONSTRAINT projections_short_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series_table(series_id) ON DELETE CASCADE;


--
-- Name: SCHEMA _timescaledb_internal; Type: ACL; Schema: -; Owner: postgres
--

REVOKE USAGE ON SCHEMA _timescaledb_internal FROM tsdbadmin;
GRANT USAGE ON SCHEMA _timescaledb_internal TO tsdbadmin;
GRANT CREATE ON SCHEMA _timescaledb_internal TO tsdbadmin WITH GRANT OPTION;


--
-- Name: SCHEMA timescale_functions; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA timescale_functions TO tsdbadmin;
GRANT USAGE ON SCHEMA timescale_functions TO PUBLIC;


--
-- Name: TABLE continuous_agg; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.continuous_agg TO tsdbadmin;


--
-- Name: TABLE continuous_agg_migrate_plan_step; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT,UPDATE ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step TO tsdbadmin;


--
-- Name: TABLE chunk_constraint; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.chunk_constraint TO tsdbadmin;


--
-- Name: FUNCTION pg_reload_conf(); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_reload_conf() TO tsdbadmin WITH GRANT OPTION;


--
-- Name: FUNCTION pg_replication_origin_advance(text, pg_lsn); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_advance(text, pg_lsn) TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_create(text); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_create(text) TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_drop(text); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_drop(text) TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_oid(text); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_oid(text) TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_progress(text, boolean); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_progress(text, boolean) TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_session_is_setup(); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_session_is_setup() TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_session_progress(boolean); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_session_progress(boolean) TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_session_reset(); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_session_reset() TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_session_setup(text); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_session_setup(text) TO ts_logical;


--
-- Name: FUNCTION pg_replication_origin_xact_setup(pg_lsn, timestamp with time zone); Type: ACL; Schema: pg_catalog; Owner: postgres
--

GRANT ALL ON FUNCTION pg_catalog.pg_replication_origin_xact_setup(pg_lsn, timestamp with time zone) TO ts_logical;


--
-- Name: FUNCTION timescaledb_post_restore(); Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON FUNCTION public.timescaledb_post_restore() FROM PUBLIC;
GRANT ALL ON FUNCTION public.timescaledb_post_restore() TO tsdbadmin;


--
-- Name: FUNCTION timescaledb_pre_restore(); Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON FUNCTION public.timescaledb_pre_restore() FROM PUBLIC;
GRANT ALL ON FUNCTION public.timescaledb_pre_restore() TO tsdbadmin;


--
-- Name: FUNCTION create_bare_readonly_role(role_name text, role_password text); Type: ACL; Schema: timescale_functions; Owner: postgres
--

REVOKE ALL ON FUNCTION timescale_functions.create_bare_readonly_role(role_name text, role_password text) FROM PUBLIC;
GRANT ALL ON FUNCTION timescale_functions.create_bare_readonly_role(role_name text, role_password text) TO tsdbadmin;


--
-- Name: FUNCTION delete_bare_readonly_role(role_name text); Type: ACL; Schema: timescale_functions; Owner: postgres
--

REVOKE ALL ON FUNCTION timescale_functions.delete_bare_readonly_role(role_name text) FROM PUBLIC;
GRANT ALL ON FUNCTION timescale_functions.delete_bare_readonly_role(role_name text) TO tsdbadmin;


--
-- Name: FUNCTION grant_tsdbadmin_to_role(rolename text); Type: ACL; Schema: timescale_functions; Owner: postgres
--

REVOKE ALL ON FUNCTION timescale_functions.grant_tsdbadmin_to_role(rolename text) FROM PUBLIC;
GRANT ALL ON FUNCTION timescale_functions.grant_tsdbadmin_to_role(rolename text) TO tsdbadmin;


--
-- Name: FUNCTION move_createrole(old_role regrole, new_role regrole); Type: ACL; Schema: timescale_functions; Owner: postgres
--

REVOKE ALL ON FUNCTION timescale_functions.move_createrole(old_role regrole, new_role regrole) FROM PUBLIC;
GRANT ALL ON FUNCTION timescale_functions.move_createrole(old_role regrole, new_role regrole) TO tsdbadmin;


--
-- Name: FUNCTION revoke_tsdbadmin_from_role(rolename text); Type: ACL; Schema: timescale_functions; Owner: postgres
--

REVOKE ALL ON FUNCTION timescale_functions.revoke_tsdbadmin_from_role(rolename text) FROM PUBLIC;
GRANT ALL ON FUNCTION timescale_functions.revoke_tsdbadmin_from_role(rolename text) TO tsdbadmin;


--
-- Name: FOREIGN DATA WRAPPER postgres_fdw; Type: ACL; Schema: -; Owner: postgres
--

GRANT ALL ON FOREIGN DATA WRAPPER postgres_fdw TO tsdbadmin;


--
-- Name: TABLE chunk; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.chunk TO tsdbadmin;


--
-- Name: TABLE chunk_column_stats; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.chunk_column_stats TO tsdbadmin;


--
-- Name: SEQUENCE chunk_column_stats_id_seq; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT UPDATE ON SEQUENCE _timescaledb_catalog.chunk_column_stats_id_seq TO tsdbadmin;


--
-- Name: SEQUENCE chunk_constraint_name; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT UPDATE ON SEQUENCE _timescaledb_catalog.chunk_constraint_name TO tsdbadmin;


--
-- Name: SEQUENCE chunk_id_seq; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT UPDATE ON SEQUENCE _timescaledb_catalog.chunk_id_seq TO tsdbadmin;


--
-- Name: TABLE compression_chunk_size; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.compression_chunk_size TO tsdbadmin;


--
-- Name: TABLE compression_settings; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.compression_settings TO tsdbadmin;


--
-- Name: TABLE continuous_agg_migrate_plan; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT,UPDATE ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan TO tsdbadmin;


--
-- Name: SEQUENCE continuous_agg_migrate_plan_step_step_id_seq; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT UPDATE ON SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq TO tsdbadmin;


--
-- Name: TABLE continuous_aggs_bucket_function; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.continuous_aggs_bucket_function TO tsdbadmin;


--
-- Name: TABLE continuous_aggs_hypertable_invalidation_log; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log TO tsdbadmin;


--
-- Name: TABLE continuous_aggs_invalidation_threshold; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold TO tsdbadmin;


--
-- Name: TABLE continuous_aggs_materialization_invalidation_log; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log TO tsdbadmin;


--
-- Name: TABLE continuous_aggs_materialization_ranges; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges TO tsdbadmin;


--
-- Name: TABLE continuous_aggs_watermark; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.continuous_aggs_watermark TO tsdbadmin;


--
-- Name: TABLE dimension; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.dimension TO tsdbadmin;


--
-- Name: SEQUENCE dimension_id_seq; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT UPDATE ON SEQUENCE _timescaledb_catalog.dimension_id_seq TO tsdbadmin;


--
-- Name: TABLE dimension_slice; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.dimension_slice TO tsdbadmin;


--
-- Name: SEQUENCE dimension_slice_id_seq; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT UPDATE ON SEQUENCE _timescaledb_catalog.dimension_slice_id_seq TO tsdbadmin;


--
-- Name: TABLE hypertable; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.hypertable TO tsdbadmin;


--
-- Name: SEQUENCE hypertable_id_seq; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT UPDATE ON SEQUENCE _timescaledb_catalog.hypertable_id_seq TO tsdbadmin;


--
-- Name: TABLE metadata; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT,UPDATE ON TABLE _timescaledb_catalog.metadata TO tsdbadmin;


--
-- Name: TABLE tablespace; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.tablespace TO tsdbadmin;


--
-- Name: TABLE telemetry_event; Type: ACL; Schema: _timescaledb_catalog; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_catalog.telemetry_event TO tsdbadmin;


--
-- Name: TABLE bgw_job; Type: ACL; Schema: _timescaledb_config; Owner: postgres
--

GRANT INSERT ON TABLE _timescaledb_config.bgw_job TO tsdbadmin;


--
-- Name: SEQUENCE bgw_job_id_seq; Type: ACL; Schema: _timescaledb_config; Owner: postgres
--

GRANT UPDATE ON SEQUENCE _timescaledb_config.bgw_job_id_seq TO tsdbadmin;


--
-- Name: TABLE bgw_job_stat_history; Type: ACL; Schema: _timescaledb_internal; Owner: postgres
--

GRANT INSERT,DELETE,TRUNCATE ON TABLE _timescaledb_internal.bgw_job_stat_history TO tsdbadmin;
GRANT SELECT ON TABLE _timescaledb_internal.bgw_job_stat_history TO tsdbadmin WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR SCHEMAS; Type: DEFAULT ACL; Schema: -; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT USAGE ON SCHEMAS TO tsdbadmin;


--
-- PostgreSQL database dump complete
--

\unrestrict hfkqOBU6Xv96cifuwF4m3Dyo2zXvBozobnq18t2Mc1XBCg9fEwMgpYf64IZHTU3

