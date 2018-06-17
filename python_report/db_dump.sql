--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.3
-- Dumped by pg_dump version 9.6.3

-- Started on 2018-06-17 14:30:42

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'SQL_ASCII';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 1 (class 3079 OID 12387)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- TOC entry 2229 (class 0 OID 0)
-- Dependencies: 1
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 186 (class 1259 OID 16859)
-- Name: blk_vals; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE blk_vals (
    id integer NOT NULL,
    sch_type character varying(50) NOT NULL,
    entity character varying(50) NOT NULL,
    blk smallint NOT NULL,
    val real NOT NULL
);


ALTER TABLE blk_vals OWNER TO postgres;

--
-- TOC entry 194 (class 1259 OID 17031)
-- Name: constituents; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE constituents (
    id integer NOT NULL,
    name character varying(50) NOT NULL,
    region character varying(50) NOT NULL
);


ALTER TABLE constituents OWNER TO postgres;

--
-- TOC entry 193 (class 1259 OID 17029)
-- Name: constituents_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE constituents_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE constituents_id_seq OWNER TO postgres;

--
-- TOC entry 2230 (class 0 OID 0)
-- Dependencies: 193
-- Name: constituents_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE constituents_id_seq OWNED BY constituents.id;


--
-- TOC entry 190 (class 1259 OID 16882)
-- Name: hour_vals; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE hour_vals (
    id integer NOT NULL,
    val_type character varying(50) NOT NULL,
    entity character varying(50) NOT NULL,
    hour_num smallint NOT NULL,
    val real NOT NULL
);


ALTER TABLE hour_vals OWNER TO postgres;

--
-- TOC entry 189 (class 1259 OID 16880)
-- Name: hour_vals_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE hour_vals_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE hour_vals_id_seq OWNER TO postgres;

--
-- TOC entry 2231 (class 0 OID 0)
-- Dependencies: 189
-- Name: hour_vals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE hour_vals_id_seq OWNED BY hour_vals.id;


--
-- TOC entry 192 (class 1259 OID 16892)
-- Name: key_vals; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE key_vals (
    id integer NOT NULL,
    val_key character varying(50) NOT NULL,
    entity character varying(50) NOT NULL,
    val character varying(50) NOT NULL
);


ALTER TABLE key_vals OWNER TO postgres;

--
-- TOC entry 202 (class 1259 OID 33322)
-- Name: key_vals_date; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE key_vals_date (
    id integer NOT NULL,
    val_time timestamp without time zone NOT NULL,
    val_key character varying(50) NOT NULL,
    entity character varying(50) NOT NULL,
    val character varying(50) NOT NULL
);


ALTER TABLE key_vals_date OWNER TO postgres;

--
-- TOC entry 201 (class 1259 OID 33320)
-- Name: key_vals_date_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE key_vals_date_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE key_vals_date_id_seq OWNER TO postgres;

--
-- TOC entry 2232 (class 0 OID 0)
-- Dependencies: 201
-- Name: key_vals_date_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE key_vals_date_id_seq OWNED BY key_vals_date.id;


--
-- TOC entry 191 (class 1259 OID 16890)
-- Name: key_vals_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE key_vals_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE key_vals_id_seq OWNER TO postgres;

--
-- TOC entry 2233 (class 0 OID 0)
-- Dependencies: 191
-- Name: key_vals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE key_vals_id_seq OWNED BY key_vals.id;


--
-- TOC entry 204 (class 1259 OID 33344)
-- Name: key_vals_limits; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE key_vals_limits (
    id integer NOT NULL,
    val_key character varying(50) NOT NULL,
    entity character varying(50) NOT NULL,
    low_val real NOT NULL,
    high_val real NOT NULL
);


ALTER TABLE key_vals_limits OWNER TO postgres;

--
-- TOC entry 203 (class 1259 OID 33342)
-- Name: key_vals_limits_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE key_vals_limits_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE key_vals_limits_id_seq OWNER TO postgres;

--
-- TOC entry 2234 (class 0 OID 0)
-- Dependencies: 203
-- Name: key_vals_limits_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE key_vals_limits_id_seq OWNED BY key_vals_limits.id;


--
-- TOC entry 198 (class 1259 OID 25094)
-- Name: line_details; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE line_details (
    id integer NOT NULL,
    line_name character varying(100) NOT NULL,
    ckt_names character varying(500) NOT NULL,
    region character varying(50) NOT NULL
);


ALTER TABLE line_details OWNER TO postgres;

--
-- TOC entry 197 (class 1259 OID 25092)
-- Name: line_details_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE line_details_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE line_details_id_seq OWNER TO postgres;

--
-- TOC entry 2235 (class 0 OID 0)
-- Dependencies: 197
-- Name: line_details_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE line_details_id_seq OWNED BY line_details.id;


--
-- TOC entry 200 (class 1259 OID 33286)
-- Name: log_messages; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE log_messages (
    id integer NOT NULL,
    log_time timestamp without time zone DEFAULT now() NOT NULL,
    log_priority character varying(100) DEFAULT 'verbose'::character varying NOT NULL,
    log_message character varying(200) NOT NULL
);


ALTER TABLE log_messages OWNER TO postgres;

--
-- TOC entry 199 (class 1259 OID 33284)
-- Name: log_messages_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE log_messages_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE log_messages_id_seq OWNER TO postgres;

--
-- TOC entry 2236 (class 0 OID 0)
-- Dependencies: 199
-- Name: log_messages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE log_messages_id_seq OWNED BY log_messages.id;


--
-- TOC entry 188 (class 1259 OID 16872)
-- Name: minute_vals; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE minute_vals (
    id integer NOT NULL,
    val_type character varying(50) NOT NULL,
    entity character varying(50) NOT NULL,
    min_num smallint NOT NULL,
    val real NOT NULL
);


ALTER TABLE minute_vals OWNER TO postgres;

--
-- TOC entry 187 (class 1259 OID 16870)
-- Name: minute_vals_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE minute_vals_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE minute_vals_id_seq OWNER TO postgres;

--
-- TOC entry 2237 (class 0 OID 0)
-- Dependencies: 187
-- Name: minute_vals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE minute_vals_id_seq OWNED BY minute_vals.id;


--
-- TOC entry 185 (class 1259 OID 16857)
-- Name: sch_vals_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE sch_vals_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE sch_vals_id_seq OWNER TO postgres;

--
-- TOC entry 2238 (class 0 OID 0)
-- Dependencies: 185
-- Name: sch_vals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE sch_vals_id_seq OWNED BY blk_vals.id;


--
-- TOC entry 196 (class 1259 OID 17041)
-- Name: volt_level_info; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE volt_level_info (
    id integer NOT NULL,
    scada_id character varying(75) NOT NULL,
    other_name character varying(75),
    volt character varying(20) NOT NULL
);


ALTER TABLE volt_level_info OWNER TO postgres;

--
-- TOC entry 195 (class 1259 OID 17039)
-- Name: volt_level_info_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE volt_level_info_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE volt_level_info_id_seq OWNER TO postgres;

--
-- TOC entry 2239 (class 0 OID 0)
-- Dependencies: 195
-- Name: volt_level_info_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE volt_level_info_id_seq OWNED BY volt_level_info.id;


--
-- TOC entry 2056 (class 2604 OID 16862)
-- Name: blk_vals id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY blk_vals ALTER COLUMN id SET DEFAULT nextval('sch_vals_id_seq'::regclass);


--
-- TOC entry 2060 (class 2604 OID 17034)
-- Name: constituents id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY constituents ALTER COLUMN id SET DEFAULT nextval('constituents_id_seq'::regclass);


--
-- TOC entry 2058 (class 2604 OID 16885)
-- Name: hour_vals id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY hour_vals ALTER COLUMN id SET DEFAULT nextval('hour_vals_id_seq'::regclass);


--
-- TOC entry 2059 (class 2604 OID 16895)
-- Name: key_vals id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals ALTER COLUMN id SET DEFAULT nextval('key_vals_id_seq'::regclass);


--
-- TOC entry 2066 (class 2604 OID 33325)
-- Name: key_vals_date id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals_date ALTER COLUMN id SET DEFAULT nextval('key_vals_date_id_seq'::regclass);


--
-- TOC entry 2067 (class 2604 OID 33347)
-- Name: key_vals_limits id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals_limits ALTER COLUMN id SET DEFAULT nextval('key_vals_limits_id_seq'::regclass);


--
-- TOC entry 2062 (class 2604 OID 25097)
-- Name: line_details id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY line_details ALTER COLUMN id SET DEFAULT nextval('line_details_id_seq'::regclass);


--
-- TOC entry 2063 (class 2604 OID 33289)
-- Name: log_messages id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY log_messages ALTER COLUMN id SET DEFAULT nextval('log_messages_id_seq'::regclass);


--
-- TOC entry 2057 (class 2604 OID 16875)
-- Name: minute_vals id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY minute_vals ALTER COLUMN id SET DEFAULT nextval('minute_vals_id_seq'::regclass);


--
-- TOC entry 2061 (class 2604 OID 17044)
-- Name: volt_level_info id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY volt_level_info ALTER COLUMN id SET DEFAULT nextval('volt_level_info_id_seq'::regclass);


--
-- TOC entry 2085 (class 2606 OID 17036)
-- Name: constituents constituents_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY constituents
    ADD CONSTRAINT constituents_pkey PRIMARY KEY (id);


--
-- TOC entry 2103 (class 2606 OID 33351)
-- Name: key_vals_limits ent_val_key_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals_limits
    ADD CONSTRAINT ent_val_key_unique UNIQUE (val_key, entity);


--
-- TOC entry 2077 (class 2606 OID 16887)
-- Name: hour_vals hour_vals_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY hour_vals
    ADD CONSTRAINT hour_vals_pkey PRIMARY KEY (id);


--
-- TOC entry 2099 (class 2606 OID 33327)
-- Name: key_vals_date key_vals_date_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals_date
    ADD CONSTRAINT key_vals_date_pkey PRIMARY KEY (id);


--
-- TOC entry 2105 (class 2606 OID 33349)
-- Name: key_vals_limits key_vals_limits_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals_limits
    ADD CONSTRAINT key_vals_limits_pkey PRIMARY KEY (id);


--
-- TOC entry 2081 (class 2606 OID 16897)
-- Name: key_vals key_vals_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals
    ADD CONSTRAINT key_vals_pkey PRIMARY KEY (id);


--
-- TOC entry 2093 (class 2606 OID 25102)
-- Name: line_details line_details_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY line_details
    ADD CONSTRAINT line_details_pkey PRIMARY KEY (id);


--
-- TOC entry 2095 (class 2606 OID 25104)
-- Name: line_details line_name_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY line_details
    ADD CONSTRAINT line_name_unique UNIQUE (line_name);


--
-- TOC entry 2097 (class 2606 OID 33293)
-- Name: log_messages log_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY log_messages
    ADD CONSTRAINT log_messages_pkey PRIMARY KEY (id);


--
-- TOC entry 2073 (class 2606 OID 16877)
-- Name: minute_vals minute_vals_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY minute_vals
    ADD CONSTRAINT minute_vals_pkey PRIMARY KEY (id);


--
-- TOC entry 2087 (class 2606 OID 17038)
-- Name: constituents name_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY constituents
    ADD CONSTRAINT name_unique UNIQUE (name);


--
-- TOC entry 2089 (class 2606 OID 17048)
-- Name: volt_level_info scada_id_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY volt_level_info
    ADD CONSTRAINT scada_id_unique UNIQUE (scada_id);


--
-- TOC entry 2069 (class 2606 OID 16866)
-- Name: blk_vals sch_type_ent_blk_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY blk_vals
    ADD CONSTRAINT sch_type_ent_blk_unique UNIQUE (sch_type, entity, blk);


--
-- TOC entry 2071 (class 2606 OID 16864)
-- Name: blk_vals sch_vals_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY blk_vals
    ADD CONSTRAINT sch_vals_pkey PRIMARY KEY (id);


--
-- TOC entry 2083 (class 2606 OID 16899)
-- Name: key_vals val_key_ent_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals
    ADD CONSTRAINT val_key_ent_unique UNIQUE (val_key, entity);


--
-- TOC entry 2101 (class 2606 OID 33329)
-- Name: key_vals_date val_time_key_ent_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY key_vals_date
    ADD CONSTRAINT val_time_key_ent_unique UNIQUE (val_time, val_key, entity);


--
-- TOC entry 2079 (class 2606 OID 16889)
-- Name: hour_vals val_type_ent_hour_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY hour_vals
    ADD CONSTRAINT val_type_ent_hour_unique UNIQUE (val_type, entity, hour_num);


--
-- TOC entry 2075 (class 2606 OID 16879)
-- Name: minute_vals val_type_ent_min_num_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY minute_vals
    ADD CONSTRAINT val_type_ent_min_num_unique UNIQUE (val_type, entity, min_num);


--
-- TOC entry 2091 (class 2606 OID 17046)
-- Name: volt_level_info volt_level_info_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY volt_level_info
    ADD CONSTRAINT volt_level_info_pkey PRIMARY KEY (id);


-- Completed on 2018-06-17 14:30:43

--
-- PostgreSQL database dump complete
--

