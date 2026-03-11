-- List of types

CREATE TYPE bm25vector (
    INPUT = _vchord_bm25_bm25vector_in,
    OUTPUT = _vchord_bm25_bm25vector_out,
    RECEIVE = _vchord_bm25_bm25vector_recv,
    SEND = _vchord_bm25_bm25vector_send,
    STORAGE = external
);

CREATE TYPE bm25query AS (
    index regclass,
    vector bm25vector
);

-- List of operators

CREATE OPERATOR <&> (
    PROCEDURE = _bm25_evaluate,
    LEFTARG = bm25vector,
    RIGHTARG = bm25query
);

-- List of functions

CREATE FUNCTION fold(int[]) RETURNS bm25vector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vchord_bm25_bm25vector_fold_wrapper';

CREATE FUNCTION unfold(bm25vector) RETURNS int[]
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vchord_bm25_bm25vector_unfold_wrapper';

CREATE FUNCTION bm25_amhandler(internal) RETURNS index_am_handler
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_bm25_amhandler_wrapper';

CREATE FUNCTION bm25query(regclass, bm25vector) RETURNS bm25query
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

-- List of access methods

CREATE ACCESS METHOD bm25 TYPE INDEX HANDLER bm25_amhandler;

-- List of operator families

CREATE OPERATOR FAMILY bm25_ops USING bm25;

-- List of operator classes

CREATE OPERATOR CLASS bm25_ops FOR TYPE bm25vector USING bm25 FAMILY bm25_ops AS
    OPERATOR 1 <&>(bm25vector, bm25query) FOR ORDER BY float_ops;
