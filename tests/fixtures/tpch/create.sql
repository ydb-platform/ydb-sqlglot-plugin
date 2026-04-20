CREATE TABLE `region` (
    r_regionkey Int32 NOT NULL,
    r_name      Utf8  NOT NULL,
    r_comment   Utf8  NOT NULL,
    PRIMARY KEY (r_regionkey)
);

CREATE TABLE `nation` (
    n_nationkey Int32 NOT NULL,
    n_name      Utf8  NOT NULL,
    n_regionkey Int32 NOT NULL,
    n_comment   Utf8  NOT NULL,
    PRIMARY KEY (n_nationkey)
);

CREATE TABLE `supplier` (
    s_suppkey   Int32  NOT NULL,
    s_name      Utf8   NOT NULL,
    s_address   Utf8   NOT NULL,
    s_nationkey Int32  NOT NULL,
    s_phone     Utf8   NOT NULL,
    s_acctbal   Double NOT NULL,
    s_comment   Utf8   NOT NULL,
    PRIMARY KEY (s_suppkey)
);

CREATE TABLE `part` (
    p_partkey     Int32  NOT NULL,
    p_name        Utf8   NOT NULL,
    p_mfgr        Utf8   NOT NULL,
    p_brand       Utf8   NOT NULL,
    p_type        Utf8   NOT NULL,
    p_size        Int32  NOT NULL,
    p_container   Utf8   NOT NULL,
    p_retailprice Double NOT NULL,
    p_comment     Utf8   NOT NULL,
    PRIMARY KEY (p_partkey)
);

CREATE TABLE `partsupp` (
    ps_partkey   Int32  NOT NULL,
    ps_suppkey   Int32  NOT NULL,
    ps_availqty  Int32  NOT NULL,
    ps_supplycost Double NOT NULL,
    ps_comment   Utf8   NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE `customer` (
    c_custkey    Int32  NOT NULL,
    c_name       Utf8   NOT NULL,
    c_address    Utf8   NOT NULL,
    c_nationkey  Int32  NOT NULL,
    c_phone      Utf8   NOT NULL,
    c_acctbal    Double NOT NULL,
    c_mktsegment Utf8   NOT NULL,
    c_comment    Utf8   NOT NULL,
    PRIMARY KEY (c_custkey)
);

CREATE TABLE `orders` (
    o_orderkey      Int32  NOT NULL,
    o_custkey       Int32  NOT NULL,
    o_orderstatus   Utf8   NOT NULL,
    o_totalprice    Double NOT NULL,
    o_orderdate     Date   NOT NULL,
    o_orderpriority Utf8   NOT NULL,
    o_clerk         Utf8   NOT NULL,
    o_shippriority  Int32  NOT NULL,
    o_comment       Utf8   NOT NULL,
    PRIMARY KEY (o_orderkey)
);

CREATE TABLE `lineitem` (
    l_orderkey      Int32  NOT NULL,
    l_partkey       Int32  NOT NULL,
    l_suppkey       Int32  NOT NULL,
    l_linenumber    Int32  NOT NULL,
    l_quantity      Double NOT NULL,
    l_extendedprice Double NOT NULL,
    l_discount      Double NOT NULL,
    l_tax           Double NOT NULL,
    l_returnflag    Utf8   NOT NULL,
    l_linestatus    Utf8   NOT NULL,
    l_shipdate      Date   NOT NULL,
    l_commitdate    Date   NOT NULL,
    l_receiptdate   Date   NOT NULL,
    l_shipinstruct  Utf8   NOT NULL,
    l_shipmode      Utf8   NOT NULL,
    l_comment       Utf8   NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
);
