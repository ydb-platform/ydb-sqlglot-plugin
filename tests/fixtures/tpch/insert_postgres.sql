-- region
INSERT INTO region (r_regionkey, r_name, r_comment) VALUES
  (0, 'AFRICA',      'lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to '),
  (1, 'AMERICA',     'hs use ironic, even requests. s'),
  (2, 'ASIA',        'ges. thinly even pinto beans ca'),
  (3, 'EUROPE',      'ly final courts cajole furiously final excuse'),
  (4, 'MIDDLE EAST', 'uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl');

-- nation
INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES
  (0, 'FRANCE',         3, 'refully final requests. regular, ironi'),
  (1, 'GERMANY',        3, 'l platelets. regular accounts x-ray: unusual, regular acco'),
  (2, 'BRAZIL',         1, 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special'),
  (3, 'CANADA',         1, 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold'),
  (4, 'JAPAN',          2, 'ously. final, express gifts cajole a'),
  (5, 'INDIA',          2, 'ss excuses cajole slyly across the packages. deposits print aroun'),
  (6, 'SAUDI ARABIA',   4, 'ts. silent requests haggle. closely express packages sleep across the blithely'),
  (7, 'UNITED KINGDOM', 3, 'eans boost carefully special requests. accounts are. carefull');

-- supplier
INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) VALUES
  (1, 'Supplier#000000001', 'N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ', 1, '17-891-367-3479', 1500.00, 'each slyly above the careful'),
  (2, 'Supplier#000000002', '89eJ5ksX3ImxJQBvxObC,',              0, '33-206-147-3644', 2500.00, ' slyly bold instructions. idle dependen'),
  (3, 'Supplier#000000003', 'q1,G3Pj6OjIuUYfUoH18BFTKP5e',       3, '16-137-649-3151', 3000.00, 'blithely silent requests after the express dependencies are sl'),
  (4, 'Supplier#000000004', '15OynASNrUy5gFb2Iu3SzmHHn2mW',      6, '30-383-701-3325',  800.00, 'carefully unusual packages. pending'),
  (5, 'Supplier#000000005', 'RkgTHFB0FRQ9gC4NqqKpAUYkQCdpHfOP', 4, '81-498-521-0098', 1200.00, 'final accounts alongside of the carefully bold');

-- part
INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) VALUES
  (1, 'forest green timber', 'Manufacturer#1', 'Brand#23', 'ECONOMY ANODIZED STEEL',   3,  'MED BOX',  100.00, 'efully alongside of the slyly final dependencies. '),
  (2, 'brass heavy gauge',   'Manufacturer#2', 'Brand#11', 'STANDARD BRASS',           15, 'SM CASE',  200.00, 'lar excuses nag fluffily'),
  (3, 'promo copper pin',    'Manufacturer#3', 'Brand#12', 'PROMO POLISHED COPPER',     5, 'SM BOX',   150.00, 'the quickly pending foxes sleep'),
  (4, 'rusty copper part',   'Manufacturer#4', 'Brand#34', 'LARGE ANODIZED STEEL',     23, 'LG BOX',   120.00, 'lly special packages'),
  (5, 'forest oak timber',   'Manufacturer#1', 'Brand#11', 'ECONOMY BURNISHED COPPER', 49, 'LG PACK',   90.00, 'final gifts. blithely');

-- partsupp
INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) VALUES
  (1, 1, 1000, 100.00, 'ully unusual packages wake bravely bold packages'),
  (1, 2,  500, 120.00, 'even asymptotes cajole. final, ironic accounts'),
  (1, 3,  800,  90.00, 'blithely regular ideas use. blithely ironic package'),
  (2, 1,  200, 150.00, 'final excuses. quickly regular accounts'),
  (3, 2,  300,  80.00, 'pending requests integrate quickly'),
  (4, 3,  400,  60.00, 'bold accounts across the'),
  (5, 1,  600, 110.00, 'regular pinto beans nag blithely'),
  (5, 3,  700,  70.00, 'regular accounts boost blithely');

-- customer
INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES
  (1, 'Customer#000000001', 'IVhzIApeRb ot,c,E',              0, '13-812-177-8836', 5000.00, 'BUILDING',    'ctions. accounts sleep furiously even requests. regular, regular accounts'),
  (2, 'Customer#000000002', 'XSTf4,NCwDVaWNe6tEgvwfmRchLXak', 1, '31-806-662-7625', 3000.00, 'AUTOMOBILE', 'ular requests are blithely pending orbits. quickly unusual'),
  (3, 'Customer#000000003', 'MG9kdTD2WBHm',                   4, '29-797-387-5751', 1000.00, 'BUILDING',    'pending pinto beans impress realms. final instructions; pending instructions'),
  (4, 'Customer#000000004', 'XxVSJsLAGtn',                    2, '23-700-547-8364', 2000.00, 'AUTOMOBILE', 'blithely. regularly even pinto beans breach'),
  (5, 'Customer#000000005', 'KvpyuHCplrB84WgAiGV6sYpZq7Tj',  0, '17-428-637-8832', 8000.00, 'HOUSEHOLD',  'special instructions sleep blithely quickly special instructions');

-- orders
INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES
  (1, 1, 'F', 5000.00, '1993-07-15', '1-URGENT',   'Clerk#000000001', 0, 'nag carefully '),
  (2, 2, 'O', 3000.00, '1994-06-15', '2-HIGH',     'Clerk#000000002', 0, 'ular instructions'),
  (3, 1, 'F', 8000.00, '1993-10-20', '3-MEDIUM',   'Clerk#000000003', 0, 'carefully regular fox'),
  (4, 3, 'O', 2000.00, '1995-01-15', '1-URGENT',   'Clerk#000000004', 0, 'requests. regular deposits x-ray'),
  (5, 4, 'F', 6000.00, '1995-06-01', '2-HIGH',     'Clerk#000000005', 0, 'regular foxes. ironic'),
  (6, 2, 'O', 1500.00, '1996-03-01', '5-LOW',      'Clerk#000000006', 0, 'pending tithes'),
  (7, 3, 'O', 1000.00, '1994-08-01', '3-MEDIUM',   'Clerk#000000007', 0, 'carefully final deposits');

-- lineitem
INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES
  (1, 2, 1, 1, 17.0, 2550.0, 0.04, 0.02, 'N', 'F', '1993-07-25', '1993-07-20', '1993-08-01', 'DELIVER IN PERSON', 'MAIL',  'carefully pending'),
  (1, 5, 3, 2, 10.0,  900.0, 0.06, 0.02, 'N', 'F', '1994-01-10', '1994-02-01', '1994-02-15', 'DELIVER IN PERSON', 'SHIP',  'blithely regular'),
  (2, 1, 2, 1,  5.0,  500.0, 0.06, 0.02, 'N', 'O', '1994-06-20', '1994-07-01', '1994-07-10', 'DELIVER IN PERSON', 'AIR',   'pending instructions'),
  (3, 4, 3, 1, 20.0, 2400.0, 0.05, 0.02, 'R', 'F', '1993-10-25', '1993-11-01', '1993-11-15', 'TAKE BACK RETURN',  'SHIP',  'ular requests'),
  (4, 1, 1, 1,  8.0,  800.0, 0.07, 0.02, 'N', 'O', '1995-03-20', '1995-04-01', '1995-04-15', 'DELIVER IN PERSON', 'AIR',   'regular accounts'),
  (5, 1, 1, 1, 10.0, 1000.0, 0.05, 0.02, 'N', 'F', '1995-06-10', '1995-07-01', '1995-07-15', 'DELIVER IN PERSON', 'AIR',   'furiously final'),
  (5, 3, 2, 2, 25.0, 3750.0, 0.08, 0.01, 'N', 'F', '1995-09-05', '1995-10-01', '1995-10-15', 'TAKE BACK RETURN',  'AIR',   'carefully unusual'),
  (6, 1, 2, 1, 12.0, 1200.0, 0.06, 0.02, 'N', 'O', '1996-01-15', '1996-02-01', '1996-02-15', 'DELIVER IN PERSON', 'AIR',   'blithely express'),
  (6, 5, 3, 2,  5.0,  450.0, 0.03, 0.02, 'N', 'O', '1994-01-20', '1994-02-10', '1994-02-20', 'TAKE BACK RETURN',  'MAIL',  'pending deposits'),
  (7, 1, 5, 1,  6.0,  600.0, 0.06, 0.02, 'N', 'O', '1994-08-10', '1994-09-01', '1994-09-15', 'DELIVER IN PERSON', 'AIR',   'ironic foxes'),
  (5, 3, 2, 3,  9.0, 1350.0, 0.05, 0.02, 'N', 'F', '1995-09-10', '1995-10-01', '1995-10-10', 'DELIVER IN PERSON', 'AIR',   'carefully even');
