drop table if exists test2.t1_md5;
drop table if exists test2.t1_md5_2;
drop table if exists test2.t2_md5;
drop table if exists test2.t2_md5_2;
drop table if exists test2.t3_md5;
drop table if exists test2.t3_md5_2;
create table test2.t1_md5(
    id int,
    md5 varchar(255),
    index(id)
);
create table test2.t1_md5_2(
    id int,
    md5 varchar(255),
    index(id)
);
create table test2.t2_md5(
    id int,
    md5 varchar(255),
    index(id)
);
create table test2.t2_md5_2(
    id int,
    md5 varchar(255),
    index(id)
);
create table test2.t3_md5(
    id int,
    md5 varchar(255),
    index(id)
);
create table test2.t3_md5_2(
    id int,
    md5 varchar(255),
    index(id)
);
insert into test2.t1_md5
SELECT id, MD5(CONCAT_WS('#',id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22)) as crc FROM test.t1;
insert into test2.t1_md5_2
SELECT id, MD5(CONCAT_WS('#',id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22)) as crc FROM test2.t1;
insert into test2.t2_md5
SELECT id, MD5(CONCAT_WS('#',id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22)) as crc FROM test.t2;
insert into test2.t2_md5_2
SELECT id, MD5(CONCAT_WS('#',id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22)) as crc FROM test2.t2;
insert into test2.t3_md5
SELECT id, MD5(CONCAT_WS('#',id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22)) as crc FROM test.t3;
insert into test2.t3_md5_2
SELECT id, MD5(CONCAT_WS('#',id,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22)) as crc FROM test2.t3;
select a.id as t1_id from test2.t1_md5 a left join test2.t1_md5_2 b on a.id = b.id where a.md5 != b.md5 or b.md5 is null;
select a.id as t2_id from test2.t2_md5 a left join test2.t2_md5_2 b on a.id = b.id where a.md5 != b.md5 or b.md5 is null;
select a.id as t3_id from test2.t3_md5 a left join test2.t3_md5_2 b on a.id = b.id where a.md5 != b.md5 or b.md5 is null;