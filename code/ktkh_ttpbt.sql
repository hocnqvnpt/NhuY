select* from chuavaobsc;
drop table KTKH_TTPBT;
update ttkd_Bsc.nhanvien 
set tinh_Bsc = 0
where thang = 202409 and donvi = 'TTKD' and ma_nv in (select mã_nv_Hrm from chuavaobsc) and tinh_bsc=  1;
commit;
rollback;
select* from ktkh_ttpbt;
drop table ktkh_ttpbt_t9;
select* from qltn.v_db_Datcoc@dataguard where kyhoadon = 20231201 and ma_Tb = 'tlong03';
delete from tien_phanbo where thang = 202312 and rkm_id in (select rkm_id from tien_thu where thang = 202401) and cuoc_tk is null and thang_bd >=202401; and ma_Tb = 'tlong03' ;
--- thang 1
create table ktkh_ttpbt as
with pb as (
    select thang,ma_Tb,loaihinh_Tb, ten_Dvvt, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202312
    group by ma_Tb,loaihinh_Tb, ten_Dvvt, thang
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202401
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202401 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202401
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, a.ton_ck ton_ck_t12, b.tien  tienthu_t1, c.tien  tien_Thoai_t1,
    d.cuoc_tk tien_pb_T1, (a.ton_ck +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t1--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where a.thang = 202312 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null, a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t1
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt);

-- thang 2
create table ktkh_ttpbt_t2 as
with pb as (
    select *
    from ktkh_ttpbt
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202402
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202402 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202402
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t2, c.tien  tien_Thoai_t2,
    d.cuoc_tk tien_pb_T2, (a.ton_t1 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t2--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null, a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t1
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;
-- thang 3
create table ktkh_ttpbt_t3 as
with pb as (
    select *
    from ktkh_ttpbt_t2
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202403
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202403 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202403
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t3, c.tien  tien_Thoai_t3,
    d.cuoc_tk tien_pb_T3, (a.ton_t2 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t3--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null,null,null,null,null, a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t3
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;
-- -- thang 4
create table ktkh_ttpbt_t4 as
with pb as (
    select *
    from ktkh_ttpbt_t3
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202404
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202404 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202404
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t4, c.tien  tien_Thoai_t4,
    d.cuoc_tk tien_pb_T4, (a.ton_t3 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t4--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null,null,null,null,null,null,null,null,null, a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t4
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;
-- -- thang 4
create table ktkh_ttpbt_t5 as
with pb as (
    select *
    from ktkh_ttpbt_t4
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202405
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202405 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202405
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t5, c.tien  tien_Thoai_t5,
    d.cuoc_tk tien_pb_T5, (a.ton_t4 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t5--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null,null,null,null,null,null,null,null,null, null,null,null,null,a.tien , c.tien, d.cuoc_tk, 
    (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t5
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;

-- -- thang 6
create table ktkh_ttpbt_t6 as
with pb as (
    select *
    from ktkh_ttpbt_t5
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202406
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202406 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202406
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t6, c.tien  tien_Thoai_t6,
    d.cuoc_tk tien_pb_T6, (a.ton_t5 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t6--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t5
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;
    drop table ktkh_ttpbt_t7;
create table ktkh_ttpbt_t7 as
with pb as (
    select *
    from ktkh_ttpbt_t6
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202407
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202407 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202407
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t7, c.tien  tien_Thoai_t7,
    d.cuoc_tk tien_pb_T7, (a.ton_t6 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t7--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t5
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;
-- thang 8
create table ktkh_ttpbt_t7 as
with pb as (
    select *
    from ktkh_ttpbt_t6
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202407
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202407
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202407
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t7, c.tien  tien_Thoai_t7,
    d.cuoc_tk tien_pb_T7, (a.ton_t2 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t7--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t5
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;
-- thang 8
create table ktkh_ttpbt_t8 as
with pb as (
    select *
    from ktkh_ttpbt_t7
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202408
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202408 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202408
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t8, c.tien  tien_Thoai_t8,
    d.cuoc_tk tien_pb_T8, (a.ton_t7 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t8--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    null,null,null,null,
    a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t5
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;
-- thang 8
create table ktkh_ttpbt_t9 as
with pb as (
    select *
    from ktkh_ttpbt_t8
)
, thoai as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thoai
    where thang = 202409
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, thu as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(tien_thanhtoan)+sum(vat) tien
    from tien_Thu
    where thang = 202409 and khoanmuctt_id = 11
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
, pbo_moi as 
(
    select ma_Tb,loaihinh_Tb, ten_Dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_ck
    from tien_phanbo
    where thang = 202409
    group by ma_Tb,loaihinh_Tb, ten_Dvvt
)
select a.* , b.tien  tienthu_t9, c.tien  tien_Thoai_t9,
    d.cuoc_tk tien_pb_T9, (a.ton_t8 +nvl(b.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t9--, d.ton_ck
from pb a
    left join thu b on a.ma_Tb =b.ma_Tb and a.loaihinh_Tb = b.loaihinh_Tb  
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
union all
select a.ma_Tb, a.ten_dvvt, a.loaihinh_tb, null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    null,null,null,null,null,null,null,null,
    a.tien , c.tien, d.cuoc_tk,  (nvl(a.tien,0) - nvl(c.tien,0)-nvl(d.cuoc_tk,0) ) ton_t5
from thu a
    left join Thoai c on a.ma_Tb = c.ma_tb and a.loaihinh_tb = c.loaihinh_Tb 
    left join pbo_moi  d on a.ma_Tb = d.ma_tb and a.loaihinh_tb = d.loaihinh_Tb 
where not exists (select 1 from pb where a.ma_tb = ma_Tb and a.loaihinh_tb = loaihinh_Tb and a.ten_dvvt = ten_dvvt)
    ;
select* from ttkdhcm_ktnv.baocao_doanhthu_dongtien_pktkh where vat<0;
select * from KTKH_TTPBT_T9; where ma_Tb = 'hcm_his00000183';
select* from qltn_Hcm.db_datcoc where ma_Tb = 'hcm_his00000183' and kyhoadon = 20240101;
delete from
;
select* from tien_Thu where thang = 202402 and ma_Tb = 'hcm_his00000183';
select* from ttkd_Bsc.ct_bsc_Tratruoc_moi where thang = 202409;
insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_PB, TONG, DA_GIAHAN_dung_hen,  DTHU_THANHCONG_DUNG_HEN, TYLE)
select THANG, 'KPI_PB'loai_tinh , 'HCM_TB_GIAHA_023' ma_kpi,MA_PB, TONG, DA_GIAHAN,  DTHU_THANHCONG_DUNG_HEN, TYLE
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
        select thang, ma_pb, phong_giao
                 , count(thuebao_id) tong
                 , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                 , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                 , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle 
        from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                         , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                from ttkd_bsc.ct_bsc_tratruoc_moi a
                                where thang = 202409
                                group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                       )
        group by thang, ma_pb, phong_giao
        order by 2
        ) a;
insert into ttkd_Bsc.tl_Giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_PB, TONG, DA_GIAHAN_dung_hen, DTHU_THANHCONG_DUNG_HEN, TYLE);
select THANG, 'KPI_PB'loai_tinh , 'HCM_TB_GIAHA_022' ma_kpi,MA_PB, TONG, DA_GIAHAN,  DTHU_THANHCONG_DUNG_HEN, TYLE
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
        select thang, ma_pb,'VNP017072' ma_nv
                 , count(thuebao_id) tong
                 , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                 , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle 
        from        (select thang, 'VNP0703000' ma_pb, a.thuebao_id, a.ma_tb,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                         , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                from ct_bsc_ghtt_bhol a
                                where thang = 202409
                                group by thang, a.thuebao_id, a.ma_tb
                       )
        group by thang, ma_pb
        order by 2
        ) a;
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202409 and ma_kpi in ('HCM_TB_GIAHA_022','HCM_TB_GIAHA_023');
rollback;
select* from ttkd_Bsc.nhanvien where ten_nv like '%Duyên';
commit;
update ttkd_Bsc.bangluong_kpi set tyle_Thuchien = 62.07, giao = 32894, thuchien = 20417,mucdo_hoanthanh = 100-2*(65-62.07);
select* from ttkd_Bsc.bangluong_kpi
where  thang = 202409 and ma_nv = 'VNP017072' and ma_kpi  ='HCM_TB_GIAHA_022';
select distinct ht_Tra_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day a where thang = 202409 and tien_khop is null and rkm_id is not null;
select* from css_hcm.hinhthuc_Tra;  
;
update ttkd_bsc.ct_bsc_tratruoc_moi_30day a set tien_khop = 1 where thang = 202409 and tien_khop is null and rkm_id is not null;