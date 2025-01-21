commit;
insert into tien_ton 
--drop table tien_ton;
--create table tien_ton as;
with ton as (
   select thuebao_id, ma_tb, loaihinh_Tb, ten_dvvt, sum(ton_t1_tinh) ton_ck
   from tien_ton where thang = 202406
   group by thuebao_id, ma_tb, loaihinh_Tb, ten_dvvt
)
, thu as (
    select a.thuebao_id, a.ma_tb, sum(a.tien_Thanhtoan+a.vat) tien, c.loaihinh_Tb, d.ten_Dvvt
    from tien_thu A
        left join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
        left join css_hcm.loaihinh_tb c on b.loaitb_id = c.loaitb_id
        left join css_hcm.dichvu_Vt d on b.dichvuvt_id = d.dichvuvt_id
    where a.thang = 202407
    group by a.thuebao_id, a.ma_Tb, c.loaihinh_Tb, d.ten_Dvvt
)
, thoai as (
    select thuebao_id, ma_Tb, sum(tien_Thanhtoan+vat) tien
    from tien_thoai where thang = 202407
    group by thuebao_id,  ma_Tb
)
, pbo as (
   select thuebao_id, ma_tb, loaihinh_Tb, ten_dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_t1_file
   from pb where thang = 202407
   group by thuebao_id, ma_tb, loaihinh_Tb, ten_dvvt
)
--select* from css_hcm.db_Thuebao where ma_Tb = 'truongconghai1';
,tthh as
(
    select 
    a.thuebao_id, a.ma_Tb, a.loaihinh_Tb, a.ten_dvvt, a.ton_ck ton_thangtruoc, b.tien thu, c.tien thoai, d.cuoc_tk pbo--, d.ton_t1_file, 
    ,( a.ton_ck + nvl( b.tien,0) +nvl( c.tien,0) - nvl(d.cuoc_tk,0) ) ton_t1_tinh,202407 thang
    from ton a
        left join thu b on a.ma_tb = b.ma_Tb 
        left join thoai c on a.ma_tb = c.ma_tb
        left join pbo d on a.ma_tb = d.ma_Tb   
    union all 
    select 
    a.thuebao_id, a.ma_Tb, a.loaihinh_Tb, a.ten_dvvt,null ton_t12, a.tien thu_t1, c.tien thoai_t1, d.cuoc_tk pbo_t1 --, d.ton_t1_file, 
    ,(nvl(a.tien,0) + nvl( c.tien,0) - nvl(d.cuoc_tk,0) ) ton_t1_tinh,202407 thang
    from Thu a
        left join thoai c on a.ma_tb = c.ma_tb
        left join pbo d on a.ma_tb = d.ma_Tb   
    where  not exists (select 1 from tien_ton where thuebao_id = a.thuebao_id and thang = 202406)
)
select* from tthh ;where TON_T1_FILE > TON_T1_TINH ; 
commit;
drop table abc;
select* from abc_4;
select* from pb where ma_tb = 'hcmmqt-a01';
create table abc_1 as select* from  (select distinct thuebao_id, trangthai_Tb 
from tinhcuoc_hcm.dbtb partition for(20240101) b --on a.thuebao_id = b.thuebao_id
    left join css_hcm.trangthai_Tb c on b.trangthaitb_id = c.trangthaitb_id
);
select distinct thang from tien_ton;
select a.*, trangthai_Tb from tien_ton a
    left join abc_1 abc on a.thuebao_id = abc.thuebao_id
where thang = 202401