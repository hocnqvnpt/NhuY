select* from ttkd_bct.db_thuebao_Ttkd where mst_chuan = '0105324298001';
select* from css_hcm.nhom_Dv;
drop table cntt;
alter table cntt rename column mã_s?_thu? to mst;
select owner , table_name from all_tab_columns where column_name = 'NHOMDV_ID';
select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2='Dich vu so doanh nghiep';
select* from ttkd_Bct.db_Thuebao_ttkd where mst_chuan= '0316101928';
alter table imp_cntt add  mst varchar2(26);
update imp_cntt set mst = regexp_replace (mã_s?_thu?, '\D', '');
NVL(MA_nV_AM,MA_nV) DN
MA_nV
CNTT: 
HDDT:
LAP DAT MOI: 
;
select* from ttkd_bsc.nhanvien_202406;
select * from ttkdhcm_ktnv.DM_MVIEW where MVIEW_NAME= upper('HD_KHACHHANG')
;
rollback;
commit;
select* from cntt_map ;where mst = '0312138412';
select* from css_hcm.hd_khachhang where ma_Gd = 'HCM-LD/00568312';
with pb as 
(
    select distinct ma_pb,ten_pb
    from ttkd_bsc.nhanvien_202406
)
select a.*, b.ma_Tb, b.loaitb_id, pb.ten_pb tenpb_ql, manv_ql, pb2.ten_pb tenpb_ptm, manv_ptm, case when dichvuvt_id in (13,14,15,16) then 1 else 0 end cntt
from imp_cntt a 
    left join cntt_map b on a.mst = b.mst
    left join pb on b.mapb_ql = pb.ma_pb
    left join pb pb2 on b.ma_pb_ptm = pb2.ma_pb
order by a.mst;

with hd as (
    select b.thuebao_id , b.ma_Tb, d.ma_nv, a.ma_Gd, row_number() over (partition by b.thuebao_id order by b.ngay_ht desc) rnk
    from css_hcm.hd_khachhang a 
        left join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id
        left join css_hcm.phieutt_hd c on a.hdkh_id = c.hdkh_id
        left join admin_hcm.nhanvien_onebss d on a.ctv_id = d.nhanvien_id
    where  b.tthd_id = 6  and c.trangthai = 1 and c.kenhthu_id != 6
)
;
drop table cntt_map;
create table cntt_map as
select  a.*, b.thuebao_id,b.ma_tb,c.loaitb_id,c.dichvuvt_id, b.mapb_ql, case when b.mapb_ql in ('VNP0702500','VNP0702300','VNP0702400') then NVL(b.MA_nV_AM,b.MA_nV) else b.ma_Nv end manv_ql
    ,manv_ptm,ma_pb_ptm
from cntt a
    left join ttkd_bct.db_thuebao_ttkd b on a.mst = b.mst_chuan
    join css_hcm.db_Thuebao c on b.thuebao_id = c.thuebao_id
--    left join hd on b.thuebao_id = hd.thuebao_id and rnk = 1
where c.trangthaitb_id not in (7,8,9); and c.loaitb_id in  (122,373,991,2116);
delete from hddt where mst = 'Mã S? thu?' ;
alter table hddt rename column mã_s?_thu? to mst;

select* from css_Hcm.loai_hd;
with abc as (
    select thuebao_id,nhomtb_id
    from css_hcm.bd_goi_Dadv
    where trangthai = 1
)
select distinct b.thuebao_id , b.khachhang_id from chutxoa a
join css_Hcm.db_Thuebao b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id
left join abc c on b.thuebao_id = c.thuebao_id 
where b.trangthaitb_id =1 and loaitb_id = 58 and c.nhomtb_id is null;
drop table chutxoa;
create table chutxoa as
select a.khachhang_id , a.thuebao_id from ds_giahan_tratruoc2 A where to_Number(to_char(ngay_kt_mg,'yyyymm')) = 202407 and nhomtb_id is not null
and not exists (select 1 from ds_giahan_tratruoc2 where khachhang_id = a.khachhang_id and to_Number(to_char(ngay_kt_mg,'yyyymm')) != to_Number(to_char(a.ngay_kt_mg,'yyyymm')));

--JOIN CSS_HCM.DB_tHUEBAO b on a.khachhang_id = b.khachhang_id and a.thuebao_id!= b.thuebao_id
--join ds_giahan_tratruoc_t7 c on b.thuebao_id = c.thuebao_id
--left join css_hcm.bd_goi_Dadv c on b.thuebao_id = c.thuebao_id and c.trangthai = 1
where  a.nhomtb_id is not null  ;and c.nhomtb_id is null ;and not exists (Select 1 from css_hcm.bd_goi_dadv where trangthai =1 and thuebao_id = b.thuebao_id);