-- LAY DANH SACH DAT DOC
select* from 
create table ds_ghtt_t8 as select* from (
with dsdc as (
--select thuebao_id from (
    select hdkh.hdkh_id, hdtb.hdtb_id, hdtb.thuebao_id, dbdc.rkm_id rkm_id_db,ctkm.datcoc_csd, dbdc.ngay_bddc,dbdc.ngay_ktdc,dbdc.ngay_huy,dbdc.ngay_thoai,hdtb.kieuld_id
        from css_hcm.hd_khachhang hdkh
        join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id
        join css_hcm.hdtb_datcoc hddc on hdtb.hdtb_id = hddc.hdtb_id
        join css_hcm.ct_khuyenmai ctkm on hddc.chitietkm_id = ctkm.chitietkm_id
        join css_hcm.db_datcoc dbdc on hddc.thuebao_dc_id = dbdc.thuebao_dc_id 
    where  ctkm.datcoc_csd > 0 and  ctkm.nhom_datcoc_id = 1  and hdtb.kieuld_id in (551, 550, 24, 13080) and dbdc.ngay_bddc >  '01/07/2023' and  hdtb.tthd_id = 6  and
    least(nvl(dbdc.ngay_thoai - 1, sysdate + interval '50' year ),nvl(dbdc.ngay_huy - 1, sysdate + interval '50' year ),dbdc.ngay_ktdc) > '01/09/2023'
    union all
    select hdkh.hdkh_id, hdtb.hdtb_id, hdtb.thuebao_id, dbkm.rkm_id rkm_id_db, ctkm.datcoc_csd, dbkm.ngay_bddc,dbkm.ngay_ktdc,dbkm.ngay_huy,dbkm.ngay_thoai,hdtb.kieuld_id
        from css_hcm.hd_khachhang hdkh
        join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id
        join css_hcm.khuyenmai_dbtb dbkm on  hdtb.hdtb_id = dbkm.hdtb_id
        join css_hcm.ct_khuyenmai ctkm on dbkm.chitietkm_id = ctkm.chitietkm_id
    where  ctkm.datcoc_csd > 0 and ctkm.nhom_datcoc_id = 1  and hdtb.kieuld_id in (551, 550, 24, 13080) and dbkm.ngay_bddc >  '01/07/2023' and hdtb.tthd_id = 6 and
    least(nvl(dbkm.ngay_thoai - 1, sysdate + interval '50' year ),nvl(dbkm.ngay_huy - 1, sysdate + interval '50' year ),dbkm.ngay_ktdc) > '01/09/2023'
),
tttt as 
(
     select  ptt.phieutt_id,ptt.hdkh_id,ptt.ma_gd, ptt.tien tong_tien,  ctp.tien ct_tien,ctp.hdtb_id, ctp.khoanmuctt_id,   ptt.trangthai,ptt.ngay_tt ,
    htt.ht_tra,kt.kenhthu, dsud.ten_ungdung 
    from  css_hcm.phieutt_hd ptt 
        join css_hcm.ct_phieutt ctp on ptt.phieutt_id = ctp.phieutt_id
        left join css_hcm.hinhthuc_tra htt on ptt.ht_tra_id = htt.ht_tra_id
        left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
        left join css_hcm.DS_UNGDUNG_ONLINE dsud on ptt.ungdung_id = dsud.ungdung_id
    where  ptt.tien > 0 and ctp.tien > 0 
)
select giao.thuebao_id ,dsdc.hdtb_id,dsdc.hdkh_id, dsdc.rkm_id_db,dsdc.datcoc_csd,  dsdc.ngay_bddc,dsdc.ngay_ktdc,dsdc.ngay_huy,dsdc.ngay_thoai,
    tttt.phieutt_id,tttt.ngay_tt,tttt.tong_tien,tttt.ct_tien,  tttt.trangthai trangthai_tt, tttt.ma_gd, tttt.ht_tra,tttt.kenhthu, tttt.ten_ungdung, giao.nhanvien_giao,
    giao.donvi_giao, giao.tbh_giao_id
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    left join dsdc on giao.thuebao_id = dsdc.thuebao_id
    left join tttt on dsdc.hdkh_id = tttt.hdkh_id and dsdc.hdtb_id = tttt.hdtb_id 
where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202308 and tttt.ngay_tt between  '01/07/2023' and '31/08/2023'  and tttt.khoanmuctt_id = 11 
    and dsdc.rkm_id_db not in (select rkm_id  from ttkdhcm_ktnv.ghtt_giao_688 giao where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202308) and tttt.trangthai = 1
)

-- DANH SACH THUE BAO CO 2 GIA HAN: (1412801,2781176,2963388,3048908,5845446,7242499,8227310,8257949,8789207,8833282,8833283,8994873,9259455,9550576,9695316,9695321)
-- LAY TI LE THANH CONG CUA NHAN VIEN
drop table tile_nhanvien_t8
create table tile_nhanvien_t8 as select* from  (
with dstc as (
    select nhanvien_giao, count(distinct thuebao_id) as sltc
    from ds_ghtt_t8
    group by nhanvien_giao 
),
dsgiao as (
    select nhanvien_giao, count(thuebao_id) as slgiao
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202308
    group by nhanvien_giao
)
select dsgiao.nhanvien_giao,  slgiao, nvl(sltc,0) as sltc, round( nvl(sltc,0)*100/ slgiao,2) as ti_le
from dsgiao 
    left join dstc on dsgiao.nhanvien_giao = dstc.nhanvien_giao
    where dsgiao.nhanvien_giao is not null
)
-- LAY TI LE THANH CONG CUA PHONG
drop table tile_phong_t8
create table tile_phong_t8 as select* from (
with dstc as (
    select donvi_giao, COUNT( DISTINCT  thuebao_id) as sltc
    from ds_ghtt_t8 
    group by donvi_giao 
),
dsgiao as (
    select donvi_giao, count(thuebao_id) as slgiao
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202308
    group by donvi_giao
)
select dsgiao.donvi_giao,  slgiao, nvl(sltc,0 )as sltc,  round(nvl(sltc,0)*100/ slgiao ,2) as ti_le
from dsgiao 
    left join dstc on dsgiao.donvi_giao = dstc.donvi_giao
)
-- LAY TI LE THANH CONG CUA TO
drop table tile_to_t8
create table tile_to_t8 as select* from (
with dstc as (
    select tbh_giao_id, count(distinct thuebao_id) as sltc
    from ds_ghtt_t8 
    group by tbh_giao_id 
),
dsgiao as (
    select tbh_giao_id, count(thuebao_id) as slgiao
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202308
    group by tbh_giao_id
)
select dsgiao.tbh_giao_id,  slgiao, nvl(sltc,0) as sltc, round( nvl(sltc,0)*100/ slgiao,2) as ti_le
from dsgiao 
    left join dstc on dsgiao.tbh_giao_id = dstc.tbh_giao_id
    where dsgiao.tbh_giao_id is not null
)
--------///////////8--------///////////
select * from ttkdhcm_ktnv.ghtt_giao_688 giao  where  giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202308  and nhanvien_giao = 'HCM013915';
select *from ttkdhcm_ktnv.ghtt_giao_688 giao  where nhanvien_giao = 'VNP029923' and giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202308
select* from ds_ghtt_t8 where  nhanvien_giao = 'HCM013915';
select* from css_hcm.hdtb_datcoc where hdtb_id  = 21742615
select* from css_hcm.phieutt_Hd where hdkh_id = 19688770;
select* from css_hcm.ct_phieutt where phieutt_id = 7563490;
select * from ds_ghtt where tbh_giao_id = 340;
select *  nhanvien_giao from ttkdhcm_ktnv.ghtt_giao_688 giao
    where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202308 
    --------------------------------------------------------------------------------------------------------------


9772470
9294789
9144732
8837214
8836937
8804275
8604608
8298879
8298878
8262777
8090852
8087426
8062116
7731096
4701941
4571926
4320337
2892904
1311229