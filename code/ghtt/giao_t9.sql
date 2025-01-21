-- LAY DANH SACH DAT DOC: LAY THANG TU DANH BA KHUYEN MAI, KHONG QUA HOP DONG KHUYEN MAI, TRUNG 2 THUE BAO: 9311589 9739272 
-- MO TA:  thue bao co cung thoi gian gia han, khac rkm_id va co 2 phieu thanh toan cho cung 1 thue bao
create table ds_ghtt_t9 as select * from (
with dsdc as (
--select thuebao_id from (
    select hdkh.hdkh_id, hdtb.hdtb_id, hdtb.thuebao_id, dbdc.rkm_id rkm_id_db,ctkm.datcoc_csd, dbdc.ngay_bddc,dbdc.ngay_ktdc,dbdc.ngay_huy,dbdc.ngay_thoai,hdtb.kieuld_id
        from css_hcm.hd_khachhang hdkh
        join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id
        join css_hcm.hdtb_datcoc hddc on hdtb.hdtb_id = hddc.hdtb_id
        join css_hcm.ct_khuyenmai ctkm on hddc.chitietkm_id = ctkm.chitietkm_id
        join css_hcm.db_datcoc dbdc on hddc.thuebao_dc_id = dbdc.thuebao_dc_id 
    where  ctkm.datcoc_csd > 0 and  ctkm.nhom_datcoc_id = 1  and hdtb.kieuld_id in (551, 550, 24, 13080) and dbdc.ngay_bddc >  '01/08/2023' and  hdtb.tthd_id = 6  and
    least(nvl(dbdc.ngay_thoai - 1, sysdate + interval '50' year ),nvl(dbdc.ngay_huy - 1, sysdate + interval '50' year ),dbdc.ngay_ktdc) > '01/10/2023'
    union all
    select hdkh.hdkh_id, hdtb.hdtb_id, hdtb.thuebao_id, dbkm.rkm_id rkm_id_db, ctkm.datcoc_csd, dbkm.ngay_bddc,dbkm.ngay_ktdc,dbkm.ngay_huy,dbkm.ngay_thoai,hdtb.kieuld_id
        from css_hcm.hd_khachhang hdkh
        join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id
        join css_hcm.khuyenmai_dbtb dbkm on  hdtb.hdtb_id = dbkm.hdtb_id
        join css_hcm.ct_khuyenmai ctkm on dbkm.chitietkm_id = ctkm.chitietkm_id
    where  ctkm.datcoc_csd > 0 and ctkm.nhom_datcoc_id = 1  and hdtb.kieuld_id in (551, 550, 24, 13080) and dbkm.ngay_bddc >  '01/08/2023' and hdtb.tthd_id = 6 and
    least(nvl(dbkm.ngay_thoai - 1, sysdate + interval '50' year ),nvl(dbkm.ngay_huy - 1, sysdate + interval '50' year ),dbkm.ngay_ktdc) > '01/10/2023'
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
select giao.thuebao_id ,dsdc.hdtb_id,dsdc.kieuld_id, dsdc.hdkh_id, dsdc.rkm_id_db,dsdc.datcoc_csd,  dsdc.ngay_bddc,dsdc.ngay_ktdc,dsdc.ngay_huy,dsdc.ngay_thoai,
    tttt.phieutt_id,tttt.ngay_tt,tttt.tong_tien,tttt.ct_tien,  tttt.trangthai trangthai_tt, tttt.ma_gd, tttt.ht_tra,tttt.kenhthu, tttt.ten_ungdung , giao.donvi_giao, giao.tbh_giao_id, giao.nhanvien_giao
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    join dsdc on giao.thuebao_id = dsdc.thuebao_id
    join tttt on dsdc.hdkh_id = tttt.hdkh_id and dsdc.hdtb_id = tttt.hdtb_id 
where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202309 and tttt.ngay_tt between  '01/08/2023' and '30/09/2023'  and tttt.khoanmuctt_id = 11 --and giao.thuebao_id =2711685
    and dsdc.rkm_id_db not in (select rkm_id  from ttkdhcm_ktnv.ghtt_giao_688 giao where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202309) and tttt.trangthai = 1
)

-- DANH SACH THUE BAO CO 2 GIA HAN TRA TRUOC
--     and giao.thuebao_id  in (1342288,2178963,2449893  ,2488636,2711685,2737663,3047877,3083166,4305825,4550283,4695625,8224920,8236430,8265347,8299514,8299537,8418080,8425987,8431424,8501474,
--    8501475,8501820,8504558,8515461,8543114,8543427,8543441,8551518,8551557,8552333,8552437,8552584,8552630,8552665,8552682,8554998,8555000,8555739,8555786,8556310,8556315,8556317,8579687,
--    8591034,8591055,8592041,8593556,8601788,8601803,8601805,8602325,8604627,8642086,8642374,8648935,8674052,8676217,8695778,8696993,8696998,8705810,8747085,8749071,8749101,8761540,8778784,8855659,
--    8858351, 8881785, 8881786,8966854,8979109,8998734,8999959,8999961,9001632,9001677,9009169,9009514,9010289,9021092,9023078,9023924,9038380, 9041037, 9055062,9055063,9083828,9104055,
--    9282559,9282563,9287561,9293701,9309594,9311556,9315144,9315145,9315983,9315985,9315986,9619713,9705650,9739293,9739298,9756772,9757180,9813934,9813934,9850114,10749725)

-- LAY TI LE THANH CONG CUA NHAN VIEN
drop table tile_nhanvien_t9
create table tile_nhanvien_t9 as select* from  (
with dstc as (
    select nhanvien_giao, count(distinct nhanvien_giao) as sltc
    from ds_ghtt_t9
    group by nhanvien_giao 
),
dsgiao as (
    select nhanvien_giao, count(nhanvien_giao) as slgiao
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202309
    group by nhanvien_giao
)
select dsgiao.nhanvien_giao,  slgiao, nvl(sltc,0) as sltc, round( nvl(sltc,0)*100/ slgiao,2) as ti_le
from dsgiao 
    left join dstc on dsgiao.nhanvien_giao = dstc.nhanvien_giao
    where dsgiao.nhanvien_giao is not null
)
-- LAY TI LE THANH CONG CUA PHONG
drop table tile_phong_t9
create table tile_phong_t9 as select* from (
with dstc as (
    select donvi_giao, count(distinct thuebao_id ) as sltc
    from ds_ghtt_t9
    group by donvi_giao 
),
dsgiao as (
    select donvi_giao, count(thuebao_id) as slgiao
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202309
    group by donvi_giao
)
select dsgiao.donvi_giao,  slgiao, nvl(sltc,0 )as sltc,  round(nvl(sltc,0)*100/ slgiao ,2) as ti_le
from dsgiao 
    left join dstc on dsgiao.donvi_giao = dstc.donvi_giao
)
-- LAY TI LE THANH CONG CUA TO
drop table tile_to_t9
create table tile_to_t9 as select* from (
with dstc as (
    select tbh_giao_id, count(distinct thuebao_id) as sltc
    from ds_ghtt_t9
    group by tbh_giao_id 
),
dsgiao as (
    select tbh_giao_id, count(thuebao_id) as slgiao
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202309
    group by tbh_giao_id
)
select dsgiao.tbh_giao_id,  slgiao, nvl(sltc,0) as sltc, round( nvl(sltc,0)*100/ slgiao,2) as ti_le
from dsgiao 
    left join dstc on dsgiao.tbh_giao_id = dstc.tbh_giao_id
    where dsgiao.tbh_giao_id is not null
)

