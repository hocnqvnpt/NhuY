-- LAY DANH SACH DAT DOC: LAY THANG TU DANH BA KHUYEN MAI, KHONG QUA HOP DONG KHUYEN MAI, TRUNG 2 THUE BAO: 9311589 9739272 
-- MO TA:  thue bao co cung thoi gian gia han, khac rkm_id va co 2 phieu thanh toan cho cung 1 thue bao
--create table dup_1 as select thuebao_id from (
drop table dup_1
create table dup_1 as select thuebao_id from (

with dsdc as (
    select hdtb.hdtb_id, hdtb.thuebao_id,hdkh.hdkh_id,hdtb.kieuld_id, dbdc.rkm_id rkm_id_db, ctkm.datcoc_csd, dbdc.ngay_bddc,dbdc.ngay_ktdc,dbdc.ngay_huy,dbdc.ngay_thoai
        from css_hcm.hd_khachhang hdkh
        join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id
        join css_hcm.hdtb_datcoc hddc on hdtb.hdtb_id = hddc.hdtb_id
        join css_hcm.ct_khuyenmai ctkm on hddc.chitietkm_id = ctkm.chitietkm_id
        join css_hcm.db_datcoc dbdc on hddc.thuebao_dc_id = dbdc.thuebao_dc_id 
    where  ctkm.datcoc_csd > 0 and  ctkm.nhom_datcoc_id = 1  and hdtb.kieuld_id in (551, 550, 24, 13080) and dbdc.ngay_bddc >  '01/09/2023' and  hdtb.tthd_id = 6  and
    least(nvl(dbdc.ngay_thoai - 1, sysdate + interval '50' year ),nvl(dbdc.ngay_huy - 1, sysdate + interval '50' year ),dbdc.ngay_ktdc) > '01/11/2023' -- and  hdtb.thuebao_id in (5915742)
    union all
    select   hdtb.hdtb_id,hdtb.thuebao_id, hdtb.hdkh_id, hdtb.kieuld_id, kmdb.rkm_id rkm_id_db,  ctkm.datcoc_csd, kmdb.ngay_bddc, kmdb.ngay_ktdc , kmdb.ngay_huy, kmdb.ngay_thoai
    from  css_hcm.hd_khachhang hdkh 
        join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdtb_id
        join css_hcm.khuyenmai_dbtb kmdb on hdtb.hdtb_id = kmdb.hdtb_id 
        join css_hcm.ct_khuyenmai ctkm  on kmdb.chitietkm_id = ctkm.chitietkm_id
    where hdtb.tthd_id = 6  
    and kmdb.ngay_bddc >  '01/09/2023'  and hdtb.kieuld_id in (551, 550, 24, 13080)  and   least(nvl(kmdb.ngay_thoai - 1, sysdate + interval '50' year ),
        nvl(kmdb.ngay_huy - 1, sysdate + interval '50' year ),kmdb.ngay_ktdc) > '01/11/2023' and ctkm.nhom_datcoc_id = 1 and ctkm.datcoc_csd>0    
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
    tttt.phieutt_id,tttt.ngay_tt,tttt.tong_tien,tttt.ct_tien,  tttt.trangthai trangthai_tt, tttt.ma_gd, tttt.ht_tra,tttt.kenhthu, tttt.ten_ungdung 
    from ttkdhcm_ktnv.ghtt_giao_688 giao
    join dsdc on giao.thuebao_id = dsdc.thuebao_id
    join tttt on dsdc.hdkh_id = tttt.hdkh_id and dsdc.hdtb_id = tttt.hdtb_id 
where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202310 and tttt.ngay_tt between  '01/09/2023' and '31/10/2023'  and tttt.khoanmuctt_id = 11 --and giao.thuebao_id =2711685
    and dsdc.rkm_id_db not in (select rkm_id  from ttkdhcm_ktnv.ghtt_giao_688 giao where giao.tratruoc = 1 and giao.loaibo = 0 and giao.thang_kt = 202310) and tttt.trangthai = 1
    and giao.thuebao_id in (select thuebao_id from dup_1)
order by thuebao_id asc , ngay_bddc asc
) group by thuebao_id having count(thuebao_id) > 1   

