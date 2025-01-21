 select hdkh_id from (
with dsdc as (

    select  hddc.hdtb_id, hddc.rkm_id,hddc.ngay_bddc,hddc.ngay_ktdc,ctkm.datcoc_csd,  hddc.nhom_datcoc_id,hdtb.kieuld_id, hdkh.loaihd_id
        from css_hcm.hdtb_datcoc hddc -- lay danh sach tra truoc
        join css_hcm.hd_thuebao hdtb on hddc.hdtb_id = hdtb.hdtb_id -- lay kieu lap dat
        join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id -- lay loai hop dong
        join css_hcm.ct_khuyenmai ctkm on hddc.chitietkm_id = ctkm.chitietkm_id -- lay tien dat coc 
        where ctkm.datcoc_csd > 0  and  ctkm.nhom_datcoc_id = 1  and hdtb.kieuld_id in (551, 550, 24, 13080) and  --hddc.hdtb_id = 17902897 and 
         ngay_bddc >  '01/07/2023' ---and  nhom_datcoc_id = 1
        and  '31/08/2023' < least(nvl(ngay_thoai - 1, sysdate + interval '50' year ),nvl(ngay_huy - 1, sysdate + interval '50' year ),ngay_ktdc)
    union all 
    select  kmtb.hdtb_id, kmtb.rkm_id,kmtb.ngay_bddc,kmtb.ngay_ktdc,ctkm.datcoc_csd, ctkm.nhom_datcoc_id,hdtb.kieuld_id, hdkh.loaihd_id
        from css_hcm.khuyenmai_hdtb kmtb 
        join css_hcm.hd_thuebao hdtb on kmtb.hdtb_id = hdtb.hdtb_id 
        join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id 
        join css_hcm.ct_khuyenmai ctkm on kmtb.chitietkm_id = ctkm.chitietkm_id
        where ctkm.datcoc_csd > 0 and ctkm.nhom_datcoc_id = 1 and  hdtb.kieuld_id in (551, 550, 24, 13080) and --kmtb.hdtb_id = 17902897 and 
         ngay_bddc <  '01/07/2023 ---and  nhom_datcoc_id = 1
        and  '31/08/2023'  < least(nvl(ngay_thoai - 1, sysdate + interval '50' year ),nvl(ngay_huy - 1, sysdate + interval '50' year ),ngay_ktdc)
                   )
,
dstb as (
    select hdtb.thuebao_id, hdtb.hdtb_id, dbtb.ngay_sd, hdtb.hdkh_id 
    from css_hcm.hd_thuebao hdtb  
        join css_hcm.db_thuebao dbtb on hdtb.thuebao_id = dbtb.thuebao_id 
        where  dbtb.trangthaitb_id = 1 and hdtb.loaitb_id = 58 and hdtb.kieuld_id in  (551, 550, 24, 13080)
        and dbtb.ngay_sd between '01/01/2022' and '31/08/2022'   -- and hdtb_id = 19983040    
                     
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
select   dstb.hdkh_id, dstb.hdtb_id, dsdc.ngay_bddc, dsdc.ngay_ktdc, dsdc.datcoc_csd cuoc_datcoc,
        tttt.phieutt_id, tttt.tong_Tien tongtien_tt, tttt.ct_tien, tttt.khoanmuctt_id, tttt.trangthai trangthai_tt,tttt.ngay_tt,tttt.ma_gd, 
    tttt.ht_tra,tttt.kenhthu, tttt.ten_ungdung 
from dstb join  dsdc on dstb.hdtb_id = dsdc.hdtb_id
    join tttt on dstb.hdkh_id = tttt.hdkh_id  and dstb.hdtb_id = tttt.hdtb_id
    
) group by hdkh_id having count(hdkh_id) > 1;
select* from tbl_final;