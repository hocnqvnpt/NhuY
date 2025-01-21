------------------------- DANH SACH FIBER -------------------------------------------
with hdtb as (select hdkh_id, hdtb_id, thuebao_id, ma_tb, loaitb_id, kieuld_id, tthd_id, ngay_ht
                                            , row_number() over (partition by thuebao_id order by KIEULD_ID) rkn
                                    from css_hcm.hd_thuebao
                                    where kieuld_id in (51, 280, 13102)
)
SELECT hdkh.hdkh_id, hdtb.hdtb_id,hdkh.ngay_yc,hdtb.thuebao_id, hdtb.ma_tb--, kld.TEN_KIEULD, kld.kieuld_id
from css_hcm.hd_khachhang hdkh
                left join hdtb on hdkh.hdkh_id = hdtb.hdkh_id and  hdtb.rkn = 1  
                left join css_hcm.kieu_ld kld on hdtb.kieuld_id = kld.kieuld_id
where hdtb.ngay_ht between '01/09/2022' and '30/09/2022' and hdtb.loaitb_id = 58 
                and hdkh.loaihd_id in (1, 27) and hdtb.tthd_id = 6 --and thuebao_id = 9415816--and hdkh.hdkh_id = 16334063
                and hdtb.kieuld_id  in (51, 280,13102)
---------------------- DANH BA DAT COCCCCCCCCCCCCCCCCCCCCCCC ----------------------
select thuebao_id from
(

with dbdc as (select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, cuoc_dc, ngay_huy, ngay_thoai, chitietkm_id
from css_hcm.db_datcoc 
where cuoc_dc > 0 and ngay_bddc < sysdate 
        and  sysdate < least(nvl(ngay_thoai - 1, sysdate + interval '50' year ),nvl(ngay_huy - 1, sysdate + interval '50' year ),ngay_ktdc) -- and tien_thoai is not null 
union all
select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, datcoc_csd, ngay_huy, ngay_thoai , chitietkm_id
from css_hcm.khuyenmai_dbtb 
where  datcoc_csd > 0 and ngay_bddc < sysdate ---and  nhom_datcoc_id = 1
        and  sysdate < least(nvl(ngay_thoai - 1, sysdate + interval '50' year ),nvl(ngay_huy - 1, sysdate + interval '50' year ),ngay_ktdc)
),
 hdtb as (select hdkh_id, hdtb_id, thuebao_id, ma_tb, loaitb_id, kieuld_id, tthd_id, ngay_ht
                                            , row_number() over (partition by thuebao_id order by KIEULD_ID) rkn
                                    from css_hcm.hd_thuebao
                                    where kieuld_id in (51, 280, 13102)
)
SELECT hdkh.hdkh_id, hdtb.hdtb_id,hdkh.ngay_yc,hdtb.thuebao_id, hdtb.ma_tb, kld.TEN_KIEULD, kld.kieuld_id,dbdc.cuoc_dc, dbdc.ngay_bddc, mc.muccuoc,td.tocdo,td.thuonghieu
    ,ttnv.nhanvien_id NhanVien_ID,ttnv.ma_nv NhanVien_Ma_NV,ttnv.ten_pb NhanVien_TenPB,ttnv.ten_to NhanVien_TenTo,
    ttctv.nhanvien_id CTV_ID,ttctv.ma_nv CTV_Ma_NV,ttctv.ten_pb CTV_TenPhongBan,ttctv.ten_to CTV_TenTo,
    ttnvgt.nhanvien_id NVGT_ID,ttnvgt.ma_nv NVGT_Ma_NV,ttnvgt.ten_pb NVGT_TenPB,ttnvgt.ten_to NVGT_TenTo
from  css_hcm.hd_khachhang hdkh
    left join hdtb on hdtb.rkn = 1 and hdkh.hdkh_id = hdtb.hdkh_id
    left join css_hcm.kieu_ld kld on hdtb.kieuld_id = kld.kieuld_id
    join css_hcm.db_thuebao dbtb on dbtb.thuebao_id = hdtb.thuebao_id
    left join dbdc on  dbtb.thuebao_id = dbdc.thuebao_id 
    join css_hcm.ct_khuyenmai ctkm on  dbdc.chitietkm_id = ctkm.chitietkm_id 
    left join css_hcm.muccuoc_tb mc on dbtb.mucuoctb_id =  mc.mucuoctb_id 
    left join css_hcm.tocdo_adsl td on  mc.mucuoctb_id = td.tocdo_id 
    left join admin_hcm.nhanvien nv on hdkh.nhanvien_id= nv.nhanvien_id
    left join admin_hcm.nhanvien ctv on   hdkh.ctv_id = ctv.nhanvien_id 
    left join admin_hcm.nhanvien nvgt on hdkh.nhanviengt_id = nvgt.nhanvien_id
    left join ttkd_bsc.nhanvien_202308 ttnvgt on nvgt.ma_nv =  ttnvgt.ma_nv 
    left join ttkd_bsc.nhanvien_202308 ttnv on  nv.ma_nv = ttnv.ma_nv 
    left join ttkd_bsc.nhanvien_202308 ttctv on ctv.ma_nv =  ttctv.ma_nv 

where hdtb.ngay_ht between '01/09/2022' and '30/09/2022' and hdtb.loaitb_id = 58 
                and hdkh.loaihd_id in (1, 27) and hdtb.tthd_id = 6 and dbtb.trangthaitb_id = 1 
                and hdtb.kieuld_id  in (51, 280,13102) and ctkm.nhom_datcoc_id = 1
)
group by thuebao_id
having count(thuebao_id) > 1

---------------------- DANH BA DAT COCCCCCCCCCCCCCCCCCCCCCCC THANG 9----------------------
select thuebao_id, manv_ctv from (
with dbdc as (select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, cuoc_dc, ngay_huy, ngay_thoai
from css_hcm.db_datcoc 
where cuoc_dc > 0 and nhom_datcoc_id = 1-- and tien_thoai is not null 
        and  to_number(to_char(ngay_bddc, 'yyyymm')) <= 202309 and  
                202309 < least(nvl(to_number(to_char(ngay_thoai - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(ngay_ktdc, 'yyyymm')) )
union all
select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, datcoc_csd, ngay_huy, ngay_thoai 
from css_hcm.khuyenmai_dbtb 
where  datcoc_csd > 0 and
    to_number(to_char(ngay_bddc, 'yyyymm')) <= 202309 and  
                202309 < least(nvl(to_number(to_char(ngay_thoai - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(ngay_ktdc, 'yyyymm')) ) 
),
 hdtb as (select hdkh_id, hdtb_id, thuebao_id, ma_tb, loaitb_id, kieuld_id, tthd_id, ngay_ht
                                            , row_number() over (partition by thuebao_id order by KIEULD_ID) rkn
                                    from css_hcm.hd_thuebao
                                    where kieuld_id in (51, 280, 13102)
)
SELECT hdkh.hdkh_id, hdtb.hdtb_id,hdkh.ngay_yc,hdtb.thuebao_id, hdtb.ma_tb, kld.TEN_KIEULD, kld.kieuld_id,dbdc.cuoc_dc, dbdc.ngay_bddc,dbdc.ngay_ktdc, mc.muccuoc,
    td.tocdo,td.thuonghieu, ctv.nhanvien_id ctv_id, nvgt.nhanvien_id manv_nvgt, nv.ma_nv manv_nv
from  css_hcm.hd_khachhang hdkh
    left join hdtb on hdtb.rkn = 1 and hdkh.hdkh_id = hdtb.hdkh_id
    left join css_hcm.kieu_ld kld on hdtb.kieuld_id = kld.kieuld_id
    join css_hcm.db_thuebao dbtb on dbtb.thuebao_id = hdtb.thuebao_id
    left join dbdc on dbdc.thuebao_id = dbtb.thuebao_id
    left join css_hcm.muccuoc_tb mc on mc.mucuoctb_id = dbtb.mucuoctb_id
    left join css_hcm.tocdo_adsl td on mc.tocdo_id = td.tocdo_id
    left join admin_hcm.nhanvien nv on hdkh.nhanvien_id= nv.nhanvien_id
    left join admin_hcm.nhanvien ctv on hdkh.ctv_id = ctv.nhanvien_id
    left join admin_hcm.nhanvien nvgt on hdkh.nhanviengt_id = nvgt.nhanvien_id

where hdtb.ngay_ht between '01/09/2022' and '30/09/2022' and hdtb.loaitb_id = 58 
                and hdkh.loaihd_id in (1, 27) and hdtb.tthd_id = 6 --and dbtb.trangthaitb_id = 1 
                and hdtb.kieuld_id  in (51, 280,13102)
)
group by thuebao_id, manv_ctv
having count(manv_ctv) > 1
--------------------------------------------
-- DANH SACH FIBER LAP DAT MOI, T9/2022, DANG HOAT DONG, KTHUC TRA TRUOC TRONG THANG 11/2023
with dbdc as (select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, cuoc_dc, ngay_huy, ngay_thoai, chitietkm_id
from css_hcm.db_datcoc 
where cuoc_dc > 0 and 
          to_number(to_char(ngay_bddc, 'yyyymm')) <= 202311 and  
                202309 < least(nvl(to_number(to_char(ngay_thoai - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(ngay_ktdc, 'yyyymm')) )
union all
select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, datcoc_csd, ngay_huy, ngay_thoai , chitietkm_id
from css_hcm.khuyenmai_dbtb 
where  datcoc_csd > 0 and
    to_number(to_char(ngay_bddc, 'yyyymm')) <= 202311 and  
                202311 < least(nvl(to_number(to_char(ngay_thoai - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(ngay_ktdc, 'yyyymm')) ) 
),
 hdtb as (select hdkh_id, hdtb_id, thuebao_id, ma_tb, loaitb_id, kieuld_id, tthd_id, ngay_ht
                                            , row_number() over (partition by thuebao_id order by KIEULD_ID) rkn
                                    from css_hcm.hd_thuebao
                                    where kieuld_id in (51, 280, 13102)
)
SELECT hdkh.hdkh_id, hdtb.hdtb_id,hdkh.ngay_yc,hdtb.thuebao_id, hdtb.ma_tb, kld.TEN_KIEULD, kld.kieuld_id,dbdc.cuoc_dc, dbdc.ngay_bddc,dbdc.ngay_ktdc, mc.muccuoc,
    td.tocdo,td.thuonghieu,dbdc.chitietkm_id, ctkm.datcoc_csd tien_dc
from  css_hcm.hd_khachhang hdkh
    left join hdtb on hdtb.rkn = 1 and hdkh.hdkh_id = hdtb.hdkh_id
    left join css_hcm.kieu_ld kld on hdtb.kieuld_id = kld.kieuld_id
    join css_hcm.db_thuebao dbtb on dbtb.thuebao_id = hdtb.thuebao_id
    left join dbdc on dbdc.thuebao_id = dbtb.thuebao_id
    join css_hcm.ct_khuyenmai ctkm on  dbdc.chitietkm_id = ctkm.chitietkm_id 
    left join css_hcm.muccuoc_tb mc on mc.mucuoctb_id = dbtb.mucuoctb_id
    left join css_hcm.tocdo_adsl td on mc.tocdo_id = td.tocdo_id
where hdtb.ngay_ht between '01/09/2022' and '30/09/2022' and hdtb.loaitb_id = 58 
                and hdkh.loaihd_id in (1, 27) and hdtb.tthd_id = 6 and dbtb.trangthaitb_id = 1 and dbdc.ngay_ktdc between '01/11/2023' and '30/11/2023'
                and ctkm.nhom_datcoc_id = 1
select ngay_ktdc from css_hcm.db_datcoc where ngay_ktdc between '01/11/2023' and '30/11/2023' and nhom_datcoc_id = 1 and ngay_huy is  null and ngay_thoai is null
select * from css_hcm.ct_phieutt where phieutt_id = 5121146
;
    select * from css_hcm.phieutt_hd where phieutt_id = 5121146;
    select* from css_hcm.hinhthuc_tra;
    select* from css_hcm.hd_thuebao
--------------------------------------*************************************----------------------------------
select hdtb_id from ( select dbdc.thuebao_id, dbdc.rkm_id, dbdc.ngay_bddc, dbdc.ngay_ktdc, dbdc.cuoc_dc, dbdc.ngay_huy, dbdc.ngay_thoai, dbdc.chitietkm_id,hdtb.hdtb_id,--, dbtb.thuebao_id,
        hdkh.hdkh_id--, dbtb.kieuld_id--,hdkh.loaihd_id
from css_hcm.db_datcoc dbdc
        join css_hcm.hd_thuebao hdtb  on dbdc.thuebao_id = hdtb.thuebao_id
        join css_hcm.db_thuebao dbtb on dbdc.thuebao_id = dbtb.thuebao_id 
        join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
where dbdc.cuoc_dc > 0 and dbdc.nhom_datcoc_id = 1 and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id = 6 and dbtb.trangthaitb_id = 1 
         and to_number(to_char(dbdc.ngay_bddc, 'yyyymm')) <= 202309 and  
                202309 < least(nvl(to_number(to_char(dbdc.ngay_thoai - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(dbdc.ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(dbdc.ngay_ktdc, 'yyyymm')) )
)
group by hdtb_id
having count(hdtb_id) >1
select * from css_hcm.kieu_ld where kieuld_id in  (551, 550, 24, 13080)
select hdkh_id from css_hcm.hd_khachhang
select* from css_hcm.hd_thuebao dbtb join css_hcm.hd_khachhang hdkh on dbtb.hdkh_id = hdkh.hdkh_id and thuebao_id = 1219344
select* from css_hcm.db_Thuebao 
--------------------------------------*************************************----------------------------------