with hddc as (
    select distinct hdtb_id, g.rkm_id rkm_id_hd, h.rkm_id rkm_id_db, nvl(h.rkm_id, g.rkm_id) rkm_id, g.ngay_bddc ngay_bddc_hd, h.ngay_bddc ngay_bddc_db,
    g.ngay_ktdc ngay_ktdc_hd, h.ngay_ktdc ngay_ktdc_db,  'datcoc' nguon, h.nhom_Datcoc_id
        from css_hcm.hdtb_datcoc g
        join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
    where hieuluc = 1 and ttdc_id = 0 and h.nhom_Datcoc_id = 1
    union all
    select distinct a.hdtb_id, a.rkm_id rkm_id_hd, b.rkm_id rkm_id_db,  nvl(b.rkm_id, a.rkm_id) rkm_id,a.ngay_bddc ngay_bddc_hd, b.ngay_bddc ngay_Bddc_db,
    a.ngay_ktdc ngay_ktdc_hd, b.ngay_ktdc ngay_ktdc_db,  'khuyenmai' nguon, c.nhom_Datcoc_id
        from css_Hcm.khuyenmai_hdtb a join css_hcm.khuyenmai_dbtb b on a.hdtb_id = b.hdtb_id 
        left join css_hcm.ct_khuyenmai c on b.chitietkm_id = c.chitietkm_id
    where hieuluc = 1 and ttdc_id = 0 and b.datcoc_csd > 0 and nhom_Datcoc_id = 1
),
ds as
(
    select hdtb_id, ngay_bddc_hd,ngay_bddc_db,ngay_ktdc_hd,ngay_ktdc_db,nguon,nhom_datcoc_id
    from hddc
    where (to_number(to_char(ngay_Bddc_db,'yyyymmdd')) >=  20230901 and to_number(to_char(ngay_Bddc_db,'yyyymmdd')) <= 20231031)
    and  to_number(to_char(ngay_Bddc_hd,'yyyymm'))-1 = to_number(to_char(ngay_Bddc_db,'yyyymm'))
) ,
cttra as (select  db.thuebao_id, b2.ky_cuoc, c.hinhthuc
        from qltn_hcm.bangphieutra a2
                join qltn_hcm.ct_tra b2 on a2.phieu_id = b2.phieu_id and a2.httt_id = 188 and b2.TRAGOC >0 and b2.ky_cuoc in (20231001, 20230901) and a2.ky_cuoc = b2.ky_cuoc
                left join css_hcm.db_thuebao db on b2.ma_tb = db.ma_tb and b2.dichvuvt_id = db.dichvuvt_id and a2.thanhtoan_id = db.thanhtoan_id 
                 , qltn_hcm.hinhthuc_tt c
                        
        where a2.httt_id = c.httt_id --and c.nhom_httt_id in (1, 7, 46, 102)  
--                                    and b2.ma_tb = 'mesh0074058'
        group by db.thuebao_id, c.hinhthuc,b2.ky_cuoc
        )
select a.thuebao_id, a.ma_tb, a.hdtb_id, b.ngay_bddc_hd,b.ngay_bddc_Db,b.ngay_ktdc_hd,b.ngay_ktdc_db,b.nguon,b.nhom_datcoc_id, a.loaitb_id
from css_hcm.hd_Thuebao a
    join ds b on a.hdtb_id = b.hdtb_id
    join cttra c on a.thuebao_id = c.thuebao_id
    where loaitb_id in  (18, 58, 59, 61, 171, 210, 222, 224) and  to_number(to_char(ngay_Bddc_db,'yyyymm')) = floor(ky_cuoc/100)
order by b.ngay_bddc_hd;
    where ma_tb = 'pdqk63'

select ngay_kt_mg from css_Hcm.db_Datcoc
--where thuebao_id in  (3087784,
--8855659,
--5932711,
--4584653,
--5855517,
--8331313,
--3087787,
--9396485)
order by a.thuebao_id
) group by thuebao_id having count (thuebao_id) > 1
select thuebao_id from css_hcm.db_Datcoc where  ma_tb = 'pdqk63'
select* from css_hcm.hdtb_datcoc where thuebao_dc_id = 4657312
SELECT  * FROM QLTN_HCM.NHOM_HTTT
  select thuebao_dc_id, ngay_bddc from css_hcm.db_Datcoc where thuebao_id = 9719047
select* from css_hcm.khuyenmai_Dbtb where rkm_id in (6111740,
6111739)

cttra as (select  db.thuebao_id, c.hinhthuc
        from qltn_hcm.bangphieutra a2
                join qltn_hcm.ct_tra b2 on a2.phieu_id = b2.phieu_id and a2.httt_id = 188 and b2.TRAGOC >0 and b2.ky_cuoc in (20231001, 20231101) and a2.ky_cuoc = b2.ky_cuoc
                left join css_hcm.db_thuebao db on b2.ma_tb = db.ma_tb and b2.dichvuvt_id = db.dichvuvt_id and a2.thanhtoan_id = db.thanhtoan_id 
                 , qltn_hcm.hinhthuc_tt c
                        
        where a2.httt_id = c.httt_id and c.nhom_httt_id in (1, 7, 46, 102)  
                                    --and b2.ma_tb = 'mesh0074058'
        group by db.thuebao_id, c.hinhthuc