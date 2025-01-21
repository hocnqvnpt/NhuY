select* from css_hcm.dichvu_Vt-- where --css_hcm.goi_dadv --(goi_id) where thuebao_id in (4557349,9219320,9408429,4557349,1946248)
select cuoc_tg from css_hcm.muccuoc_Tb
select* from dba_tab_columns where column_name = 'CUOC_TG'
select* from css_hcm.hd_thuebao where thuebao_id = 10733166
select* from css_Hcm.kieu_ld where ten_kieuld like 'D?ch chuy?n%' and sudung =1 and kieuld_id in (13171, 11 ,13212 ,13213, 815 )--(556,31,13264,11,13265)
13171 11 13212 13213 815 
select distinct kieuld_id from css_hcm.hd_thuebao a
join assss b on a.thuebao_id = b.thuebao_id and a.ngay_ht = b.lsu_dchuyen_fiber
 where LSU_DCHUYEN_FIBER is not null and LOAIHINH_TB ='Fiber';
 
 select a.*, kh.khdn
-- c.ten_tb, to_number(to_char(NGAY_KT_MG, 'yyyymm')) thang_kt, b.pbh_ql_id
--                , b.ma_nv
--                  ,  tento to_cs
--    ,   ten_pb phong_cs
from hocnq_ttkd.nvc_ghtt_2024 a
    left join css_hcm.db_khachhang db on a.khachhang_id = db.khachhang_id
                left join css_hcm.loai_kh kh on db.loaikh_id = kh.loaikh_id
                order by a.thuebao_id 
--                left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
--                left join css_hcm.db_Thuebao c on a.thuebao_id = c.thuebao_id
--                left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
--    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id