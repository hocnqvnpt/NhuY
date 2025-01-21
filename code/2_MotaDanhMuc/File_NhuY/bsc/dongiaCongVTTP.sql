select* from 
(select  sum(tien_dongia) a from ttkd_bsc.dongia_vttp where thang = 202402  ) --where a> 0
where a > 0-- and ma_nv = 'CTV029115'
; --141233050
select distinct dv_Cap2 from dongia_vttp where thang = 202402-- group by thuebao_id having count(Thuebao_id) >  1;

rollback;
select * from ttkd_bsc.dongia_vttp where thang = 202402-- and donvi_id = 283468;
delete from dongia_vttp where thang = 202402;
commit; 
select distinct ma_Nv from ttkd_bsc.dongia_vttp where thang = 202312 and ma_nv not in (select ma_Nv from ttkd_bsc.nhanvien_vttp where thang = 202312)
select distinct loai_tinh from ttkd_bsc.dongia_vttp where thang = 202310
            ----update dongia TTVT
select* from all_tables
where owner = 'CSS_HCM' AND table_name like 'DB_%'
select sum(tien_dongia) from (
  -- insert into dongia_vttp
            select THANG, LOAI_TINH, MA_KPI, a.MA_NV, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO
                        , MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA
                        , DTHU, NGAY_TT, NHOMTB_ID, KHACHHANG_ID, HESO, tien_khop, 
                         case 
                         when ht_tra_online = 0 then 0 
                         WHEN ht_tra_online > 0 then  
                         round(DONGIA*HESO*tien_khop, 0) end tien_dongia
                         , nv1.ten_nv, dv1.ten_dv dv_cap1, dv2.ten_dv dv_cap2, dv2.donvi_id 
            from ttkd_bsc.ct_dongia_tratruoc a
                         , admin_hcm.nhanvien_onebss nv1, admin_hcm.donvi dv1, admin_hcm.donvi dv2
            where thang = 202402 and ma_kpi like 'DONGIA%'  --and LOAI_TINH in ('DONGIATRA30D')
                            and a.ma_nv = nv1.ma_nv (+) and nv1.donvi_id = dv1.donvi_id (+) and dv1.donvi_cha_id = dv2.donvi_id (+)
--                                   and  exists (select * from ttkd_bsc.nhanvien_vttp where thang = 202402 and ma_nv = a.ma_nv)
                            and not exists (select * from ttkd_bsc.nhanvien_202402 where  ma_nv = a.ma_nv)
                                and dv2.donvi_id  not in 
            (11134, 11051, 284212, 284317, 11564, 11563, 11352, 284316, 2941, 2945, 2947, 2946, 2948, 2944, 283413, 283429, 283527, 284199, 283427,2942,2943)
            and thuebao_id not in (select thuebao_id from  ttkd_bsc.dongia_vttp where thang = 202402)
             )
    select thuebao_id from ttkd_bsc.dongia_vttp where thang = 202312 group by thuebao_id having count(Thuebao_id) > 1
    ;
 select thuebao_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id
                                                        and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
       group by thuebao_id having count(thuebao_id) > 1     ;
       
    select* from css_hcm.bd_goi_dadv where trangthai = 1 and thuebao_id in (9548654,4333295,10806394,9699919,4518541);
    
    select ma_tb, ma_Kh from css_hcm.db_thuebao a join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id  and thuebao_id in (9548654,4333295,10806394,9699919,4518541);
       --   and nhomtb_id not in (2691065)
                            
                            select* from ttkd_bsc.nhanvien_202402 where ten_nv like '%Ý'
select* from ttkd_bsc.nhanvien_Vttp where ma_nv in ('024359157','CTV080974','CTV029038') and thang = 202309
    select* from  admin_hcm.donvi where donvi_id not  in (11134, 11051, 284212, 284317, 11564, 11563, 11352, 284316, 2941, 2945, 2947, 2946, 2948, 2944, 283413, 283429, 283527, 284199, 283427)
select* from ct_bsc_tratruoc_moi where ma_Tb = 'mesh0080658' and thang = 202311
select avG(sothang_dc) from ttkd_bsc.ct_dongia_Tratruoc where thang = 202310 and loai_tinh LIKE 'DONGIA%'-- and ma_kpi = 'HCM_TB_GIAHA_022'
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202307 and loai_tinh = 'KPI_PB' and ma_kpi = 'HCM_TB_GIAHA_022'
delete  from ttkd_bsc.dongia_vttp  where thang = 202311 and dv_Cap2 like 'Phòng %'
commit;
select distinct heso from ttkd_bsc.dongia_vttp where thang = 202310
select* from ttkd_bct.ds_ketqua_capnhat_dai where thang_kt=0 and to_number(to_char(ngay_cn, 'yyyymm')) = 202312
