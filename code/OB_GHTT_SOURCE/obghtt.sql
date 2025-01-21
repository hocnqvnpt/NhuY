 insert into nhuy_ct_bsc_ipcc_obghtt;
 create table vttp as 
    with t0 as (select  c0.thuebao_id, c0.phieutt_id, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi,c0.ngay_kt_moi, c0.tien_hopdong,c0.tien_thanhtoan, c0.vat_thanhtoan vat
                                , c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                , c0.nvtuvan_id, c0.nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, 
                                nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan, c0.loaitb_id
               from TMP_ob c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
--                            where c0.nvtuvan_id is null
                        )
                 , km0 as (     
                                ----------------TT Thang tang tren 1 dong-------------
                            select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                            , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                                            , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all km 
                            where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
                                            --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                            and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                           union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
                            select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                            , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                            , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                            from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                        ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                            where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
                                           -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                                           and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                     )
                , ds as (select thang,ob_id, ngay_tao ngay_ob, thuebao_id, ma_Tb, ngay_bddc, ngay_ktdc ngay_kt_dc, td_th, ma_nd, nhanvien_id,ma_Nv,so_thangDc,cuoc_dc,
                        to_number(to_char(ngay_ktdc,'yyyymm')) thang_kt,nhomtb_id, thanhtoan_id, khachhang_id, row_number() over (partition by thuebao_id order by ob_id desc) rnk
                        from ttkd_bsc.nhuy_ct_ipcc_obghtt
                        where thang = 202404
                        )
--                    select* from ttkd_Bsc.nhuy_ct_ipcc_obghtt;
--                    select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt
            , c as (select  t0.thuebao_id, t0.phieutt_id,  t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.ngay_kt_moi,t0.tien_hopdong,t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang,t0.loaitb_id
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                ) --select* from c;
    select a.thang thang_ob, a.ob_id, a.ngay_ob, a.thuebao_id, a.ma_tb,a.ngay_bddc ngay_bddc_cu, 
            a.ngay_kt_dc ngay_ktdc_cu,a.cuoc_dc cuoc_dc_cu, a.so_thangdc so_thangdc_cu, c.ma_gd, c.rkm_id , c.ngay_bd_moi, c.ngay_kt_moi,c.tien_hopdong, c.tien_thanhtoan, 
            c.vat, c.ngay_tt, c.ngay_hd, c.ngay_Nganhang,c.soseri, c.seri, c.kenhthu, c.ten_nganhang, c.ten_ht_Tra
            ,a.td_th, a.ma_nd ma_nd_ob, a.nhanvien_id nhanvien_ob_id,a.ma_nv manv_ob,nvob.ma_to,nvob.ma_pb, nvtp.ma_pb pb_thphuc,  a.thang_kt, a.nhomtb_id nhomtb_id_cu , c.hdtb_id, c.hdkh_id,
            NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, MANV_THUNGAN, c.SO_THANGDC so_thangdc_moi, c.AVG_THANG, 
            NVGIAOPHIEU_ID, DVGIAOPHIEU_ID    
               ,  (select nhomtb_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 and a.thuebao_id = thuebao_id 
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                               and rownum = 1
                    ) 
                    nhomtb_id
                , a.khachhang_id, a.nhomtb_id goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id, c.loaitb_id, a.thanhtoan_id, 
                
                 case when c.rkm_id is null then null
                                when c.ht_tra_id in (1, 7,204) then 1
                                when c.ht_tra_id in (2, 4,5) then 0 else null end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
                
    from ds a
                                join css_hcm.db_thuebao dbtb
                                    on a.thuebao_id = dbtb.thuebao_id
                                join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
--                                left join ttkd_bsc.dm_phongban pb
--                                    on a.pbh_ql_id = pb.pbh_id and pb.active = 1
                  
--                                left join  ttkd_bct.phongbanhang pb_giao
--                                    on a.donvi_giao = pb_giao.pbh_id
--                                left join  ttkd_bct.phongbanhang pb_th
--                                    on a.pbh_id_th = pb_th.pbh_id
                                left join c 
                                    on a.thuebao_id = c.thuebao_id --and a.nhanvien_id = c.nvtuvan_id-- and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien_vttp nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.thang = 202404
                                left join ttkd_bsc.nhanvien_202404 nvob on a.ma_nv = nvob.ma_nv
                    where a.rnk = 1;
   -- tinh don gia              
delete from dongia_moi where 'DONGIATRA_OB' = loai_tinh and thang = 202404;
commit;
select* from dongia_moi where 'DONGIATRA_OB' = loai_tinh and thang = 202404;;
insert into dongia_moi (THANG, LOAI_TINH, MA_KPI, THUEBAO_ID, LOAITB_ID, THANHTOAN_ID, MANV_OB, TIEN_DC_CU, MA_TO, MA_PB, MA_TB, ma_nv , NHOMTB_CU_ID, 
NHOMTB_MOI_ID, KHACHHANG_ID, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP, DONGIA)
        select 
        THANG,'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, LOAITB_ID, THANHTOAN_ID, ma_Nv MANV_OB, tien_Dc_Cu, MA_TO, MA_PB, MA_TB, MANV_THUYETPHUC ma_nv, NHOMTB_CU_ID, NHOMTB_MOI_ID, KHACHHANG_ID, SOTHANG_DC, 
     HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT,  TIEN_KHOP, 
                     case  when tien_khop = 1 then 7500
                         WHEN tien_khop =2 then 15000
                         when tien_khop = 3 then 12000
                         when tien_khop = 4 then 6000
                         else 0
                         end dongia 
        from (
        with hs as (select thang, khachhang_id from nhuy_ct_bsc_ipcc_obghtt xu
                where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
                )

             select     
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                        ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id nhomtb_cu_id ,  a.nhomtb_id nhomtb_moi_id
                                            , hs.khachhang_id,sum( cuoc_dc_cu) tien_Dc_Cu
--                          
                                    ,max(a.so_Thangdc_moi) sothang_dc
--                                     ,sum(case 
--                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
--                                            else 0 end) ht_tra_online
--                                    
--                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
--                                                        else 0 end) kenhthu_tainha
--                                    
                                    , case
                                            when max(a.so_Thangdc_moi) >=12 then 1.2
                                            when max(a.so_Thangdc_moi) < 12 and max(a.so_Thangdc_moi) >= 6 then 1
                                            when max(a.so_Thangdc_moi) < 6 and max(a.so_Thangdc_moi) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----                                    
                                    , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                        when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0) , 0)
                                                                                        -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                    , 0)
--
                                                        ----Dich vu Mesh he so 0.2
                                                        when a.loaitb_id = 210 then 0.2  
                                                        ---MyTV he so 0.25 
                                                        when a.loaitb_id in (61, 171, 18) then 0.25  
                                                    else 0 
                                        end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.nhomtb_id
                                    , nvl(tien_khop,0) tien_khop, row_number() over (partition by a.thuebao_id order by max(nvl(tien_khop,0))) rnk
                        from nhuy_ct_bsc_ipcc_obghtt a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
                        where a.rkm_id is not null --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                            , a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id
                                            , hs.khachhang_id, a.tien_khop
        ) a
        where  dthu > 0 and not exists (select 1 from ttkd_Bsc.ct_dongia_tratruoc where thang =  202403
        and loai_tinh in ( 'DONGIATRA30D','DONGIATRA') and DONGIA > 0 and tien_khop = 1 and ma_nv is not null and a.thuebao_id = thuebao_id);
        rollback;
       select * from ttkd_bsc.tl_giahan_Tratruoc where thang = 202404 and loai_tinh in ('DONGIATRA_OB');
INSERT INTO ttkd_bsc.tl_giahan_Tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB,  DA_GIAHAN_DUNG_HEN
                                                        , TIEN)
SELECT thang, loai_tinh, ma_kpi,  ma_Nv, MA_tO, MA_PB
                      , sum(case when dthu > 0 then 1 else 0 end) da_giahan--, null
                      , round(sum(dongia*heso_chuky*heso_dichvu)) tien
-- select * from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'
--from TTKD_BSC.ct_dongia_tratruoc  a
from dongia_moi  a
where ma_kpi = 'DONGIA' --and loaitb_id in (58,59)
            and loai_tinh in ('DONGIATRA_OB') and thang = 202404
group by thang, loai_tinh, ma_kpi, ma_Nv, MA_TO, MA_PB;
-- tinh bsc
  insert into ttkd_Bsc.tl_giahan_tratruoc 
                         select 202404 thang,
                         'KPI_NV' loai_tinh,
                         'HCM_SL_ORDER_001' ma_kpi,
                         a.manv_thuyetphuc ma_nv, ma_to, ma_pb,
                         36 tong,
                         round(sum(case when dthu > 0 and tien_khop > 0 then heso_chuky*heso_dichvu else 0 end)) da_giahan,
                           round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                      , sum(dthu) DTHU_thanhcong
                                      , null tyle, null dongia, null,null
--      select*
        from (

             select     
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_Nv, a.ma_to, a.ma_pb
                                        ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id nhomtb_cu_id ,  a.nhomtb_id nhomtb_moi_id
                                            , a.khachhang_id,sum( cuoc_dc_cu) tien_Dc_Cu
--                          
----                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
----                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    ,max(a.SO_THANGDC_MOI) sothang_dc
                                     ,sum(case 
                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
                                            else 0 end) ht_tra_online
                                    
                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
                                                        else 0 end) kenhthu_tainha
                                    
                                    , case
                                            when max(a.SO_THANGDC_MOI) >=12 then 1.2
                                            when max(a.SO_THANGDC_MOI) < 12 and max(a.SO_THANGDC_MOI) >= 6 then 1
                                            when max(a.SO_THANGDC_MOI) < 6 and max(a.SO_THANGDC_MOI) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----                                    
                                   , case ----Fiber tinh he so 1 (neu khong duy tri goi dadv -0.5), MyTV tinh he so 0.5 (neu co Fiber cung ky 0.4), Mesh tinh he so 0.5 (neu co Fiber cung ky 0.2) 
                                            when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                    * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                            , 0)

                                            ----Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                            when a.loaitb_id = 210 then 0.5 - nvl(0.3* (select distinct 1 from nhuy_ct_bsc_ipcc_obghtt xu
                                                                                        where xu.loaitb_id in (58, 59)
                                                                                                        and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                            ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                            when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from nhuy_ct_bsc_ipcc_obghtt xu
                                                                                                where xu.loaitb_id in (58, 59)
                                                                                                                and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                        else 0 
                                       end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.nhomtb_id
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                        from nhuy_ct_bsc_ipcc_obghtt a 
--                                           
                        where a.rkm_id is not null --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                            , a.thuebao_id, a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id, a.khachhang_id
--                                            , hs.khachhang_id, ms.khachhang_id
        ) a
        where rnk = 1 and dthu > 0 
        group by a.thang, a.manv_thuyetphuc, ma_to, ma_pb;
-- don gia tra sau chuyen tra truoc
  insert into dongia_moi(THANG, LOAI_TINH, MA_KPI, THUEBAO_ID, MA_TB,ma_nv,ma_to,ma_pb,NHOMTB_MOI_ID,NHOMTB_CU_ID,  SOTHANG_DC, HT_TRA_ONLINE,
                KENHTHU_TAINHA, HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP,  loaitb_id,dongia)
                select THANG, 'DONGIA_TS_TT' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, ma_tb, MANV_THUYETPHUC, MA_TO, MA_PB, NHOMTB_ID, goi_old_id, SOTHANG_DC, HT_TRA_ONLINE,
                KENHTHU_TAINHA, HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP,  loaitb_id
--   
                   , cast( 18000 as number(5)) dongia
  from (
        with hs as (select thang, khachhang_id from ct_bsc_trasau_tp_tratruoc_final xu
                where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
                )
--         
            
             
             select     
             a.thang, a.thuebao_id, a.ma_tb,A.MANV_THUYETPHUC,  a.ma_to, a.ma_pb
                                           ,a.nhomtb_id NHOMTB_ID , a.goi_old_id , a.loaitb_id
--                                            , hs.khachhang_id--,sum( cuoc_dc_cu) tien_Dc_Cu
------                          
--------                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
--------                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    , max(a.SO_THANGDC) sothang_dc
                                     , sum(case 
                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
                                            else 0 end) ht_tra_online
                                    
                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
                                                        else 0 end) kenhthu_tainha
                                    
                                    , case
                                            when max(a.SO_THANGDC) >=12 then 1.2
                                            when max(a.SO_THANGDC) < 12 and max(a.SO_THANGDC) >= 6 then 1
                                            when max(a.SO_THANGDC) < 6 and max(a.SO_THANGDC) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
------                                    
                                    , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                        when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0) , 0)
                                                                                        -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                    , 0)
--
                                                        ----Dich vu Mesh he so 0.2
                                                        when a.loaitb_id = 210 then 0.2  
                                                        ---MyTV he so 0.25 
                                                        when a.loaitb_id in (61, 171, 18) then 0.25  
                                                    else 0 
                                        end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                        from ct_bsc_trasau_tp_tratruoc_final a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
--                                            left join goi on a.thuebao_id = goi.thuebao_id
--                                            left join  km on a.rkm_id = km.rkm_id
                        where a.rkm_id is not null AND A.MANV_tHUYETPHUC IS NOT NULL --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, A.MANV_THUYETPHUC,  a.ma_to, a.ma_pb, A.LOAITB_ID
                                           ,a.nhomtb_id, a.goi_old_id
                                            , hs.khachhang_id, a.ma_Tb
        ) a
        where rnk = 1 and dthu > 0;