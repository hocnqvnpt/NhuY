select* from css_hcm.phieutt_hd where phieutt_id = 8284869;
select* from css_hcm.ct_phieutt  where phieutt_id = 8284869;

alter table ttkd_bsc.ct_dongia_tratruoc add (heso_chuky number);
alter table ttkd_bsc.ct_dongia_tratruoc rename column heso to heso_dichvu;
alter table ttkd_Bsc.nhuy_ct_bsc_
select * from ttkd_bsc.blkpi_danhmuc_kpi where thang_bd = 202404; 
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_SL_ORDER_001' and thang_kt is null
;
select* from ttkd_bsc.nhanvien_202404;
select* from nhuy_ct_bsc_ipcc_obghtt where manv_ob = 'CTV080930';
select* from nhuy_ct_bsc_ipcc_obghtt where thuebao_id = 8992442  ;
alter table nhuy_ct_bsc_ipcc_obghtt rename column manv_ob to ma_Nv; 
desc nhuy_ct_bsc_ipcc_obghtt;
commit;
select* from  ttkd_bsc.ct_dongia_tratruoc ;
desc dongia_moi ; -- where thang = 202404;
insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB,  THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, 
SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA, DTHU, NGAY_TT, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO_DICHVU, TIEN_KHOP, GHICHU, TYLE_THANHCONG,heso_chuky)

select THANG, LOAI_TINH, MA_KPI,ma_Nv, MA_TO, MA_PB,thuebao_id,ma_tb, null,maNv_ob,null, MA_NV, SOTHANG_DC, null, null, dongia, dthu, ngay_Tt, null, null, NHOMTB_MOI_ID, KHACHHANG_ID, 
HESO_DICHVU,tien_khop, null, null,heso_chuky
from dongia_moi
where thang = 202404
order by heso_chuky;
commit;
rollback;
alter table ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt add (goi_old_id number(5), kenhthu VARCHAR2(300), pb_thphuc VARCHAR2(20), td_Th date ,ten_Ht_tra  VARCHAR2(500),
TEN_NGANHANG VARCHAR2(200))
select column_name from all_tab_columns where owner=upper('NHUY' )and table_name=upper('ct_Bsc_tratruoc_moi_tr30day')
and column_name not in (
select column_name from all_tab_columns where owner=upper('ttkd_bsc' )and table_name=upper('ct_Bsc_tratruoc_moi_tr30day')
)
;
select column_name from all_tab_columns where owner=upper('ttkd_bsc' )and table_name=upper('ct_Bsc_tratruoc_moi_tr30day')
and column_name not in (
select column_name from all_tab_columns where owner=upper('nhuy' )and table_name=upper('ct_Bsc_tratruoc_moi_tr30day')
);
insert into ttkd_bsc.ct_Bsc_tratruoc_moi_tr30day(THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_QL, PHONG_QL, MA_TO, MA_PB, MANV_GIAO, 
PHONG_GIAO, MANV_TH, PHONG_TH, MA_NV_CN, MANV_THUYETPHUC, KQ_POPUP, KQ_DVI_CN, PHONG_CN, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, 
TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_HD, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, MANV_TCTN, PBH_OA_ID, MANV_OA, NHOMTB_ID, 
KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, TIEN_KHOP, MA_CHUNGTU, MAPB_THPHUC, HESO_GIAO, HT_TRA_ID, KENHTHU_ID, KHDN, MANV_GT, MANV_THUNGAN, NGAY_NGANHANG)
select thang, gh_id, pbh_ql_id, pbh_giao_id, tbh_Giao_id, pbh_Th_id, pbh_cn_id, ma_tb, manv_cs, phong_cs, ma_to, ma_pb, manv_giao,
from ct_Bsc_tratruoc_moi_tr30day;

select* from ttkd_Bsc.ct_bsc_Tratruoc_moi where thang = 202404;









insert into ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt (THANG, OB_ID, NGAY_OB, THUEBAO_ID, MA_TB, LOAITB_ID, KHACHHANG_ID, THANHTOAN_ID, NHOMTB_ID, NGAY_BDDC_CU, NGAY_KTDC_CU, 
CUOC_DC_CU, SO_THANGDC_CU, CHITIETKM_ID_CU, MA_ND_OB, NHANVIEN_ID_OB, MANV_OB, KIEULD_ID, RKM_ID, NGAY_BD, NGAY_KT, PHIEUTT_ID, MA_GD, NGAY_HD, NGAY_TT, SOSERI, SERI, 
TIEN_HOPDONG, VAT_HOPDONG, TIEN_THANHTOAN, VAT_THANHTOAN, HDTB_ID, HDKH_ID, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, KENHTHU_ID, HT_TRA_ID,
MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, TTHD_ID, MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_NGANHANG, MA_CHUNGTU, TIEN_KHOP, MA_TO, MA_PB,goi_old_id,  KENHTHU, 
PB_THPHUC, TD_TH, TEN_HT_TRA, TEN_NGANHANG)
select thang, ob_id, ngay_ob, thuebao_id, ma_tb, loaitb_id, khachhang_id, thanhtoan_id, nhomtb_id, ngay_bddc_cu,ngay_ktdc_cu, cuoc_dc_cu, SO_THANGDC_CU,null, ma_Nd_ob, nhanvien_ob_id,
ma_Nv, null, rkm_id, ngay_bD_moi, ngay_kt_moi,phieutt_id, ma_gd, ngay_hd, ngay_tt, soseri, seri, tien_hopdong, null, tien_thanhtoan, vat, hdtb_id, hdkh_id, nvgiaophieu_id,
dvgiaophieu_id, nvtuvan_id, nvthu_id, thungan_tt_id, kenhthu_id, ht_Tra_id, manv_Cn, phong_cn, manv_thuyetphuc, manv_gt, null, manv_Thungan, so_thangdc_moi, avg_thang, ngay_nganhang,
ma_chungtu, tien_khop, ma_to, ma_pb , goi_old_id, kenhthu, pb_Thphuc, td_Th, ten_ht_Tra, ten_Nganhang
from nhuy_ct_bsc_ipcc_obghtt 
where thang = 202404;
commit;
alter table ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt  modify (goi_old_id number(10))
rollback;
desc ct_Bsc_tratruoc_moi_tr30day;
alter table ttkd_Bsc.ct_Bsc_tratruoc_moi_tr30day add (heso_giao number, ht_Tra_id number, kenhthu_id number, khdn number,  MANV_GT VARCHAR2(20), MANV_THUNGAN VARCHAR2(20),NGAY_NGANHANG date)
select KQ_DVI_CN from ttkd_bsc.ct_bsc_tratruoc_moi_tr30day
;
 select 
        THANG,'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, LOAITB_ID, THANHTOAN_ID, ma_Nv, tien_Dc_Cu, MA_TO, MA_PB, MA_TB, MANV_THUYETPHUC ma_nv, NHOMTB_CU_ID, NHOMTB_MOI_ID, KHACHHANG_ID, SOTHANG_DC, 
        HT_TRA_ONLINE, kenhthu_tainha,HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, NHOMTB_ID, TIEN_KHOP, RNK,
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
--            , goi as (select nhomtb_id , thuebao_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 
--                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
--                                   --  and nhomtb_id not in (2691065)
--                ) 
                
--            , km1 as (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
--         from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100 and thang_bddc > 202301)
--             , km as (
--                select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
--                                , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
--                                , km.thangdc + km.thangkm SOTHANG_DC, km.khuyenmai_id
--                from v_thongtinkm_all km 
--                where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
--                                --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
--                                and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
--                                and thang_bddc > 202310
--                union all
------------------TT giam cuoc or thang tang tren 2 dong-------------
--                
--                select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
--                                , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
--                                , km.thangdc + nvl(km1.thangkm, 0) SOTHANG_DC, km.khuyenmai_id
--                from v_thongtinkm_all km left join km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
--                where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
--                               -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
--                               and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
--                               and km.thang_bddc > 202301
--             )
             select     
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                        ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id nhomtb_cu_id ,  a.nhomtb_id nhomtb_moi_id
                                            , hs.khachhang_id,sum( cuoc_dc_cu) tien_Dc_Cu
--                          
----                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
----                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    ,max(a.so_Thangdc_moi) sothang_dc
                                     ,sum(case 
                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
                                            else 0 end) ht_tra_online
                                    
                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
                                                        else 0 end) kenhthu_tainha
                                    
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
                                    , max(nvl(tien_khop,0)) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                        from ct_bsc_trasau_tp_tratruoc a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
--                                            left join goi on a.thuebao_id = goi.thuebao_id
--                                            left join  km on a.rkm_id = km.rkm_id
                        where a.rkm_id is not null --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                            , a.thuebao_id, a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id
                                            , hs.khachhang_id
        ) a
        where rnk = 1 and dthu > 0 ;
        create table ct_bsc_trasau_tp_tratruoc_ph as
        with dadv as (select nhomtb_id, thuebao_id, row_number() over (partition by thuebao_id order by nhomtb_id desc) rnk
          from tinhcuoc.v_sd_goi_dadv@dataguard 
          where trangthai = 1 and KYHOADON = 20240301
        )
select a.*, b.nhomtb_id goi_old_id
from ct_bsc_trasau_tp_tratruoc a
      left join dadv b on a.thuebao_id = b.thuebao_id and b.rnk = 1
      where a.thang = 202404
select rkm_id from ct_bsc_trasau_tp_tratruoc where thang = 202404 group by rkm_id having count(rkm_id)>1;
update ct_bsc_trasau_tp_tratruoc_ph a set nhomtb_id = ( select nhomtb_id from css_hcm.bd_Goi_dadv where trangthai = 1 and 
 goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000 and to_number(to_char(ngay_dk,'yyyymm')) < 202404 and thuebao_id = a.thuebao_id)
 where a.thang = 202404 and a.nhomtb_id is null and a.goi_old_id is not null
SELECT * FROM ct_bsc_trasau_tp_tratruoc_ph where nvl(nhomtb_id,0) != nvl(goi_old_id,0);
select* from css_hcm.bd_Goi_dadv where thuebao_id = 4641831;
commit;
update nhuy_ct_Bsc_ipcc_obghtt set nhomtb_id = goi_old_id where goi_old_id in (
select nhomtb_id from css_hcm.bd_Goi_dadv where trangthai = 1 and thuebao_id in (select thuebao_id from nhuy_ct_Bsc_ipcc_obghtt where  nhomtb_id is null and goi_old_id is not null)
and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000 and to_char(ngay_dk,'yyyy') < '2024')