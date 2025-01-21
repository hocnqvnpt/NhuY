WITH KM AS (
    select 1 from v_thongtinkm_all where cuoc_dc > 0 
        and ngay_bddc <= to_date('31/03/2024', 'dd/mm/yyyy')
        and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR))
                            >= to_date('01/01/2024', 'dd/mm/yyyy') 
);
select* from b_ts_tt
drop table trsau_tp_Trtruoc;
create table trsau_tp_Trtruoc as
with t0 as (select THUEBAO_ID, MA_TB, KIEULD_ID, LOAITB_ID, RKM_ID, NGAY_BD, NGAY_KT, PHIEUTT_ID, TRANGTHAI, MA_GD, NGAY_HD, NGAY_TT, SOSERI, SERI, TIEN_HOPDONG, VAT_HOPDONG, 
  TIEN_THANHTOAN, VAT_THANHTOAN, HDTB_ID, HDKH_ID, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, KENHTHU_ID, HT_TRA_ID, TTHD_ID,
  nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan
               from b_ts_tt c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
                        )
  , km0 as (  
                                        ----------------TT Thang tang tren 1 dong-------------
                    select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                    , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                                    , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                    from v_thongtinkm_all km 
                    where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0 and hieuluc = 1 and ttdc_id = 0
                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                   union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
                    select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                    , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                    , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                    from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                                                                                    from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                                                                                ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                    where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0 and hieuluc = 1 and ttdc_id = 0
                                   and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                          
)
 , ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
            from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                            join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
            group by bb.phieu_id)

        select cast(202403 as number(6)) thang, a.thuebao_id, a.ma_tb, c.ma_nv manv_Thuyetphuc, c.ma_To, c.ma_pb, a.ma_gd, a.rkm_id,  a.tien_thanhtoan,
        a.vat_Thanhtoan vat, a.ngay_tt, 
        a.loaitb_id, a.ngay_bd, a.ngay_kt, a.phieutt_id, 
        SOSERI, SERI, KENHTHU,  HT_TRA,
        ct.ngay_nganhang, (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID) ma_chungtu ,
        case when a.ht_tra_id in (1, 7,204) then 1
                     when a.ht_tra_id in (2, 4,5) then 0 
                     when a.ht_Tra_id is null then null end tien_khop
                     ,  MANV_GT, MANV_THUNGAN, a.HT_TRA_ID, a.KENHTHU_ID--, dadv.nhomtb_id
      
from t0 a
    join km0 b on a.rkm_id =b.rkm_id
    left join ttkd_Bsc.nhanvien_202403 c on a.MANV_THUYETPHUC = c.ma_nv
    left join css_hcm.kenhthu kt on a.kenhthu_id = kt.kenhthu_id
    left join css_hcm.hinhthuc_Tra ht on a.ht_tra_id = ht.ht_Tra_id
    left join ct on a.phieutt_id = ct.phieu_id
--    left join dadv on a.thuebao_id = dadv.thuebao_id
where to_number(to_char(a.ngay_Tt,'yyyymm')) = 202403 
 ;
 commit;
 create table ct_bsc_TRSAU_TP_TRTRUOC as
 with dadv as (select nhomtb_id, thuebao_id, row_number() over (partition by thuebao_id order by nhomtb_id desc) rnk
          from tinhcuoc.v_sd_goi_dadv@dataguard 
          where trangthai = 1 and KYHOADON = 20240201
        )
 select a.*, nhomtb_id
 from TRSAU_TP_TRTRUOC a 
 left join dadv on a.thuebao_id = dadv.thuebao_id and rnk = 1;

    select* from ct_bsc_trasau_tp_tratruoc where thang = 202404;
    select* from v_Thongtinkm_all where thuebao_id = 4320859;
    select* from css_hcm.hinhthuc_Tra;
    insert into ttkd_bsc.tl_Giahan_Tratruoc
    select* from nhuy.tl_Giahan_Tratruoc where thang = 202403 and ma_kpi  in ( 'HCM_CL_OBDAI_007');
    commit;
    select* from v_thongtinkm_all where thuebao_id = 11774276;
    
update ct_Bsc_TRSAU_TP_TRTRUOC a set TIEN_KHOP = 1
                -- select * from TRSAU_TP_TRTRUOC a
                where thang = 202404 and a.rkm_id is not null and tien_khop = 0
                                and ht_tra_id in (2, 4,5)-- and a.phieutt_id = 7675246    
                               -- and ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')
                                and  exists 
                                (
                                    select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                    where  phieu_id = a.phieutt_id
                                    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM  -10
                                    );
                                    COMMIT;
                                    drop table tb_trsau_Tp_trtruoc purge;
create table tb_trsau_Tp_trtruoc as
select a.*, khachhang_id 
from CT_BSC_TRSAU_TP_TRTRUOC a
 left join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
     ROLLBACK;   
     commit;
        ---
        drop table ts_Tt;
  ;      INSERT INTO DONGIA_MOI 
  ;
  create table ct_bsc_trasau_tp_tratruoc_final as select a.*, b.khachhang_id from ct_bsc_trasau_tp_tratruoc_ph a left join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id
  create table ts_Tt as
  select* from dongia_moi
  insert into dongia_moi(THANG, LOAI_TINH, MA_KPI, THUEBAO_ID, MA_TB,ma_nv,ma_to,ma_pb,NHOMTB_MOI_ID,NHOMTB_CU_ID,  SOTHANG_DC, HT_TRA_ONLINE,
                KENHTHU_TAINHA, HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP,  loaitb_id,dongia)
                select THANG, 'DONGIA_TS_TT' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, ma_tb, MANV_THUYETPHUC, MA_TO, MA_PB, NHOMTB_ID, goi_old_id, SOTHANG_DC, HT_TRA_ONLINE,
                KENHTHU_TAINHA, HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP,  loaitb_id
--        THANG,'DONGIA_TS_TT' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, LOAITB_ID, THANHTOAN_ID, MANV_OB, NULL, MA_TO, MA_PB, MA_TB, MANV_THUYETPHUC ma_nv, NHOMTB_ID_CU, NHOMTB_ID_MOI,
--        KHACHHANG_ID, SOTHANG_DC, 
--        HT_TRA_ONLINE, kenhthu_tainha,HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, NHOMTB_ID, TIEN_KHOP, RNK,
                   , cast( 18000 as number(5)) dongia
  from (
        with hs as (select thang, khachhang_id from ct_bsc_trasau_tp_tratruoc_final xu
                where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
                )
--            , goi as (select nhomtb_id , thuebao_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 
--                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
--                                   --  and nhomtb_id not in (2691065)
--                ) 
--                
--            , km1 as (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
--         from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100 and thang_bddc > 202301
--                                                                                        )
--             , km as (
--             select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
--                                , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
--                                , km.thangdc + km.thangkm SOTHANG_DC, km.khuyenmai_id
--                from v_thongtinkm_all km 
--                where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
--                                --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
--                                and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
--                                and thang_bddc > 202310
--               union all
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
        select* from dongia_moi where thang = 202404 and loai_tinh = 'DONGIA_TS_TT'
SELECT* FROM ts_tt
select a.*, b.nhomtb_id, c.khachhang_id
from TRSAU_TP_TRTRUOC a
      left join dadv b on a.thuebao_id = b.thuebao_id and b.rnk = 1
      left join css_hcm.db_Thuebao c on a.thuebao_id = c.thuebao_id
      ;
      rollback;
      commit;
      select *from ts_Tt where ma_Tb not in (select distinct  ma_tb 
                                              from ttkd_bct.ds_ketqua_capnhat_dai a 
                                              where thang_kt=0 and to_number(to_char(ngay_cn, 'yyyymm')) = 202403   ---change n
                                                            )
      select distinct thuebao_id from ct_bsc_trasau_tp_tratruoc_final --where rkm_id is not null and MANV_THUYETPHUC  is not null