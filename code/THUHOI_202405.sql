select a.*, b.ten_vtcv from  thuhoi_bsc_dongia a join ttkd_Bsc.nhanvien b on a.thang = b.thang and a.manv_Thuyetphuc =b.ma_nv where a.thang = 202408;
alter table thuhoi_bsc_dongia add ipcc number;
delete from thuhoi_bsc_dongia where thang = 202407;
select* from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt 
rollback;
commit;
insert into thuhoi_bsc_dongia
with  km0 as (      
                                ----------------TT Thang tang tren 1 dong-------------
                            select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                            , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
--                                            , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all km 
                            where thang_Bddc >= 202405 and (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
                                            --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                            and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                           union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
                            select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                            , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
--                                            , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                            from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                        ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                            where  (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
                                           -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                                           and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
)
select a.THANG, a.OB_ID, a.NGAY_OB, a.THUEBAO_ID, a.MA_TB, a.NGAY_BDDC_CU, a.NGAY_KTDC_CU, a.CUOC_DC_CU, a.SO_THANGDC_CU, a.MA_GD, a.RKM_ID, a.NGAY_BD, a.ngay_kt, 
a.TIEN_HOPDONG, a.TIEN_THANHTOAN, a.VAT_thanhtoan, a.NGAY_TT, a.NGAY_HD, a.NGAY_NGANHANG, a.SOSERI, a.SERI, a.KENHTHU, a.TEN_NGANHANG, a.TEN_HT_TRA, a.TD_TH, a.MA_ND_OB, 
a.NHANVIEN_id_ob, a.MANV_OB, a.MA_TO, a.MA_PB, null, a.goi_old_id, a.HDTB_ID, a.HDKH_ID, a.NVTUVAN_ID, a.NVTHU_ID, a.THUNGAN_TT_ID, a.MANV_CN, a.PHONG_CN,
a.MANV_THUYETPHUC, a.MANV_GT, a.MANV_THUNGAN, a.SO_THANGDC_MOI, a.AVG_THANG, a.NGAY_YC, a.NVGIAOPHIEU_ID, a.DVGIAOPHIEU_ID, a.NHOMTB_ID, a.KHACHHANG_ID, a.PHIEUTT_ID, 
a.HT_TRA_ID, a.KENHTHU_ID, a.LOAITB_ID, a.THANHTOAN_ID, c.NHANVIEN_XUATHD, a.TIEN_KHOP, a.MA_CHUNGTU, c.DONGIA, c.TIEN_XUATHD, c.TIEN_THUYETPHUC, c.IPCC
from ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt a 
--left join km0 b on a.rkm_id = b.rkm_id
left join ttkd_Bsc.ct_dongia_tratruoc c on a.thuebao_id = c.thuebao_id and a.thang = c.thang and c.loai_Tinh =  'DONGIATRA_OB'
where a.thang = 202410 and not exists (select 1 from km0 where rkm_id >= a.rkm_id and ttdc_id = 0 and hieuluc = 1 and thuebao_id = a.thuebao_id);
SELECT A.* FROM BSC_OB_tHUHOI A JOIN TTKD_BSC.NHANVIEN B ON A.MA_nV =B.MA_NV WHERE MA_VTCV = 'VNP-HNHCM_BHKV_47' and b.thang = 202410;

commit;
se
UPDATE ttkd_bsc.ct_dongia_tratruoc A SET NHANVIEN_XUATHD = (SELECT NHANVIEN_XUATHD FROM DONGIA_OB_FINAL 
WHERE A.THUEBAO_ID = THUEBAO_ID AND A.MA_nV = MA_NV) ;WHERE THANG = 202404 AND LOAI_TINH IN ('DONGIATRA_OB');
select* from thuhoi_bsc_dongia where thuebao_id in (select thuebao_id from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202404 and nvl(tien_khop,0) > 0);

update ttkd_Bsc.tl_giahan_tratruoc set tien = 0 where thang = 202405 and loai_tinh = 'THUHOI_DONGIA_GHTT' and ma_pb in ('VNP0702300','VNP0702300','VNP0702300');

delete from thuhoi_Bsc_dongia a where thang = 202405 and exists (select 1 from v_thongtinkm_all where a.rkm_id < rkm_id and a.thuebao_id = thuebao_id and hieuluc = 1 and ttdc_id = 0);
rollback;
commit;
select* from v_Thongtinkm_all where thuebao_id = 8233413;
select a.*, b.ma_nv, b.ma_vtcv, b.ten_vtcv  from thuhoi_Bsc_dongia a 
join ttkd_Bsc.nhanvien_202406 b on a.manv_thuyetphuc = b.ma_nv 
where thang = 202405   ; and exists (select 1 from v_thongtinkm_all where a.rkm_id < rkm_id and a.thuebao_id = thuebao_id and hieuluc = 1 and ttdc_id = 0);


insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, DA_GIAHAN_DUNG_HEN, DTHU_THANHCONG_DUNG_HEN)
with a as
(
             select     
             (a.thang +1) thang, a.thuebao_id, a.ma_to, a.ma_pb, a.ma_Tb
                                        ,a.MANV_THUYETPHUC ma_nv, a.goi_old_id, a.nhomtb_id
                                          ,sum( a.cuoc_dc_cu) tien_Dc_Cu
--                          
----                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
----                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    ,max(a.SO_THANGDC_MOI) sothang_dc
--                                     ,sum(case 
--                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
--                                            else 0 end) ht_tra_online
--                                    
--                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
--                                                        else 0 end) kenhthu_tainha
--                                    
                                    , case
                                            when max(a.SO_THANGDC_MOI) >=12 then 1.2
                                            when max(a.SO_THANGDC_MOI) < 12 and max(a.SO_THANGDC_MOI) >= 6 then 1
                                            when max(a.SO_THANGDC_MOI) < 6 and max(a.SO_THANGDC_MOI) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----   ----Fiber tinh he so 1 (neu khong duy tri goi dadv -0.5), MyTV tinh he so 0.5 (neu co Fiber cung ky 0.4), Mesh tinh he so 0.5 (neu co Fiber cung ky 0.2)                                  
                           , case 
                                    when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                    , 0)

                                    ----Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                    when a.loaitb_id = 210 then 0.5 - nvl(0.3* (select distinct 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                                                                                where xu.loaitb_id in (58, 59)
                                                                                                and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                    ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                    when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                                                                                        where xu.loaitb_id in (58, 59)
                                                                                                        and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                else 0 
                               end Heso_dichvu
                            ,  sum(a.tien_thanhtoan) DTHU--, max(ngay_tt) ngay_tt, a.nhomtb_id, max(ngay_yc)
                            , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by max(a.rkm_id)) rnk
                        from thuhoi_bsc_dongia b 
                            join ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt a on b.rkm_id = a.rkm_id and a.thang = b.thang
--                              
                        where a.rkm_id is not null and a.thang = 202406 --and c.ma_pb = 'VNP0701400'--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id,  a.ma_to, a.ma_pb
                                          ,a.MANV_THUYETPHUC,  a.khachhang_id, a.goi_old_id, a.nhomtb_id, a.loaitb_id, a.ma_tb
--                   
        ) 
 select thang,
                         'KPI_NV' loai_tinh,
                         'THUHOI_BSC_DONGIA' ma_kpi,
                         a.ma_nv, a.ma_to, a.ma_pb,
                         round(sum(case when dthu > 0 and tien_khop > 0 then heso_chuky*heso_dichvu else 0 end)) da_giahan
--                        ,   round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                      , sum(dthu) DTHU_thanhcong--, b.ma_Vtcv
--      select *
        from  a 
            join ttkd_Bsc.nhanvien_202406 b on a.ma_nv = b.ma_nv
            join ttkd_Bsc.nhanvien_202406 c on a.ma_nv = c.ma_nv 
        where rnk = 1 and dthu > 0 --and c.ma_vtcv = 'VNP-HNHCM_BHKV_47' and b.ma_Vtcv = 'VNP-HNHCM_BHKV_47'
        group by a.thang, a.ma_nv, a.ma_to, a.ma_pb;--, b.ma_vtcv;
select* from thuhoi_bsc_dongia where thang = 202410;
update  ttkd_bsc.bangluong_KPI set thuchien=thuchien -1 WHERE  thang = 202406 and ma_kpi ='HCM_SL_ORDER_001' AND TEN_VTCv LIKE 'T?%' and ma_to in 
(select MA_TO, thuchien,ten_pb FROM  ttkd_bsc.bangluong_KPI   WHERE ma_nv in ('VNP016868', 'VNP016953', 'VNP017064') and thang = 202406 and ma_kpi ='HCM_SL_ORDER_001');
UPDATE ttkd_bsc.bangluong_KPI   SET TYLE_THUCHIEN = 0.53, MUCDO_HOANTHANH=0.53 WHERE MA_to = 'VNP0701404'  AND TEN_VTCv LIKE 'T?%' and thang = 202406 and ma_kpi ='HCM_SL_ORDER_001';

COMMIT;
SELECT* FROM TTKD_BSC.tl_giahan_tratruoc WHERE THANG =202407 AND 'THUHOI_DONGIA_GHTT' = LOAI_TINH;
insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, MA_NV, MA_TO, MA_PB, LOAI_TINH, MA_KPI, TIEN)
select b.ten_vtcv, b.ten_pb, a.* from thuhoi_bsc_dongia a
    join ttkd_Bsc.nhanvien b on a.manv_thuyetphuc = b.ma_nv and a.thang = b.thang 
where a.thang = 202407 and a.ma_pb is not null;
select* from ttkd_bsc.nhanvien where ma_nv = 'VNP017254';
insert into ttkd_bsc.tl_giahan_tratruoc(THANG, MA_NV, MA_TO, MA_PB, LOAI_TINH, MA_KPI, TIEN)
WITH TIEN AS 
(
SELECT  B.MA_NV,C.TIEN_THUYETPHUC*HESO_CHUKY*HESO_DICHVU tien, A.TIEN_KHOP, A.THUEBAO_ID , a.thang, b.ten_Vtcv, c.ipcc, c.ma_Tb, c.tien_xuathd, b.ten_pb, ten_to
FROM THUHOI_BSC_DONGIA A 
    JOIN ttkd_Bsc.nhanvien b on a.manv_thuyetphuc = b.ma_nv and a.thang = b.thang 
    join ttkd_Bsc.ct_dongia_tratruoc c on a.thang = c.thang and loai_tinh = 'DONGIATRA_OB' AND A.THUEBAO_ID = C.THUEBAO_ID AND A.MANV_THUYETPHUC = C.MA_NV
WHERE A.THANG = 202410 and a.ma_pb is not null-- and a.ma_Tb not in  ('p0530_fiber14','tuanle0723','ctylab99','thang0707tk','thanh2b','baominhtatsu','duy10107')
)


    select 202411 thang,a.ma_nv, b.ma_to, b.ma_pb, 'THUHOI_DONGIA_GHTT' LOAI_TINH,'DONGIA' MA_KPI, sum(A.tien) tien--, b.ten_to--, ten_pb
from tien a
    LEFT join ttkd_bsc.nhanvien b on a.ma_nv = b.ma_nv and a.thang = b.thang
where b.thang = 202410
GROUP BY a.ma_nv, b.ma_to, b.ma_pb,b.ten_to
;and b.donvi = 'TTKD'  and b.ma_pb not in ('VNP0702300','VNP0702500','VNP0702400','VNP0700600')
;
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202408 and loai_tinh = 'THUHOI_DONGIA_GHTT';
ROLLBACK;
commit;
SELECT * FROM TTKD_bSC.nhanvien_202406 ;where thang = 202405;
update ttkd_bsc.bangluong_dongia_202407 a set
thuhoi_dongia_ghtt =  (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc
                                where thang = 202407 and ma_kpi = 'DONGIA' and loai_tinh = 'THUHOI_DONGIA_GHTT' and ma_nv = a.MA_NV_HRM
                                group by ma_nv 
                                having  sum(tien)  <> 0)