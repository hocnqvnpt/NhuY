'VNP0701100','VNP0701400','VNP0701800'

insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, DA_GIAHAN_DUNG_HEN, DTHU_THANHCONG_DUNG_HEN)
with a as
(

             select     
             a.thang, a.thuebao_id, a.ma_to, a.ma_pb, ma_Tb
                                        ,a.MANV_THUYETPHUC ma_nv, goi_old_id, nhomtb_id
                                          ,sum( cuoc_dc_cu) tien_Dc_Cu
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
                            ,  sum(tien_thanhtoan) DTHU--, max(ngay_tt) ngay_tt, a.nhomtb_id, max(ngay_yc)
                            , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by max(a.rkm_id)) rnk
                        from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a 
--                              
                        where a.rkm_id is not null and thang = 202410 --and c.ma_pb = 'VNP0701400'--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id,  a.ma_to, a.ma_pb
                                          ,a.MANV_THUYETPHUC,  a.khachhang_id, goi_old_id, nhomtb_id, loaitb_id, ma_tb
--                   
        ) 
 select thang,
                         'KPI_NV' loai_tinh,
                         'HCM_SL_ORDER_001' ma_kpi,
                         a.ma_nv, a.ma_to, a.ma_pb,
                         round(sum(case when dthu > 0 and tien_khop > 0 then heso_chuky*heso_dichvu else 0 end)) da_giahan
--                        ,   round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                      , sum(dthu) DTHU_thanhcong
--      select *
        from  a 
        where rnk = 1 and dthu > 0 
        group by a.thang, a.ma_nv, a.ma_to, a.ma_pb;

---
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                	, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)                                                  
select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round( sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc
where thang = 202408 and MA_KPI in ('HCM_SL_ORDER_001')  and loai_tinh = 'KPI_NV' --and ma_TO = 'VNP0702219'
group by THANG, MA_KPI, MA_TO, MA_PB;   
commit;
DELETE from ttkd_Bsc.tl_giahan_tratruoc where thang =202410 and MA_KPI in ('HCM_SL_ORDER_001'); and loai_tinh = 'KPI_TO';
update TTKD_BSC.bangluong_kpi a set 
        thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_to = a.ma_to and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_SL_ORDER_001') ,
        tyle_thuchien = (select tyle  from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_to = a.ma_to and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_SL_ORDER_001'),
        mucdo_hoanthanh =  (select tyle  from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_to = a.ma_to and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_SL_ORDER_001')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_SL_COMBO_005')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) and thang = 202406 and  ma_Kpi = 'HCM_SL_ORDER_001';
select* from ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202406 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_SL_ORDER_001') 
;
delete from ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202406 and loai_tinh = 'KPI_PB' and a.MA_KPI in ('HCM_SL_ORDER_001') 
;
COMMIT;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                	, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN,  heso_giao)
select a.THANG, 'KPI_PB', a.MA_KPI, b.ma_nv, null, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
--                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc a left join (select DISTINCT ma_nv, ma_to, ma_kpi, thang, ma_pb  from ttkd_bsc.blkpi_dm_to_pgd where thang = 202408 and dichvu is null) b 
    on a.ma_to = b.ma_to 
where a.thang = 202408 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_SL_ORDER_001') -- and b.ma_nv = 'VNP017585'
group by a.THANG, a.MA_KPI, b.ma_nv, a.MA_PB ;
SELECT* FROM ttkd_bsc.blkpi_dm_to_pgd WHERE THANG = 202408 and dichvu is null;
select* from ttkd_bsc.bangluong_kpi where thang =202406 and MA_KPI in ('HCM_SL_ORDER_001') ;
ROLLBACK;
select *  from ttkd_bsc.blkpi_dm_to_pgd where thang = 202406 and  ma_nv = 'VNP017585';
select* from ttkd_bsc.nhanvien_202406 where ten_nv like '%S?n';
update ttkd_bsc.bangluong_kpi set MUCDO_HOANTHANH = MUCDO_HOANTHANH*100  where thang =202406 and MA_KPI in ('HCM_SL_ORDER_001') and MUCDO_HOANTHANH < 1;
select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202406 and MA_KPI in ('HCM_SL_ORDER_001') ;
select* from ttkd_bsc.bangluong_kpi   where thang =202406 and MA_KPI in ('HCM_SL_ORDER_001') and ma_nv = 'VNP017585';
commit;
select* from ;
update ttkd_bsc.bangluong_kpi set thuchien = 1894, tyle_Thuchien = round(1894/(660*7),4)*100  where thang =202406 and MA_KPI in ('HCM_SL_ORDER_001') and ma_nv = 'VNP017585';
