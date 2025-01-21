create table tmp_update as 
with ma_Tb as 
(select  ma_tb, thuebao_id, hdkh_id,to_number(to_char(ngay_yc,'yyyymmdd')) ngay_yc from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang =202405 and manv_thuyetphuc = 'MEDIAPAY')
, ds as (select b.thuebao_id, b.ma_tb, max(a.hdkh_id) hdkh_id, max(a.ngay_yc) ngay_yc--, a.ma_gd
from css_hcm.hd_khachhang a 
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id
    join ma_tb c on b.thuebao_id = c.thuebao_id
where a.hdkh_id < c.hdkh_id and b.ngay_tt is not null and to_number(to_char(a.ngay_yc+45,'yyyymmdd'))  >= c.ngay_yc 
group by b.thuebao_id, b.ma_Tb)
--select * from ds where ma_Tb ='tminhson105';
select ds.*, c.ma_to, c.ma_pb, c.ma_nv
from ds
    left join css_Hcm.hd_khachhang a on ds.hdkh_id = a.hdkh_id
    left join admin_hcm.nhanvien_onebss b on a.ctv_id = b.nhanvien_id
    left join ttkd_Bsc.nhanvien_202405 c on b.ma_nv = c.ma_nv
;

SELECT* FROM TMP_UPDATE;
update ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a
--SELECT distinct THUEBAO_ID FROM ttkd_bsc.CT_DONGIA_tRATRUOC a
set
ma_to = (select ma_to from ttkd_bsc.nhanvien_202405 where a.manv_thuyetphuc = ma_nv ),
ma_pb = (select ma_pb from ttkd_bsc.nhanvien_202405 where a.manv_thuyetphuc = ma_nv )
where thang in ( 202405) AND THUEBAO_ID IN (sELECT THUEBAO_ID FROM TMP_UPDATE);
--ROLLBACK;
select* from css_Hcm.hd_khachhang where ma_gd = 'HCM-TT/02751128'
;
COMMIT;
create table 
ROLLBACK;
UPDATE ttkd_bsc.CT_DONGIA_tRATRUOC 
SET THANG = 202405, LOAI_TINH = 'DONGIATRA_OB';
select* from ttkd_bsc.tl_giahan_tratruoc;
insert into ttkd_bsc.tl_giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, DA_GIAHAN_Dung_Hen, dthu_thanhcong_dung_hen)
with a as (
select* from (
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
----                                    
                                   , case ----Fiber tinh he so 1 (neu khong duy tri goi dadv -0.5), MyTV tinh he so 0.5 (neu co Fiber cung ky 0.4), Mesh tinh he so 0.5 (neu co Fiber cung ky 0.2) 
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
                        where a.rkm_id is not null and thang = 202405 and thuebao_id in (select thuebao_id from tmp_update) --and c.ma_pb = 'VNP0701400'--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id,  a.ma_to, a.ma_pb
                                          ,a.MANV_THUYETPHUC,  a.khachhang_id, goi_old_id, nhomtb_id, loaitb_id, ma_tb
--                   
        ) where rnk = 1 and dthu > 0  ) 
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
        drop table backup_kpi;
        create table backup_kpi as select* from ttkd_Bsc.bangluong_kpi_202405;
        COMMIT;
        create table backup_ldg as select* from ttkd_Bsc.bangluong_dongia_202405;
        delete from TTKD_BSC.tl_Giahan_Tratruoc where thang = 202405 and loai_tinh= 'DONGIATRA_OB';
     INSERT INTO tl_Giahan_Tratruoc  select* from TTKD_BSC.tl_Giahan_Tratruoc where thang = 202405 and loai_tinh= 'DONGIATRA_OB';
INSERT INTO TTKD_bSC.TL_GIAHAN_TRATRUOC (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TIEN)
with tp as 
(
    select ma_nv, SUM(NVL(tien_thuyetphuc,0)*heso_chuky*heso_dichvu) TIEN--, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and ipcc = 1
    GROUP BY MA_nV
)
,
XP AS 
(
    select NHANVIEN_XUATHD, SUM(NVL(TIEN_XUATHD,0)*heso_chuky*heso_dichvu) TIEN--, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB' --AND NHANVIEN_XUATHD IS NOT NULL
    GROUP BY NHANVIEN_XUATHD
)
, tien as (
select* from tp union all 
select* from xp)

select 202405 thang, 'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI , a.ma_nv, b.ma_to, b.ma_pb, SUM(A.TIEN) TIEN
from tien a 
    join ttkd_Bsc.nhanvien_202405 b on a.ma_nv = b.ma_nv 
WHERE MA_PB NOT IN ('VNP0702500','VNP0702400','VNP0702300')
GROUP BY a.ma_nv, b.ma_to, b.ma_pb;
select a.ma_Nv, a.tien, b.tien , (A.TIEN-B.TIEN) from TTKD_BSC.tl_Giahan_Tratruoc a
join tl_Giahan_Tratruoc b on a.ma_nv = b.ma_nv and a.thang = b.thang and a.loai_tinh = b.loai_tinh
where a.thang = 202405 and a.loai_tinh= 'DONGIATRA_OB' and a.tien > b.tien;
select a.ma_Nv, a.hcm_sl_order_001, b.hcm_sl_order_001
from backup_kpi a join ttkd_bsc.bangluong_kpi_202405 b on a.ma_Nv = b.ma_nv
where a.hcm_sl_order_001 < b.hcm_sl_order_001;
COMMIT;
select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202405 and loai_Tinh = 'DONGIATRA_OB' AND THUEBAO_ID  IN (SELECT THUEBAO_ID FROM TMP_UPDATE);
UPDATE TTKD_BSC.ct_dongia_tratruoc A SET KHACHHANG_ID = (sELECT KHACHHANG_ID FROM CSS_HCM.DB_tHUEBAO WHERE A.THUEBAO_ID = THUEBAO_ID) WHERE THANG = 202405 AND  THUEBAO_ID  IN (SELECT THUEBAO_ID FROM TMP_UPDATE) ;
UPDATE TTKD_BSC.ct_dongia_tratruoc A SET IPCC = 1 WHERE  THUEBAO_ID  IN (SELECT THUEBAO_ID FROM TMP_UPDATE) 
AND EXISTS (SELECT 1 FROM TTKD_BSC.NHUY_CT_IPCC_OBGHTT WHERE THANG = 202405 AND KHACHHANG_ID = A.KHACHHANG_ID) AND THANG = 202405;

select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202405 and loai_Tinh = 'DONGIATRA_OB';