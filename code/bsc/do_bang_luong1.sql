-- nhanvien
SELECT DISTINCT ma_Vtcv FROM   ttkd_bsc.bangluong_kpi where MA_KPI ='HCM_CL_TONDV_001' AND TYLE_THUCHIEN is not null AND THANG = 202406 AND MA_VTCV IN 
(select DISTINCT A.MA_VTCV from TTKD_BSC.blkpi_danhmuc_kpi_vtcv A where ma_kpi in ( 'HCM_TB_GIAHA_024','HCM_TB_GIAHA_026') 
                                and thang=202407);
SELECT DISTINCT MA_vTCV, LOAI_TINH FROM TTKD_bSC.TL_GIAHAN_TRATRUOC A JOIN TTKD_BSC.NHANVIEN_202406 B ON A.MA_NV = B.MA_NV WHERE THANG = 202406 AND MA_KPI ='HCM_TB_GIAHA_024';
select DISTINCT A.* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv A where ma_kpi in ( 'HCM_TB_GIAHA_022','HCM_TB_GdIAHA_026') 
                                and thang=202407 ;
                                
select * from ttkd_Bsc.bangluong_kpi_202407 where HCM_TB_GIAHA_024 is not null;
UPDATE ttkd_Bsc.bangluong_kpi_202407 SET HCM_TB_GIAHA_024=HCM_TB_GIAHA_026*100 where HCM_TB_GIAHA_026 is not null;
ROLLBACK;
COMMIT;
select * from ttkd_Bsc.bangluong_kpi_202407 where HCM_TB_GIAHA_024 is not null;
select ma_Gd from ttkdhcm_ktnv.ghtt_chotngay_271  where ma_Tb in ('hcm_ca_00076372','hcm_ca_00039355') and ngay_chot=to_date('20240503','yyyymmdd');
select * from ttkd_Bsc.bangluong_kpi_202402 where HCM_TB_GIAHA_023 is not null and ma_vtcv in ('VNP-HNHCM_KHDN_4','VNP-HNHCM_KHDN_3');
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_SL_COMBO_006 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_SL_COMBO_006' ) 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_COMBO_006') and thang=202407 and MA_VTCV = a.MA_VTCV) ;AND MA_NV = 'VNP027256';
;
commit;
select* from ttkd_bsc.tl_giahan_tratruoc  where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm');
select a.*,a.HCM_SL_ORDER_001, b.HCM_SL_ORDER_001 from ttkd_Bsc.bangluong_kpi_202404 a 
join nhuy.bsc b on a.ma_nv_hrm = b.ma_nv_hrm 
where nvl(a.HCM_SL_ORDER_001,0) > nvl(b.HCM_SL_ORDER_001,0);
----------------
commit;

SELECT* FROM  ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' AND MA_KPI ='HCM_SL_COMBO_005';
select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO,HCM_SL_ORDER_001 from TTKD_BSC.bangluong_kpi_202404 where HCM_SL_ORDER_001 is not null;
--------------- Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202407 a 
set HCM_TB_GIAHA_022 =(select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to 
and ma_pb = a.ma_donvi
and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_022')
    and thang=202407 and MA_VTCV = a.MA_VTCV) --AND ma_TO ='VNP0702407'
;
select* from ttkd_Bsc.nhanvien_202404 where sdt = '0919171777';
commit;
SELECT* FROM ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB'and ma_kpi = 'HCM_SL_COMBO_006'   ;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202407 a 
set HCM_TB_GIAHA_022 = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
and ma_nv = a.ma_nv_hrm 
and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_022') and thang=202407 and MA_VTCV = a.MA_VTCV) ;

-- BSC 30 ngay'
select distinct ten_vtcv , ma_vtcv from TTKD_BSC.bangluong_kpi_202402 where HCM_TB_GIAHA_022 is not null;
update TTKD_BSC.bangluong_kpi_202407 a set 
HCM_TB_GIAHA_022 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_022') and thang=202407 and MA_VTCV = a.MA_VTCV)

;
COMMIT;
select* from nhuy.bk_ca;
select* from ttkd_bsc.tl_Giahan_tratruoc where ma_kpi = 'HCM_TB_GIAHA_022' and ma_nv = 'VNP017802';
select hcm_Tb_giaha_022 from TTKD_BSC.bangluong_kpi_202404 where hcm_Tb_giaha_022 is not null and ma_vtcv = 'VNP-HNHCM_BHKV_48';
select* from css_hcm.khuvuc;
---------------Ty le cua To truong -----
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202402 and ma_nv = '';
update TTKD_BSC.bangluong_kpi_202407 a 
set HCM_TB_GIAHA_023 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_023')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_023')
and thang=202407 and MA_VTCV = a.MA_VTCV); AND MA_TO = 'VNP0702509'
;   
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202406 and ma_Kpi = 'HCM_TB_GIAHA_023' AND LOAI_TINH = 'KPI_PB';
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202407 a 
set HCM_SL_COMBO_006 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv_hrm 
and ma_kpi = 'HCM_SL_COMBO_006' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_SL_COMBO_006') and thang=202407 and MA_VTCV = a.MA_VTCV);

commit;
select* from ttkd_Bsc.nhanvien_202404 where ma_nv in ('VNP019511','VNP019531','VNP019500','CTV021923');
	select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_025' and thang=202407

-- CA 
-- NHAN VIEN 
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_nv = 'VNP017400' --and ma_vtcv = 'VNP-HNHCM_BHKV_18'
select HCM_TB_GIAHA_025,HCM_TB_GIAHA_024 from TTKD_BSC.bangluong_kpi_202402  where ma_vtcv = 'VNP-HNHCM_BHKV_41'
update TTKD_BSC.bangluong_kpi_202404 a 
set 
 HCM_TB_GIAHA_025 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_025')

where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in
('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') and thang=202407 and MA_VTCV = a.MA_VTCV) AND MA_NV ='VNP017930';
and A.ma_vtcv = 'VNP-HNHCM_BHKV_41'
--and hcm_tb_giaha_025 is  null or hcm_tb_giaha_024 is null
select distinct a.ma_nv from ttkd_Bsc.tl_giahan_tratruoc a join ttkd_Bsc.nhanvien_202402 b on a.ma_nv = b.ma_nv where thang = 202402 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') 
and b.ma_vtcv  = 'VNP-HNHCM_BHKV_41'
rollback;
COMMIT;
select* from nhuy.bk_ca;
-- TO TRUONG
select* from ttkd_bsc.nhanvien_202402 where ma_vtcv = 'VNP-HNHCM_BHKV_41'

SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'

update TTKD_BSC.bangluong_kpi_202404 a 
set  HCM_TB_GIAHA_025 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to  AND MA_PB =A.MA_DONVI and ma_kpi = 'HCM_TB_GIAHA_025')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') 
                                and thang=202407 and MA_VTCV = a.MA_VTCV) AND MA_tO ='VNP0702308';

commit;

-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi_202404 a 
                set HCM_TB_GIAHA_025 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                    where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                                                 and ma_nv = a.ma_nv_hrm AND MA_PB  = A.MA_DONVI and ma_kpi = 'HCM_TB_GIAHA_025' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025')  
                                            and thang=202407 and MA_VTCV = a.MA_VTCV) AND MA_NV = 'VNP017763'
     COMMIT;
;
select * from ttkd_bsc.bangluong_kpi_202404 where ma_nv in ('VNP017305','VNP017763','VNP017191')
select a.HCM_TB_GIAHA_025, b.HCM_TB_GIAHA_025, a.* from ttkd_Bsc.bangluong_kpi_202404 a join bk_t b on a.ma_nv = b.ma_nv where nvl(a.HCM_TB_GIAHA_025,0) != nvl(b.HCM_TB_GIAHA_025,0);
create table bk_t as select* from ttkd_bsc.bangluong_kpi_202404;
select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_TB_GIAHA_025 from ttkd_Bsc.bangluong_kpi_202404 where ma_Nv in ('VNP017639','VNP017930','VNP017763');ma_to = 'VNP0702308';
select* from ttkd_bsc.nhanvien_202403 where ma_To = 'VNP0702301';
insert into ttkd_bsc.ct_bsc_giahan_cntt select* from nhuy.bk_ca where phong_giao != 'Phòng Bán Hàng Khu V?c Tân Bình';
commit;
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202404 and loai_tinh = 'KPI_TO' AND MA_tO IN (
select ma_to from ttkd_bsc.blkpi_dm_to_pgd where thang = 202402 and ma_pb = 'VNP0701600' and ma_kpi = 'HCM_TB_GIAHA_025');
HCM_TB_GIAHA_025
select a.ma_nv, a.ma_to, a.ten_vtcv, a.hcm_tb_giaha_024,b.hcm_tb_giaha_024, a.hcm_tb_giaha_025,b.hcm_tb_giaha_025
from ttkd_bsc.bangluong_kpi_202403 a
left join BU_BL b on a.ma_nv =b.ma_nv
where a.hcm_tb_giaha_024 != b.hcm_tb_giaha_024 or a.hcm_tb_giaha_025 != b.hcm_tb_giaha_025;
select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, hcm_tb_giaha_024 from ttkd_bsc.bangluong_kpi_202403
where ma_nv in ('VNP027256','VNP019527');
select* from bk_ca;
-- TEN MIEN
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_nv = 'VNP017400' --and ma_vtcv = 'VNP-HNHCM_BHKV_18'
update TTKD_BSC.bangluong_kpi_202404 a 
set 
 HCM_TB_GIAHA_026 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_026')

where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang=202407 and MA_VTCV = a.MA_VTCV)  AND MA_NV = 'VNP016578';
SELECT * FROM TTKD_bSC.NHANVIEN_202403 WHERE  MA_NV = 'VNP016578';
and ma_Vtcv = 'VNP-HNHCM_BHKV_41'
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202403 where 
rollback;
COMMIT;
-- TO TRUONG
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'

update TTKD_BSC.bangluong_kpi_202404 a 
set HCM_TB_GIAHA_026= (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_026') 
                                and thang=202407 and MA_VTCV = a.MA_VTCV);

commit;
-- PHO GIAM DOC
(select * from ttkd_bsc.tl_giahan_Tratruoc WHERE THANG = 202403 AND MA_KPI in ('HCM_TB_GIAHA_026')  AND MA_NV = 'VNP017305')

select* from ttkd_Bsc.bangluong_kpi_202403;
select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_TB_GIAHA_026  from  ttkd_Bsc.bangluong_kpi_202403  where ma_Nv ='VNP017305';
update ttkd_Bsc.bangluong_kpi_202403  set HCM_TB_GIAHA_026 = 100 where ma_Nv ='VNP017305';
update TTKD_BSC.bangluong_kpi_202404 a 
                set HCM_TB_GIAHA_026 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = 202404 and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_026' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_026')  
                                            and thang=202407 and MA_VTCV = a.MA_VTCV) AND MA_NV = 'VNP017305'
commit;

SELECT THUEBAO_ID FROM (;
select LOAI_TINH ,A.MA_tB ,A.THUEBAO_ID, TO_nUMBER(TO_CHAR(NGAY_KT_MG,'YYYYMM'))THANG_KT , NGAY_BDDC , a.NGAY_TT, c.rkm_id, ROW_NUMBER() OVER (PARTITION BY A.THUEBAO_ID ORDER BY NGAY_KT_MG ) RN
    from ttkd_Bsc.ct_dongia_Tratruoc A
    JOIN DS_GIAHAN_tRATRUOC2 B ON A.THUEBAO_ID = B.THUEBAO_ID --AND NHOM_DATCOC_ID = 1
    join ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt c on a.thuebao_id = c.thuebao_id and c.rkm_id is not null;

GROUP BY THUEBAO_ID HAVING COUNT(THUEBAO_ID) > 1;

SELECT* FROM TTKD_BSC.CT_DONGIA_tRATRUOC WHERE MA_tB = 'hcm_alocongthuong';
select* from v_thongtinkm_all where ma_Tb ='ttthu0714'