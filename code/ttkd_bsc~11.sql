update ttkd_Bsc.ct_Bsc_giahan_cntt
set tien_khop = 1 where thang = 202404 and ma_tb in ('hcm_ca_00055400','hcm_ca_00039226');
commit;
update ttkd_Bsc.tl_Giahan_tratruoc set VNP027256;

 select bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                    where  phieu_id = 8334088
                                    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                    having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM  -10;
                                    
                                    CREATE TABLE FFFFFFFFFFFF AS SELECT* FROM TTKD_bSC.BANGLUONG_KPI_202404 ;

update TTKD_BSC.bangluong_kpi_202404 a 
set 
 HCM_TB_GIAHA_024 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang_kt is null and MA_VTCV = a.MA_VTCV); -- AND MA_NV = 'VNP016578';
SELECT * FROM TTKD_bSC.NHANVIEN_202404 WHERE  MA_NV = 'VNP016578';
and ma_Vtcv = 'VNP-HNHCM_BHKV_41'
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202403 where 
rollback;
COMMIT;
-- TO TRUONG
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'
;
update TTKD_BSC.bangluong_kpi_202404 a 
set HCM_TB_GIAHA_024= (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024') 
                                and thang_kt is null and MA_VTCV = a.MA_VTCV);

commit;
-- PHO GIAM DOC
(select * from ttkd_bsc.tl_giahan_Tratruoc WHERE THANG = 202403 AND MA_KPI in ('HCM_TB_GIAHA_026')  AND MA_NV = 'VNP017305')

select* from ttkd_Bsc.bangluong_kpi_202403;
select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_TB_GIAHA_026  from  ttkd_Bsc.bangluong_kpi_202403  where ma_Nv ='VNP017305';
update ttkd_Bsc.bangluong_kpi_202403  set HCM_TB_GIAHA_026 = 100 where ma_Nv ='VNP017305';
update TTKD_BSC.bangluong_kpi_202404 a 
                set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = 202404 and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_024' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024')  
                                            and thang_kt is null and MA_VTCV = a.MA_VTCV);
commit;

select a.HCM_TB_GIAHA_024,b.HCM_TB_GIAHA_024, a.* from ttkd_bsc.bangluong_kpi_202404 a join nhuy.FFFFFFFFFFFF b on a.ma_nv = b.ma_nv 
where a.HCM_TB_GIAHA_024 != b.HCM_TB_GIAHA_024;

update ttkd_bsc.tl_giahan_tratruoc set DA_GIAHAN_DUNG_HEN = 16, TYLE = round(16*100/25,2)where ma_to = 'VNP0702513' and thang = 202404 and loai_tinh = 'KPI_TO' AND MA_KPI = 'HCM_TB_GIAHA_024';

select HCM_TB_GIAHA_024,  a.* from ttkd_bsc.bangluong_kpi_202404  a where ma_to = 'VNP0702513';
    select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202404 and ma_Tb in ('hcm_ca_00039355 ','hcm_ca_00076372 ');
    
    select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202404 and loaitb_id in (147,148) and ma_Tb in ('hcm_tmvn_00000159','hcm_tmvn_00000415','hcm_tmvn_00003099')
    update ttkd_Bsc.ct_bsc_giahan_cntt 
    set tien_khop = 1
    where thang = 202404 and loaitb_id in (147,148) and ma_Tb in ('hcm_tmvn_00000159','hcm_tmvn_00003099','hcm_tmvn_00000415');
    insert into tl_giahan_tratruoc DELETE FROM  ttkd_Bsc.tl_giahan_tratruoc where thang =202404 and ma_kpi ='HCM_TB_GIAHA_026';
    COMMIT;