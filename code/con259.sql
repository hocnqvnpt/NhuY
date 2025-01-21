select sum(da_giahan_dung_Hen) from ttkd_Bsc.tl_giahan_tratruoc  where thang = 202406 and ma_kpi =  'HCM_SL_ORDER_001' and loai_tinh ='KPI_NV' and ma_To = 'VNP0701404';
select* from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202406 and ma_to = 'VNP0701404';
ma_Tb in ('hcm_ca_00042654','hcm_ca_00042655','hcm_ca_ivan_00021689');
select* from ttkd_Bsc.nhuy_Ct_bsc_ipcc_obghtt where thang = 202406;
select distinct ma_To from ttkd_bsc.nhanvien_202406 where teN_to='T? Ch?m Sóc Khách Hàng';
with pb as 
(
    select distinct ma_pb, case when ma_pb = 'VNP0702300' then 'Phòng Bán Hàng Doanh Nghi?p 1'
                                WHEN ma_pb = 'VNP0702400' then 'Phòng Bán Hàng Doanh Nghi?p 2'
                                WHEN ma_pb = 'VNP0702500' then 'Phòng Bán Hàng Doanh Nghi?p 3'
                                ELSE TEN_PB END TEN_PB
    from ttkd_bsc.nhanvien_202406
)
select A.*, PB.ma_pb, cast(null as varchar2(20)) ma_to, null ma_Nv from nhuy.ct_bsc_slpt_chua_xuly a
left join pb on a.ten_pBH  = PB.TEN_PB
;
select distinct ma_kpi from ttkd_bsc.tl_Giahan_tratruoc where thang = 202406;
select* from ttkd_Bsc.blkpi_danhmuc_kpi_vtcv where ma_kpi = 'HCM_CL_TONDV_001';
desc ttkd_bsc.nhanvien_202406;
rollback;
select* from css_hcm.trangthai_hd;
delete from ct_bsc_slpt_chua_xuly where MAGD_CHUAHT is null ;
commit;
select * from ct_Bsc_slpt_chua_xuly;
select  from nhuy.nhuy_ct_Bsc_slpt_chua_xuly
;
Select distinct ma_pb, ten_pb From ttkd_bsc.nhanvien where thang = 202406 and donvi = 'TTKD'  Group by ma_pb, ten_pb;
select distinct ma_pb, ten_pb from ;
drop table nhuy_ct_Bsc_slpt_chua_xuly;
update ttkd_bsc.nhanvien  set ten_pb = 'Phòng Bán Hàng Khu V?c Nam Sài Gòn'  where thang = 202406 and donvi = 'TTKD' and ma_pb = 'VNP0701400' 
and ten_pb != 'Phòng Bán Hàng Khu V?c Nam Sài Gòn' ;
Select ma_pb, ten_pb From ttkd_bsc.nhanvien where thang = 202406 and donvi = 'TTKD' and ma_pb = 'VNP0701200' Group by ma_pb, ten_pb;
flashback table nhuy_ct_Bsc_slpt_chua_xuly to before drop;
commit;
delete from nhuy_ct_Bsc_slpt_chua_xuly where thang = 202407;
insert into nhuy_ct_Bsc_slpt_chua_xuly (THANG, THUEBAO_ID, KHACHHANG_ID, MA_TB,  THANG_KT, TRANGTHAI_TB, TEN_TB, DIACHI_LD,  MA_GD, KETQUA_GH, MA_NV,
TEN_NV_TIEPTHI, TEN_PB_TIEPTHI, TEN_TO_TIEPTHI, MANV_TAODH, TEN_NV_TAODH, TEN_DV_TAODH,  MAGD_CHUAHT, BAOHONG_ID, LUONGGIAO_TTVT, TRANGTHAI_BH, 
TRANGTHAI_PH, MANV_OB, NHANVIEN_ID_OB, TEN_NV_OB, THUEBAO_ID_DAIDIEN, KETQUA_OB, TT_KETNOI_LS, MA_PB, MA_TO, CHUADEN_HEN_TT, HOAN_HEN_TT, QUA_HEN_TT_TTKD, PAKH_TTKD, KHONG_KQ, 
KHAC, DANG_OB, CHUA_CHAM, KH_HEN_GOILAI, PHIEU_CSGH_CHAM, TEN_PBH)
with dm_to as 
(
select distinct ma_To, ma_pb  ,  case when ma_pb = 'VNP0702300' then 'Phòng Bán Hàng Doanh Nghi?p 1'
                                WHEN ma_pb = 'VNP0702400' then 'Phòng Bán Hàng Doanh Nghi?p 2'
                                WHEN ma_pb = 'VNP0702500' then 'Phòng Bán Hàng Doanh Nghi?p 3'
                                ELSE TEN_PB END TEN_PB
        from ttkd_bsc.nhanvien where thang =202407 and teN_to='T? Sau Bán Hàng'
)
--, cham as (
--    select thuebao_id, mapb_ql
--    from ttkd_Bct.db_thuebao_ttkd 
--)
select 202407 thang ,a.THUEBAO_ID, a.KHACHHANG_ID, a.MA_TB, THANG_KT, TRANGTHAI_TB, TEN_TB, DIACHI_LD,  MA_GD, KETQUA_GH, MANV_TIEPTHI ma_Nv,
TEN_NV_TIEPTHI, TEN_PB_TIEPTHI, TEN_TO_TIEPTHI, MANV_TAODH, TEN_NV_TAODH, TEN_DV_TAODH,    MAGD_CHUAHT, BAOHONG_ID, LUONGGIAO_TTVT, 
TRANGTHAI_BH, TRANGTHAI_PH, MANV_OB, NHANVIEN_ID_OB, TEN_NV_OB, THUEBAO_ID_DAIDIEN, KETQUA_OB, TT_KETNOI_LS,  t.ma_pb, t.ma_to,chuaden_hen_tt,hoan_hen_tt,qua_hen_tt_ttkd,
pakh_ttkd,khong_kq,khac,dang_ob,chua_cham, kh_hen_goilai, phieu_csgh_cham, a.ten_pbh
from ct_Bsc_slpt_chua_xuly  a
--    left join cham b on a.thuebao_id =b.thuebao_id
    left join dm_to t on a.ten_pbh = t.ten_pb
where a.thang = 202407 and (chuaden_hen_tt = 1 or hoan_hen_tt = 1 or qua_hen_tt_ttkd = 1 or pakh_ttkd =1 or khong_kq =1 or khac = 1 or dang_ob = 1 or chua_cham = 1 
or kh_hen_goilai = 1)
;
COMMIT;
not exists (select 1 )
select* from ttkd_bsc.tl_Giahan_tratruoc where ma_kpi = 'HCM_CL_TONDV_001';
insert into  ttkd_bsc.tl_Giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN)
select THANG, LOAI_TINH,'HCM_CL_TONDV_001' MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN
from  ttkd_bsc.tl_Giahan_tratruoc where ma_kpi ='HCM_TB_GIAHA_022' and thang = 202407 and loai_tinh in ('KPI_PB');
(select count (distinct thuebao_id) sl, ma_to,ma_pb from nhuy_ct_Bsc_slpt_chua_xuly where thang = 202407 group by ma_to, ma_pb);
select thang, 'HCM_CL_TONDV_001'ma_kpi, loai_Tinh from ttkd_Bsc.tl_Giahan_tratruoc where ma_kpi = 'HCM_CL_TONDV_001';
select *
from nhuy_ct_Bsc_slpt_chua_xuly where ten_pb = 'Phòng Bán Hàng Khu V?c Bình Chánh' and  qua_hen_tt_ttkd = 1;
(chuaden_hen_tt + hoan_hen_tt + qua_hen_tt_ttkd + pakh_ttkd + khong_kq + khac + dang_ob + chua_cham +phieu_csgh_cham+kh_hen_goilai) > 1;
order by;;
create table ct_bsc_slpt_chua_xuly_1 as select* from ct_Bsc_slpt_chua_xuly ;
group by thuebao_id; 
update ttkd_bsc.tl_Giahan_tratruoc a 
set SL_PHIEUTON = (select count (thuebao_id) from nhuy_ct_Bsc_slpt_chua_xuly where ma_pb = a.ma_pb AND THANG = 202407 group by ma_to, ma_pb)
where  ma_kpi = 'HCM_CL_TONDV_001' and loai_tinh in( 'KPI_PB','KPI_TO') and thang = 202407;
UPDATE ttkd_bsc.tl_Giahan_tratruoc a SET TYLE = ROUND((SL_PHIEUTON)*100/TONG,2)
where  ma_kpi = 'HCM_CL_TONDV_001' and loai_tinh IN ('KPI_TO', 'KPI_PB') and thang = 202407;
SELECT* FROM ttkd_bsc.tl_Giahan_tratruoc
where  ma_kpi = 'HCM_CL_TONDV_001' and loai_tinh IN ('KPI_TO', 'KPI_PB') and thang = 202407;
ROLLBACK;
COMMIT;
update 
