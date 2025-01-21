select * from ttkd_bsc.blkpi_dm_to_pgd WHERE THANG = 202401

select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_022')
                and thang_kt is null
;
select* from ttkd_bsc.nhanvien_202401 where ma_nv ='VNP017304'
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202401 and loai_tinh = 'DONGIATRU'
SELECT* FROM V_tHONGTINKM_ALL WHERE MA_tB ='concung-btn2'
SELECT*FROM TTKD_BSC.CT_BSC_tRATRUOC_MOI WHERE THANG = 202401 AND rkm_id is not null AND TIEN_KHOP = 0
select distinct ma_to, ten_vtcv, ma_vtcv from ttkd_Bsc.nhanvien_202401 where ten_pb in ('Phòng Khách Hàng Doanh Nghi?p 1','Phòng Khách Hàng Doanh Nghi?p 2','Phòng Khách Hàng Doanh Nghi?p 3')

select* from  ttkd_bsc.ct_dongia_tratruoc where thang = 202401
rollback;
select distinct ma_vtcv, ten_vtcv,round( thang_ktdc_cu/100,0) from ttkd_bsc.ct_bsc_tratruoc_moi_30day a join 
ttkd_bsc.nhanvien_202401 b on a.manv_giao = b.ma_nv where thang = 202401 
union all 
select distinct ma_vtcv, ten_vtcv,round( thang_ktdc_cu/100,0) from ttkd_bsc.ct_bsc_tratruoc_moi a join 
ttkd_bsc.nhanvien_202401 b on a.manv_giao = b.ma_nv where thang = 202401 ;

select* from ttkd_Bsc.ct_bsc_Giahan_cntt where thang_ktdc_Cu = 202401 and thang = 202401 
and loaitb_id  in (147,148)--(55 ,80 ,116 ,117,132,140,154,181,288,318 ) and ma_pb is not null and maNv_giao is not null and ma_To is not null-- and tien_khop is null
select* from css_Hcm.db_cntt where thuebao_id in (8757230,8757231)

insert into tl_homecombo
    select thang, 'KPI_NV' LOAI_TINH
                    ,  'HCM_CT_CLUOC_001' ma_kpi
                    , ma_nv, ma_to, ma_pb, null sogiao
                    , count(distinct ma_tb) sothuchien
                    , null tyle2, null
--    select *
    from ct_bsc_homecombo 
    where loai_kpi like 'Fiber_%' and thang = 202401 and ma_pb is not null
                   ---     and goi_id in (16006, 16008, 16010, 16012, 16014, 16007, 16009, 16011, 16013, 16015)
    group by thang, ma_nv, ma_to, ma_pb
;
------Update to truong----------
insert into tl_homecombo
    select thang, 'KPI_TO' LOAI_TINH, ma_kpi, null ma_nv, ma_to, ma_pb
                    , sum(sogiao) sogiao, sum(sothuchien) sothuchien, round(sum(sothuchien) * 100/sum(sogiao), 2) tyle
                    , null
    from tl_homecombo
    where thang = 202401 and LOAI_TINH = 'KPI_NV' and  ma_kpi = 'HCM_CT_CLUOC_001' 
    group by thang, ma_kpi, ma_to, ma_pb
;
select* from ttkd_bsc.tl_homecombo where thang = 202312 and ma_kpi = 'HCM_CT_CLUOC_001'
------Update Pho Giam doc----------
insert into tl_homecombo 
delete from ttkd_Bsc.tl_homecombo where thang = 202311 and loai_tinh = 'KPI_PB'
select* from ttkd_Bsc.tl_homecombo where thang = 202401 and loai_tinh = 'KPI_PB'
commit;
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 1
delete from ttkd_bsc.tl_homecombo where thang = 202401 and loai_tinh = 'KPI_PB' and ma_kpi = 'HCM_CT_CLUOC_001' ;
commit;
insert into tl_homecombo
    select a.thang, 'KPI_PB' LOAI_TINH, a.ma_kpi, b.ma_nv ma_nv, null ma_to, a.ma_pb
                    , sum(sogiao) sogiao, sum(sothuchien) sothuchien, round(sum(sothuchien) * 100/sum(sogiao), 2) tyle, b.ma_nv ldp_phutrach
    from tl_homecombo a left join (select * from ttkd_bsc.blkpi_dm_to_pgd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
    where a.thang = 202401 and LOAI_TINH = 'KPI_TO'  and  a.ma_kpi = 'HCM_CT_CLUOC_001' 
    group by a.thang, a.ma_kpi, a.ma_pb, b.ma_nv
    
select* from tl_homecombo where thang = 202401 and LOAI_TINH = 'KPI_PB';
select* from ttkd_bsc.tl_homecombo where thang = 202401 and LOAI_TINH = 'KPI_PB';

;