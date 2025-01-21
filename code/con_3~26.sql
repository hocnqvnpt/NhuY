select count(distinct thuebao_id) from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202404  and thang_ktdC_cu = 202405 and ma_pb = 'VNP0701500' and nvl(tien_khop,0) != 0;
select count(distinct thuebao_id) from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202404 and loaitb_id not in (147,148) and ma_pb = 'VNP0701500' and thang_ktdC_cu = 202404 and tien_khop is not null;

select* from bk_ca;

select* from ttkd_bsc.bangluong_kpi_202404 where HCM_CL_TNGOI_004 is not null --ma_nv like 'VNP028833';

select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_CL_TNGOI_004' and thang_kt is null;

select* from v_Thongtinkm_all where ma_Tb ='mesh0059927';
'phuchung6-14' t?m d?ng c?t xong ho?t ??ng l?i ngày 13/3
mesh0125538, mesh0003454, hcm_hnhue,hcm_quangthnh_21, mesh0059927 không có ngày BDDC, ;
select* from ttkd_Bsc.ct_Bsc_giahan_Cntt where ma_Tb = 'hcm_ca_00094709'    ;
select ma_gd, ma_tb from ttkD_bsc.ct_bsc_Giahan_cntt where ma_Tb in ('hcm_ca_00039786', 'hcm_ca_00076765', 'hcm_ca_00076811', 'hcm_ca_00076748') and thang = 202404;
select a.*, e.* from gan_nvcs a 
    left join ttkd_bct.db_Thuebao_ttkd b on a.ma_tb = b.ma_tb
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id