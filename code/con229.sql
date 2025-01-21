select* from ttkd_Bsc.dongia_Vttp where thang = 202403
and ma_Nv in 
(select ma_nv from dongia_Vttp where thang = 202403 and thuebao_id not in (select thuebao_id from ttkd_Bsc.dongia_Vttp where thang = 202403));

select* from ttkd_bsc.ct_dongia_tratruoc a where thang = 202403 and  ma_kpi like 'DONGIA%'  and manv_Thuyetphuc in (select ma_Nv from ttkd_Bsc.nhanvien_vttp where thang = 202403)
select* from admin_hcm.donvi where donvi_id = 283413;

create table nhanvien_202405 as select* from ttkd_Bsc.nhanvien_202403 where 1 =2;

select* from hocnq_ttkd.ds_giahan_tratruoc2 where thuebao_id in (8115779,
8972667,
9669975,
10693569);

select* from v_Thongtinkm_All where thuebao_id in (8972667,9669975)
(8115779,10693569)--,
8972667,
9669975,
10693569);