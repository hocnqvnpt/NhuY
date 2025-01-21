select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202403 and ma_To is null and ma_pb is not null-- and ma_pb = 'VNP0702500' and thang_ktdc_cu = 202403 and loaitb_id in (147,148)
update ttkd_Bsc.ct_bsc_giahan_cntt a
set ma_To = (select ma_to from ttkd_bsc.nhanvien_202403 where ma_nv = a.manv_Giao)
where thang = 202403 and ma_to is null 
commit;

select* from ttkd_Bsc.nhanvien_202403 where ma_Nv in ('VNP032920','CTV077029','CTV080065','CTV080946','VNP032920')