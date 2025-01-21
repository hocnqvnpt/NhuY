select* from ttkd_Bsc.ct_Dongia_tratruoc where ma_Tb ='ngcdung93848438';
select* from ttkd_bsc.dongia_vttp  where ma_Tb ='ngcdung93848438';
select* from ttkd_Bsc.nhanvien where ten_nv like '%D?ng';
insert into ttkd_bsc.blkpi_danhmuc_sms
select ma_nv, ten_nv, sdt, ma_pb, ten_pb from ttkd_Bsc.nhanvien where thang = 202408 and ma_Nv in ('VNP001694','VNP043893');
commit;