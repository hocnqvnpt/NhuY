select sum(s?_ti?n)-- a.*, b.ma_Nv,b.ma_To,b.ma_pb, 202408 thang
from dgia_nv a
join ttkd_bsc.nhanvien b on a.tên_Nv = b.ten_nv and b.thang = 202408 and donvi = 'TTKD' AND MA_PB NOT IN ('VNP0702500') and ten_to = 'T? Bán Hàng Online';
select* from ttkd_bsc.nhanvien where thang = 202406 and ma_Nv = 'VNP017576';
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202408 and manv_giao = 'VNP017576' ;and ma_to = 'VNP0702308' ;

select* from ttkd_Bsc.blkpi_danhmuc_kpi;
update ttkd_bsc.ct_bsc_giahan_cntt
set ma_to ='VNP0702309'
where manv_giao = 'VNP017576' and thang = 202408 and ma_to != 'VNP0702309';
commit;
update ttkd_Bsc.tl_giahan_tratruoc set ma_To = 'VNP0702309'
where thang = 202408 and ma_Nv = 'VNP017576' and ma_To != 'VNP0702309';
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_to = 'VNP0702308';

