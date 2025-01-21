select* from ttkd_Bsc.nhanvien where ten_Nv = 'Tiêu Lê Tôn';

select* from 
--update 
ttkd_Bsc.ct_Dongia_tratruoc a
--set tien_xuathd = 5000
where thang = 202408 and loai_tinh = 'DONGIATRA_OB' AND TIEN_KHOP = 1 AND TYLE_THANHCONG IS not NULL and ghichu  !='gach no tu dong'
and tien_thuyetphuc + tien_xuathd <> 10000 and tien_xuathd <> 0 ; and ma_Nv != nhanvien_Xuathd; AND NVL(TIEN_THUYETPHUC,0) = 0;
commit;

select thungan_tt_id, thungan_hd_id , nhanvien_id, ma_gd, so_Ct
from css_hcm.phieutt_hd where ma_Gd in (
select b.ma_gd
from ttkd_Bsc.ct_Dongia_tratruoc a 
join ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt b on a.thuebao_id = b.thuebao_id and a.thang = b.thang and nvl(a.ma_nv,'a') = nvl(b.manv_thuyetphuc,'a')
where a.thang = 202408 and loai_tinh = 'DONGIATRA_OB' AND a.TIEN_KHOP = 1 and  TYLE_THANHCONG IS not NULL and 
tien_thuyetphuc + tien_xuathd <> 10000 and ghichu  !='gach no tu dong' 
and a.nhanvien_xuathd not in (select ma_Nv  from ttkd_Bsc.nhanvien where thang = 202408 and ma_pb = 'VNP0700600' )
); 
select thungan_tt_id, thungan_hd_id , nhanvien_id from css_hcm.phieutt_hd where ma_Gd = 'HCM-TT/02909937';
select* from ;
select ma_Gd , ma_Chungtu, nhanvien_xuathd from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202408 and ma_gd = 'HCM-TT/02959398';
select* from admin_hcm.nhanvien where nhanvien_id in (451517,839);