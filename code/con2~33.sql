SELECT* FROM TTKD_BSC.BANGLUONG_DONGIA_QLDB WHERE THANG =202403;

SELECT* FROM TTKD_bSC.Ct_DONGIA_TRATRUOC WHERE THANG =202403 AND MA_PB = 'VNP0701100' AND LOAI_TINH LIKE'DONGIA%';
select* from ttkdhcm_ktnv.ghtt_giao_688 
select a.ma_Tb, a.ma_pb, a.* from ttkd_bsc.ct_bsc_Tratruoc_moi a
left join ttkdhcm_ktnv.ghtt_giao_688 b on a.thuebao_id = b.thuebao_id
where a.thang = 202403 and b.thang_kt = 202403 and tratruoc = 1 and loaibo = 0 and km =1 
and a.manv_giao != b.nhanvien_giao
select* from ttkd_bsc.bangluong_kpi_202403
delete from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202403;
commit;
;
select* from css_hcm.goi_Dadv_lhtb where goi_id = 19113;
select* from bcss.v_banggia_goi@dataguard where  goi_id = 19113;
select* from css_hcm.trangthai_Hd
select trangthai from css_hcm.phieutt_hd where
select tthd_id from css_hcm.hd_thuebao where hdkh_id in (
select hdkh_id from css_hcm.hd_khachhang where ma_gd in ('HCM-LD/01607246','HCM-LD/01603782','HCM-LD/01607265','HCM-LD/01610826'));
select distinct a.ma_Vtcv --, b.ma_vtcv, a.ma_to, b.ma_to, a.ma_pb, b.ma_pb 
from ttkd_Bsc.nhanvien_202403_lan2 a

left join ttkd_bsc.nhanvien_202403 b on a.ma_nv = b.ma_nv 
where   b.ma_vtcv != a.ma_vtcv  --a.ma_vtcv != b.ma_vtcv or or
select distinct ma_To from ttkd_Bsc.ct_bsc_tratruoc_moi where ma_pb = 'VNP0702200' and thang = 202403
select distinct ten_to, ma_to from ttkd_Bsc.nhanvien_202403 where ma_to in ('VNP0702218','VNP0702221')
select a.* from ttkd_Bsc.nhanvien_202403 a
left join ttkd_Bsc.nhanvien_202312 b on a.ma_nv = b.ma_nv where  b.ma_TO IN ('VNP0702215')--,'VNP0702221')