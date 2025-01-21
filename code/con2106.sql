select * from ttkd_bsc.ct_dongia_Tratruoc where thang = 202411 and ma_Nv in ;
(select distinct  b.ten_Vtcv, b.ten_Nv --, ten_pb, tien, ten_nv,a.ma_Nv, b.ma_pb
from ttkd_Bsc.tl_giahan_Tratruoc A
join ttkd_Bsc.nhanvien b on a.thang = b.thang and b.donvi = 'TTKD' and a.ma_nv = b.ma_nv
where A.thang= 202410 and loai_tinh = 'DONGIATRA_OB' and tien> 0
and a.ma_pb = 'VNP0701100');
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202411 and loai_Tinh= 'DONGIATRA_OB';
select* FROM TTKD_bSC.NHANVIEN WHERE THANG = 202411 AND TEN_NV LIKE '%Ng?c H?nh';
update ttkd_Bsc.ct_Dongia_Tratruoc  set thang = 112024 where ma_to in 
    (select distinct ma_to from ttkd_Bsc.nhanvien where thang = 202411 and ten_to = 'T? Bán Hàng Online' ) and loai_tinh = 'DONGIATRA_OB'
    and thang = 202411;
    commit;
delete from ttkd_Bsc.tl_giahan_Tratruoc A where A.thang= 202410 and loai_tinh = 'DONGIATRA_OB' and ma_pb = 'VNP0700600';
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_SL_ORDER_001';
select* from (
select NGANHANG, MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB, MA_GD, LOAIHINH_TB, GHICHU, TRAGOC, TRATHUE, TONGTRA, HTTT_ID, TRA_TRUOC, CHUNGTU_ID
    ,A.NV_GACH ma_nv, TINH_DONGIA, A.TINH_BSC, a.thang, B.MA_TO,case when a.nv_gach like 'dl_%' then 'VNP0700900' else B.MA_PB end ma_pb. a.sl_matb
from nhuy.ct_BSC_chungtu a
   left join ttkd_Bsc.nhanvien b on a.nv_Gach = b.ma_Nv and b.thang = a.thang and b.donvi ='TTKD'
where a.thang = 202411) ;
select* from ct_BSC_chungtu
