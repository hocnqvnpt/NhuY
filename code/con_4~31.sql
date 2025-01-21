select * from ttkd_Bsc.ct_bsc_Tratruoc_moi_30day where THANG = 202403 AND manv_thphuc = 'CTV081976';
select* from v_Thongtinkm_all where ma_tb= 'kimchi4910238'
select* from ttkd_Bsc.ct_bsc_trasau_tp_tratruoc where   ma_to ='VNP0703001' and thang = 202403;
select* from ttkd_Bsc.ct_bsc_Tratruoc_moi where ma_to ='VNP0703001' and thang = 202403;
select* from ttkd_bsc.ct_Dongia_Tratruoc where THANG = 202403 AND MA_NV ='VNP017719' -- ma_tb = 'phuongthuyhcm81';
select rkm_id from v_thongtinkm_all  where ma_tb= 'nguyenthithanhbinh';
select thuebao_dc_id from css_hcm.db_datcoc where rkm_id = 6759926;
select* from ttkdhcm_ktnv.ghtt_giao_688 where ma_Tb ='qdat82020'
select ngay_tt,trangthai
from css_hcm.hdtb_datcoc a left join css_hcm.ct_phieutt b on a.hdtb_id = b.hdtb_id
    left join css_hcm.phieutt_hd c on b.phieutt_id = c.phieutt_id
    where thuebao_Dc_id = 4990801;
select* from ttkd_Bsc.nhanvien_202403 where ten_Nv ='Lê Th? Th?y';
    
select* from ttkd_Bsc.ct_dongia_tratruoc where loai_tinh = 'DONGIATRU_CA' AND THANG = 202403;

select* from v_Thongtinkm_All where ma_Tb = 'dunganh1134085'