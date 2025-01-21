select ten_pbh, count(thuebao_id) sltb
from ct_bsc_slpt_chua_xuly
where thang = 202407 and (chuaden_hen_Tt = 1 or hoan_hen_tt = 1 or qua_hen_tt_ttkd = 1 or pakh_ttkd = 1 or khac = 1 or kh_Hen_goilai = 1 or khong_kq = 1
    or dang_ob = 1 or chua_cham = 1 or phieu_csgh_cham = 1)
group by ten_pbh;

select* from ct_Trtruoc where thang = 202407 and not exists (select * from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where ;
select distinct ma_kpi from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202407 and ma_kpi != 'DONGIA';
select thungan_hd_id from css_hcm.phieutt_hd where ma_gd = 'HCM-TT/02887182';
select * from admin_hcm.nhanvien_onebss where nhanvien_id = 451341;

select distinct ten_pb from ttkd_Bsc.nhanvien where thang = 202408 and ten_to = 'T? Bán Hàng Online';
select a.*, b.ten_to from ttkd_Bsc.tl_giahan_Tratruoc a 
join (select distinct ma_to , ten_To from ttkd_Bsc.nhanvien where thang = 202408) b on a.ma_to = b.ma_To 
where thang = 202408 and ma_kpi in ('HCM_TB_GIAHA_023','HCM_TB_GIAHA_022') AND LOAI_TINH = 'KPI_TO';