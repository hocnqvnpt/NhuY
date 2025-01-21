update ttkd_bsc.ct_bsc_giahan_cntt a
set rkm_id = (select rkm_id from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt = 202310 and phieutt_id = a.phieutt_id and thuebao_id = a.thuebao_id)
where thang_ktdc_cu = 202310 and rkm_id is null and a.thuebao_id =
        (select thuebao_id from  ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt = 202310 and phieutt_id = a.phieutt_id and thuebao_id = a.thuebao_id)
        and a.phieutt_id = (select phieutt_id from  ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt = 202310 and phieutt_id = a.phieutt_id and thuebao_id = a.thuebao_id)

rollback;
select* from 
select 1 from ttkd_bsc.ct_bsc_giahan_cntt a
left join ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 b on a.phieutt_id = b.phieutt_id and a.thuebao_id = b.thuebao_id
where a.thang_ktdc_Cu = 202310 and b.thang_kt = 202310 and a.ma_gd != b.ma_gd
select* from ttkd_bsc.ct_bsc_giahan_cntt  a where thang_ktdc_cu = 202310 and ma_gd is not null
select*