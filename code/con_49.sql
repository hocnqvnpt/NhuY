select* from v_Thongtinkm_all where thuebao_id = 8600193;
select a.thuebao_dc_id, d.ngay_tt, ngay_bddc,a.rkm_id, trangthai, ma_Gd, ht_Tra_id, so_Ct from css_hcm.db_datcoc a
left join css_hcm.hdtb_Datcoc b on a.thuebao_dc_id = b.thuebao_dc_id
left join css_hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id 
left join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id 
where a.rkm_id=6246744;
select ngay_tt from css_hcm.
select* from  hocnq_ttkd.ds_giahan_tratruoc where ma_Tb='kido-nhabe';
select * from ttkd_bsc.ct_bsc_tratruoc_moi where ma_tb = 'khanhhung1837';

s
