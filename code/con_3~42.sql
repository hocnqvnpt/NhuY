select* from ttkd_bsc.bangluong_kpi where thang = 202410 and ten_kpi like '%Home%';
hcm_ca_ivan_00018752
hcm_ca_00046852
hcm_ivan_00029337
hcm_ivan_00029115
hcm_ivan_00029111
hcm_ivan_00028989
hcm_ca_00086581
hcm_smartca_00098492
hcm_smartca_00093495
hcm_smartca_00093311
hcm_smartca_00093279
hcm_smartca_00375231
hcm_smartca_00375347
hcm_smartca_00380769;
--- 
;
commit;
create table dc_Cu as 
select a.thuebao_id, a.ma_Tb,sum(d.tien+d.vat) tien
from css.v_hd_Thuebao@dataguard a
    join css.v_Hd_khachhang@dataguard b on a.hdkh_id = b.hdkh_id
    join css.v_phieutt_hd@dataguard c on b.hdkh_id = c.hdkh_id
    join css.v_ct_phieutt@dataguard d on c.phieutt_id = d.phieutt_id and a.hdtb_id = d.hdtb_id
where c.trangthai = 1 and a.ma_Tb in ('hcm_ca_ivan_00018752','hcm_ca_00046852','hcm_ivan_00029337','hcm_ivan_00029115','hcm_ivan_00029111','hcm_ivan_00028989','hcm_ca_00086581',
'hcm_smartca_00098492','hcm_smartca_00093495','hcm_smartca_00093311','hcm_smartca_00093279','hcm_smartca_00375231','hcm_smartca_00375347','hcm_smartca_00380769')
group by a.thuebao_id,ma_tb;
update ttkd_Bsc.ct_dongia_Tratruoc a set tien_Dc_Cu  = (select tien from dc_cu where thuebao_id = a.thuebao_id )
where thang=202410 and loai_tinh= 'DONGIATRU_CA' and a.ma_Tb in ('hcm_ca_ivan_00018752','hcm_ca_00046852','hcm_ivan_00029337','hcm_ivan_00029115','hcm_ivan_00029111','hcm_ivan_00028989','hcm_ca_00086581',
'hcm_smartca_00098492','hcm_smartca_00093495','hcm_smartca_00093311','hcm_smartca_00093279','hcm_smartca_00375231','hcm_smartca_00375347','hcm_smartca_00380769');and ma_Tb= 'hcm_ca_00046852';
rollback;
update  ttkd_Bsc.ct_dongia_Tratruoc a set dongia = tien_dc_Cu / 10;
select* from css_hcm.dichvu_vt;
select* from  ttkd_Bsc.ct_dongia_Tratruoc a
where thang=202410 and loai_tinh= 'DONGIATRU_CA' and a.ma_Tb in ('hcm_ca_ivan_00018752','hcm_ca_00046852','hcm_ivan_00029337','hcm_ivan_00029115','hcm_ivan_00029111','hcm_ivan_00028989','hcm_ca_00086581',
'hcm_smartca_00098492','hcm_smartca_00093495','hcm_smartca_00093311','hcm_smartca_00093279','hcm_smartca_00375231','hcm_smartca_00375347','hcm_smartca_00380769')