select* from ttkd_Bsc.ct_bsc_Tratruoc_moi_30day where thang = 202406 and rkm_id is not null and tien_khop is null;
hcm_tmqt_00000617 'hcm_ca_ivan_00016709','hcm_ca_00070032', hcm_ca_00057790
select* from  ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a where thang = 202406 and ipcc = 1;rkm_id in (Select rkm_id from tmp_3 where lan = 308);
rollback;
select* from ttkdhcm_ktnv.ghtt_giao_688 where ma_Tb = 'szekeress';
select* from ct_Bsc_slpt_Chua_xuly where thang = 202407 and ma_tb = 'thienphu1715605';
select* from ttkd_Bsc.ct_dongia_Tratruoc where ma_tb = 'hcm_szekeress1';
merge into ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a
using (
  select distinct khachhang_id, phieutt_id, ma_GD, HDKH_ID, MA_ND_OB,NHANVIEN_ID_OB,MANV_OB, THANG, NVTUVAN_ID
  from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt
  WHERE THANG = 202407 AND OB_ID IS NOT NULL
) b
on (a.THANG = b.THANG AND A.KHACHHANG_ID = B.KHACHHANG_ID AND A.PHIEUTT_ID = B.PHIEUTT_ID AND A.MA_GD = B.MA_GD AND A.NVTUVAN_ID = B.NVTUVAN_ID AND A.HDKH_ID = B.HDKH_ID)
when matched then
  update set a.MA_nD_OB = B.MA_ND_OB , A.NHANVIEN_ID_OB = B.NHANVIEN_ID_OB, A.MANV_OB = B.MANV_OB
WHERE A.OB_ID IS NULL;
commit;
update  ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a set tien_khop = 3 where thang = 202406 and tien_khop = 4 and ma_Tb in (Select ma_Tb from phithu_tainha where thang =202406);
commit;
select* from tuyenngo.SBH_GIAODICH_202407_CT_new c where not exists (select 1 from tmp3_ob where ma_gd = c.ma_gd and ma_tb = c.ma_tb);
select* from css_hcm.kieu_ld where kieuld_id in (551, 550, 24, 13080) ;
select* from 
with cte as (
        select distinct b.ma_Tb, a.ma_Gd
        from css_hcm.hd_khachhang a
            join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id
            join css_hcm.phieutt_hd c on a.hdkh_id = c.hdkh_id
            join css_hcm.ct_phieutt d on c.phieutt_id = d.phieutt_id  and b.hdtb_id = d.hdtb_id
        where kieuld_id in (551, 550, 24, 13080)  and d.khoanmuctt_id =  11 and d.tien>0
)
select count(1) from tuyenngo.SBH_GIAODICH_202407_CT_new c where not exists (select 1 from cte where ma_gd = c.ma_gd and ma_tb = c.ma_tb);