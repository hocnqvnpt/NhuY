select * from bcss_hcm.ct_no partition for (20240501) ;group by thuebao_id having count(distinct ky_cuoc)>1;
select owner , table_name from all_tab_columns where column_name = 'KIEUNO';
create table kt_psc as;
with  no as (
    select b.ma_Tb--,sum(a.nogoc)+sum(a.thue) tien_cps--, b.*
    from qltn_hcm.ct_no  partition for (20240501) b
--        join qltn_hcm.ct_no  partition for (20240501) b on a.ma_tb = b.ma_tb
--    where a.nogoc > 0 --and  b.ma_Tb  is null
    group by b.ma_tb
    having sum(b.nogoc) > 0
),
cps as (
    select a.thuebao_id,sum(a.nogoc)+sum(a.thue) tien_cps--, b.*
    from bcss_hcm.ct_no  partition for (20240501) a
--        join qltn_hcm.ct_no  partition for (20240501) b on a.ma_tb = b.ma_tb
    where exists (select 1 from no where ma_Tb = a.ma_tb)--and  b.ma_Tb  is null
    group by a.thuebao_id
    having sum(a.nogoc) > 0
)
--select* from cps;

-- select* from bcss_hcm.ct_no   partition for (20240501);
select a.ma_tb, b.ten_kh, b.ma_kh,ngay_bd_moi, ngay_kt_moi , ma_gd, tien_Thanhtoan, vat_thanhtoan,c.ht_tra, d.ma_nv nv_thanhtoan, dvc.ten_dv, cps.tien_cps, g.thang_kt
from xyz a
    join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    join css_hcm.hinhthuc_Tra c on a.ht_Tra_id = c.ht_tra_id
    left join admin_hcm.nhanvien_onebss d on a.thungan_tt_id = d.nhanvien_id
    left join admin_hcm.donvi dv on d.donvi_id = dv.donvi_id
    left join admin_hcm.donvi dvc on dv.donvi_cha_id = dvc.donvi_id
    join cps on a.thuebao_id= cps.thuebao_id
    left join (select thuebao_id, max(thang_kt) thang_kt from ttkdhcm_ktnv.ghtt_giao_688 group by thuebao_id) g on a.thuebao_id = g.thuebao_id
    where  tien_cps > 0 ;--ngay_bd_moi >= 20240601 and;
    select* from css_hcm.hinhthuc_tra where ht_tra_id = 214;
select* from v_thongtinkm_all where ma_tb in  ('kieuphuong.gd3','tram895nvk','hcmtai490','hcmtram895nvk','tai490','hcmtntl9960')
order by ma_tb desc, ngay_bddc ;

select* from kt_psc;
select* from v_thongtinkm_all where ma_Tb = 'haily_2019';

(select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc , h.thang_bd
                                            from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                            where  hdtb_id = 25000364
 ) ;
 
 select* from css_hcm.ct_phieutt where phieutt_id =8426041;
 select* from ttkd_bsc.nhanvien_202405 where ten_nv like '%Khanh';