select* from css_hcm.hd_thuebao where hdtb_id = 23950882;
select* from css_hcm.hinhthuc_tra where ht_tra_id in (select *  from chot_hopdong where ht_Tra_id = 208);
(select *  from chot_hopdong where ht_Tra_id = 208);
select* from v_Thongtinkm_all where 6646284 = rkm_id;

select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_CL_OBDAI_007' and thang_kt is null;
select* from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202309
select * from CHOT_HOPDONG-- group by thuebao_id having count(distinct tthd_id) > 1
