select* from hocnq_ttkd.x_Tempppp  where rkm_id in (select rkm_id from hocnq_ttkd.x_Tempppp group by rkm_id having count(rkm_id) > 1);
alter table nhuy_ct_bsc_ipcc_obghtt rename column thang_ob to thang;
commit;