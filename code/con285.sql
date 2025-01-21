select* from ttkd_Bsc.nhanvien where ten_Nv like '%H?c';
select* from css_hcm.kenhthu where kenhthu_id in (22,23,29); 
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt set tien_khop = 1 where thang = 202409 and tien_khop = 0 and ht_tra_id = 214;
select distinct ht_Tra_id, kenhthu_id from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202409 and tien_khop = 1;
commit;
select * from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202409 ;and tien_khop = 0 and ht_Tra_id = 2 and kenhthu_id = 22;
select so_ct, ghichu from css_hcm.phieutt_Hd where ma_Gd = 'HCM-TT/02945645';
select* from ttkd_bsc.ct_Bsc_tratruoc_moi where thang = 202408;
select* from admin_Hcm.nhanvien_onebss where nhanvien_id in (13948,	89175)
;
select* from ttin_Chungtu_t9 where ma_Gd = 'HCM-TT/02965322';
delete from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202409 and rkm_id in ;
(select rkm_id from ttkd_bsc.ct_Bsc_tratruoc_moi where thang = 202409 group by rkm_id having count(rkm_id)>1);
insert into ct_Bsc_tratruoc_moi select* from ttkd_bsc.ct_Bsc_tratruoc_moi where thang = 202409;
 DELETE FROM ttkd_bsc.ct_Bsc_tratruoc_moi
WHERE thang = 202409 and rowid not in
(SELECT MIN(rowid)
FROM ttkd_bsc.ct_Bsc_tratruoc_moi where thang = 202409
GROUP BY thuebao_id, rkm_id);
rollback;
commit;
