select* from ttkd_Bsc.ct_bsc_Tratruoc_moi_30day where thang = 202411 and ma_tb='mg24ts';
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where ma_ct='VCB_20241118_467098';
select*  from ttkdhcm_ktnv.ds_Chungtu_Nganhang_sub_oneb where chungtu_id = 467051;
select *
  from ttkd_Bsc.bangluong_kpi a
 where thang=202411
   and ten_nv like '%Kiên';
 select *
  from ttkd_Bsc.nhanvien a where thang=202410
   and ma_nv='CTV088522';
      delete from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_030';

   select* from ttkd_Bsc.BLKPI_DANHMUC_KPI where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_030';
   select* from ttkd_Bsc.nhanvien  where thang = 202412 and ma_nv in (
   select ma_nv from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG a 
where a.thang =202412 and a.ten_kpi ='2.CT PTM thuê bao gói Home Sành/Home ch?t'  and ma_nv not in (
   select ma_Nv from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_SL_COMBO_006' and giao is not null));
      select a.ma_kpi t11, a.ten_kpi, b.ma_kpi r12 from ttkd_Bsc.BLKPI_DANHMUC_KPI a 
      left join ttkd_Bsc.BLKPI_DANHMUC_KPI b on a.ma_Kpi = b.ma_kpi 
      where a.thang = 202411 and b.thang = 202412;
      select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_Kpi = 'HCM_TB_GIAHA_038';
   select* from   ttkd_Bsc.BLKPI_DANHMUC_KPI a
      where a.thang = 202411 and ma_kpi not in (select ma_kpi from   ttkd_Bsc.BLKPI_DANHMUC_KPI b 
      where b.thang = 202412 );
update ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt set tien_khop = 1 where tien_khop = 0 and ma_gd in (  
select c.ma_Gd from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a 
join ttkdhcm_ktnv.ds_chungtu_nganhang_pqt_sudung b on a.ma = b.ma_Gd
join ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt c on b.ma_gd = c.ma_Gd
where c.thang = 202411);
commit;

select c.* from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a 
join ttkdhcm_ktnv.ds_chungtu_nganhang_pqt_sudung b on a.ma = b.ma_Gd
join ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt c on b.ma_gd = c.ma_Gd
where c.thang = 202411 and c.tien_khop = 0;
commit;

select* from tt where ma_tb='sangb1806';
select* from v_Thongtinkm_all where ma_tb='sangb1806';