SELECT * FROM TTKD_bSC.nhuy_ct_Bsc_ipcc_obghtt WHERE THANG = 202406 and ipcc is null;
select* from css_hcm.hinhthuc_tra where ht_Tra_id = 204;
update TTKD_bSC.ct_Bsc_trasau_tp_Tratruoc set tien_khop = 1 WHERE THANG = 202406 and tien_khop is null and rkm_id is not null and ht_Tra_id in  (214,216);
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb ;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = 207361;
SELECT COUNT(*) FROM all_indexes WHERE table_name = upper('NHUY_CT_BSC_IPCC_OBGHTT');
commit;
     CREATE INDEX IDX_TB ON TTKD_BSC.NHUY_CT_BSC_IPCC_OBGHTT (THANG,thuebao_id);
     CREATE INDEX IDX_TB ON TTKD_BSC.NHUY_CT_BSC_IPCC_OBGHTT (THANG,thuebao_id);
     CREATE INDEX IDX_RKM ON TTKD_BSC.NHUY_CT_BSC_IPCC_OBGHTT (THANG,RKM_ID);
    ith to_ as (
    select distinct a.ma_To, a.ten_to, tbh_id
    from ttkd_Bsc.nhanvien_202406 a
        join ttkd_bsc.dm_to b on a.ma_to = b.ma_to_hrm
)

update ttkd_bsc.ct_Bsc_Tratruoc_moi  a set ma_to = (select ma_to_hrm from ttkd_bsc.dm_to where a.tbh_giao_id = tbh_id and a.pbh_giao_id = pbh_id) where thang = 202406;
select tbh_id from ttkd_bsc.dm_to group by tbh_id,pbh_id having count(tbh_id) > 1;


merge into ttkd_bsc.ct_Bsc_Tratruoc_moi a
using (
    select distinct a.ma_To, a.ten_to, tbh_id
    from ttkd_Bsc.nhanvien_202406 a
        join ttkd_bct.toBanhang b on a.ma_to = b.ma_to_hrm
    where tbh_id is not null
) b
on (REGEXP_REPLACE(a.tbh_giao_id, '\D', '') = b.tbh_id)
when matched then
  update set a.ma_to = b.ma_to
WHERE A.ma_to IS NULL and a.thang = 202406 and a.tbh_giao_id is not null and a.ma_tb ='vanphong93758398' ;

select* from ttkd_bct.toBanhang;
desc ttkd_Bsc.dm_to;
select to_number(a.tbh_giao_id) from ttkd_bsc.ct_Bsc_Tratruoc_moi a where thang = 202406;
select* from  ttkd_bsc.ct_Bsc_Tratruoc_moi a where thang = 202406;
update ttkd_bsc.ct_Bsc_Tratruoc_moi a
set ma_To = (select  distinct a.ma_To
            from ttkd_Bsc.nhanvien_202406 a
                join ttkd_bsc.dm_to b on a.ma_to = b.ma_to_hrm and a.tbh_giao_id = b.tbh_id)
where thang = 202406 and ma_to is null;
    where tbh_id is not null
with ac as (   select distinct a.ma_To, a.ten_to, tbh_id
    from ttkd_Bsc.nhanvien_202406 a
        join ttkd_bsc.dm_to b on a.ma_to = b.ma_to_hrm
    where tbh_id is not null)
    select tbh_id from ac group by tbh_id having count(tbh_id) = 1;
    ROLLBACK;
    COMMIT;
update ttkd_bsc.tl_giahan_tratruoc 
set ma_To = 'VNP0701804'
where thang = 202406 and ma_pb = 'VNP0701800' and ma_to is null and ma_kpi = 'HCM_TB_GIAHA_022' and loai_tinh = 'KPI_TO';
select * from ttkd_Bsc.nhanvien_202406 WHERE MA_VTCV = 'VNP-HNHCM_BHKV_48';
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_022' and thang_kt is null
