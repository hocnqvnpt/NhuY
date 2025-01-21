with tt as 
(
    select thuebao_id, row_number() over (partition by thuebao_id order by ngay_Bddc ) rn, to_char(ngay_bddc,'yyyymm') thang_bddc, to_Char(ngay_kt_mg,'yyyymm') thang_ktdc
    from ds_giahan_Tratruoc2
    where to_number(to_char(ngay_kt_mg,'yyyymm'))>=202411
)
select a.*, c.trangthai_Tb, case when tt.thuebao_id is not null then 1 else 0 end tra_Truoc, thang_bddc, thang_ktdc, d.so_Dt, e.so_Dt
from stb_chithuy a  
    left join css_hcm.db_thuebao b on a.MÃ_USERNAME = b.ma_tb
    left join css_hcm.trangthai_tb c on b.trangthaitb_id = c.trangthaitb_id
    left join tt on b.thuebao_id = tt.thuebao_id and rn = 1;
select d.so_Dt, e.so_Dt
    from css.v_db_Thuebao@dataguard b 
    left join css.v_db_khachhang@dataguard d on b.khachhang_id = d.khachhang_id
    left join css.v_db_Thanhtoan@dataguard e on b.thanhtoan_id = e.thanhtoan_id
where ma_Tb in ('hcmvtuan210122','hcmdvc7138','hcmqlquyen_01','hcmptcvan_tpttk','hcmtamthu');
HCM_TB_GIAHA_029
select* from ttkd_bsc.blkpi_danhmuc_kpi where thang = 202410 ;
HCM_SL_BHOL_002
;
commit;
update  ttkd_bsc.blkpi_danhmuc_kpi set diem_cong = 1, diem_tru = 1 where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_029';