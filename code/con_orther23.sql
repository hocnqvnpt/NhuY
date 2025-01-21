select a.*, b.so_dt, b.email , check_telconame_new(b.so_dt) from matt_ a
    left join css_hcm.db_thanhtoan b on a.matt_dhsx = b.ma_Tt
;
select so_Dt from css_hcm.db_Thuebao;
select MATB_DD , check_telconame_new(MATB_DD) a from  sdttb
where check_telconame_new(MATB_DD) is not null;