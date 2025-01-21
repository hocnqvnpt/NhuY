select distinct b.hinhthuc, c.nhom_httt , c.nhom_httt_id, b.httt_id from qltn_hcm.bangphieutra  partition for (20240101)  a
    left join qltn_hcm.hinhthuc_tt b on a.httt_id = b.httt_id
    left join qltn_hcm.nhom_httt c on b.nhom_httt_id = c.nhom_httt_id
--    where a.httt_id = 113-- (83,155,188,160,113,97);
    ;
    
    select b.ma_tb, b.*, a.*
    from qltn_hcm.bangphieutra  partition for (20240101)  a
        left join qltn_hcm.ct_tra partition for (20240101)  b on a.phieu_id = b.phieu_id
        where httt_id = 125;