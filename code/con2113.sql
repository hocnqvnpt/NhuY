with p as (
    select b.hdtb_id, c.ma_kmtt
    from css.v_phieutt_Hd@dataguard a
        join css.v_ct_phieutt@dataguard b on a.phieutt_id = b.phieutt_id and a.phanvung_id = b.phanvung_id 
        join css.v_khoanmuc_tt@dataguard c on b.khoanmuctt_id = c.khoanmuctt_id
    where a.trangthai = 1 and a.phanvung_id = 28
)
, tb as (
    select loaihinh_tb, hdtb_id
    from css.v_hd_thuebao@dataguard a
        join css.loaihinh_Tb@dataguard b on a.loaitb_id = b.loaitb_id
)
select distinct ma_kmtt, loaihinh_Tb
from p
    join tb on p.hdtb_id = p.hdtb_id