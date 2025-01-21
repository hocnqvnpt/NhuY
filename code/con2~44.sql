select a.thuebao_id
    , a.ma_tb
    , a.ten_tb
    , to_char(a.ngay_sd,'dd/mm/yyyy hh:mi:ss') ngay_sd
    , decode(a.dichvuvt_id, 7, 'Metronet', 8,'Megawan',null) dichvu_vt
    , ( nvl(b.cuoc_tk,0) + nvl(b.cuoc_tc,0) + nvl(b.cuoc_ht,0) +  nvl(b.cuoc_tbi,0) + nvl(b.cuoc_ip,0) + nvl(b.cuoc_nix,0) + nvl(b.cuoc_isp,0)) cuoc_tk
    , a.doituong_id
    , c.ten_dt
    , d.trangthai_tb
    , b.chuquan_id
    , decode(a.doituong_id, 37, 'Kênh ch?y Vfone do VTTP tri?n khai', null) nodes
from css.v_db_thuebao@dataguard a, css.v_db_mgwan@dataguard b, css.doituong@dataguard c, css.trangthai_tb@dataguard d
where a.phanvung_id = 28
    and b.phanvung_id = 28
    and a.dichvuvt_id in(7,8)
    and nvl(b.cuoc_tk,0) + nvl(b.cuoc_tc,0) + nvl(b.cuoc_ht,0) +  nvl(b.cuoc_tbi,0) + nvl(b.cuoc_ip,0) + nvl(b.cuoc_nix,0) + nvl(b.cuoc_isp,0) = 0
    and a.thuebao_id = b.thuebao_id
    and a.doituong_id = c.doituong_id
    and a.trangthaitb_id = d.trangthaitb_id
    and a.trangthaitb_id not in(7,8,9)
    and b.chuquan_id = 145
union
select a.thuebao_id
    , a.ma_tb
    , a.ten_tb
    , to_char(a.ngay_sd,'dd/mm/yyyy hh:mi:ss') ngay_sd
    , decode(a.dichvuvt_id,9,'Kênh thuê riêng', null) dichvu_vt
    , ( nvl(b.cuoc_tk,0) + nvl(b.cuoc_tc,0) + nvl(b.cuoc_ht,0) +  nvl(b.cuoc_tbi,0)) cuoc_tk
    , a.doituong_id
    , c.ten_dt
    , d.trangthai_tb
    , b.chuquan_id
    , decode(a.doituong_id, 37, 'Kênh ch?y Vfone do VTTP tri?n khai', null) nodes
from css.v_db_thuebao@dataguard a, css.v_db_tsl@dataguard b, css.doituong@dataguard c, css.trangthai_tb@dataguard d
where a.phanvung_id = 28
    and b.phanvung_id = 28
    and a.dichvuvt_id = 9
    and nvl(b.cuoc_tk,0) + nvl(b.cuoc_tc,0) + nvl(b.cuoc_ht,0) +  nvl(b.cuoc_tbi,0)  = 0
    and a.thuebao_id = b.thuebao_id
    and a.doituong_id = c.doituong_id
    and a.trangthaitb_id = d.trangthaitb_id
    and a.trangthaitb_id not in(7,8,9) 
    and b.chuquan_id = 145