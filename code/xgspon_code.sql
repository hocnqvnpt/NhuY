--- 1
create table xgspon as 
with xgspon as (
select  'HCM' ma_tinh,  c.ma_tb, cn.congnghe,vt.ten_Vt thietbi, tb.trangthaitb_id,  kh.loaihd_id,lhd.ten_loaihd,c.kieuld_id,kld.ten_kieuld , c.ngay_ht, 
    dv1.ten_dv pbh, dv2.ten_dv ttvt ,tt.trangthai_Tb
from qlvt.v_phieu_vt@dataguard a 
    join qlvt.v_thietbi@dataguard b on a.thietbi_id = b.thietbi_id and a.phanvung_id = b.phanvung_id
    join css.v_hd_thuebao@dataguard c on a.hdtb_id = c.hdtb_id
    join css.v_db_adsl@dataguard db on c.thuebao_id = db.thuebao_id and db.congnghe_id in (3,11)
    join css.congnghe@dataguard cn on db.congnghe_id = cn.congnghe_id
    join qlvt.v_vattu@dataguard vt on b.vattu_id = vt.vattu_id
    left join css.v_hd_khachhang@dataguard kh on c.hdkh_id =kh.hdkh_id
    left join css.loai_Hd@dataguard lhd on kh.loaihd_id = lhd.loaihd_id
    left join css.v_Db_Thuebao@dataguard tb on c.thuebao_id = tb.thuebao_id
    left join css.kieu_ld@dataguard kld on c.kieuld_id = kld.kieuld_id
    left join admin.nhanvien@dataguard nv on kh.ctv_id = nv.nhanvien_id
    left join admin.donvi@dataguard dv on nv.donvi_id = dv.donvi_id
    left join admin.donvi@dataguard dv1 on dv.donvi_cha_id = dv1.donvi_id
    left join admin.donvi@dataguard tt on a.donvi_id = tt.donvi_id
    left join admin.donvi@dataguard dv2 on tt.donvi_cha_id = dv2.donvi_id
    left join css.trangthai_tb@dataguard tt on tb.trangthaitb_id = tt.trangthaitb_id
where a.phanvung_id =28  and  kh.loaihd_id in(1,13,8)  and c.tthd_id = 6 and tb.loaitb_id = 58
and to_number(to_char(c.ngay_Ht,'yyyymmdd')) > 20241231)
select* from xgspon;
--- 2
create table cdcn_td as 
with cdcn_td as ( select b.thuebao_id, b.ma_Tb,nv.ten_Nv, nv.ma_nv,  kieuld_id, loaihd_id, e.ten_dv dv_Thuyetphuc, dv2.ten_dv dv_rp, b.hdtb_id
--        row_number() over (partition by  b.thuebao_id, b.ma_Tb, kieuld_id, loaihd_id order by  b.hdtb_id desc) rn
from css.v_hd_khachhang@dataguard a
    join css.V_hd_thuebao@dataguard b on a.hdkh_id = b.hdkh_id
    left join admin.nhanvien@dataguard c on a.ctv_id = c.nhanvien_id
    left join admin.donvi@dataguard d on c.donvi_id = d.donvi_id
    left join admin.donvi@dataguard e on d.donvi_cha_id = e.donvi_id
    left join admin.donvi@dataguard dv1 on a.donvi_id = dv1.donvi_id
    left join admin.donvi@dataguard dv2 on dv1.donvi_cha_id = dv2.donvi_id
    left join admin.nhanvien@dataguard nv on a.ctv_id = nv.nhanvien_id
where kieuld_id in (24,743,51,71,72,19581,19582,19584)
) 
select* from cdcn_td;
--- 3
create table cte as
with cte as (
    select thuebao_id,ma_tb, row_number() over (partition by   ma_Tb order by  hdtb_id desc) rn , dv_rp, ten_Nv, ma_Nv
                from cdcn_td where  
                kieuld_id in (24,743,19581,19582,19584)
) 
select* from cte;

--- 4
create table hdong as
with tmp_th as 
(
select a.*, case when a.kieuld_id in (71,72) then  b.dv_rp end thaythe--,b.ma_nv,  b.ten_nv             
from cdcn_td a 
    left join 
              cte  b on a.thuebao_id = b.thuebao_id and rn = 1
    join xgspon c on a.ma_tb = c.ma_Tb
) 

,  hdong as (
    select a.*, 
    row_number() over (partition by   ma_Tb order by  hdtb_id desc) rn 
    from tmp_th a
) select* from hdong;

--- 5
select a.*, 
            case when hd.loaihd_id = 1 then nvl(dv_thuyetphuc ,dv_rp)
                 when hd.kieuld_id in (71,72) then nvl(thaythe, dv_rp)
                else dv_rp end ten_dv
         , hd.ten_Nv, hd.ma_Nv
from xgspon a 
    join hdong hd on a.ma_tb = hd.ma_Tb and hd.rn = 1
;
drop table xgspon;
drop table cdcn_td;
drop table cte;
drop table hdong;
