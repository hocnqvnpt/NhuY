drop table tl;
create table tl as 
select distinct a.ma_Tb, c.ma_Gd , d.ma_nv, d.ten_nv, e.ten_dv , b.tthd_id,b.ngay_ht
from thanhly a
    left join css_Hcm.hd_thuebao b on a.thuebao_id = b.thuebao_id 
    left join css_hcm.hd_khachhang c on b.hdkh_id = c.hdkh_id 
    left join admin_hcm.nhanvien_onebss d on c.nhanvien_id = d.nhanvien_id
    left join admin_hcm.donvi e on c.donvi_id = e.donvi_id
where c.loaihd_id = 4 and to_number(to_char(b.ngay_ht,'yyyymmdd')) between 20240821 and 20240826 ;
delete from tl where ma_Gd  = 'HCM-TL/02106837';
commit;
select * from css_hcm.loai_Hd ;
commit;
drop table dv;
create table dv as select* from (
select a.ma_Tb, c.ma_Gd , d.ma_nv, d.ten_nv, e.ten_dv , b.tthd_id,b.ngay_ht, row_number() over (partition by a.ma_tb order by b.hdtb_id desc) rn
from tddv a
    left join css_Hcm.hd_thuebao b on a.thuebao_id = b.thuebao_id 
    left join css_hcm.hd_khachhang c on b.hdkh_id = c.hdkh_id 
    left join admin_hcm.nhanvien_onebss d on c.nhanvien_id = d.nhanvien_id
    left join admin_hcm.donvi e on c.donvi_id = e.donvi_id
where c.loaihd_id = 7 and tthd_id != 7 and to_number(to_char(b.ngay_ht,'yyyymmdd')) between 20240821 and 20240826 ) where rn = 1;
drop table tt;
create table tt as select* from (
select distinct t.ma_tb, hdkh.ma_Gd,  d.ma_nv, d.ten_nv, e.ten_dv , hdtb.tthd_id ,hdtb.ngay_ht, row_number() over (partition by t.ma_tb order by hdtb.hdtb_id desc) rn
 from  css_hcm.hd_khachhang hdkh
    join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id  and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id = 6
--    join css.v_ct_tienhd@dataguard a on hdtb.hdtb_id = a.hdtb_id
    join css_hcm.ct_phieutt b on hdtb.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11 and b.tien > 0
    join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id and p.trangthai= 1 and  p.kenhthu_id not in (6) 
    join tratruoc t on hdtb.thuebao_id = t.thuebao_id
    left join admin_hcm.nhanvien_onebss d on hdkh.nhanvien_id = d.nhanvien_id
    left join admin_hcm.donvi e on hdkh.donvi_id = e.donvi_id
where to_char(p.ngay_tt,'yyyymm') in ('202406','202408','202407') and tthd_id != 7 and to_number(to_char(hdtb.ngay_ht,'yyyymmdd')) between 20240821 and 20240826 );
--create table tt as select* from (
select distinct t.ma_tb, hdkh.ma_Gd,  d.ma_nv, d.ten_nv, e.ten_dv , hdtb.tthd_id ;--, row_number() over (partition by t.ma_tb order by hdtb.hdtb_id desc) rn
with hd as (
select distinct hdtb.thuebao_id, hdkh.ma_Gd,  d.ma_nv, d.ten_nv, e.ten_dv , hdtb.tthd_id, hdtb.ngay_ht
from  css_hcm.hd_khachhang hdkh
    join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id  and hdtb.kieuld_id in (136) and hdtb.tthd_id = 6 
--    join css.v_ct_tienhd@dataguard a on hdtb.hdtb_id = a.hdtb_id
    join css_hcm.ct_phieutt b on hdtb.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 3130 and b.tien > 0
    join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id and p.trangthai= 1 and  p.kenhthu_id not in (6) 
    
    left join admin_hcm.nhanvien_onebss d on hdkh.nhanvien_id = d.nhanvien_id
    left join admin_hcm.donvi e on hdkh.donvi_id = e.donvi_id
where to_char(p.ngay_tt,'yyyymm') in ('202406','202408','202407') and tthd_id != 7  )
select tp.ma_tb, hd.* 
from tp 
left join hd on tp.thuebao_id = hd.thuebao_id
where  to_number(to_char(hd.ngay_ht,'yyyymmdd')) between 20240821 and 20240826 ;
select* from tt;