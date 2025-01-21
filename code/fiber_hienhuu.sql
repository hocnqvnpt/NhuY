select * from hocnq_ttkd.fiber_hh where cuoc_saukm is null-- 0
select doituong_id from css_hcm.dichvu_Vt
select * from css_Hcm.diachi_tb
select* from hocnq_ttkd.fiber_hh
drop table fiber_hh
commit;
drop table fiber_hienhuu

create table fiber_hienhuu_t11 as 
select a.thuebao_id, a.ma_tb, a.ngay_sd, c.thuonghieu,  nvl2(goi1.goi_id, goi1.tien_goi, b.cuoc_tg) cuoc_tg, 
decode(skm.tyle_sd, null, nvl2(goi1.goi_id, goi1.tien_goi,b.cuoc_tg), 0, nvl2(goi1.goi_id, goi1.tien_goi, b.cuoc_tg) - skm.cuoc_sd, (100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, b.cuoc_tg)/100) cuoc_saukm, 
nvl (e.ma_goi,0) goi_Tichhop, f.madoicap, c.tocdo_id, c.tocdothuc, c.iptinh, c.soluong_ip, b.muccuoc_id, f.trangbi_id, b.mucuoctb_id, a.doituong_id,
d.nhomtb_id, a.khachhang_id, h.quan_id, h.phuong_id
from css_hcm.db_Thuebao a
    left join css_hcm.muccuoc_tb b on a.mucuoctb_id = b.mucuoctb_id 
    left join css_hcm.tocdo_adsl c on b.tocdo_id = c.tocdo_id
    left join css_hcm.db_adsl f on a.thuebao_id = f.thuebao_id
    left join css_hcm.bd_goi_dadv d on a.thuebao_id = d.thuebao_id and d.trangthai = 1
    left join  css_hcm.goi_dadv_lhtb goi1 on d.goi_id = goi1.goi_id and c.tocdo_id = goi1.tocdo_id 
                    and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1
    left join css_hcm.goi_dadv e on d.goi_id = e.goi_id 
    left join (select thuebao_id, loaitb_id, tyle_sd, cuoc_sd, congvan_id from v_thongtinkm_all
            where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11)
            and 202311 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
        ) skm on a.thuebao_id = skm.thuebao_id and skm.loaitb_id in (58, 59)
    left join css_hcm.diachi_tb g on a.thuebao_id = g.thuebao_id 
    left join css_hcm.diachi h on g.diachild_id = h.diachi_id
where a.loaitb_id in  (58,59) and a.trangthaitb_id not in  (7,8,9) and chuquan_id in (266, 145, 264) and to_number(to_char(a.ngay_sd, 'yyyymm')) <= 202311 --and e.tocdothuc <=90

select nhomtb_id from fiber_hienhuu --group by thuebao_id having count(thuebao_id) > 1

select ma_Tb, ten_km, ngay_bddc, ngay_ktdc, hieuluc, a.* from hocnq_ttkd.v_Thongtinkm_all a where thuebao_id in(
select thuebao_id
from fiber_hienhuu 
group by thuebao_id having count(thuebao_id) > 1 )
order by 3 desc
where a.thuebao_id = b.thuebao_id and a.nhomtb_id = b.nhomtb_id
select* from fiber_hienhuu
select tien_goi from css_hcm.goi_dadv 
select iptinh, soluong_ip from css_hcm.tocdo_adsl
select madoicap from css_hcm.db_adsl    
select* from css_hcm.goi_tichhop where ten_goi = 'HomeTV3_DB_042022_S_NT'

create table trung as 
select thuebao_id from fiber_hienhuu 
group by thuebao_id having count(thuebao_id) > 1
select a.* from hocnq_ttkd.v_Thongtinkm_all a
join trung b on a.thuebao_id = b.thuebao_id
join (
select * from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id
and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000 -- ) c on 
    and thuebao_id in (      select thuebao_id from  css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id
and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
group by thuebao_id having count(thuebao_id) > 1 )  --   and nhomtb_id not in (2691065)
select muccuoc_id from css_hcm.muccuoc_tb
select* from css_hcm.db_datcoc where ma_Tb = 'nhthienc31605'
