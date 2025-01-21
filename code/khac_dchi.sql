delete from khac_Diachi where thang_kt = 202412
commit;
insert into khac_Diachi
with ds_chot as
(
    select* from ds_tb_chot where thang_kt = 202412
)
select 
a.thuebao_id,a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt, p.ten_phuong||', ' ||q.ten_quan||', '||t.tentinh as dia_Chi,
 RTRIM(XMLAGG(XMLELEMENT(E,b.ma_tb,',').EXTRACT('//text()') ORDER BY b.thuebao_id).GetClobVal(),',') AS ds_matb_phu  
 ,count (b.thuebao_id) as sl_thuebao_phu
from ds_chot a
    join ds_Thuebao_cung_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id and a.thang_kt = b.thang_kt 
    left join DCHI_TOANBO_THUEBAO c on a.thuebao_id = c.thuebao_id 
    left join css_hcm.tinh t on c.tinh_id = t.tinh_id
    left join css_hcm.quan q on c.quan_id = q.quan_id
    left join css_hcm.phuong p  on c.phuong_id = p.phuong_id
    left join DCHI_TOANBO_THUEBAO d on b.thuebao_id = d.thuebao_id 
where nvl(c.quan_id,0) != nvl(b.quan_id,0) or nvl(c.tinh_id,0) != nvl(b.tinh_id,0) or nvl(c.phuong_id,0) != nvl(b.phuong_id,0) 
group by a.thuebao_id, a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt, p.ten_phuong,q.ten_quan,t.tentinh;
----
commit;
select* from khac_diachi where thuebao_id = 11822516
select khachhang_id from khac_diachi group by khachhang_id, thang_kt having count(khachhang_id) > 1
select a.khachhang_id,a.thang_kt, a.thuebao_id, a.loaitb_id, a.ma_Tb, a.dia_chi,
b.ma_nv,f.ten_nv, b.tbh_ql_id, b.pbh_ql_id
    ,  ma_to_hrm ma_To,tento
    ,  ma_pb, ten_pb,
    c.loaihinh_tb, a.ma_tt, a.sl_thuebao_phu, a.ds_matb_phu, lkh.khdn
from khac_diachi a
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
    left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join css_hcm.loaihinh_tb c on a.loaitb_id = c.loaitb_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id 
    left join admin_hcm.nhanvien_onebss f on b.ma_nv = f.ma_nv
    where ten_nv = 'Kh??ng Th? Hoài Thu'
