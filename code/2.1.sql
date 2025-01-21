select* from ds_Thuebao;
create table khac_diachi as
select 
a.thuebao_id,a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt, p.ten_phuong||', ' ||q.ten_quan||', '||t.tentinh as dia_Chi,
 RTRIM(XMLAGG(XMLELEMENT(E,b.ma_tb,',').EXTRACT('//text()') ORDER BY b.thuebao_id).GetClobVal(),',') AS ds_matb_phu  
 ,count (b.thuebao_id) as sl_thuebao_phu

from ds_tb_chot a
    join ds_thuebao b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id
    left join dchi_Tb c on a.thuebao_id = c.thuebao_id 
    left join css_hcm.tinh t on c.tinh_id = t.tinh_id
    left join css_hcm.quan q on c.quan_id = q.quan_id
    left join css_hcm.phuong p  on c.phuong_id = p.phuong_id
    left join dchi_tb d on b.thuebao_id = d.thuebao_id 
where nvl(c.quan_id,0) != nvl(b.quan_id,0) or nvl(c.tinh_id,0) != nvl(b.tinh_id,0) or nvl(c.phuong_id,0) != nvl(b.phuong_id,0) 
group by a.thuebao_id, a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt, p.ten_phuong,q.ten_quan,t.tentinh
select* from khac_diachi

select a.khachhang_id,202401 thang_kt, a.thuebao_id, a.loaitb_id, a.ma_Tb, 
b.ma_nv,f.ten_nv, b.tbh_ql_id, b.pbh_ql_id
    ,  ma_to_hrm ma_To,tento
    ,  ma_pb, ten_pb,
    c.loaihinh_tb, a.ma_tt, a.sl_thuebao_phu, a.ds_matb_phu
from khac_diachi a
select thuebao_id from css_Hcm.db_Thuebao where ma_Tb ='ctymayin'
select* from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202309 and tien_khop =0 and ma_chungtu is not null -- ='ctymayin'
    select * from css_hcm.hd_thuebao where thuebao_id = 8141115
select* from css_hcm.ct_phieutt where hdtb_id in (22222504,22408635)
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join css_hcm.loaihinh_tb c on a.loaitb_id = c.loaitb_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id 
    left join admin_hcm.nhanvien_onebss f on b.ma_nv = f.ma_nv;
