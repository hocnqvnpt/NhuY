select b.ma_Tb, c.ten_Dv
from stb a
    join css_hcm.db_thuebao b on a.acc_mytv = b.ma_tb
    left join admin_Hcm.donvi c on b.donvi_id = c.donvi_id;
    select* from admin.donvi@dataguard where donvi_id =10690;
select donvi_id from css.v_hd_khachhang@dataguard where ma_Gd='HCM-LD/00766550';

select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202410; AND MA_PB = 'VNP0703000' and ht_Tra_id = 214;

select* from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202410 and ma_Tb='pnt2024';
select* from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202410 and ma_nv='CTV086123';
select sum(tien_Thuyetphuc*heso_chuky*heso_dichvu) from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202410 and ma_nv= 'CTV086123'
union all 
select sum(tien_xuathd*heso_chuky*heso_dichvu) from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202410 and nhanvien_xuathd= 'CTV086123'
;
select* from ttkd_bsc.bangluong_dongia_202410 where ma_nv= 'CTV086123';
select* from ttkd_Bsc.nhanvien where thang = 202410 and ten_Nv like '%Ph??ng Th?o';