select a.ma_tb,to_number(to_char(ngay_kt_mg,'yyyymm')) thang_kt, b.so_Dt sdt_tt, d.so_dt sdt_kh 
from ds_Giahan_tratruoc2 a
    left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
    left join css_hcm.db_thanhtoan b on c.thanhtoan_id = b.thanhtoan_id
    left join css_hcm.db_khachhang d on c.khachhang_id = d.khachhang_id
where to_number(to_char(ngay_kt_mg,'yyyymm')) between 202406 and 202412 and nvl(b.so_Dt,0) = nvl(d.so_dt,0);

select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202402 and ma_To = 'aaa' and ma_pb = 'bbb' and ma_kpi = 'ccc';
select* from ttkd_bsc.nhanvien_202404 where ten_nv like '%Oanh';


select* from css_hcm.phieutt_hd ;