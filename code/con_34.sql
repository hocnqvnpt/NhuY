LISTAGG( ma_Tb , '; ') WITHIN GROUP (ORDER BY thuebao_id)
select* from abcdefgh
select* from finallll
create table xyz as 
select a.*, row_number() over (partition by a.thang_kt, a.khachhang_id order by a.rkm_id desc) rnk
from khdn a

               select* from ttkd_bsc.ct_bsc_giahan_cntt where thang =202401 --where ten_nv like '%An Chi'        
select* from css_hcm.khoanmuc_tt where ten_kmtt like '%oken%'
select* from ttkd_bsc.nhanvien_202401 where ten_Nv like '%Khánh'
commit;
select * from ttkd_Bsc.bldg_danhmuc_qldb_luong_dhanh_kh