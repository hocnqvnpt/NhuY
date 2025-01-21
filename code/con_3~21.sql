with loaihinhdv as 
(
    select 
    from css_hcm.goi_dadv_lhtb a
        left join css_hcm.loaihinh_Tb 
        
    
)
select a.ma_tb
from css_hcm.db_Thuebao a
    left join css_Hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    left join css_hcm.loaihinh_Tb c on a.loaitb_id = c.loaitb_id
    left join css_hcm.db_adsl d on a.thuebao_id = d.thuebao_id
    left join css_hcm.trangthai_tb e on d.trangthaitb_id = e.trangthaitb_id
    left join css_hcm.diachi_tb dc on e.thuebao_id = dc.thuebao_id
    left join css_hcm.diachi dch on dc.diachild_id = dch.diachi_id
    left join css_hcm.quan q on dchi.quan_id = q.quan_id
    left join css_hcm.
    select* from v_Thongtinkm_all where thuebao_id in (10547196,8187604,8187583,7284192,8251026,8247008,9295113,8488044,8823293,9207615,9097339)  
    and datcoc = 
    select* from aaaa
select* from css_hcm.goi_dadv_lhtb
    select owner , table_name from all_tab_columns where column_name = 'NHOMGOI_DADV_ID';
select* from aaaa where chuquan_id in (332,332,278,266)
select* from css_hcm.chuquan where chuquan_id in (332,332,278,266)
select chuquan_id from  css_Hcm.db_adsl where thuebao_id in (11765573,11765487,11780932,8068368)
select* from css_hcm.db_Thuebao where  thuebao_id in (11765573,11765487,11780932,8068368)
select* from v_Thongtinkm_all where thuebao_id = 8914295-- rkm_id = 5321100 --ma_Tb = 'msigvn_fibervnn'