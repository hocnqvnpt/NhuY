with 
dchi_tb as -- lay thong tin dia chi thue bao
(
    select /*+ RESULT_CACHE */ a.thuebao_id, c.tinh_id, c.quan_id, c.phuong_id
    from css_hcm.db_thuebao a
        left join css_hcm.diachi_Tb b on a.thuebao_id = b.thuebao_id
        left join css_hcm.diachi c on b.diachild_id = c.diachi_id 
),
ds_thuebao as
(
      select  a.thuebao_id,  a.loaitb_id, a.khachhang_id, a.ma_tb,  f.ma_tt
      ,  c.tinh_id, c.quan_id, c.phuong_id 
      from css_hcm.db_Thuebao a
          join css_hcm.db_thanhtoan f on a.thanhtoan_id = f.thanhtoan_id
          left join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id
          join dchi_tb c on a.thuebao_id = c.thuebao_id
      where a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) and a.trangthaitb_id not in (7,8,9) and chuquan_id = 145
)  select* from ds_thuebao
--select distinct thuebao_id from ds_thuebao where khachhang_id = 9361641;
-- lay danh sach khach hang cung ma thanh toan, cung dia chi
select a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, b.tinh_id,b.quan_id,b.phuong_id--,c.tinh_id,c.quan_id,c.phuong_id
--,RTRIM(XMLAGG(XMLELEMENT(E, b.ma_tb,', ').EXTRACT('//text()') ORDER BY b.thuebao_id).GetClobVal(),',') ds_matb_phu
--    , count (b.ma_Tb) + 1 as sl_thuebao
from ds_tb_chot a
    left join dchi_tb c on a.thuebao_id = c.thuebao_id
    left join ds_thuebao b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id
where  c.quan_id = b.quan_id and c.tinh_id =b.tinh_id and c.phuong_id = b.phuong_id and a.ma_tt = b.ma_tt
group by a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, b.tinh_id,b.quan_id,b.phuong_id
order by  count (b.ma_Tb) desc ;
