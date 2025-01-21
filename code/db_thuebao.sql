select khachhang_id from css_hcm.db_thuebao where  ma_Tb in ('ktxdhsg44', 'hudc93651209')
select* from ds_Ghtt_202401 where khachhang_id = 8804076 --thuebao_id in (9329339,
9329336)
select * from css_hcm.bd_Goi_dadv where thuebao_id = 8804076;
select distinct ma_Tb from css_hcm.db_thuebao where khachhang_id = 9640140 and trangthaitb_id not in (7,8,9)--thuebao_id in (9329339,
create table ds_cung_matt as select* from 
(
with 
thong_tin as (
    select a.*, b. ma_Tb
    from ds_ghtt_202401 a join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
),
goi_dadv as 
(
    select thuebao_id, nhomtb_id from css_hcm.bd_goi_dadv a
    where trangthai = 1 and exists (select 1 from css_hcm.goi_dadv_lhtb where goi_id = a.goi_id  
    group by GOI_ID having count(distinct loaitb_id)>1 
)
),
ds_thuebao as
(
      select a.thuebao_id, a.diachi_ld, a.loaitb_id, a.khachhang_id, a.ma_tb, a.thanhtoan_id,c.nhomtb_id,f.ma_tt
--      ,  e.tinh_id, e.quan_id, e.phuong_id 
--      ,case when nvl(pho_id,0) > 0 then pho_id
--        when ap_id > 0 then ap_id 
--        when khu_id > 0 then khu_id
--        end diachi, sonha,
      from css_hcm.db_Thuebao a
          left join goi_dadv c on a.thuebao_id = c.thuebao_id  
          left join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id
          left join css_Hcm.diachi_tb d on a.thuebao_id = d.thuebao_id
--          left join css_hcm.diachi e on d.diachild_id = e.diachi_id
        join css_hcm.db_thanhtoan f on a.thanhtoan_id = f.thanhtoan_id
      where a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) and a.trangthaitb_id not in (7,8,9) and chuquan_id = 145
) 
select a.khachhang_id,  b.ma_tt, c.ma_tb, 
     LISTAGG(case when c.ma_tb != b.ma_tb then b.ma_tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tb_phu,count (distinct b.ma_Tb) as sl_thuebao,
     LISTAGG(distinct b.nhomtb_id, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) nhomtb_id
from kh_cung_matt a 
    join thong_tin c on a.khachhang_id = c.khachhang_id
    join ds_Thuebao b on a.khachhang_id = b.khachhang_id
group by a.khachhang_id, b.ma_tt, c.ma_tb
)
select khachhang_id, ma_Tt, ma_Tb, ds_ma_Tb_phu, sl_thuebao, nhomtb_id, case when nhomtb_id is not null then 1 else 0 end goi_dadv
from DS_CUNG_MATT --group by khachhang_id having count(khachhang_id ) > 1
----------------------------------
with 
thong_tin as (
    select a.*, b. ma_Tb, c.ma_Tt
    from ds_ghtt_202401 a
        join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
        join css_hcm.db_thanhtoan c on b.thanhtoan_id = c.thanhtoan_id 
    
),
select* from cung_matt_dc

goi_dadv as 
(
    select thuebao_id, nhomtb_id from css_hcm.bd_goi_dadv a
    where trangthai = 1 and exists (select 1 from css_hcm.goi_dadv_lhtb where goi_id = a.goi_id  
    group by GOI_ID having count(distinct loaitb_id)>1)
),
ds_thuebao as
(
      select a.thuebao_id, a.diachi_ld, a.loaitb_id, a.khachhang_id, a.ma_tb, a.thanhtoan_id,c.nhomtb_id,f.ma_tt
--      ,  e.tinh_id, e.quan_id, e.phuong_id 
--      ,case when nvl(pho_id,0) > 0 then pho_id
--        when ap_id > 0 then ap_id 
--        when khu_id > 0 then khu_id
--        end diachi, sonha,
      from css_hcm.db_Thuebao a
--          left join goi_dadv c on a.thuebao_id = c.thuebao_id  
          left join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id
          left join css_Hcm.diachi_tb d on a.thuebao_id = d.thuebao_id
--          left join css_hcm.diachi e on d.diachild_id = e.diachi_id
        join css_hcm.db_thanhtoan f on a.thanhtoan_id = f.thanhtoan_id
      where a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) and a.trangthaitb_id not in (7,8,9) and chuquan_id = 145
)  
select a.khachhang_id, a.ma_tt, a.ma_Tb,a.thang_kt, RTRIM(XMLAGG(XMLELEMENT(E, b.ma_tt,',').EXTRACT('//text()') ORDER BY b.thuebao_id).GetClobVal(),',') ds_matt,
RTRIM(XMLAGG(XMLELEMENT(E, b.ma_tt,',').EXTRACT('//text()') ORDER BY b.thuebao_id).GetClobVal(),',') ds_matb
--LISTAGG(case when a.ma_tb != b.ma_tb then b.ma_tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tb_phu
--LISTAGG(b.ma_tb  , '; ' ) WITHIN GROUP (ORDER BY b.thuebao_id) AS desc
--RTRIM(XMLAGG(XMLELEMENT(E, b.ma_tb,',').EXTRACT('//text()') ORDER BY b.thuebao_id).GetClobVal(),',') AS ds_matb_phu
,count (b.thuebao_id) as sl_thuebao, count (distinct b.ma_tt) as st_Matt
from thong_tin a 
    left join ds_thuebao b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id
where not exists (select 1 from DS_CUNG_MATT where a.khachhang_id = khachhang_id)  --and a.khachhang_id = 9662923
group by a.khachhang_id,a.ma_tb,a.thang_kt , a.ma_tt

select 