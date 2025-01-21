create table kh_Cung_matt as select* from  (
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

select a.khachhang_id ,
--, LISTAGG(case when a.ma_tb != b.ma_tb then b.ma_tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tb_phu,
count (distinct b.ma_Tb) as sl_thuebao
--,
--LISTAGG(distinct b.nhomtb_id, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) nhomtb_id

from thong_Tin  a
    join ds_Thuebao b on a.khachhang_id = b.khachhang_id
group by a.khachhang_id
having count(distinct b.ma_tt) = 1
) 
select* from ds_kh_Cung_matt
drop table ds_kh_Cung_matt
commit;
--
flashback table nhuy.ds_kh_Cung_matt to before drop;
with 
hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                ),
kmtb as (select * from css_hcm.khuyenmai_dbtb 
        where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
),
ds_ghtt as
(
        select nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                    , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                    , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                    , a.phieutt_id, a.trangthai
                    , a.ma_gd, a.ngay_hd, a.ngay_tt
                   , b.hdtb_id, hdkh.hdkh_id, hdtb.thuebao_id, hdkh.khachhang_id
        from css_hcm.phieutt_hd a
                    join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                    left join hddc on b.hdtb_id = hddc.hdtb_id
                    join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                    join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                    left join kmtb on b.hdtb_id = kmtb.hdtb_id
        where a.kenhthu_id not in (6) and a.trangthai = 1
) ,
db_ghtt as
(
    select a.khachhang_id, a.thuebao_id, a.rkm_id, to_number(to_char(nvl(b.ngay_bddc, c.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                    , to_number(to_char(nvl(b.ngay_ktdc, c.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi,
                    to_number(to_char(nvl(b.ngay_ktdc, c.ngay_ktdc),'yyyymm')) thang_kt
                    
    from ds_ghtt a
        left join css_hcm.db_datcoc b on a.rkm_id = b.rkm_id
        left join css_hcm.khuyenmai_dbtb c on a.rkm_id = c.rkm_id
    where  ( 202312 <= least(nvl(to_number(to_char(b.ngay_thoai - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(b.ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(b.ngay_ktdc, 'yyyymm')))  
               or 
               202312 <= least(nvl(to_number(to_char(c.ngay_thoai - 1, 'yyyymmdd')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(c.ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(c.ngay_ktdc, 'yyyymm')))  )
               and ( to_number(to_char(nvl(b.ngay_ktdc, c.ngay_ktdc),'yyyymm')) = 202401 ) 
    
),
chot as (
    select a.*, b.loaitb_id , case when loaitb_id in (58,59) then 1
        when loaitb_id = 210 then 2 
        when loaitb_id in (18,61) then 3 
        else 4 end do_uutien
    from db_Ghtt a
        join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id
                       and loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) 
),
tb_goc as (
select khachhang_id,loaitb_id,thang_kt, thuebao_id, do_uutien, row_number() over (partition by khachhang_id order by do_uutien) rnk
from chot
where khachhang_id =9640140
--where loaitb_id = 58
--group by khachhang_id,loaitb_id,thang_kt, thuebao_id,do_uutien
) select* from tb_goc ;
--select 
--    a.khachhang_id,
--            LISTAGG(case when a.thuebao_id = b.thuebao_id then  a.ma_tb end, '; ') WITHIN GROUP (ORDER BY a.thuebao_id) ma_tb, 
----    ,     a.tinh_id, 
----        a.quan_id, a.phuong_id , a.diachi,
--        LISTAGG(case when a.thuebao_id != b.thuebao_id and a.khachhang_id = b.khachhang_id then  a.ma_tb end, '; ') WITHIN GROUP (ORDER BY a.thuebao_id) ds_ma_tb , 
--        count(distinct a.thuebao_id) as sl_thuebao
----        , LISTAGG( distinct a.ma_Tt, '; ') WITHIN GROUP (ORDER BY a.thuebao_id) ma_tt
----             LISTAGG( distinct a.nhomtb_id  , '; ') WITHIN GROUP (ORDER BY a.thuebao_id) ds_nhomtb_id,
----             case when LISTAGG( distinct a.nhomtb_id, '; ') WITHIN GROUP (ORDER BY a.thuebao_id) is null then 0 else 1 end goi_DADV
--from ds_ghtt_202401 b
--    join ds_thuebao a on b.khachhang_id = a.khachhang_id
--group by   a.khachhang_id
--having count (distinct ma_tt) = 1 
--order by 4


select nhomtb_id from ds_ghtt_202401 a join css_hcm.bd_goi_dadv b on a.thuebao_id = b.thuebao_id
--select* from ds_matt where ma_tb = 'mesh0076931'
--select* from css_hcm.bd_goi_dadv
,
tonghop as
(
select a.khachhang_id , tinh_id,quan_id,phuong_id,diachi
    ,  LISTAGG(b.ma_tb, '; ') WITHIN GROUP (ORDER BY b.ma_tb) ma_tb, 
    LISTAGG( distinct b.ma_Tt, '; ') WITHIN GROUP (ORDER BY b.ma_tb) ma_tt,
    LISTAGG( distinct b.nhomtb_id, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) nhomtb_id
from (select distinct khachhang_id from db_ghtt) a
    join ds_matt b on a.khachhang_id = b.khachhang_id
group by a.khachhang_id,  tinh_id,quan_id,phuong_id,diachi
)
select khachhang_id, a.tinh_id, a.quan_id, a.phuong_id,a.diachi , ma_tb, ma_tt, nhomtb_id, 
 ten_pho || ', '||ten_phuong ||', ' || ten_quan ||', ' || tentinh
from tonghop        a                
    left join css_hcm.tinh t on a.tinh_id = t.tinh_id
    left join css_hcm.quan q on a.quan_id = q.quan_id
    left join css_Hcm.phuong p on a.phuong_id = p.phuong_id
    left join css_hcm.pho pho on a.diachi = pho_id
select* from css_Hcm.diachi
select * from css_hcm.pho;   
select* from ds_Ghtt_202401