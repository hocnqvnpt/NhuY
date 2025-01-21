-- lay danh sach thue bao het han thang 1/2024
drop table 
select* from ds_hethan_2024
commit;
create table ds_hethan_2024 as (
insert into ds_hethan_2024
    select a.thuebao_id,  to_number(to_char(b.ngay_ktdc, 'yyyymm')) thang_kt, b.rkm_id ,a.khachhang_id, a.loaitb_id, a.thanhtoan_id, a.ma_tb
    from css_hcm.khuyenmai_dbtb b
        join css_hcm.db_thuebao a on b.thuebao_id = a.thuebao_id
    where 202301 <= least(nvl(to_number(to_char(b.ngay_thoai - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(b.ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(b.ngay_ktdc, 'yyyymm')))  and  to_number(to_char(b.ngay_ktdc,'yyyymm')) = 202401 --between 202402 and 202412
               and a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224)
    union all
    select a.thuebao_id,  to_number(to_char(b.ngay_ktdc, 'yyyymm')) thang_kt, b.rkm_id ,  a.khachhang_id , a.loaitb_id,  a.thanhtoan_id, a.ma_tb
    from css_hcm.db_datcoc b
        join css_hcm.db_thuebao a on b.thuebao_id = a.thuebao_id
    where 202401 <= least(nvl(to_number(to_char(b.ngay_thoai - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm'))),
               nvl(to_number(to_char(b.ngay_huy - 1, 'yyyymm')), to_number(to_char(sysdate + interval '50' year ,'yyyymm')) ) ,
               to_number(to_char(b.ngay_ktdc, 'yyyymm')))  and  to_number(to_char(b.ngay_ktdc,'yyyymm')) = 202401 -- between 202402 and 202412
                and a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224)
) ;
--
drop table ds_tb_chot;
select* from ds_tb_chot
drop table ds_hethan_202401;
select* from ds_hethan_202401 where khachhang_id = 29767 distinct
select khachhang_id from ds_hethan_2024 group by khachhang_id having count(distinct thang_kt) > 1;
select distinct thang_kt from ds_tb_chot
-- 1. BUOC 1
select* from ds_tb_chot where thang_kt = 202408
delete from ds_tb_chot where thang_kt = 202408
commit;
insert into  ds_tb_chot 
with temp_1 as 
(
    select a.thuebao_id, a.thang_kt, a.khachhang_id, a.loaitb_id, a.thanhtoan_id,a.ma_tb,
    case  when loaitb_id in (58,59) then 1
        when loaitb_id = 210 then 2 
        when loaitb_id in (18,61) then 3 
        else 4 end do_uutien 
    from ds_hethan_2024 a
    where thang_kt = 202408
    group by  a.thuebao_id, a.thang_kt, a.khachhang_id, a.loaitb_id, a.thanhtoan_id,a.ma_tb
) 
select* from
(
    select a.thuebao_id, a.thang_kt, a.khachhang_id, a.loaitb_id, a.thanhtoan_id,a.ma_tb, a.do_uutien,b.ma_tt,
        row_number() over (partition by a.khachhang_id order by a.do_uutien) rnk
    from temp_1 a
        join css_hcm.db_Thanhtoan b on a.thanhtoan_id = b.thanhtoan_id
) where rnk = 1  ;
select * from DS_TB_CHOT where ;-- group by khachhang_id having count(khachhang_id) >1; -- danh sach cac thue bao lam thue bao chinh gia han tra truoc
--
-- lay danh sach cac thuebao BRCD hien huu
drop table dstb_cung_matt;
select* from ds_tb_chot where thang_kt <> 202402
create table dstb_cung_matt as 
select* from dchi_tb
--with 
--create table
select* from dchi_tb_2024
select* from DS_THUEBAO_2024
---- lay thong tin dia chi thuebao theo tung thang
--;
--create table
--dchi_Toanbo_thuebao as -- lay thong tin dia chi thue bao
--(
--    select /*+ RESULT_CACHE */ a.thuebao_id, c.tinh_id, c.quan_id, c.phuong_id
--    from css_hcm.db_thuebao a
--        left join css_hcm.diachi_Tb b on a.thuebao_id = b.thuebao_id
--        left join css_hcm.diachi c on b.diachild_id = c.diachi_id 
--    where --exists (select 1 from ds_tb_chot where thang_kt= 202402 and khachhang_id = a.khachhang_id) and
--    a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) and a.trangthaitb_id not in (7,8,9)
--);
select*
delete from ds_Thuebao_cung_kh where thang_kt = 202408
;create table
-- 2. BUOC 2 LAY DANH SACH THUEBAO CUNG KHACH HANG
insert into
--with
ds_Thuebao_cung_kh 
(
      select  a.thuebao_id,  a.loaitb_id, a.khachhang_id, a.ma_tb,  f.ma_tt
      ,  c.tinh_id, c.quan_id, c.phuong_id , d.thang_kt
      from css_hcm.db_Thuebao a
          join css_hcm.db_thanhtoan f on a.thanhtoan_id = f.thanhtoan_id
          left join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id
          join DCHI_TOANBO_THUEBAO c on a.thuebao_id = c.thuebao_id
          join ds_tb_chot d on a.khachhang_id = d.khachhang_id and d.thang_kt = 202408
      where a.loaitb_id in (11, 18, 58, 59, 61, 171, 210, 222, 224) and a.trangthaitb_id not in (7,8,9) and chuquan_id = 145
) 
-- lay danh sach khach hang cung ma thanh toan, cung dia chi
;
insert into cung_matt_dc as
SELECT*FROM ds_Thuebao_cung_kh where THANG_KT = 202308
delete FROM cung_matt_dc_2024 WHERE THANG_KT = 202403
commit;
-------------------- FFFFFFFFFFFFFFFFFFFFFFFFFFFFFF---------------------------
create table 
select* from ds_thuebao_2024 where thang_kt = 202403
delete
select* from  cung_matt_dc_2024 where thang_kt = 202408
commit;
-- 3. DANH SACH CUNG KHACH_KHANG CUNG MA THANH TOAN CUNG DIA CHI
select* from cung_matt_dc_2024 where thang_kt = 202408
insert  into cung_matt_dc_2024 
select a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, b.tinh_id,b.quan_id,b.phuong_id, b.thuebao_id tb_phu, b.ma_tb matb_phu, a.thang_kt
from ds_tb_chot a
    left join DCHI_TOANBO_THUEBAO c on a.thuebao_id = c.thuebao_id 
    left join ds_Thuebao_cung_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id AND A.THANG_KT = B.THANG_KT
where  a.thang_kt = 202408 and c.quan_id = b.quan_id and c.tinh_id =b.tinh_id and c.phuong_id = b.phuong_id and a.ma_tt = b.ma_tt
    and nvl(c.quan_id,0) > 0 and nvl(c.phuong_id,0) > 0;
commit;
--group by a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, b.tinh_id,b.quan_id,b.phuong_id,b.thuebao_id
select* from dstb_cung_matt
order by  1 desc ;
-- lay nhomtb_id cua dstb cung matt
--------FFFFFFFFFFFFFFFFFFFFFFFFFFFF-----------
select*
delete from DS_CUNG_GOI_2024 where thang_kt = 202403
commit;

create table DS_CUNG_GOI_2024 as
--CREATE TABLE  ds_goi_dadv as (
--    select nhomtb_id, thuebao_id from css_hcm.bd_goi_dadv a
--    where trangthai = 1 and exists (select 1 from css_hcm.goi_dadv_lhtb where goi_id = a.goi_id --and goi_id not between 1715 and 1726
--    group by GOI_ID having count(distinct loaitb_id)>1) and goi_id not between 1715 and 1726
--)   , 
select*
delete from cung_matt_dc_2024 where thang_kt = 202408
delete from DS_CUNG_GOI_2024 where thang_kt = 202403
commit;
-- 4. GOM DANH KHAC GOI VAO 1 DONG
select* from DS_CUNG_GOI_2024 where thang_kt = 202408
insert into DS_CUNG_GOI_2024 --danh sach cung dia chi, cung ma tt , khac goi
WITH tt_dadv_KHACGOI as (
select a.*, b.nhomtb_id, c.nhomtb_id nhomtb_phu
from cung_matt_dc_2024 a
    left join ds_goi_dadv b on a.thuebao_id = b.thuebao_id
    left join ds_goi_dadv c on a.tb_phu = c.thuebao_id
    where (nvl(b.nhomtb_id,0) != nvl(c.nhomtb_id,0) or (b.nhomtb_id is null and c.nhomtb_id is null)) AND A.THANG_KT = 202408
)  

select a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, a.tinh_id,A.THANG_KT,a.quan_id,a.phuong_id, a.nhomtb_id--, a.nhomtb_phu
    ,LISTAGG(A.matb_phu , '; ') WITHIN GROUP (ORDER BY a.tb_phu) ds_ma_tb_phu
    , count (a.tb_phu)  as sl_thuebao_phu
    ,LISTAGG(distinct A.nhomtb_phu , '; ') WITHIN GROUP (ORDER BY a.tb_phu) ds_nhomtb_phu
from tt_dadv_KHACGOI a
group by a.khachhang_id, a.thuebao_id, a.loaitb_id, a.ma_tb, a.ma_tt, a.tinh_id,a.quan_id,a.phuong_id, a.nhomtb_id,A.THANG_KT;
COMMIT;
--- DANH SACH CUNG GOI
select*
delete from DS_CUNG_GOI_2024 where thang_kt = 202408
SELECT* FROM CHOT_202402 WHERE thang_kt = 202408
-- 5.LAY DANH SACH THUEBAO CUNG GOI 
rollback;
insert into CHOT_202402 
WITH cung_goi as (
    select a.*, b.nhomtb_id, c.nhomtb_id nhomtb_phu
    from cung_matt_dc_2024 a
    left join ds_goi_dadv b on a.thuebao_id = b.thuebao_id
    left join ds_goi_dadv c on a.tb_phu = c.thuebao_id
    where nvl(b.nhomtb_id,0) = nvl(c.nhomtb_id,0) and (b.nhomtb_id is not null and c.nhomtb_id is not null) AND A.THANG_KT = 202408
) ,
ds_cung_Goi as 
(
    select khachhang_id, LISTAGG(A.matb_phu , '; ') WITHIN GROUP (ORDER BY a.tb_phu) ds_ma_tb_cung_goi, count(a.tb_phu) as sl_tb_cung_goi, A.THANG_KT
    from cung_goi a
    group by khachhang_id, A.THANG_KT
) 

    SELECT A.KHACHHANG_ID,A.THUEBAO_ID, A.LOAITB_ID,A.MA_TB,A.MA_tT,A.TINH_ID,A.QUAN_ID,A.PHUONG_ID,A.THANG_KT,A.NHOMTB_ID,A.DS_MA_TB_PHU TB_KHAC_GOI, A.SL_THUEBAO_PHU,
    A.DS_NHOMTB_PHU DS_NHOMTB_KHAC_GOI, B.ds_ma_tb_cung_goi,B.sl_tb_cung_goi
    FROM DS_CUNG_GOI_2024  A -- DANH SACH KHACH HANG CO CAC THUE BAO CHUA CUNG GOI (CHOT)
        LEFT JOIN DS_CUNG_GOI B ON A.KHACHHANG_ID = B.KHACHHANG_ID AND A.THANG_KT = B.THANG_KT
    WHERE A.THANG_KT = 202408;
commit;
---------------- CHOT BANG ---------------
-- 6.CHOT BANG
select distinct thang_kt from CHOT_202402 --where THANG_KT = 202402;
insert into ds_chuagg
select khachhang_id from ds_chuagg group by khachhang_id, thang_kt having count (khachhang_id) > 1
--select distinct thang_kt from ds_chuagg
--commit;
select a.khachhang_id, a.thang_kt, a.thuebao_id, a.loaitb_id, a.ma_Tb, 
b.ma_nv,f.ten_nv, b.tbh_ql_id, b.pbh_ql_id
    ,  ma_to_hrm ma_To,tento
    ,  ma_pb, ten_pb,
    c.loaihinh_tb, a.ma_tt, a.sl_thuebao_phu SL_TB_Chua_Ghep_Goi, a.tb_khac_goi DS_TB_Chua_Ghep_Goi,a.ds_ma_tb_cung_goi DS_TB_CUNG_GOI, A.sl_tb_cung_goi ,
   ( a.sl_thuebao_phu + NVL(A.SL_TB_CUNG_GOI,0))  TONG_TB
    ,a.nhomtb_id, a.ds_nhomtb_khac_goi, lkh.khdn
from CHOT_202402 a
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
    left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id 
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join css_hcm.loaihinh_tb c on a.loaitb_id = c.loaitb_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id 
    left join admin_hcm.nhanvien_onebss f on b.ma_nv = f.ma_nv
    WHERE A.THANG_KT = 202408 ;
    -------------------------------
    select avg (thangdc ) from v_Thongtinkm_all
alter table chua_ghep_goi add  tb_cung_goi varchar2(4000);
alter table chua_ghep_goi add  TONG_TB_PHU number;
COMMIT;
select* from ttkd_bct.db_thuebao_ttkd where thuebao_id = 10774376
;
update chua_ghep_goi a
select a.*, lkh.khdn
from chua_ghep_goi a
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
        left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id 

    --set tb_Cung_goi = (select ds_ma_tb_cung_goi from DA_GHEP_GOI where a.khachhang_id = khachhang_id),
SET A.TONG_TB_PHU =A.sl_thuebao_phu +  A.SL_TB_CUNG_GOI -- 0 WHERE SL_TB_CUNG_GOI IS NULL -- (select sl_tb_cung_goi from DA_GHEP_GOI where a.khachhang_id = khachhang_id)
select a.thuebao_id,a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt, t.tentinh,q.ten_quan,p.ten_phuong,
    b.ma_Tb,b.thuebao_id
from chot_202402 a
    left join ds_thuebao b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id
    left join css_hcm.loai_kh lkh on kh.loaikh_id = loaikh_id 
    left join dchi_Tb c on a.thuebao_id = b.thuebao_id 
    left join css_hcm.tinh t on c.tinh_id = t.tinh_id
    left join css_hcm.quan q on c.quan_id = q.quan_id
    left join css_hcm.phuong p  on c.phuong_id = p.phuong_id
    left join dchi_tb d on b.thuebao_id = d.thuebao_id 
where (c.quan_id != b.quan_id or c.tinh_id != b.tinh_id or c.phuong_id != b.phuong_id ) -- and nvl(c.quan_id,0) > 0 and nvl(c.phuong_id,0) > 0;
select * from css_hcm.phuong
select* from css_hcm.loai_kh
-- 
select* from ds_chuagg;
insert into 
ds_lechky  (
    select a.khachhang_id ,a.thuebao_id, LISTAGG(b.ma_tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tb_lechky, a.thang_kt, count (b.thuebao_id) as sl_lechky
    from ds_tb_chot a
        left join  ds_tb_chot b on a.khachhang_id = b.khachhang_id and a.thang_kt != b.thang_kt
    where a.thang_kt = 202412
    group by a.khachhang_id ,a.thuebao_id, a.thang_kt
);
commit;
Select * from(
Select a.*, row_number() over(partition by thang_kt order by rkm_id desc) ra
From  a)
Where ra = 1)
select a.*, b.ds_ma_tb_lechky, b.sl_lechky 
from ds_chuagg a
    left join ds_lechky b on  a.khachhang_id = b.khachhang_id and a.thang_kt = b.thang_kt and a.thuebao_id = b.thuebao_id