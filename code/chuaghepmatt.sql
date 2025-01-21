drop table chua_Ghep_ma_tt
desc chua_Ghep_ma_tt
create table chua_Ghep_ma_tt 
(
THUEBAO_ID         NUMBER(12)     ,
THANG_KT           NUMBER   ,      
KHACHHANG_ID       NUMBER(12)  primary key   ,
LOAITB_ID          NUMBER(10)     ,
MA_TB              VARCHAR2(100)  ,
MA_TT              VARCHAR2(50)   ,
SL_TB_PHU          NUMBER         ,
DS_MA_TB_PHU       clob ,
DS_MA_TT_PHU       clob 
)
insert into chua_Ghep_ma_tt 
select a.thuebao_id, a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb, a.ma_tt, count (b.thuebao_id) as sl_tb_phu--, count (distinct b.ma_tt) as sl_matt_phu
,LISTAGG(b.ma_tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tb_phu
,LISTAGG(distinct b.ma_tt , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) ds_ma_tt_phu

from ds_tb_chot a
    left join ds_thuebao b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id
    left join dchi_tb c on a.thuebao_id = c.thuebao_id
where c.quan_id = b.quan_id and c.tinh_id =b.tinh_id and c.phuong_id = b.phuong_id and a.ma_tt != b.ma_tt and a.khachhang_id not in (9662923)
and nvl(c.quan_id,0) > 0 and nvl(c.phuong_id,0) > 0
group by a.thuebao_id,a.thang_kt,a.khachhang_id,a.loaitb_id,a.ma_tb, a.ma_Tt;
SELECT MA_tT FROM DS_THUEBAO GROUP BY MA_TT,MA_TB HAVING COUNT (MA_tT) > 1
create table chua_Ghep_ma_tt as
insert into chua_Ghep_ma_tt
select a.thuebao_id, a.thang_kt, a.khachhang_id, a.loaitb_id, a.ma_tb,a.ma_tt,count (b.thuebao_id) as sl_tb_phu,--, count (distinct b.ma_tt) as sl_tb_phu,
RTRIM(XMLAGG(XMLELEMENT(E,B.MA_TB,',').EXTRACT('//text()') ORDER BY B.THUEBAO_ID).GetClobVal(),',') AS DS_MATB,
RTRIM(XMLAGG(XMLELEMENT(E,B.MA_TT,',').EXTRACT('//text()') ORDER BY B.THUEBAO_ID).GetClobVal(),',') AS DS_MATT


from ds_tb_chot a
    left join ds_thuebao b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id
    left join dchi_tb c on a.thuebao_id = c.thuebao_id
where c.quan_id = b.quan_id and c.tinh_id =b.tinh_id and c.phuong_id = b.phuong_id and a.ma_tt != b.ma_tt and a.khachhang_id  in (9662923)
and nvl(c.quan_id,0) > 0 and nvl(c.phuong_id,0) > 0 
group by a.thuebao_id,a.thang_kt,a.khachhang_id,a.loaitb_id,a.ma_tb, a.ma_Tt;
select * from ttkd_bsc.ct_dongia_Tratruoc where nhomtb_id  is  null and thang = 202310 and loai_tinh in ('DONGIATRA','DONGIATRA30D')
commit;


select chuquan_id from css_hcm.db_adsl where thuebao_id = 8138207
select a.thuebao_id ,a.thang_kt, a.khachhang_id, a.ma_tb,b.ma_nv,f.ten_nv, b.tbh_ql_id, b.pbh_ql_id
    ,  ma_to_hrm ma_To,tento
    ,  ma_pb, ten_pb,
    c.loaihinh_tb, a.ma_tt, a.sl_tb_phu, a.ds_ma_tb_phu, a.ds_ma_Tt_phu
from CHUA_GHEP_MA_TT a
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join css_hcm.loaihinh_tb c on a.loaitb_id = c.loaitb_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id 
    left join admin_hcm.nhanvien_onebss f on b.ma_nv = f.ma_nv
select* from css_hcm.loaihinh_tb 
select * from ttkd_bct.tobanhang where hieuluc  = 1