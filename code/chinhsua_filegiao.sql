select* from loaibo where thuebao_id in (9866366,9866341)
select* from css_hcm.duan
select*  from css_hcm.toanha
select* from ct_Bsc_tratruoc_moi where thuebao_id in (
select thuebao_id from ct_Bsc_tratruoc_moi where thang = 202312 --rkm_id in (6302017,6301997)
group by thuebao_id having count(thuebao_id) > 1)
select * from ct_bsc_tratruoc_moi_30day where thang = 202312-- thuebao_id in (9866366,9866341)
select* from ttkdhcm_ktnv.ghtt_giao_688 where thuebao_id in (9866341) and loaibo = 0 and km = 1 and tratruoc  =1
select* from css_hcm.db_thuebao where ma_Tb like '%khtn%' --and trangthaitB_id in (1,5)
select* from css_hcm.khuyenmai_Dbtb where rkm_id=5397260-- in (5397291,5397260)
select* from css_hcm.db_Datcoc where rkm_id= 6089877-- in (4974599)--,6089853,6089877)
select* from css_hcm.db_datcoc where thuebao_id = 9866341

-----Update ds tra truoc
update ds_giahan_tratruoc a
                    set tratruoc = 0
--   drop table loaibo         
--   select* from css_hcm.ct_khuyenmai where chitietkm_id in (6556
--create table loaibo as
select * from    ttkdhcm_ktnv.ghtt_giao_688 a
where  tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202311--and SO_THANGDC <=2
and not exists (select congvan_id from ttkdhcm_ktnv.ghtt_giao_688 where  tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202311
and congvan_id not in (190, 343, 483, 491, 545, 8922) and a.thuebao_id = thuebao_id)
and chitietkm_id  in (select chitietkm_id from css_hcm.ct_khuyenmai where huong_km = 0)    
--and SO_THANGDC <=2
--and a.thuebao_id = thuebao_id)
;
select* from css_Hcm.congvan where congvan_id = 190
select* from hocnq_ttkd.ds_giahan_tratruoc
----Update ds tra truoc
update ds_giahan_tratruoc a
                    set tratruoc = 0
-- select * from hocnq_ttkd.ds_giahan_tratruoc a
where so_thangdc < 3 and tratruoc = 1
select* from loaibo