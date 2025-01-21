select a.thang,d.ma_pb, d.ten_pb 
--    count( a.thuebao_id ) as SLTB_Fiber
, sum(case when a.loaitb_id  in (58,59) then 1 else 0 end) sltb_fb,
sum(case  when a.loaitb_id not in (58,59) then 1 else 0 end) sltb_fb

from hocnq_ttkd.ds_giahan_tratruoc_v4 a
left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
        left join  ttkd_bsc.dm_phongban d on b.pbh_ql_id = d.pbh_id and d.active = 1 
where a.tratruoc = 1 -- and a.loaitb_id  NOT in (58,59)
group by a.thang, d.ma_pb, d.ten_pb 
-- —> 4
select* from ttkd_bct.db_thuebao_ttkd where pbh_ql_id  =12
select * from ttkd_bsc.dm_phongban-- where tratruoc = 1 -- —> 5
select count(thuebao_id) from hocnq_ttkd.ds_giahan_tratruoc_v6 where tratruoc = 1 ---- —> 6
select * from hocnq_ttkd.ds_giahan_tratruoc_v4 where tratruoc = 1 ;
select* from ttkd_bct.db_thuebao_ttkd

select* from css_hcm.db_Datcoc  a where to_char(ngay_ktdc , 'yyyymm') = '202404' and not exists (select 1 from  
css_hcm.db_Datcoc where to_char(ngay_ktdc , 'yyyymm') > '202404' and a.thuebao_id = thuebao_id)