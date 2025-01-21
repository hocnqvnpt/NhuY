select* from ttkd_bsc.dm_phongban where pbh_id in (select distinct pbh_ql_id from ds_Giahan_tratruoc_4 where tratruoc = 1) and active = 1;
select thang ,  ma_pb, ten_pb ,  count(distinct thuebao_id) sltb
FROM DS_GIAHAN_TRATRUOC_6 a
left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = a.pbh_ql_id
WHERE TRATRUOC = 1
GROUP BY THANG, ma_pb, ten_pb   ;
select* from abcc