select distinct kenhthu_id from ttkd_bsc.ct_bsc_tratruoc_moi
select* from css.v_bd_goi_dadv@dataguard where thuebao_id in 
(
select  * from css.v_bd_goi_dadv@dataguard where dichvuvt_id = 4 and trangthai = 1
 and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
and thuebao_id in (select thuebao_id from css.v_bd_goi_dadv@dataguard where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id
                        and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                                                    group by thuebao_id having count(thuebao_id) > 1                                                                          --   and nhomtb_id not in (2691065)
                            ) 
                            
select* from hocnq_ttkd.v_thongtinkm_all where thuebao_id in (
select thuebao_id from fiber_hienhuu_t11 group by thuebao_id having count(thuebao_id) > 1) and 202310 between to_number(to_char(ngay_bddc,'yyyymm'))
and to_number(to_char(ngay_ktdc,'yyyymm')) and 
hieuluc = 1