delete
-- create table aaaa as 
-- select* 
 from ttkd_bsc.ct_dongia_tratruoc where thang = 202311 and loai_tinh = 'DONGLUCTT' ; -- and ma_nv ='VNP027554' and tien_khop = 1
 select *  from ttkd_bsc.ct_dongia_tratruoc where thang = 202310 and loai_tinh = 'DONGLUCTT' ;-- and ma_nv ='VNP027554' and tien_khop = 1
 create table abc as
select* from ttkd_bsc.ct_dongia_Tratruoc where thang = 202311 and loai_tinh = 'DONGLUCTT' and tien_khop = 0 and ma_tb not in (select ma_tb from backup_dongluc
where tien_khop = 0)
delete from 
   ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and loai_tinh = 'DONGLUCTT';
   select* from    ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and loai_tinh = 'DONGLUCTT';
rollback
commit;
select* from ttkd_bsc.ct_dongia_tratruoc where rownum = 1

insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MANV_THUYETPHUC
                                                                      , SOTHANG_DC, HT_TRA_ONLINE, DTHU, NGAY_TT, khdn, TIEN_KHOP)
--insert into ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, DTHU, NGAY_TT, khdn, TIEN_KHOP)                                                                      
select thang, 'DONGLUCTT' LOAI_TINH,  'DONGLUC' ma_kpi, nv.ma_nv, nv.ma_to, nv.ma_pb
          , phong_cs, thuebao_id, ma_tb, tien_dc_cu, manv_giao, manv_thphuc
          , SOTHANG_DC, decode(ht_tra_online, 0, 0, 1) HT_TRA_ONLINE, DTHU, NGAY_TT, khdn, TIEN_KHOP
from  (select thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_cs, a.thuebao_id, a.ma_tb, tien_dc_cu
                        , a.manv_giao, a.manv_thphuc
                        , max(so_thangdc) sothang_dc
                        , sum(case when a.ht_tra_id in (1) then 0
                                                when a.ht_tra_id in (2,7,4) then 1 end) ht_tra_online
                        , 0 dongia
                        ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.khdn
                        , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                                    from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                    where a.thang = 202402            ------------n------------
                                    group by thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_giao, a.manv_cs
                                                        , a.thuebao_id, a.ma_tb, tien_dc_cu, a.khdn, a.manv_thphuc--, a.ma_nv_cn, a.pbh_cn_id
                    ) a
            , ttkd_bsc.nhanvien_202402 nv
where a.manv_giao = nv.ma_nv and rnk = 1
            and nvl(nv.MA_PB, 'khongco') not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
            and not (nvl(nv.ma_to, 'khongco') in ('VNP0701402', 'VNP0702215', 'VNP0702216')
                            and nvl(nv.ma_vtcv, 'khongco') in ('VNP-HNHCM_BHKV_14', 'VNP-HNHCM_BHKV_18'))
                            
;
ROLLBACK;
select * from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202312 and manv_giao = 'VNP017088' and loai_tinh = 'DONGLUCTT' AND TIEN_KHOP =1 and (manv_Giao = manv_thuyetphuc or manv_thuyetphuc = 'khongco') 
commit;
select sum(tien) from ttkd_bsc.tl_giahan_tratruoc where ma_kpi like 'DONG%' and thang = 202309 and LOAI_TINH in ('DONGLUCTT'); 101 38385000
delete from ttkd_bsc.tl_giahan_tratruoc where ma_kpi like 'DONG%' and thang = 202309 and LOAI_TINH in ('DONGLUCTT');


---8----thong ke tong tien dong luc
----nhan vien giao trung 1 in 2 (manv thuyetphuc, ma_nvcn) theo chu quan hoac manv thuyetphuc va ma_nvcn is null theo khach quan
-- tyle tinh = 65% from tbao 65% de tinh theo gia dong luc
-- select sum(dongia) from (
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
select thang, LOAI_TINH, ma_kpi, ma_nv, ma_to, ma_pb
, count(thuebao_id) tong
  , sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1
                            when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                        else 0 end) da_giahan
  , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
  , sum(dthu) DTHU_thanhcong
  , round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                        when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                        else 0 end)*100/count(thuebao_id), 2) tyle
  , case 
                when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                    when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                    else 0 end)*100/count(thuebao_id), 2) >= 85 
                        then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                        when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                        else 0 end) - (count(thuebao_id) * 0.65) + 1, 0) * 35000
                when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                    when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                    else 0 end)*100/count(thuebao_id), 2) >= 80
                        then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                        when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                        else 0 end) -  (count(thuebao_id) * 0.65) + 1, 0) * 30000
                when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                    when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                    else 0 end)*100/count(thuebao_id), 2) >= 75
                        then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                        when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                        else 0 end) -  (count(thuebao_id) * 0.65) + 1, 0) * 25000
                when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                    when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                    else 0 end)*100/count(thuebao_id), 2) >= 70 
                        then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                        when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                        else 0 end) -  (count(thuebao_id) * 0.65    ) + 1, 0) * 20000
                when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                    when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                    else 0 end)*100/count(thuebao_id), 2) >= 65 
                        then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                        when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                        else 0 end) -  (count(thuebao_id) * 0.65) + 1, 0) * 15000
                else 0
        end dongia
--    select *
from ttkd_bsc.ct_dongia_tratruoc 
where ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTT' --and ma_nv = nvl(manv_thuyetphuc, ma_nv_cn) 
and thang = 202402 --and ma_pb = 'VNP0701100'-- and ma_nv = 'VNP017802' --and ngay_tt < add_months(trunc(sysdate, 'month'), -1)
group by thang, LOAI_TINH, ma_kpi, ma_nv, ma_to, ma_pb
order by 2
--                  )
;

commit;