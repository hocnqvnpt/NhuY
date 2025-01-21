-- 60 ngay
CREATE TABLE RMP_BSC_PHONG AS;
select* from RMP_BSC_PHONG where thang = 202412 and ma_pb = 'VNP0702300';
commit;
delete from RMP_BSC_PHONG where ten_kpi ='T? l? thuy?t ph?c khách hàng GHTT TC tháng T+1 ( D?ch v? Fiber, MyTV, Mesh)' and thang = 202410;
select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_027' AND MA_NV IN ('VNP017658','VNP022082');
UPDATE TTKD_bSC.bangluong_kpi SET GIAO = GIAO+152,THUCHIEN = THUCHIEN+38, TYLE_tHUCHIEN = ROUND((THUCHIEN+38)*100/(GIAO+152),2)
where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_027' AND MA_NV IN ('VNP017658','VNP022082');
COMMIT;
select* from rmp_bsc_phong where thang = 202412 and ten_kpi like 'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)%';
update rmp_bsc_phong set thang = -202412 where thang = 202412 and ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)';
delete from RMP_BSC_PHONG where thang = 202412 and ma_pb not in  ('VNP0702300','VNP0702400','VNP0702500')
and ten_kpi ='Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)';
commit;
insert into RMP_BSC_PHONG;
--- 3 phong doanh nghiep
select thang, ma_pb, phong_giao, case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
ma_kpi ,'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null, null,null
from 
(
        select thang, ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_1tr a
                                        where thang = 202412 and ma_pb is not null
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
);
commit;
insert into RMP_BSC_PHONG
--- 9 phong khu vuc
select thang, ma_pb, phong_giao, case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_10' else 'HCM_KHDN_BSC_07' end
ma_kpi ,'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)_BHKV' ten_kpi, tong, da_giahan, tyle, null, null,null
from 
(
        select thang, ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when  tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 4) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi a
                                        where thang = 202412 and ma_pb is not null  and ma_pb not in ('VNP0702300','VNP0702400','VNP0702500')
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
);
union all;
create table tpm as;
commit;
update rmp_bsc_phong set thang = 2024102 where ten_kpi  ='T? l? thuy?t ph?c khách hàng GHTT TC tháng T ( D?ch v? Fiber, MyTV, Mesh)' and thang = 202410 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500');
-- 30 ngay
update rmp_bsc_phong set thang = -202412 where thang = 202412 and ten_kpi ='Ty le thuyet phuc khách hàng GHTT TC tháng T ( Dich vu Fiber, MyTV, Mesh)';
insert into rmp_bsc_phong
select thang, ma_pb, phong_giao, case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_11' else 'HCM_KHDN_BSC_08' end
ma_kpi ,'Ty le thuyet phuc khách hàng GHTT TC tháng T ( Dich vu Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan, tyle, null,null,null
from 
(
          select thang, ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when  tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                        where thang = 202412 --and ma_pb in ('VNP0702300','VNP0702400','VNP0702500')
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
);
--- BHOL
insert into rmp_bsc_phong(THANG, MA_PB, phong_Giao, TEN_KPI, tong, da_giahan, TYLE)
select distinct thang, ma_pb, ten_pb, ten_kpi, giao, thuchien, tyle_thuchien
from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi='HCM_TB_GIAHA_022' and ma_pb ='VNP0703000';
select distinct thuebao_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
where thang = 202410 and ma_pb = 'VNP0702400' and tien_khop is not null;  ('VNP0702300','VNP0702400','VNP0702500') 
union all;
commit;
rollback;
delete from rmp_bsc_phong where thang = 202410 and ma_pb is null;
update rmp_bsc_phong set tong = 223++59, da_giahan = 109
 where thang = 202410 and phong_Giao ='Phòng Khách Hàng Doanh Nghi?p 1' 
and ten_kpi ='T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? VNPT CA-IVAN)';

update rmp_bsc_phong set tyle =round(da_Giahan*100/tong,2)
 where thang = 202410 and phong_Giao ='Phòng Khách Hàng Doanh Nghi?p 1' 
and ten_kpi ='T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? VNPT CA-IVAN)';
update rmp_bsc_phong set;
update  rmp_bsc_phong set thang = 2024101 where thang = 202410 and ten_kpi ='T? l? thuy?t ph?c khách hàng GHTT TC tháng T+1 ( D?ch v? Fiber, MyTV, Mesh)';
commit;
insert into tpm
delete from rmp_bsc_phong where ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? VNPT CA-IVAN)';
select* from rmp_bsc_phong where thang = 202410 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? VNPT CA-IVAN)'; where thang in (-202410, 202410) and 

update rmp_bsc_phong set thang = 2024102 where thang = 202410 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? VNPT CA-IVAN)';
delete rmp_bsc_phong where thang = 202410 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? VNPT CA-IVAN)';
select * from rmp_bsc_phong where thang = -202412 and ten_kpi ='Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)';
update  rmp_bsc_phong set thang = -202412 where thang = 202412 and ten_kpi ='Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)';
commit;
--- CA
insert into rmp_bsc_phong (thang,MA_PB, PHONG_GIAO, MA_KPI, TEN_KPI, GIAO, THUCHIEN, TYLE,mdht)
--commit;
select 202412 thang, ma_pb, phong_giao, case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_12' else 'HCM_KHDN_BSC_09' end
ma_kpi , 'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)' ten_kpi, tong, da_giahan, tyle, null
from 
(
select thang, ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_Bsc.ct_bsc_giahan_cntt a
                                          where thang_ktdc_cu = 202412 and thang = 202412 and loaitb_id  not in (147,148) and ma_pb is not null --in (55 ,80 ,116 ,117,132,140,154,181,288,318 )
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
)
;
commit;
union all
;
update rmp_bsc_phong a set giao_truoc_loaitru = (select giao from rmp_bsc_phong where a.ten_kpi = ten_kpi and a.ma_pb = ma_pb and thang = 20241218)
            ,tyle_Truoc_loaitru =  (select tyle from rmp_bsc_phong where a.ten_kpi = ten_kpi and a.ma_pb = ma_pb and thang = 20241218)
where thang = 202412 and ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)';

commit;
select* from rmp_bsc_phong where thang in ( 202412) and ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)';
update rmp_bsc_phong set giao = 165+152, thuchien = 74+38, tyle = round( (74+38)*100/(165+152),2)
where thang in ( 202412) and ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)' and ma_pb = 'VNP0702300';
rollback;
commit;
insert into ct_Bsc_giahan_Cntt select* from ttkd_Bsc.ct_Bsc_giahan_Cntt where thang = 202412;
delete from ct_Bsc_giahan_Cntt a
--    join css_Hcm.db_khachhang b on a.khachhang_id = b.khachhang_id 
    where thang = 202412 and thuebao_id  in (select thuebao_id from ttkd_Bsc.ct_Bsc_giahan_Cntt where thang = 202412);
update ct_Bsc_giahan_Cntt a set phong_giao = (select distinct phong_giao from ct_Bsc_giahan_Cntt where thang = a.thang and ma_pb =a.ma_pb and phong_Giao is not null) where phong_Giao is null;
update rmp_bsc_phong set thang = 2024101 where ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? Tên mi?n)' and thang = 202410;
commit;
select* from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202412 and ma_pb= 'VNP0702400' and ma_to ='VNP0702409' and loaitb_id not in (147,148);
-- TEN MIEN
insert into rmp_bsc_phong(thang,MA_PB, PHONG_GIAO, MA_KPI, TEN_KPI, GIAO, THUCHIEN, TYLE,mdht)
select sum(tyle) from (
select thang, ma_pb, phong_giao, case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_20' else 'HCM_KHDN_BSC_18' end
ma_kpi , 'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu Tên mien)' ten_kpi, tong, da_giahan, tyle, null
from 
(
select 202412 thang, ma_pb, phong_giao
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_giahan_cntt a
                                          where thang_ktdc_cu = 202412 and loaitb_id in (147,148 ) and thang = 202412 and ma_pb is not null
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
));
commit;
delete
rmp_bsc_phong where thang = 202410 and ma_pb is null; and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T ( D?ch v? Fiber, MyTV, Mesh)' and phong_giao like '%Khu%';
-- phieu ton
union all ;
commit;
update rmp_bsc_phong set tyle= 14.84
where thang = 202410 and ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh' and ma_pb = 'VNP0702400';
insert into rmp_bsc_phong
select 202412 THANG, MA_PB,  PHONG_GIAO, MA_KPI, TEN_KPI, 0 TONG, thuchien, 0 TYLE, 5 MDHT, null,null
from rmp_bsc_phong where thang = 202411 and ten_kpi like '%t?n%'; = 'Ty le phieu ton dich vu Bang rong chua xu lý cuoi ky thuoc trách nhiem cua kinh doanh';--and ma_pb = 'VNP0702500';
insert into rmp_bsc_phong(THANG, MA_PB, PHONG_GIAO, MA_KPI, TEN_KPI,  mdht)
select 202410 thang, ma_pb, ten_pb,case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_21' else 'HCM_KHDN_BSC_19' end
ma_kpi ,'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh' ten_kpi, mucdo_hoanthanh
from ttkd_Bsc.bangluong_kpi where thang= 202412 and ma_kpi = 'HCM_CL_TONDV_003' and mucdo_hoanthanh is not null;
union all
select distinct 202410 thang, ma_pb, ten_pb,case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_21' else 'HCM_KHDN_BSC_19' end
ma_kpi ,'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh' ten_kpi, null, null, 5
from ttkd_Bsc.nhanvien where thang = 202410 and ma_pb = 'VNP0702200'
union all
select distinct 202410 thang, ma_pb, ten_pb,case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_21' else 'HCM_KHDN_BSC_19' end
ma_kpi ,'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh' ten_kpi, null, null, 5
from ttkd_Bsc.nhanvien where thang = 202410 and ma_pb = 'VNP0702300'
union all ;
commit;
rollback;
select sum(thuchien) from rmp_bsc_phong where thang = 202412 and ten_kpi = 'PTM thuê bao gói Home Sành/Chat';
commit;
-- HOME 
insert into rmp_bsc_phong
--update rmp_bsc_phong set thang = -202412 where thang = 202412 and ten_kpi ='PTM thuê bao gói Home Sành/Chat';
select 202412 thang, ma_pb, ten_pb,case when ma_pb not in ('VNP0702300','VNP0702400','VNP0702500') then 'HCM_BHKV_BSC_09' else 'HCM_BHOL_BSC_06' end
ma_kpi ,'PTM thuê bao gói Home Sành/Chat' ten_kpi, null,  soluong_goi,null, null, null,null
from
(
   select a.thang, a.ma_pb, pb.ten_pb, count(distinct a.ma_Tb) soluong_goi from ttkd_Bsc.ct_Bsc_homecombo a
             join ttkd_bsc.dm_phongban pb on a.ma_pb = pb.ma_pb and pb.active = 1
   where thang = 202412 and loai_kpi in ('Fiber_hh','Fiber_moi')   and a.ma_pb not in ('VNP0700400','VNP0700600')
   group by a.thang, a.ma_pb , pb.ten_pb
)
;
commit;
update  rmp_bsc_phong set da_giahan = tyle, tyle = null where thang =202410 and ten_kpi ='PTM thuê bao gói Home Sành/Ch?t';and  ten_nv like '%Cúc Ph??ng%';
;
SELECT* FROM rmp_bsc_phong;
select* from ttkd_Bsc.dongia_vttp 
    where loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')  ;
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and MA_KPI = 'HCM_TB_GIAHA_024' and ma_nv ='VNP017743';
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202410 and ma_to = 'VNP0702311' and ma_kpi = 'HCM_TB_GIAHA_024';
alter table rmp_bsc_phong add mdht number;
INSERT INTO rmp_bsc_phong(THANG, MA_PB, TEN_KPI,TONG,DA_GIAHAN, TYLE, MDHT)
select THANG, MA_PB,'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh', TONG,DA_GIAHAN_DUNG_hEN, TYLE, 
CASE WHEN TYLE < 1 THEN 0 ELSE -TYLE END
from ttkd_bsc.tl_giahan_tratruoc where thang = 202410 and ma_kpi = 'HCM_CL_TONDV_003';
update rmp_bsc_phong set mdht = -5 where  mdht < -5 and thang = 202410;
select* from rmp_bsc_phong where  mdht < -5 and thang = 202410;
rollback;
insert into rmp_bsc_phong
select THANG,MA_PB, 'Phòng Khách Hàng Doanh Nghi?p 3' PHONG_GIAO, MA_KPI, TEN_KPI, TONG, DA_GIAHAN,-1.82 TYLE
from rmp_bsc_phong  where ma_pb = 'VNP0702300' and ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh';
commit;
select* from ttkd_Bsc.dm_phongban where active = 1 and ten_pb like 'Phòng Bán Hàng Khu V?c%';
UPDATE rmp_bsc_phong A SET ma_kpi = (SELECT DISTINCT ma_kpi FROM TTKD_bSC.NHANVIEN WHERE MA_PB = A.MA_PB and ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh')
WHERE THANG = 202410 and ma_kpi is null;
delete from rmp_bsc_phong where thang = 202410 and ma_pb is null;
update rmp_bsc_phong set mdht = tyle where ma_pb not in('VNP0702400','VNP0702500','VNP0702300') and ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh';