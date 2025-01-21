 select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc,  least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_ktdc
                                    , case when km1.ngay_kt_mg is not null then km1.ngay_kt_mg else least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) end ngay_kt_mg
                                    , km.ngay_huy, km.NGAY_THOAI, km.tien_td, km.cuoc_dc
                                    , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
                    from v_thongtinkm_all_ol km left join (select thuebao_id, thang_bd_mg, LAST_DAY(to_date(least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_kt_mg, rkm_id, thangkm
                                                                                                        from v_thongtinkm_all_ol
                                                                                                        where thang_bd_mg <= least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                                                                                                        and (tyle_sd = 100 or tyle_tb = 100) and thang_bd_mg > 202310
                                                                                                    ) km1 on km1.thuebao_id = km.thuebao_id and km.ngay_ktdc + 1 =  to_date(km1.thang_bd_mg, 'yyyymm')
                    where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0
                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc
                                    and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                                    and km.thang_ktdc > 202310

select*from yeucau_2501
--delete
from yeucau_2501 where thang_kt  between 202402 and 202406;
commit;
with dstb as (
    select a.khachhang_id, count(a.thuebao_id) as sltb, count(distinct b.ma_tt) as sl_matt 
     ,RTRIM(XMLAGG(XMLELEMENT(E,a.ma_tb,',').EXTRACT('//text()') ORDER BY a.thuebao_id).GetClobVal(),',') AS ds_matb
--     ,LISTAGG(distinct b.ma_tt , '; ') WITHIN GROUP (ORDER BY b.thanhtoan_id) ds_ma_tt
    from css_hcm.db_Thuebao a
        left join css_hcm.db_thanhtoan b on a.thanhtoan_id = b.thanhtoan_id
        left join css_hcm.db_adsl c on a.thuebao_id = c.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and c.chuquan_id = 145
    group by a.khachhang_id 
)
;
drop table muc_1;
create table muc_1 as select* from (
select kh.ma_kh,  a.ma_tb,lh.loaihinh_tb,a.rkm_id, a.thuebao_id, a.loaitb_id, a.ngay_bddc, a.ngay_ktdc, a.ngay_kt_mg, a.ngay_Huy, a.ngay_thoai, a.tien_td, a.cuoc_dc,
    a.so_thangdc, a.so_thangkm, a.congvan_id, a.khuyenmai_id, a.chitietkm_id, a.sl_datcoc,a.nhomtb_id, a.goi_id,
    row_number() over (partition by rkm_id, nhom_Datcoc_id order by rkm_id) rnk ,
    tt.trangthai_tb,a.khachhang_id ,a.thang_kt, pb.ma_pb, pb.ten_pb, tbh.ma_to_hrm, tbh.tento, t.ma_tt, kh.so_dt, nv.ma_nv, nv.ten_nv
from yeucau_2501 a
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
    left join ttkd_bct.db_thuebao_ttkd cs on a.thuebao_id = cs.thuebao_id
    left join ttkd_bsc.dm_phongban pb on cs.pbh_ql_id = pb.pbh_id and pb.active = 1
    left join ttkd_bct.tobanhang tbh on cs.tbh_ql_id = tbh.tbh_id and tbh.hieuluc = 1 and cs.pbh_ql_id = tbh.pbh_id
    left join admin_hcm.nhanvien_onebss nv on cs.ma_nv = nv.ma_nv
    left join css_hcm.trangthai_Tb tt on a.trangthaitb_id = tt.trangthaitb_id
    left join css_Hcm.loaihinh_tb lh on a.loaitb_id = lh.loaitb_id
    left join css_hcm.db_thuebao tb on tb.thuebao_id = a.thuebao_id
    left join css_hcm.db_Thanhtoan t on tb.thanhtoan_id = t.thanhtoan_id
 ) where rnk = 1
 
;
select a.khachhang_id, a.ma_kh ,b.so_dt, c.khdn, stats_mode(a.ma_pb)ma_pb, stats_mode(a.ten_pb)ten_pb, stats_mode(a.ma_to_hrm)ma_to , stats_mode(a.tento)ten_to
,stats_mode(a.ma_nv) ma_nv,stats_mode(a.ten_nv) ten_nv, count(distinct thuebao_id) as sltb, count(distinct ma_Tt) as sltt,
LISTAGG(distinct a.thang_kt , '; ') WITHIN GROUP (ORDER BY a.thang_kt) ds_thang_kt
--,LISTAGG(distinct a.ma_tb , '; ') WITHIN GROUP (ORDER BY a.thuebao_id) ds_ma_tb
from muc_1 a
    join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
group by a.khachhang_id ,a.ma_kh,c.khdn,b.so_dt; 
select a.ma_kh ,c.khdn, a.ma_pb, a.ten_pb,a.ma_to_hrm,a.tento,a.ma_nv,a.ten_nv,
    count(distinct thuebao_id) as sltb, count(distinct ma_Tt) as sltt,  
    row_number() over (partition by a.ma_kh,a.ma_pb,a.ma_to_hrm,a.ma_nv order by a.ma_kh) rnk 
from muc_1 a
    join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
group by a.ma_kh,c.khdn,a.ma_pb, a.ten_pb,a.ma_to_hrm,a.tento,a.ma_nv,a.ten_nv;
-- gom
select a.ma_kh
from muc_1 a
    join css_hcm.db_khachhang b
group
-- yeu cau 2
select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day
;
select a.thang thang_kt, a.ma_tb, t.ma_tt,kh.ma_kh,kh.so_dt so_dt_kh, a.ngay_tt, to_date(a.thang_ktdc_cu,'yyyy/mm/dd') ngay_ktdc_cu,lh.loaihinh_tb,a.thuebao_id, a.rkm_id, a.tien_thanhtoan,  a.so_thangdc, km.ngay_bddc, km.ngay_ktdc, km.ngay_thoai, km.ngay_huy,
    pb.ma_pb, pb.ten_pb, tbh.ma_to_hrm, tbh.tento,  nv.ma_nv, nv.ten_nv
from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
    left join ttkd_bct.db_thuebao_ttkd cs on a.thuebao_id = cs.thuebao_id
    left join ttkd_bsc.dm_phongban pb on cs.pbh_ql_id = pb.pbh_id and pb.active = 1
    left join ttkd_bct.tobanhang tbh on cs.tbh_ql_id = tbh.tbh_id and tbh.hieuluc = 1 and cs.pbh_ql_id = tbh.pbh_id
    left join admin_hcm.nhanvien_onebss nv on cs.ma_nv = nv.ma_nv
    left join v_Thongtinkm_all km on a.rkm_id = km.rkm_id 
    left join css_hcm.loaihinh_tb lh on a.loaitb_id = lh.loaitb_id
    left join css_hcm.db_Thuebao tb on a.thuebao_id = tb.thuebao_id
    left join css_hcm.db_Thanhtoan t on tb.thanhtoan_id = t.thanhtoan_id
    left join css_hcm.trangthai_tb trt on tb.trangthaitb_id = trt.trangthaitb_id
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
where round(thang_ktdc_cu/100 ,0) = thang -- in (202310,202311,202312)
and ngay_tt is not null and ngay_tt + 15 < to_date(thang_ktdc_cu,'yyyymmdd');
select a.ten_ttvt, c.khdn, count(a.thuebao_id) sl_donle from tmp_donle a
    left join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    left join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
where a.ten_ttvt = 'TTVT Ch? L?n'
group by a.ten_ttvt,c.khdn;
select t