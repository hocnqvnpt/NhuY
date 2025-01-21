with quahan as 
(select thuebao_id,ma_tb,loaitb_id,khachhang_id,ngay_kt_mg, so_thangDc, cuoc_Dc, to_number(to_char(ngay_Kt_Mg,'yyyymm')) thang_kt, row_number() over (partition by thuebao_id order by ngay_kt_mg desc) rn, 0 quahan
from ds_giahan_tratruoc2
where to_number(to_char(ngay_Kt_Mg,'yyyymm')) < 202406 )
, conhan as (
select thuebao_id,ma_tb,loaitb_id,khachhang_id,ngay_kt_mg, so_thangDc, cuoc_Dc, to_number(to_char(ngay_Kt_Mg,'yyyymm')) thang_kt, row_number() over (partition by thuebao_id order by ngay_kt_mg ) rn, 1 conhan
from ds_giahan_tratruoc2
where to_number(to_char(ngay_Kt_Mg,'yyyymm')) >= 202406 )
, ds_F as (
select a.thuebao_id, a.khachhang_id, a.loaitb_id,a.ma_tb, nvl(b.ngay_kt_mg,a.ngay_kt_mg) ngay_kt_mg,nvl(b.thang_kt, a.thang_kt) thang_kt, nvl(b.so_thangdc, a.so_thangDc) so_thangdc, nvl(b.cuoc_dc,a.cuoc_dc) cuoc_dc
from quahan a left join conhan b on a.thuebao_id = b.thuebao_id 
where a.rn = 1 and nvl(b.rn,1) = 1 and a.loaitb_id in (58,59)
union all
select a.thuebao_id, a.khachhang_id,  a.loaitb_id,a.ma_tb,a.ngay_kt_mg, a.thang_kt, a.so_thangdc, a.cuoc_dc
from conhan a
where rn = 1 and thuebao_id not in (select thuebao_id from quahan)and a.loaitb_id  in (58,59))

, dstb as (
select a.thuebao_id, a.khachhang_id, a.loaitb_id,a.ma_tb, nvl(b.ngay_kt_mg,a.ngay_kt_mg) ngay_kt_mg,nvl(b.thang_kt, a.thang_kt) thang_kt, nvl(b.so_thangdc, a.so_thangDc) so_thangdc, nvl(b.cuoc_dc,a.cuoc_dc) cuoc_dc
from quahan a left join conhan b on a.thuebao_id = b.thuebao_id 
where a.rn = 1 and nvl(b.rn,1) = 1 and a.loaitb_id in (61,171,210)
union all
select a.thuebao_id, a.khachhang_id,  a.loaitb_id,a.ma_tb,a.ngay_kt_mg, a.thang_kt, a.so_thangdc, a.cuoc_dc
from conhan a
where rn = 1 and thuebao_id not in (select thuebao_id from quahan)and a.loaitb_id in (61,171,210))
,sl as (select a.THUEBAO_ID, a.KHACHHANG_ID, a.LOAITB_ID, a.MA_TB, a.NGAY_KT_MG, a.THANG_KT, a.SO_THANGDC, a.CUOC_DC
    , count(b.thuebao_id) sltb_lechky
--   , 
from ds_F a
    left join dstb b on a.khachhang_id = b.khachhang_id and a.thang_kt != b.thang_kt
group by a.THUEBAO_ID, a.KHACHHANG_ID, a.LOAITB_ID, a.MA_TB, a.NGAY_KT_MG, a.THANG_KT, a.SO_THANGDC, a.CUOC_DC
)
, ds as (
    select thuebao_id, LISTAGG( distinct ma_Tb , '; ') WITHIN GROUP (ORDER BY ma_Tb) DSTB_lechky from
    (select a.thuebao_id, b.ma_Tb, row_number() over (partition by a.thuebao_id order by b.thuebao_id) rn
    from ds_f a 
    left join dstb b on a.khachhang_id = b.khachhang_id and a.thang_kt != b.thang_kt 
    )
    where rn < 50 
    group by thuebao_id
)select sl.*, ds.dstb_lechky , c.ten_dvql ttvt_ql,  e.ten_pb phong_cs, kh.ma_kh, tt.ma_tt, lkh.khdn from sl 
    left join ds on sl.thuebao_id = ds.thuebao_id
    left join css_hcm.db_thuebao a on sl.thuebao_id = a.thuebao_id
    left join ttkd_Bct.db_Thuebao_ttkd b on sl.thuebao_id = b.thuebao_id
    left join admin_hcm.donvi c on a.donvi_id = c.donvi_id
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id
    left join css_hcm.db_khachhang kh on sl.khachhang_id = kh.khachhang_id
    left join css_hcm.db_thanhtoan tt on a.thanhtoan_id = tt.thanhtoan_id
    left join css_hcm.loai_kh lkh on kh.loaikh_id = lkh.loaikh_id
    ; 3019002
select* from css_hcm.db_Thuebao where thuebao_id = 1219394;