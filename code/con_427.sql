drop table thd;

create table thd as
select b.thuebao_id, b.ma_Tb, kieuld_id, loaihd_id, e.ten_dv, dv2.ten_dv dv_rp,
        row_number() over (partition by  b.thuebao_id, b.ma_Tb, kieuld_id, loaihd_id order by  b.hdtb_id desc) rn
    from css.v_hd_khachhang@dataguard a
        join css.V_hd_thuebao b on a.hdkh_id = b.hdkh_id
        left join admin_hcm.nhanvien_onebss c on a.ctv_id = c.nhanvien_id
        left join admin_Hcm.donvi d on c.donvi_id = d.donvi_id
        left join admin_hcm.donvi e on d.donvi_cha_id = e.donvi_id
        left join admin_hcm.donvi dv1 on a.donvi_id = dv1.donvi_id
        left join admin_hcm.donvi dv2 on dv1.donvi_cha_id = dv2.donvi_id
    where b.tthd_id  = 6
;
with thd as 
(
    select THUEBAO_ID, MA_TB, KIEULD_ID, LOAIHD_ID, TEN_DV, DV_RP, HDTB_ID, 
    row_number() over (partition by  thuebao_id, ma_Tb, kieuld_id, loaihd_id order by  hdtb_id desc) rn 
    from hd
)
select a.*, nvl(hd.ten_dv, dv_rp) ten_dv
from xgspon a
    join thd hd on a.ma_tb = hd.ma_Tb and a.loaihd_id = hd.loaihd_id and a.kieuld_id = hd.kieuld_id and rn = 1;
    select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202409 and loai_tinh = 'DONGIATRA_OB' and tien_khop = 1 and ma_pb = 'VNP0703000';
;
select* from ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day where thang = 202410 and tien_khop = 1; and tien_khop is null and phieutt_id is not null;
update ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day set tien_khop = 1 where thang = 202410 and tien_khop is null and phieutt_id is not null
and ht_Tra_id = 216;
commit;

select a.ma_Tb,a.mapb_ql,a.ma_nv, ma_nv_am , a.TBH_QL_ID , A.PBH_QL_ID , D.TENTO,  E.TEN_PB, b.thang_ktdc_Cu, nv.ten_nv
from ttkd_bct.db_Thuebao_ttkd_202409 a
    join ttkd_Bsc.ct_bsc_tratruoc_moi_30day b on a.thuebao_id = b.thuebao_id and b.thang = 202410
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d 
        on d.tbh_id = A.tbh_ql_id and A.pbh_ql_id = d.pbh_id 
     left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e
        on e.pbh_id = A.pbh_ql_id
    left join admin_hcm.nhanvien nv on a.ma_Nv = nv.ma_nv --and nv.thang = 202409
where a.loaitb_id = 58 and a.mapb_ql  in ('VNP0702300','VNP0702400','VNP0702500');
and  D.TENTO != nv.ten_to;
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' and giao is null 
and ma_nv in (select * from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang = 202410 and upper(ten_kpi) like '%HOME%' );
select * from ttkd_bct.ID372_GIAO_C2_CHOTTHANG where thang = 202410;
update ttkd_Bsc.bangluong_kpi set thuchien = (select