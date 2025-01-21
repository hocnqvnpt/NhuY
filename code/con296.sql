select a.*,thungan_tt_id, c.nhanvien_id from ttkd_bct.bangiao_chungtu_tinhbsc a
    left join css_hcm.phieutt_hd b on a.ma = b.ma_gd
    left join css_Hcm.hd_khachhang c on b.hdkh_id = c.hdkh_id
where thang = 202410 and tratruoc = 1 and nhanvien_xl != thungan_tt_id ;
select* from ttkd_Bsc.blkpi_danhmuc_kpi where thang = 202410;
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day;
create table line as
select a.ma_Tb,a.mapb_ql,a.ma_nv, ma_nv_am , a.TBH_QL_ID , A.PBH_QL_ID ,d.ma_to_hrm ma_to, D.TENTO,  E.TEN_PB, b.thang_ktdc_Cu, nv.ten_nv
from ttkd_bct.db_Thuebao_ttkd_202409 a
    join ttkd_Bsc.ct_bsc_tratruoc_moi_30day b on a.thuebao_id = b.thuebao_id and b.thang = 202410
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d 
        on d.tbh_id = A.tbh_ql_id and A.pbh_ql_id = d.pbh_id 
     left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e
        on e.pbh_id = A.pbh_ql_id
    left join admin_hcm.nhanvien nv on a.ma_Nv = nv.ma_nv --and nv.thang = 202409
where a.mapb_ql  in ('VNP0702300','VNP0702400','VNP0702500');
update  ttkd_Bsc.ct_bsc_tratruoc_moi_30day a
set manv_cs = (select distinct ma_nv from line where ma_Tb = a.ma_Tb)
,  ma_to = (select distinct ma_to from line where ma_tb = a.ma_tb)
where thang = 202410;
select thuebao_id from line group by thuebao_id having count(distinct ma_to) > 1;
commit;
select* from  ttkd_Bsc.ct_bsc_tratruoc_moi_30day a where a.ma_pb  in ('VNP0702300','VNP0702400','VNP0702500') and thang = 202410;

select* from ttkdhcm_ktnv.ds_chungtu_nganhang_Sub_oneb where chungtu_id =282578;
select* from ttkd_bct.bangiao_chungtu_tinhbsc where ghi_chu is not null  ;
select* from ct_Bsc_chungtu where chungtu_id in (select chungtu_id from ttkd_bct.bangiao_chungtu_tinhbsc where ghi_chu is not null );
select distinct ten_vtcv, ten_to, ten_pb, ma_Vtcv, ma_to, ma_pb 
from ttkd_Bsc.nhanvien where donvi = 'TTKD' And thang= 202411;