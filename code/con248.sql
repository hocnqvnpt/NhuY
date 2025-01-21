create table nv_capnhat as 
select chungtu_id,ma_gd,ma_ct,tien_tt,nhanvien_tt, nhanvien_id
,(select ma_nv  from admin_hcm.nhanvien where nhanvien_id=x.nhanvien_cn) nhanvien_cn--, x.nhanvien_cn
,ngay_tt,thuchien
from
(select a.chungtu_id,a.ma ma_gd
,(select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id=a.chungtu_id) ma_ct,a.tongtra tien_tt
,(select ma_nv from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) nhanvien_tt, nhanvien_id
,(select nhanvien_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
    
    and ghilog_id in (select max(ghilog_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
                      where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
                        and timeinsert in (select max(timeinsert) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
                                            where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)))) nhanvien_cn
,to_char(a.ngay_tt,'dd/mm/yyyy') ngay_tt
,decode(a.bosung,1,'nhancong','xuatfile') thuchien
from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd a --4991
where  (EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = a.chungtu_id and hoantat=1)
or EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where chungtu_id = a.chungtu_id and hoantat=1))
and a.tratruoc = 1) x;
select a.NHANVIEN_XUATHD, b.nhanvien_tt from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt a 
join NV_CAPNHAT b on a.ma_gd = b.ma_gd
where a.NHANVIEN_XUATHD = b.nhanvien_tt;
select* from NV_CAPNHAT;
select ngay_insert from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where sochungtu = 	186852;

select ma_Gd from NV_CAPNHAT group by ma_Gd having count(ma_gd) > 1;
select* from nv_capnhat where ma_gd = 'HCM-LD/01682054';
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where ma_Gd ='HCM-LD/01682054';