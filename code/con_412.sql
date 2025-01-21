select* from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202405 and ma_gd = 'HCM-TT/02812248';

select bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM,tienthoai_ghino,tien_nhapthem,TONGTIENCT_NHOM,TONGTIENHD_NHOM
    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
    where   phieu_id = 8211278;--a.phieutt_id 
    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
        having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM - 10;
        
select* from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB  where tien_ct<tien_tt and hoantat = 1;
with nsg as (;
create table dongia_bosung_t4_1 as 
select 202505 thang, loai_tinh, ma_kpi, thuebao_id, tien_dc_cu, ma_to, ma_pb, ma_nv, ma_Tb, sothang_dc, heso_chuky, heso_dichvu, dthu, ngay_tt, tien_khop, nhanvien_xuathd,
dongia, tien, tien_thuyetphuc, tien_xuathd
from nhuy.dongia_ob_final a
where not exists (select 1 from ct_dongia_temp where thang = 202404 and thuebao_id = a.thuebao_id and a.tien_khop > 0)
    and not exists (select 1 from nhuy.bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id)
--    and  a.ma_tb in (select ma_Tb from ma_tb where nguon = 'nsg');
--union all
--select 202505 thang, loai_tinh, ma_kpi, thuebao_id, tien_dc_cu, a.ma_to, a.ma_pb, a.ma_nv, ma_Tb, sothang_dc, heso_chuky, heso_dichvu, dthu, ngay_tt, tien_khop, nhanvien_xuathd,
--dongia, tien, tien_thuyetphuc, tien_xuathd
--from nhuy.dongia_ob_final a
--    join ttkd_bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv
--where not exists (select 1 from nhuy.ct_dongia_temp where thang = 202404 and thuebao_id = a.thuebao_id and a.tien_khop > 0)
--    and not exists (select 1 from nhuy.bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id) and b.ma_pb != 'VNP0701400'
    ) --select* from nsg;
 , tien as ( select ma_nv , tien_thuyetphuc
from nsg
union all
select nhanvien_xuathd, tien_xuathd
from nsg) 
select a.ma_nv, sum(tien_thuyetphuc)
from tien a 
    join ttkd_Bsc.nhanvien_202404 b on a.ma_Nv = b.ma_nv
    where b.ma_pb = 'VNP0701400'
group by a.ma_nv;, b.ten_pb;

