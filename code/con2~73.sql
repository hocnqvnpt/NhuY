update  ct_Bsc_Chungtu_temp a
set tinh_dongia = 0, tinh_bsc = 0
where exists (select distinct chungtu_id, ma from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc 
where to_char(ngay_ht,'yyyymm') = '202410' and tudong = 1 and a.chungtu_id = chungtu_id and nvl(a.ma_Gd,a.ma_tt)= ma);
select count(distinct chungtu_id ) 
from ct_Bsc_Chungtu where thang < 202410 and chungtu_id in (select chungtu_id from xx_chungtu_202410  where ghi_Chu is not null);
insert into nhuy.ct_Bsc_Chungtu_temp ct (MA_CHUNGTU, MA_TT, TRA_TRUOC, GHIcHU, CHUNGTU_ID, NGAY_TT, TIEN_ChungTu,tinh_bsc,tinh_dongia, MA_NV, nv_Gach)
select ma_Ct ma_Chungtu,ma ma_Tt,tratruoc,ghi_chu,chungtu_id, to_char(ngay_Ht,'yyyy/mm/dd') ngay_tt,tongtra tien_ct,1,1, ma_Nv,ma_Nv
from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a
join admin_Hcm.nguoidung b on a.nguoi_Cn = b.ma_nd
join admin_Hcm.nhanvien c on b.nhanvien_id = c.nhanvien_id
 where ghi_chu is not null and traTruoc = 0;
 delete from nhuy.ct_Bsc_Chungtu_temp ct where ghichu ='bo sung theo ra soat PNVC' and tra_Truoc = 0;
 rollback;
commit;
select* from ct_Bsc_Chungtu_temp where ghichu is not null;