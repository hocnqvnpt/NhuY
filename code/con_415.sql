	select a.chungtu_id,a.ma ma_gd
					,(select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id=a.chungtu_id) ma_ct,a.tongtra tien_tt
					,(select ma_nv || ' - ' || ten_nv from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) nhanvien_tt
					,(select nhanvien_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
					    
					    and ghilog_id in (select max(ghilog_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
									  where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
									    and timeinsert in (select max(timeinsert) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
													    where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)))) nhanvien_cn
					,to_char(a.ngay_tt,'dd/mm/yyyy') ngay_tt
					,decode(a.bosung,1,'nhancong','xuatfile') thuchien
from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a --4991
where  (EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = a.chungtu_id and hoantat=1)
						or EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where chungtu_id = a.chungtu_id and hoantat=1))
						and a.tratruoc = 1
			and to_number(to_char(a.ngay_tt,'yyyymm')) = 202406
			;and nvl(a.bosung, 999) <> 1
			and exists (select 1 from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ND_CT = a.ma)
	;