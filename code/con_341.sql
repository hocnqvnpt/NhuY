drop table tmp_ct_bsc;
update ttkd_Bsc.nhanvien_vttp_potmasco set thang = 202409 where thang is null;
delete from ttkd_Bsc.nhanvien_vttp_potmasco where ma_Nv is null;
commit;
begin 
    bsc_hcm_tb_giaha_022_test(20240901,20240902,5);
end;

select* from v_Thongtinkm_all where thuebao_id =7218409;  in (10748292,9427231,12034648,12283261,12268547,9076496,4488700,9848292,8979216,8261113,11804404,12163687,8111016,9427226,
12059710,9818837,9818834,9818831,8979215,2757049,9427229,9818830,12268389,7218409,8979217,9247460,11766031);
select* from css_hcm.db_Thuebao where thuebao_id = 10748292;
select* from ds_Giahan_Tratruoc2 where thuebao_id = 9247460;
select* from tmp_ct_bsc;
create table tmp_ct_bsc as
					 with t0 as (select c0.thang_kt, c0.thuebao_id, c0.phieutt_id, c0.ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
										, c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
									--    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
										, c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
										, c0.nvtuvan_id, c0.nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan
					   from TMP3_30NGAY c0
									left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
									left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
									left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
									left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
									left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
								where lan = 4
								)
						 , km0 as (   select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
													, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
													, km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
									from v_thongtinkm_all km 
									where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
													--and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
													and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
								   union all
		----------------TT giam cuoc or thang tang tren 2 dong-------------
									select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
													, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
													, km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
									from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
																													from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
																												) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
									where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
												   -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
												   and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
							 )
				, ds as (select min(ghtt_id) gh_id, donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_nv_th, donvi_oa, nhanvien_oa, nvl(duan_id, 0) duan_id, ma_nv, tbh_ql_id, pbh_ql_id
                        , ma_tb, heso, min(to_char(ngay_kt_mg, 'yyyymmdd')) thang_ktdc_cu, max(SO_THANGDC) thangdc, sum(cuoc_dc) cuoc_dc, khachhang_id, thuebao_id, loaitb_id, goi_id, thang_kt
							from ttkdhcm_ktnv.ghtt_giao_688
							where tratruoc = 1 and km = 1 and loaibo = 0 
							group by donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_tb, ma_nv_th, donvi_oa, nhanvien_oa, heso, khachhang_id, thuebao_id, loaitb_id, goi_id, duan_id, ma_nv, tbh_ql_id, pbh_ql_id, thang_kt
							)
					, c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
										, t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
										, t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
								from t0
											join km0 on t0.rkm_id = km0.rkm_id
						), goi as (select thuebao_id, nhomtb_id , row_number() over (partition by thuebao_id order by nhomtb_id desc) rn
						from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 
									   and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
							)

			select 202409 thang_kt, a.gh_id, a.pbh_ql_id, a.donvi_giao pbh_giao_id, a.tbh_giao_id
						, a.pbh_id_th, c.dvgiaophieu_id pbh_cn_id
						, a.ma_tb, a.ma_nv manv_cs, pb.ten_pb phong_cs
						 , (select ma_to_hrm from ttkd_bct.tobanhang where tbh_id = a.tbh_giao_id and a.donvi_giao = pbh_id and hieuluc = 1) ma_to
						 , (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb
						 , a.nhanvien_giao manv_giao, pb_giao.tenphong PHONG_GIAO, a.ma_nv_th, pb_th.tenphong PHONG_TH
						 , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC
						 , nvtp.ma_pb MAPB_THPHUC
						, c.manv_gt, c.manv_thungan
						 , lkh.khdn, a.heso
						, a.THANG_KTDC_CU, a.cuoc_dc TIEN_DC_CU
						, c.MA_TT, c.ma_gd, c.rkm_id, c.ngay_BD_MOI, c.so_thangdc, c.avg_thang, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT, c.ngay_nganhang
											, c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG, c.TEN_HT_TRA
						, tt.trangthai_tb, a.thuebao_id, a.loaitb_id, donvi_oa pbh_oa_id, nhanvien_oa manv_oa
						 , 
						 goi.nhomtb_id
						, a.khachhang_id, a.goi_id goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id

						, case when c.rkm_id is null then null
										when c.ht_tra_id in (1, 7,204) then 1
										when c.ht_tra_id in (2, 4,5,207,214) then 0 else null end tien_khop
						, (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu

			from ds a
										join css_hcm.db_thuebao dbtb
											on a.thuebao_id = dbtb.thuebao_id
										join css_hcm.db_khachhang dbkh
												on dbtb.khachhang_id = dbkh.khachhang_id
										join css_hcm.loai_kh lkh
												on dbkh.loaikh_id = lkh.loaikh_id
										join css_hcm.trangthai_tb tt
											on dbtb.trangthaitb_id = tt.trangthaitb_id
										left join ttkd_bsc.dm_phongban pb
											on a.pbh_ql_id = pb.pbh_id and pb.active = 1

										left join  ttkd_bct.phongbanhang pb_giao
											on a.donvi_giao = pb_giao.pbh_id
										left join  ttkd_bct.phongbanhang pb_th
											on a.pbh_id_th = pb_th.pbh_id
										left join c 
											on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt
										left join ttkd_bsc.nhanvien nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.donvi = 'TTKD' and nvtp.thang =202408
										left join goi on a.thuebao_id = goi.thuebao_id and rn = 1

			where  a.thang_kt = 202408  ;
            select* from css_hcm.phieutt_hd where ma_gd like '%HCM-000%';