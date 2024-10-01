--- Ngay quet:
---Lan 1: ngay 2 chot het ngay 1 la ngay tt ma GD doi voi hinh thuc tra khac CK
---Lan 2: ngay 5 chot het ngay 4 la ngay tt ma GD va ngay ngan hang phai la ngay cuoi thang 30/31 (do con xu ly chung tu nhan cong) doi voi hinh thuc tra la CK
---Ngay thanh toan:
---Het ngay cuoi thang doi voi ngay ngan hang (chung tu)
---Het ngay 1 doi voi cac hinh thuc con lai (do ngay 1 moi hoan cong cac hop dong doi toc do)
---GHTC: doi voi congvan_id = 190 (tra truoc tru dan) xet tien AVG thang moi >= AVG thang cu
---Mỗi năm backup 1 bảng lưu trữ ttkd_bsc.ct_bsc_tratruoc_moi_30day_20xx
drop table tmp3_30ngay;
commit;
select * from css_hcm.khuyenmai_dbtb
;
delete from ct_Bsc_tratruoc_moi_30day where thang in (202408,202409);

---tao PROCEDURE
create or replace procedure bsc_hcm_tb_giaha_022_test
    (ngay_bd_tt number, ngay_kt_tt number, ins number)
    AUTHID CURRENT_USER
as
    thang_Bsc number(6);
    sql_query varchar2(4000);
    sql_query2 varchar2(4000);
    sql_query3 varchar2(4000);
    

begin 
	-- ins > 1: insert them vao bang tmp
    select to_number(to_char(sysdate,'yyyymm')-1) into thang_bsc from dual;
	if ins > 1 then	
		insert into tmp3_30ngay
		with hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
		from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id)
				, gh as (select khachhang_id, thuebao_id, duan_id, ma_tb, ma_tt, loaitb_id, thang_kt , row_number() over (partition by thuebao_id order by thuebao_id desc) rnk
								from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt =  thang_bsc   )       ---change n-1
				, kmtb as (select * from css_hcm.khuyenmai_dbtb 
									where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)

				, kq_ghtt as (select  gh.khachhang_id, gh.thuebao_id, gh.duan_id, gh.ma_tb, gh.ma_tt, gh.loaitb_id, gh.thang_kt, ins as lan
                                    , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                    , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                                    , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                                    , a.phieutt_id, a.trangthai
                                    , a.ma_gd, a.ngay_hd, a.ngay_tt, null, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                    , kt.kenhthu
                                    , nh.ten_nh ten_nganhang
                                    , ht.ht_tra ten_ht_tra
                                    , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id
                                    , hdkh.nhanviengt_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                             from css_hcm.phieutt_hd a
                                                join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                                left join hddc on b.hdtb_id = hddc.hdtb_id
                                                join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                                join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                                                join gh on hdtb.thuebao_id = gh.thuebao_id and rnk = 1
                                                left join kmtb on b.hdtb_id = kmtb.hdtb_id
                                                left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                                left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                                left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
                             where a.kenhthu_id not in (6) and a.trangthai = 1
                                            and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between ngay_bd_Tt and ngay_kt_tt                 ----change--3 thang- ngay 2
																	 
										)
				select * from kq_ghtt a
				where  a.ngay_bd_moi is NOT null 
									and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi where rkm_id = a.rkm_id and thang >=thang_Bsc-1)
									and not exists (select 1 from ttkdhcm_ktnv.ghtt_giao_688 where a.rkm_id = rkm_id and thang_kt = a.thang_kt and tratruoc =1 and loaibo =0)
							;
            commit;
            -- tao bang tmp moi 
		elsif  ins = 1 then
            EXECUTE IMMEDIATE 'drop table tmp3_30ngay';
            sql_query := '
			create table tmp3_30ngay as
					with hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
					from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id)
				, gh as (select khachhang_id, thuebao_id, duan_id, ma_tb, ma_tt, loaitb_id, thang_kt , row_number() over (partition by thuebao_id order by thuebao_id desc) rnk
								from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt =  thang_bsc   )       ---change n-1
				, kmtb as (select * from css_hcm.khuyenmai_dbtb 
									where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)

				, kq_ghtt as (select  gh.khachhang_id, gh.thuebao_id, gh.duan_id, gh.ma_tb, gh.ma_tt, gh.loaitb_id, gh.thang_kt, ins as lan
														, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
														, to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), ''yyyymmdd'')) ngay_bd_moi
														, to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), ''yyyymmdd'')) ngay_kt_moi
														, a.phieutt_id, a.trangthai
														, a.ma_gd, a.ngay_hd, a.ngay_tt, null, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
														, kt.kenhthu
														, nh.ten_nh ten_nganhang
														, ht.ht_tra ten_ht_tra
														, b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
										 from css_hcm.phieutt_hd a
															join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
															left join hddc on b.hdtb_id = hddc.hdtb_id
															join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
															join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
															join gh on hdtb.thuebao_id = gh.thuebao_id and rnk = 1
															left join kmtb on b.hdtb_id = kmtb.hdtb_id
															left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
															left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
															left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
										 where a.kenhthu_id not in (6) and a.trangthai = 1
														and to_number(to_char(a.ngay_tt, ''yyyymmdd'')) between ngay_bd_Tt and ngay_kt_tt                 ----change--3 thang- ngay 2
																	 
										)
				select * from kq_ghtt a
				where  a.ngay_bd_moi is NOT null 
									and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi where rkm_id = a.rkm_id and thang >='||thang_Bsc-1||')
									and not exists (select 1 from ttkdhcm_ktnv.ghtt_giao_688 where a.rkm_id = rkm_id and thang_kt = a.thang_kt and tratruoc =1 and loaibo =0)
							';
                      dbms_output.put_line(sql_query);
                EXECUTE IMMEDIATE sql_query;
            end if;
            
--			--- tao bang trung gian
			SQL_QUERY := 'create table tmp_ct_bsc as
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
								where lan = ' || ins	|| '
								)
						 , km0 as (  ';
										----------------TT Thang tang tren 1 dong-------------
						sql_query3:= 'select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
													, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
													, km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
									from v_thongtinkm_all km 
									where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
													--and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), ''yyyymm''),0),''yyyymm''))  ---cong 2 thang
													and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL ''50'' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL ''50'' YEAR)) >= ngay_bddc + 90
								   union all
		----------------TT giam cuoc or thang tang tren 2 dong-------------
									select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
													, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
													, km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
									from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
																													from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
																												) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
									where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
												   -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), ''yyyymm''),0),''yyyymm''))
												   and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL ''50'' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL ''50'' YEAR)) >= ngay_bddc + 90
							 )
				, ds as (select min(ghtt_id) gh_id, donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_nv_th, donvi_oa, nhanvien_oa, nvl(duan_id, 0) duan_id, ma_nv, tbh_ql_id, pbh_ql_id
                        , ma_tb, heso, min(to_char(ngay_kt_mg, ''yyyymmdd'')) thang_ktdc_cu, max(SO_THANGDC) thangdc, sum(cuoc_dc) cuoc_dc, khachhang_id, thuebao_id, loaitb_id, goi_id, thang_kt
							from ttkdhcm_ktnv.ghtt_giao_688
							where tratruoc = 1 and km = 1 and loaibo = 0 
							group by donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_tb, ma_nv_th, donvi_oa, nhanvien_oa, heso, khachhang_id, thuebao_id, loaitb_id, goi_id, duan_id, ma_nv, tbh_ql_id, pbh_ql_id, thang_kt
							)
					, c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
										, t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
										, t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
								from t0
											join km0 on t0.rkm_id = km0.rkm_id
						)';
				sql_query2:= ', goi as (select thuebao_id, nhomtb_id , row_number() over (partition by thuebao_id order by nhomtb_id desc) rn
						from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 
									   and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
							)
                        
			select  202409 thang_kt, a.gh_id, a.pbh_ql_id, a.donvi_giao pbh_giao_id, a.tbh_giao_id
						, a.pbh_id_th, c.dvgiaophieu_id pbh_cn_id
						, a.ma_tb, a.ma_nv manv_cs, pb.ten_pb phong_cs
						 , (select ma_to_hrm from ttkd_bct.tobanhang where tbh_id = a.tbh_giao_id and a.donvi_giao = pbh_id and hieuluc = 1) ma_to
						 , (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb
						 , a.nhanvien_giao manv_giao, pb_giao.tenphong PHONG_GIAO, a.ma_nv_th, pb_th.tenphong PHONG_TH
						 , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, ''khongco'') MANV_THUYETPHUC
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
						, (select listagg(MA_CAPNHAT, '', '') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
						
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
										left join ttkd_bsc.nhanvien nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.donvi = ''TTKD'' and nvtp.thang ='|| thang_bsc ||'
										left join goi on a.thuebao_id = goi.thuebao_id and rn = 1
								
			where  a.thang_kt = ' ||thang_bsc ||'  ';
--            EXECUTE IMMEDIATE sql_query  ||' '|| sql_query3||''|| sql_query2;
			commit;
              dbms_output.put_line('my_var');
            dbms_output.put_line(sql_query  ||' '|| sql_query3||''|| sql_query2);
--            insert into ct_bsc_tratruoc_moi_30day
--		(THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO
--        , MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU
--        , MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG
--        , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
--        )
--		select* from tmp_ct_bsc;
		commit;
--		EXECUTE IMMEDIATE 'drop table tmp_ct_bsc';
        delete from ct_bsc_tratruoc_moi_30day where thang = 202409 and rkm_id is null and thuebao_id in (select thuebao_id from tmp3_30ngay where lan = ins);
		commit;
        update ct_bsc_tratruoc_moi_30day a set tien_khop = 1
        -- select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
        where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = thang_bsc+1 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 308)
        and exists 
            (
                select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
                from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                                join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                where   bb.hoantat = 1 and aa.ma_gd = a.ma_gd 
        --                                    group by aa.chungtu_id 
            );
-- phieu con co tien bang
        update ct_bsc_tratruoc_moi_30day a set tien_khop = 1
        -- select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
        where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = thang_bsc+1 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 308)
        and exists 
            (
                select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
                from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                                join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                                join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
                where   aa.tien_sub_ct >=  (cc.tien+cc.vat) and aa.ma_gd = a.ma_gd 
        --                                    group by aa.chungtu_id 
            );
-- bang cha du tien
        update ct_bsc_tratruoc_moi_30day a set tien_khop = 1
        -- select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
        where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = thang_bsc+1 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 308)
        and exists 
            (
                select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
                from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                                join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                                 join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
                where   bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
        --        and   aa.tien_sub_ct <  (cc.tien+cc.vat)--and aa.hoantat = 0
        --                                    group by aa.chungtu_id 
            );
        commit;
		--- chay bang tinh BSC cho giam doc
         delete from tl_giahan_tratruoc where thang = thang_Bsc+1;

    insert into tl_giahan_tratruoc (thang,ma_kpi, loai_tinh, ma_pb, tong,da_giahan_dung_hen,dthu_duytri, dthu_thanhcong_dung_hen, tyle)
        select thang,'HCM_TB_GIAHA_022','KPI_PB', ma_pb, tong,da_giahan,dthu_duytri, dthu_thanhcong_dung_hen, tyle
        from (
        --****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                        select thang, ma_pb, phong_giao
                                 , count(thuebao_id) tong
                                 , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                                 , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                 , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                        from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                                     , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                                 from ct_bsc_tratruoc_moi_30day a
                                                where thang = 202409
                                                group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                                       )
                        group by thang, ma_pb, phong_giao
                        order by 2
                        ) a  
                ;
                
        commit;
end bsc_hcm_tb_giaha_022_test;


begin 
    bsc_hcm_tb_giaha_022_test(20240901,20240902,5);
end;
select* from tmp3_30ngay where lan = 4;
select* from ct_bsc_tratruoc_moi_30day where thang = 202409
select* from ttkd_Bsc.nhanvien where thang = 202409 and ten_nv like '%Tuy?t%';
select* from tl_giahan_Tratruoc where thang = 202409;
-----1-------------Danh sach tham gia han tra truoc--------------PL1(BH, Dai)

select * from ttkdhcm_ktnv.DM_MVIEW ;
    ---Buoc 1--tao danh sach hong dong tra truoc trong tgian quy dinh
create table tmp3_60ngay as;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb;
select distinct ht_Tra_id from tmp3_60ngay;
--insert into ghtt_null_ngay_bddc
--insert into tmp3_60ngay
create table tmp3_30ngay as
--insert into tmp3_30ngay
with hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id)
, gh as (select khachhang_id, thuebao_id, duan_id, ma_tb, ma_tt, loaitb_id, thang_kt , row_number() over (partition by thuebao_id order by thuebao_id desc) rnk
                from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202404)             ---change n-1
, kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                                    group by bb.phieu_id)
--, ct2 as (select min(aa.ngay_insert) ngay_insert, bb.phieu_id
--         from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb aa
--            join ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd bb on aa.chungtu_id = bb.chungtu_id
--            group by bb.phieu_id
--)
, kq_ghtt as (select  gh.khachhang_id, gh.thuebao_id, gh.duan_id, gh.ma_tb, gh.ma_tt, gh.loaitb_id, gh.thang_kt, 22 as lan
                        --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                                        , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                        , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                                        , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                                        , a.phieutt_id, a.trangthai
                                        , a.ma_gd, a.ngay_hd, a.ngay_tt, ct.ngay_nganhang, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                        , kt.kenhthu
                                        , nh.ten_nh ten_nganhang
                                        , ht.ht_tra ten_ht_tra
                                        , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                         from css_hcm.phieutt_hd a
                                            join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                            left join hddc on b.hdtb_id = hddc.hdtb_id
                                            join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                            join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                                            join gh on hdtb.thuebao_id = gh.thuebao_id and rnk = 1
                                            left join kmtb on b.hdtb_id = kmtb.hdtb_id
                                            left join ct on a.phieutt_id = ct.phieu_id
                                            left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                            left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
--                                            left join ct2 on a.phieutt_id = ct2.phieu_id
                         where a.kenhthu_id not in (6) and a.trangthai = 1
                                        and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20240301 and 20240602                  ----change--3 thang- ngay 2
                                                                 ----change--3 thang- ngay 2
                           --             and gh.ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')   ----loai taykhi can---
                                  --   and gh.ma_tb like 'nutifood%'
                                    --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                    --and a.ma_gd = 'HCM-TT/02290172'
                        )
select * from kq_ghtt a
where  a.ngay_bd_moi is NOT null 
                    and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi where rkm_id = a.rkm_id and thang >=202404)
                    and not exists (select 1 from ttkdhcm_ktnv.ghtt_giao_688 where a.rkm_id = rkm_id and thang_kt = a.thang_kt and tratruoc =1 and loaibo =0)
                  and not exists (select rkm_id from tmp3_60ngay where rkm_id = a.rkm_id) 
            ;
            select rkm_id from tmp3_ts group by rkm_id having count(rkm_id) > 1
            select ngay_tt , ma_gd from css_hcm.phieutt_hd where phieutt_id  = 8278166
--     and a.ma_gd  not like 'HCM-LD%'
--    and not exists (select thuebao_id from v_thongtinkm_all where cuoc_dc >0 and thuebao_id = a.thuebao_id group by thuebao_id having count(*)=1 )
    --and thuebao_id = 7172624
--    and ma_tb = 'hcm_thanhv_19'
;
commit;
----delete coc da thoai
select rkm_id || ',', thuebao_id || ',', ma_tb from ttkd_bsc.ct_bsc_tratruoc_moi v where thang = 202308 and exists (select 1 from css_hcm.db_datcoc where v.rkm_id = rkm_id and ngay_thoai>= '01/07/2023' and ngay_thoai - ngay_bddc <1)
                                                                                                                                            and exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi  where thang = 202308 and rkm_id is not null and thuebao_id = v.thuebao_id group by thuebao_id having count(*)>1);
select count(*) from all_tab_columns where owner='NHUY' and table_name='TMP_OB';

-----2------------Chi tiet ket qua tham gia tra truoc theo ky---------------
--drop table bsc_tratruoc_201804 purge;
update ttkd_bsc.ct_bsc_tratruoc_moi set MANV_GIAO = '01956' where thang = 202011 and ma_tb = 'fibervnn-1138856';
---Kiem tra tien dat coc cu---
    select count(distinct thuebao_id) sluong, round(sum(cuoc_dc/1.1), 2) 
                from ttkdhcm_ktnv.ghtt_giao_688
                    where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202308 ; 53389	67629935178.18
    ;
    select round(sum(cuoc_dc/1.1), 2) from(
    select distinct thuebao_id, cuoc_dc from ttkdhcm_ktnv.ghtt_giao_688
                    where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202308); 67629935178.18

    ;
    select count(distinct thuebao_id) from (
     select * from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202309
     );-- 42469
    group by thuebao_id;
    
    select sum(TIEN_THANHTOAN) from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202112;31743455991 
    commit;
    rollback;
--End---
select * from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0  and thang_kt = 202308 and ma_tb in ('cmc_bdh2'); and duan_id in (345, 1382) and THANGDC <3; and length(nhanvien_giao) >10; -- chi Huyen huong dan lay file giao
select *   from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202310; and ma_tb in ('sumitex05tdt', 'sumitex_vip100', 'sumitex_fiber70');
    and nvl(rkm_id, 0)  not in (select rkm_id from tmp3); and ma_tb in ('soqt70') ; and ma_gd is null;, 'ttm781', 'nhudao_70', 'thanhhiencc642', 'tho_200918', 'ztech61'); and rkm_id = 3369696; and thuebao_id in (select thuebao_id from ttkd_bct.moi_giahan_tratruoc_moi where kycuoc = 201912 and tratruoc = 1 and loaibo = 1);
select * from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202308; and manv_giao is null
;

select *   from tmp3 a where 
     rkm_id not in (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202307 and a.thuebao_id = thuebao_id)
    ;
--truncate table hocnq_ttkd.ct_bsc_tratruoc_moi;
delete from nhuy.ct_bsc_tratruoc_moi WHERE thang = 202312;
commit;
    SELECT rkm_id FROM nhuy.ct_bsc_tratruoc_moi_30day WHERE thang = 202401 
    group by rkm_id having count(rkm_id) > 1;--and tien_thanhtoan < 20000;rkm_id is not null and ngay_tt <='05/09/2023';
-- kiem tra trung 
SELECT * FROM ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202404 and tien_khop is  null and ht_Tra_id is not null
select rkm_id from tmp3_ts group by rkm_id having count(rkm_id) > 1
update nhuy.ct_bsc_tratruoc_moi set tien_khop = null where thuebao_id in (4568396,9710800,9350934,9434808,9195836);
---Buoc 2-----tao ds chot
insert into  ttkd_bsc.ct_bsc_tratruoc_moi
--  insert into nhuy.ct_bsc_tratruoc_moi
        (THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO
        , MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU
        , MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG
        , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
        )
 --   create table ct_bsc_tratruoc_moi as
with t0 as (select c0.thang_kt, c0.thuebao_id, c0.phieutt_id, c0.ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                                                , c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                                                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                                                , c0.nvtuvan_id, c0.nhanviengt_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan
                                               from tmp3_60ngay c0
                                                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                                                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                                                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                                                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nhanviengt_id
                                                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
--                                                        where lan = 22
                    )
, km1 as (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
         from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100 and thang_bddc > 202307
                                                                                        )
, km0 as (  
    
----------------TT Thang tang tren 1 dong-------------5813343
--5865861               
                select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                                , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                from v_thongtinkm_all km 
                where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
                                --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                                and thang_bddc > 202310
               union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
                
                select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                from v_thongtinkm_all km left join km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
                               -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                               and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                               and km.thang_bddc > 202307
--         ) km on c0.rkm_id = km.rkm_id 
                                                        --     and not(km.so_thangdc < 3 and c0.khachhang_id <> 2796008     --2-----ket qua tra truoc >= 3T except ma_kh HCM000971574 PBHKVCL yc eO 899817
                                                        --                        and km.so_thangdc < 3 and nvl(c0.duan_id, 0) not in (345, 1382))     --2-----ket qua tra truoc >= 3T xcept 2 ma_duan Tancang, Vinhome PBHKVSG yc eO 944829
                                                        
                )
, ds as (select min(ghtt_id) gh_id, donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_nv_th, donvi_oa, nhanvien_oa, nvl(duan_id, 0) duan_id, ma_nv, tbh_ql_id, pbh_ql_id
                            , ma_tb, heso, min(to_char(ngay_kt_mg, 'yyyymmdd')) thang_ktdc_cu, max(SO_THANGDC) thangdc, sum(cuoc_dc) cuoc_dc, khachhang_id, thuebao_id, loaitb_id, goi_id, thang_kt
                from ttkdhcm_ktnv.ghtt_giao_688
                where tratruoc = 1 and km = 1 and loaibo = 0 
                group by donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_tb, ma_nv_th, donvi_oa, nhanvien_oa, heso, khachhang_id, thuebao_id, loaitb_id, goi_id, duan_id, ma_nv, tbh_ql_id, pbh_ql_id, thang_kt
                )
, c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                            , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nhanviengt_id, t0.thungan_tt_id
                            , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
                    from t0
                                join km0 on t0.rkm_id = km0.rkm_id
            )
, goi as (select thuebao_id, nhomtb_id , row_number() over (partition by thuebao_id order by nhomtb_id desc) rn
                from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id 
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                    )
select 202408 thang, a.gh_id, a.pbh_ql_id, a.donvi_giao pbh_giao_id, a.tbh_giao_id
            , a.pbh_id_th, c.dvgiaophieu_id pbh_cn_id
            , a.ma_tb, a.ma_nv manv_cs, pb.ten_pb phong_cs
            , (select ma_to_hrm from ttkd_bct.tobanhang where tbh_id = a.tbh_giao_id and a.donvi_giao = pbh_id and hieuluc = 1) ma_to
            , (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb
             , a.nhanvien_giao manv_giao, pb_giao.tenphong PHONG_GIAO, a.ma_nv_th, pb_th.tenphong PHONG_TH
             , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC
             
             , nvtp.ma_pb MAPB_THPHUC
             , c.manv_gt, c.manv_thungan
             --, a.kq_popup    ----1: gd, 2:ck,3: tainha  --> Dai Thanh Cong
             , lkh.khdn, a.heso
             
             , a.THANG_KTDC_CU, a.cuoc_dc TIEN_DC_CU
             , c.MA_TT, c.ma_gd, c.rkm_id, c.ngay_BD_MOI, c.so_thangdc, c.avg_thang, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT, c.ngay_nganhang
                                , c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG, c.TEN_HT_TRA
             , tt.trangthai_tb, a.thuebao_id, a.loaitb_id, donvi_oa pbh_oa_id, nhanvien_oa manv_oa
               ,  goi.nhomtb_id
              , a.khachhang_id, a.goi_id goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id
              --, null tien_khop
              , case when c.rkm_id is null then null
                            when c.ht_tra_id in (1, 7,204) then 1
                            when c.ht_tra_id in (2, 4,5, 207,214) then 0 else null end tien_khop
            , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
 
from ds a
                 --           left join ttkd_bct.db_thuebao_ttkd db on a.thuebao_id = db.thuebao_id and db.trangthaitb_id is not null     ---n-1---
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
                            left join c on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt 
                           left join ttkd_bsc.nhanvien nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.thang =  202408
                           left join goi on a.thuebao_id = goi.thuebao_id and goi.rn = 1
                                    -- join km on c.rkm_id = km.rkm_id 
          /*                  
                            left join (select c0.thang_kt, c0.thuebao_id, c0.phieutt_id, c0.ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                                                , c0.ngay_tt, c0.ngay_hd, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                                                                , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc, c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                                                , c0.nvtuvan_id, c0.nhanviengt_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_gt, nv2.ma_nv manv_thuyetphuc
                                               from hocnq_ttkd.tmp3 c0
                                                            left join admin_hcm.nhanvien nv on nv.nhanvien_id = c0.nvgiaophieu_id
                                                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                                                            left join admin_hcm.nhanvien nv1 on nv1.nhanvien_id = c0.nhanviengt_id
                                                            left join admin_hcm.nhanvien nv2 on nv2.nhanvien_id = c0.nvtuvan_id
                                                            join (
                                                                        ----------------TT Thang tang tren 1 dong-------------5813343
--5865861
                                                                                                        select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                                                                                                        , km.tien_td, km.cuoc_dc
                                                                                                                        , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                                                                                                        from v_thongtinkm_all km 
                                                                                                        where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0
                                                                                                                        --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                                                                                                        and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                                                                                                       union all
                                                                            ----------------TT giam cuoc or thang tang tren 2 dong-------------
                                                                                                        select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                                                                                                        , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc
                                                                                                                        , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                                                                                                        from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                                                                                                                                                                        from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                                                                                                                                                                    ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                                                                                                        where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0
                                                                                                                       -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                                                                                                                       and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                                                                    ) km on c0.rkm_id = km.rkm_id 
                                                                                                    --     and not(km.so_thangdc < 3 and c0.khachhang_id <> 2796008     --2-----ket qua tra truoc >= 3T except ma_kh HCM000971574 PBHKVCL yc eO 899817
                                                                                                    --                        and km.so_thangdc < 3 and nvl(c0.duan_id, 0) not in (345, 1382))     --2-----ket qua tra truoc >= 3T xcept 2 ma_duan Tancang, Vinhome PBHKVSG yc eO 944829
                                                        ) c on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt 
                               */                                                            
where  a.thang_kt = 202407 --and rkm_id != 3369696 --a.ma_tb  in ('')
            /*    and not (a.thangdc < 3 and nvl(c.so_thangdc, 1) < 3 ---khong tinh so giao chu ky old < 3 va chu ky new <3 
                                            and not (a.khachhang_id = 2796008 and a.donvi_giao = 4)             ---except ma_kh HCM000971574 PBHKVCL yc eO 899817
                                    )
                and not (a.thangdc < 3 and nvl(c.so_thangdc, 1) < 3 ---khong tinh so giao chu ky old < 3 va chu ky new <3 
                                            and not (a.duan_id in (345, 1382) and a.donvi_giao = 5)              ---except 2 ma_duan Tancang, Vinhome PBHKVSG yc eO 944829
                                    ) 
         */     --   and a.thangdc < 3 and c.so_thangdc>= 3 
          --     and a.ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')
          --     and a.thuebao_id in (9020326)
          --    and a.ma_tb like 'nutifood%'
      ;         and exists (select * from tmp3_60ngay where lan = 22 and thuebao_id = a.thuebao_id )
             --   and rownum < 3
--select * from css_hcm.toanha where duan_id in (345, 1382)
; 
select rkm_id from tmp3_60ngay
rollback;
select rkm_id from ttkd_Bsc.ct_bsc_tratruoc_moi where thang  = 202403 group by rkm_id having count(rkm_id) > 1
select * from ct_bsc_tratruoc_moi where thang  = 202312 and rkm_id is not null--group by rkm_id having count(rkm_id) > 1
select distinct ht_tra_id from  ct_bsc_tratruoc_moi where thang  = 202312;
commit;
select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202408 group by rkm_id having count(rkm_id) > 1;
select* from bcss_hcm.ctkmtc_int_20230901
---Buoc 3---Check and except tra truoc khong b_30dayang cuoc PS
    create table ct_bsc_tratruoc_moi_trudan as;
    select * from ct_bsc_tratruoc_moi_trudan where thang = 202305;
    insert into ct_bsc_tratruoc_moi_trudan --(CONGVAN_ID, KHUYENMAI_ID, CHITIETKM_ID, TEN_KM, CUOC_DC, TIEN_TD, CPS, THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_QL, PHONG_QL, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO, MANV_TH, PHONG_TH, MA_NV_CN, MANV_THUYETPHUC, KQ_POPUP, KQ_DVI_CN, PHONG_CN, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_HD, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, MANV_TCTN, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID)
                select km.CONGVAN_ID, KHUYENMAI_ID, CHITIETKM_ID, TEN_KM, cuoc_dc, round(tien_td/1.1, 0) tien_td, COALESCE(ps, ps_m, ps_htv) cps_Tn
                            , THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, a.MA_TB, MANV_QL, PHONG_QL, MA_TO, MA_PB, MANV_GIAO
                            , PHONG_GIAO, MANV_TH, PHONG_TH, MA_NV_CN, MANV_THUYETPHUC, KQ_POPUP, KQ_DVI_CN, PHONG_CN, THANG_KTDC_CU, TIEN_DC_CU
                            , MA_TT, MA_GD, a.RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_HD, SOSERI, SERI, KENHTHU
                            , TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, a.THUEBAO_ID, a.LOAITB_ID, MANV_TCTN, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID
                from ttkd_bsc.ct_bsc_tratruoc_moi a left join v_thongtinkm_all km on a.rkm_id = km.rkm_id
                          --      left join (select * from ttkd_bct.moi_giahan_tratruoc_moi where kycuoc = 202301 and tratruoc = 1 and loaibo != 1) dc on a.gh_id = gh_id
                            left join (select ma_tb, sum(tien) ps from bcss_hcm.ctkmtc_int_20231201 where khoanmuctc_id not in (521, 421, 4067, 453)group by ma_tb
                                                ) ps on a.ma_tb = ps.ma_tb and a.loaitb_id in (58, 59)---thang n
                            left join (select ma_tb, sum(tien) ps_m from bcss_hcm.ctkmtc_mytv_20230901 where khoanmuctc_id not in (521, 4639, 1431)group by ma_tb
                                                ) ps_m on a.ma_tb = ps_m.ma_tb and a.loaitb_id in (61)---thang n
                             left join (select ma_tb, sum(tien) ps_htv from bcss_hcm.ctkmtc_htv_20230901 where khoanmuctc_id not in (521)group by ma_tb
                                                ) ps_htv on a.ma_tb = ps_htv.ma_tb and a.loaitb_id in (18)---thang n
                  
                where a.thang = 202310 and congvan_id = 190 and nhom_datcoc_id in (1) and TIEN_DC_CU <> TIEN_THANHTOAN + VAT
                                ---tien tru dan >= tien ps thang n là OK
                                and TIEN_TD   < COALESCE (ps, ps_m, ps_htv, 9999999) 
                                ---import < 20k
                           --     and a.ma_tb in ('hcm_thuhien0719')
                                and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi where thang = a.thang and TIEN_DC_CU = TIEN_THANHTOAN + VAT and thuebao_id = a.thuebao_id)
                            --    and exists (select 1 from qltn_hcm.ct_no_01042023 where ma_tb = a.ma_tb group by ma_tb, dichvuvt_id having sum(decode(sign(chukyno-20230401), 0, nogoc, 0)) >0)
                order by 6
    ;
        select rkm_id from  ct_bsc_tratruoc_moi a  where thang = 202312  group by rkm_id having count(rkm_id) > 1
commit;
    select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202404 and thuebao_id in (select thuebao_id from tmp3_30ngay where lan != 5)
--    update 
--    set tien_khop = 0 
    where thang = 202312  and TIEN_THANHTOAN < 10000
    -- DO QUA BANG CHINH 
    insert into ttkd_bsc.ct_bsc_tratruoc_moi 
    select* from ct_bsc_tratruoc_moi where thang = 202401
    -- 
    commit;
    commit;
    ;
        update ttkd_bsc.ct_bsc_tratruoc_moi a set tien_khop = null
        where a.thang = 202310 and rkm_id in (select rkm_id from ct_bsc_tratruoc_moi_trudan where thang = a.thang)
           
           ;
------Check trung record-----
        select * from v_thongtinkm_all 
        where cuoc_dc > 0 and khuyenmai_id not in (1977, 9038, 9037, 2056, 2150, 8731) and thang_bddc <= least(nvl(thang_huy, 99999999), nvl(thang_kt_dc, 9999999999))
        and 202006 between thang_bddc and thang_ktdc  and thuebao_id in (8150994)
        ;
-------Update ma_hrm cho mnv_giao
select distinct ma_chungtu, ht_Tra_id, phieutt_id, ma_gd from  ttkd_bsc.ct_bsc_tratruoc_moi a  where thang = 202403 and tien_khop = 0; and ma_tb = 'hcmtuananh1';

select* from ttkd_bsc.ct_bsc_tratruoc_moi a where thang = 202403 and ht_tra_id in (204,7) -- and tien_khop = 0 and exists (select 1 from css_hcm.phieutt_hd where phieutt_id = a.phieutt_id
and trangthai != 1)
update ttkd_bsc.ct_bsc_tratruoc_moi a
    set a.ma_to = (select ma_to from ttkd_bsc.nhanvien_202309 where a.manv_giao = ma_nv)
            , a.TBH_GIAO_ID = 312
-- select * from ttkd_bsc.ct_bsc_tratruoc_moi a
where thang = 202309 and exists (select * from ttkd_bsc.nhanvien_202309 where a.manv_giao = ma_nv)
                and a.manv_giao in ('CTV048677') and ma_to = 'VNP0702215'
;    
 --ktra khac ma_to
                select a.manv_giao, a.ma_to to_old, a.ma_pb pbold, b.* from ttkd_bsc.ct_bsc_tratruoc_moi a join ttkd_bsc.nhanvien_202309 b on a.manv_giao = b.ma_nv
                where a.ma_to <> b.ma_to and thang = 202309; 
       select sum(tien_thanhtoan) from ttkd_bsc.ct_bsc_Tratruoc_moi where thang = 202311       
    --ktra 1 manv nhieu ma_to
                 select manv_giao from ttkd_bsc.ct_bsc_tratruoc_moi 
                where thang = 202309 group by manv_giao having count(distinct nvl(MA_TO, 'a')) >1
;
--------------Thong ke BSC gia han tra truoc theo Phong--------------
--*********C.1.1, C.1.2 (DAI, KTTT) Ty le thue bao ghtt khong thanh cong giao Dai thuyet phuc
        select pbh_giao_id, phong_giao
                      , count(*) tong
                      , sum(case when dthu > 0 then 0 else 1 end) khong_giahan
                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                      , sum(dthu) DTHU_thanhcong, round(sum(case when dthu > 0 then 0 else 1 end)*100/count(thuebao_id), 2) tyle
        from  (select thuebao_id, ma_tb, pbh_oa_id pbh_giao_id, 'Phong Ban Hang Online' phong_giao, tien_dc_cu, sum(tien_thanhtoan) DTHU
                        from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202112 and pbh_oa_id in (29)
                        group by thuebao_id, ma_tb, pbh_oa_id, tien_dc_cu
                        union all
                                    select thuebao_id, ma_tb, pbh_giao_id, bo_dau(phong_giao) phong_giao
                                                , tien_dc_cu, sum(tien_thanhtoan) DTHU
                                    from ttkd_bsc.ct_bsc_tratruoc_moi 
                                    where thang = 202112 and pbh_giao_id in (29) and nvl(pbh_oa_id, 0) not in (29)
                                    group by thuebao_id, ma_tb, pbh_giao_id, phong_giao, tien_dc_cu
                        ) a
        group by pbh_giao_id, phong_giao
        order by 2
        
;
        --****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao quan ly, cskh---
                        select ma_pb, phong_ql
                                      , count(thuebao_id) tong
                                      , sum(case when dthu > 0 then 0 else 1 end) khong_giahan
                                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                      , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 then 0 else 1 end)*100/count(thuebao_id), 2) tyle
                        from        (select thang, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.ma_to, a.thuebao_id, a.ma_pb, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                                from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                where a.thang = 202112                                                                                          -------change n------------
                                                group by thang, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                                            )
                            group by phong_ql, ma_pb
                        order by 2
;            
    --khong thu hien from T11*********I.4.2 BSC Dai thuyet phuc thanh cong giu lai de tiep tuc
        select pbh_giao_id, phong_giao
                      , count(*) tong
                      , count(case when dthu > 0 then 1 end) da_giahan
                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                      , sum(dthu) DTHU_thanhcong
        from (select thuebao_id, ma_tb, pbh_giao_id, phong_giao, tien_dc_cu, sum(tien_thanhtoan) DTHU
                    from 
                            (
                                select  a.thuebao_id, a.ma_tb, a.pbh_giao_id, a.phong_giao, a.tien_dc_cu, tien_thanhtoan, nvl2(d.rnk,1, 0) Dai_CN, nvl2(e1.gh_id, 1, 0) Chuyen_KV
                                from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                                        left join (select gh_id, max(idno) rnk from TTKD_BCT.ds_ketqua_capnhat_dai group by gh_id) d on a.gh_id = d.gh_id   
                                                                        left join (select gh_id, max(TIENTRINH_GHTT_ID) rnk from TTKD_BCT.tientrinh_ghtt where congviec_id != 2 group by gh_id) e on e.gh_id = d.gh_id
                                                                        left join TTKD_BCT.tientrinh_ghtt e1 on e.rnk = e1.TIENTRINH_GHTT_ID and e1.donvi_id not in (14, 17, 18, 26, 29)
                                   where  a.thang = 202010 and a.pbh_giao_id in (17, 18)
                       ) 
                       where Chuyen_KV = 0
                       group by thuebao_id, ma_tb, pbh_giao_id, phong_giao, tien_dc_cu
                )
        group by pbh_giao_id, phong_giao
        order by 2
;


        --khong thuc hien 202110****C.7.1(BHKV), C.1.1(KHDN) Ty le doanh thu duy tri tren tap KH thuoc BHKV, BHDN quan ly, cskh---
        select pbh_ql_id, ma_pb, phong_ql
                      , count(thuebao_id) tong
                      , sum(case when dthu > 0 then 1 else 0 end) da_giahan_dung_hen
                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                      , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(dthu)*100/sum(tien_dc_cu/1.1), 2) tyle
        from        (select thang, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.ma_to, a.thuebao_id, a.ma_pb, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                            from ttkd_bsc.ct_bsc_tratruoc_moi a
                            where a.thang = 202109                                                                                          -------change n------------
                            group by thang, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                            )
            group by pbh_ql_id, phong_ql, ma_pb
        order by 3
        
;            

    --khong thuc hien tu T6****C.7.2(BHKV), C.1.2(BHDN) Ty le thue bao donvi thuyet phuc from Dai thuyet phuc khong thanh cong + BHKV&BHDN tu thuc hien--
     insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI
                , MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE);
        select a.thang, 'KPI_PB' LOAI_TINH, 'HCM_DT_GIAHA_006' MA_KPI, b.ma_pb
                    , a.phong_giao                                                                    ---change yes/no
                       , count(thuebao_id) tong
                      , sum(case when dthu > 0 then 1 else 0 end) da_giahan
                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                      , sum(dthu) DTHU_thanhcong, round(sum(case when dthu > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
      from       (select a.thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.thuebao_id, a.ma_tb, a.tien_dc_cu, sum(a.tien_thanhtoan) DTHU, 0 dai_id
                                                from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                where a.pbh_giao_id not in (17, 18, 26, 29)                         
                                                group by a.thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.thuebao_id, a.ma_tb, a.tien_dc_cu
                                                ----danh sach giao BHKV, BHDN tu thuyet phuc---
                                                union all
                                                ----danh sach Dai, Don vi ngoai va POPup thuyet phuc khong TC----
                                                select a.thang, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.thuebao_id, a.ma_tb, a.tien_dc_cu, sum(a.tien_thanhtoan) DTHU, 1
                                                from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                where a.pbh_giao_id in (17, 18, 26, 29) and  a.kq_dvi_cn =0 and a.kq_popup not in (1, 2, 3) ----change------
                                                group by a.thang, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.thuebao_id, a.ma_tb, a.tien_dc_cu
                                                ) a --on zi.manv_hrm = a.manv_giao and a.thang = bi.thang
                            left join ttkd_bsc.dm_phongban b on a.pbh_giao_id = b.pbh_id and active = 1
        where a.thang = 202105
       group by a.thang, b.ma_pb, a.phong_giao
        order by 5
;
        --khong thu hien from T11--****C.7.3(BHKV), C.1.3(BHDN) BSC giao don vi ban dau kq= 2-----
        select pbh_giao_id, phong_giao
                       , count(case when kq = 2 then 1 end) tong
                      , count(case when kq = 2 and dthu > 0 then 1 end) da_giahan
                      , round(sum(case when kq = 2 then tien_dc_cu/1.1 end), 0) DTHU_DUYTRI
                      , sum(case when kq = 2 and dthu > 0 then dthu end) DTHU_thanhcong
      from (
                    select a.pbh_ql_id, a.phong_ql, a.manv_ql, a.ma_to, a.ma_pb, a.ma_tb, a.tien_dc_cu, sum(a.tien_thanhtoan) DTHU
                                                                                            , case 
                                                                                                            when a.pbh_giao_id not in (17, 18) then 2                                        --Giao cac don vi kq = 2---
                                                                                                            when c.kq_th is null and a.kq_popup not in (1, 2, 3) then 0                         --Giao Dai, Dai ko thuyet phuc, ko popup xem nhu Dai hok thuye phuc----
                                                                                                                       when c.kq_th is null and a.kq_popup in (1, 2, 3) then 1           --Giao Dai, Dai ko thuyet phuc, KH su dung popup xem nhu Dai thuyet phuc thanh cong-----
                                                                                                            else c.kq_th end kq, c.kq_th, a.kq_popup, a.pbh_giao_id, a.phong_giao            --Giao Dai, can cu ket qua cap nhat Dai-----
                    from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                                                                    left join (select b.gh_id, b.idno, b.kq_th, b.ngay_hen, b.pbh_id_cn
                                                                                                                        from 
                                                                                                                                        ( select max (idno)  idno_dai from ttkd_bct.ds_ketqua_capnhat_dai where pbh_id_cn in (17,18) 
                                                                                                                                         group by gh_id) a
                                                                                                                                         , ttkd_bct.ds_ketqua_capnhat_dai b
                                                                                                                        where a.idno_dai = b.idno) c on a.gh_id = c.gh_id 
                                                                                           --      left join ttkd_bct.moi_giahan_tratruoc_moi aa on a.gh_id = aa.gh_id and aa.kycuoc = 202008                        ----change------
                    where a.thang = 202010 and (a.pbh_giao_id not in (17, 18) or (a.pbh_giao_id in (17, 18) and (c.pbh_id_cn in (17, 18) or c.pbh_id_cn is null)) ) ----change------
                    group by a.pbh_giao_id, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.ma_to, a.ma_pb, a.ma_tb, a.tien_dc_cu, c.kq_th, a.kq_popup, a.phong_giao
                    )
        where pbh_giao_id not in (17, 18)
        group by pbh_giao_id, phong_giao
        order by 2
;
commit;
rollback;
-------Buoc 4--------Thong ke BSC gia han tra truoc theo Nhan vien Phong-------------------
        select *  from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and ma_kpi  in ('HCM_TB_GIAHA_023') and ma_nv = 'VNP020227' ;
        select distinct ma_kpi from ttkd_bsc.tl_giahan_tratruoc a where thang = 202305; and not exists (select ma_nv from TTKD_BSC.bangluong_kpi_201910 where ma_nv_hrm = a.ma_nv);
        select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_CL_PTTBB_005, HCM_DT_GIAHA_002 from TTKD_BSC.bangluong_kpi_201910 where HCM_CL_PTTBB_005 is not null or HCM_DT_GIAHA_002 is not null ;
        select * from ttkd_bct.phongbanhang;
        select * from TTKD_BCT.moi_giahan_tratruoc_moi where kycuoc = 201910 and tratruoc = 1;
        select * from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202308; and ma_tb = 'hcmkieutrangfm'; VNP027554
       delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202309 and ma_kpi in ('HCM_TB_GIAHA_023');
      ;
--------1------C4---Tyle thue bao khong gia han tren Tap khach hang het han tra truoc giao BHKV, BHDN thuyet phuc------------HCM_TB_GIAHA_013, HCM_TB_GIAHA_017
                            ----khong tinh so giao thang_dc <3 ---khong ap dung he so quy doi vb 411 BSC
                            ----T04 ap dung kiem tra chung tu doi voi hinh thuc chuyen khoan
        insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                         , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)

        select thang, 'KPI_NV' LOAI_TINH
                     , case when ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_023'
                                    when ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_023'
                                else null
                        end ma_kpi
                     , a.ma_nv, a.ma_to, a.ma_pb
                       , count(thuebao_id) tong
                      , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                      , sum(dthu) DTHU_thanhcong
                      , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle, sum(heso_giao) heso
                      
        from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                                            , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                                            , sum(a.tien_khop), heso_giao
                                            from ttkd_bsc.ct_bsc_tratruoc_moi a
                                            where a.thang = 202312  --    and a.manv_giao = 'VNP020227'              ------------n------------
                                            group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, heso_giao
                            ) a 
      where ma_pb is not null 
      group by a.thang, a.ma_nv, a.ma_to, a.ma_pb
        order by 2
;      ---khong thuc hien tu T7------tinh nhan vien quan ly dia ban giao ban dau thuyet phuc thanh cong, khong tinh To truong, LDP----------HCM_TB_GIAHA_015
                ----khong tinh so giao thang_dc <3
                ----sogiao = IF Tong tbao giao <= 385, 385*90%, vice versa, IF Tong tbao giao > 385, tong tbao giao * 90%
                ----sothuchien = IF Tong tbao giao > 385 and Sothuchien > so giao da tinh, Sothuchien * 1.1
                ----Ap dung he so quy doi so thuc hien vb 411 BSC, khong ap dung voi so giao
                
        insert into ttkd_bsc.tl_giahan_tratruoc
                select thang, 'KPI_QLDB' LOAI_TINH
                                , 'HCM_TB_GIAHA_015' ma_kpi
                                , a.ma_nv, ma_to, ma_pb
                               , case when sum(heso) <= 385 then round(385 * 0.9, 0)
                                            when sum(heso) > 385 then round(sum(heso) * 0.9, 0) else 0
                               end tong
                              
                              , round(case when sum(heso) > 385 and sum(case when dthu > 0 and tien_khop > 0 then heso else 0 end) > round(sum(heso) * 0.9, 0) 
                                                                                            then sum(case when dthu > 0 and tien_khop > 0 then heso else 0 end) * 1.1
                                            else sum(case when dthu > 0 and tien_khop > 0  then heso else 0 end)
                              end, 0) da_giahan
                              , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                              , sum(dthu) DTHU_thanhcong
                              , null tyle, null dongia, null
                from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                                         , case ----Fiber tinh he so 1 (neu khong duy tri goi dadv -0.5), MyTV tinh he so 0.5 (neu co Fiber cung ky 0.4), Mesh tinh he so 0.5 (neu co Fiber cung ky 0.2) 
                                                                                                when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                                                                                                                * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                                                                                                        , 0)

                                                                                                ----Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                                                                                when a.loaitb_id in(210) then 0.5 - nvl(0.3* (select distinct 1 from ttkd_bsc.ct_bsc_tratruoc_moi xu
                                                                                                                                                                                            where xu.loaitb_id in (58, 59)
                                                                                                                                                                                                            and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                                                                                ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                                                                                when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from ttkd_bsc.ct_bsc_tratruoc_moi xu
                                                                                                                                                                                            where xu.loaitb_id in (58, 59)
                                                                                                                                                                                                            and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                                                                            else 0 
                                                                                end Heso
                                                                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                                        from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                        where a.thang = 202306                    ------------n------------
                                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, loaitb_id, khachhang_id, goi_old_id, nhomtb_id
                                    ) a 
                            
              where ma_pb is not null and ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
              group by a.thang, a.ma_nv, ma_to, ma_pb
            order by 2
;
        

;
---------------Chay binh quan To, Phong -----
    select  * from ttkd_bsc.tl_giahan_tratruoc where thang = 202310  and MA_KPI in ('HCM_TB_GIAHA_023'); and loai_tinh = 'KPI_PB';
  --  delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202304; and loai_tinh = 'KPI_TO' and MA_KPI in ('HCM_TB_GIAHA_015','HCM_TB_GIAHA_013', 'HCM_TB_GIAHA_017');
    select ma_kpi, ma_nv, count(ma_to) from ttkd_bsc.tl_giahan_tratruoc where thang = 202309 group by ma_kpi, ma_nv having count(nvl(ma_to, 'a'))>1;
    insert into hocnq_ttkd.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                            , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
                    select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                        , round( sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                                        , sum(heso_giao) heso
                    from hocnq_ttkd.tl_giahan_tratruoc
                    where thang = 202310 and MA_KPI in ('HCM_TB_GIAHA_023')
                    group by THANG, MA_KPI, MA_TO, MA_PB
    ;
     insert into hocnq_ttkd.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                            , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
                        select a.THANG, 'KPI_PB', a.MA_KPI, b.ma_nv, null, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                            , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                                            , sum(heso_giao) heso
                        from hocnq_ttkd.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd@dhsxkd where thang = 202310) b on a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
                        where a.thang = 202310 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_023')
                        group by a.THANG, a.MA_KPI, b.ma_nv, a.MA_PB
    ;
commit;
rollback;
    --Buoc 5--Update nhan vien bang KPI----
    update TTKD_BSC.bangluong_kpi_202309 a set 
    HCM_TB_GIAHA_023 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc@ttkddb where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_023')
    where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_023') and thang_kt is null and MA_VTCV = a.MA_VTCV)
                  
    ;
    ---------------Ty le cua To truong -----
    update TTKD_BSC.bangluong_kpi_202309 a set 
                                                                                                       HCM_TB_GIAHA_023 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc@ttkddb where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_023')
                                                                                                     --     HCM_TB_GIAHA_017 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_017')
    where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_023')
                                                            and thang_kt is null and MA_VTCV = a.MA_VTCV)
                                                     
    ;
commit;

/*kiem tra
rollback;
        update TTKD_BSC.bangluong_kpi_202309 a set HCM_TB_GIAHA_023 = null ;
                                                                                                           
        select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202201; and loai_tinh ='KPI_TO'; and ma_kpi = 'HCM_DT_GIAHA_004' ;
        select * from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202112; and manv_ql = 'VNP017703';
        select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202007 and ((LOAI_TINH = 'KPI_TO' and ma_kpi in ('HCM_DT_GIAHA_004')) or (LOAI_TINH = 'KPI_PB' and ma_kpi in ('HCM_DT_GIAHA_006')));
        select * from  TTKD_BSC.bangluong_kpi_202208;
*/  
        select *--MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_TB_GIAHA_023
        from TTKD_BSC.bangluong_kpi_202310@dhsxkd where HCM_TB_GIAHA_023  is not null --HCM_TB_GIAHA_015  is not null  or HCM_TB_GIAHA_017 is not null
                                                                                                          ; 
                                                                                                            
                                                                                                  
        


            ---chi tiet dongia chi tra gui Nsu---
 /****kiem tra***
 
 update  ttkd_bsc.ct_bsc_tratruoc_moi set pbh_cn_id = 5, ma_nv_cn = 'VNP017854' where pbh_cn_id is null and pbh_giao_id = 5 and thang = 202110 and ma_tb in ('l31808',
            'kayoung123',
            );
            commit;
            rollback;
            select * from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202307;
       
            select sum(dongia) from ttkd_bsc.ct_dongia_tratruoc where thang = 202309 and ma_kpi like 'DONG%' and LOAI_TINH in ('DONGIATRA', 'DONGIATRU'); 2246 7608915
           -- delete from ttkd_bsc.ct_dongia_tratruoc where thang = 202309 and ma_kpi like 'DONG%' and LOAI_TINH in ('DONGIATRA', 'DONGIATRU');
            select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202302 and ma_kpi like 'DONG%';
            ---chi tiet dongia tru gui Nsu---
            select ma_pb, manv_ql, manv_tctn, ma_tb, thuebao_id, nvl2(max(rkm_id), 1, 0) ghtt_tc
            from ttkd_bsc.ct_bsc_tratruoc_moi 
            where thang = 202110 and  ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500')
            group by ma_pb, manv_ql, manv_tctn, ma_tb, thuebao_id;
            rollback;
    */
        --Buoc 6-- chot chi tiet don gia ghtt theo ma_nv thuyey phuc, doi voi ds giao TTVT chi tinh NV thuyet phuc la BHOL, nguoc lai TTVT
        --1--tinh don gia theo ai lam thi duoc huong-------QLDB, QLDB(CSKH), BHOL, GDV-------khong tinh CSKH AS2 cac KHDN
                ---tu T3 khong QLDB CC, tu T4 QLDB Can gio (VNP0701402)
            select a.manv_thuyetphuc, nv.ma_nv from ttkd_bsc.ct_bsc_tratruoc_moi a
                                join css_hcm.hd_khachhang b on a.ma_gd = b.ma_gd
                                join ttkd_bsc.nhanvien_202308 nv on b.ctv_id = nv.nhanvien_id
                where rkm_id is not null and a.thang = 202308 and nvl(a.manv_thuyetphuc, 'a') <> nv.ma_nv
            ;
            insert into ttkd_bsc.ct_dongia_tratruoc
                    with dc as (select /*+ RESULT_CACHE */ dctb.thuebao_id, 'CanGio' ten_quan from css_hcm.diachi_tb dctb, css_hcm.diachi dc 
                                                                where dctb.diachild_id = dc.diachi_id and dc.quan_id in (3998))
                                , hs as (select thang, khachhang_id from ttkd_bsc.ct_bsc_tratruoc_moi xu
                                                where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id)
                                                
                                select THANG, cast('DONGIATRA' as varchar(30)) LOAI_TINH,  cast('DONGIA' as varchar(30)) ma_kpi, MANV_DONGIA ma_nv, nv.ma_to, nv.ma_pb
                                                     , PHONG_cs, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, null MA_NV_CN, MANV_THPHUC
                                                     , SOTHANG_DC, HT_TRA_ONLINE, Khuvuc, dongia, DTHU, NGAY_TT, heso_giao, khdn, nhomtb_id, khachhang_id, heso, tien_khop
                                from (select a.khachhang_id, a.thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                                                                            , a.manv_giao, a.manv_thphuc
                                                                   /*         , case
                                                                                        ---Dai TC --> Dai xuly --> QLDB
                                                                                       when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 1 and a.ma_nv_cn = a.manv_thuyetphuc then a.manv_thuyetphuc
                                                                                        ---Dai TC --> Dai xuly -->  manv_thuyetphuc sau cung
                                                                                        when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 1 and a.manv_thuyetphuc is not null then a.manv_thuyetphuc
                                                                                        ---Dai TC --> Dai xuly --> con lai
                                                                                        when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 1 and a.manv_thuyetphuc is null then a.ma_nv_cn

                                                                                        ----Dai KTC, QLDB ko xu ly ---> manv_thuyetphuc sau cung
                                                                                        when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 0 and a.pbh_cn_id is null and a.manv_thuyetphuc is not null then a.manv_thuyetphuc
                                                                                        ----Dai KTC, QLDB xu ly ---> QLDB
                                                                                        when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 0 and a.ma_nv_cn = a.manv_thuyetphuc then a.manv_thuyetphuc
                                                                                        ----Dai KTC, QLDB xu ly --> manv_thuyetphuc sau cung
                                                                                        when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 0 and a.manv_thuyetphuc is not null then a.manv_thuyetphuc
                                                                                        ----Dai KTC, QLDB xu ly --> con lai
                                                                                        when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 0 and a.manv_thuyetphuc is null then a.ma_nv_cn
                                                            --
                                                                                        ----PBH OB, QLDB ko xu ly (ko Dai) ---> manv_thuyetphuc sau cung
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is not null and a.pbh_cn_id is null then a.manv_thuyetphuc
                                                                                        ----PBH OB, QLDB xu ly ---> QLDB
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is not null and a.pbh_cn_id is not null and a.ma_nv_cn = a.manv_thuyetphuc then a.manv_thuyetphuc
                                                                                        ----PBH OB, QLDB xu ly ---> manv_thuyetphuc sau cung
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is not null and a.pbh_cn_id is not null and a.manv_thuyetphuc is not null then a.manv_thuyetphuc
                                                                                        ----PBH OB, QLDB xu ly ---> con lai
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is not null and a.pbh_cn_id is not null and a.manv_thuyetphuc is null then a.ma_nv_cn
                                                                                        
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is null and a.pbh_cn_id is null then a.manv_giao
                                                                                        ----Khong giao ai xu ly ---> nhan vien thuyet phuc bat ky
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is null and manv_giao is null and a.manv_thuyetphuc is not null then a.manv_thuyetphuc                                                                                                
                                                                                        ----Giao VTTP xu ly, PBHOL xy ly, khong ma thuyet phuc ---> manv online
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is null and a.pbh_cn_id in (26, 29) and a.manv_thuyetphuc is null then a.ma_nv_cn
                                                                                        ----Giao VTTP xu ly, co dvi xy ly ---> manv_thuyetphuc sau cung (chi xet PBH Online)
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is null and a.pbh_cn_id is not null and a.mapb_thphuc = 'VNP0703000' then a.manv_thuyetphuc
                                                                                        ----Giao VTTP xu ly, co dvi xy ly, manv_thuyetphuc PBH Online hoac khong nhap --> manv_giao VTTP
                                                                                        when a.pbh_giao_id not in (26, 29) and ma_pb is null and a.pbh_cn_id is not null and nvl(a.mapb_thphuc, 'a') <> 'VNP0703000' then a.manv_giao
                                                                                        
                                                                                        else null
                                                                            end manv_dongia
                                                                            */
                                                                              -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                                                            , case when a.mapb_thphuc = 'VNP0703000' then a.manv_thphuc
                                                                                    --    when a.ma_pb is null then a.manv_giao
                                                                                        when a.ma_pb is not null and to_number(to_char(max(a.ngay_tt), 'yyyymm')) <= 202309 then a.manv_giao
                                                                                        else a.manv_thphuc
                                                                                end manv_dongia
                                                                            , max(so_thangdc) sothang_dc
                                                                             , sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                when a.ht_tra_id in (2) then 1 end) ht_tra_online
                                                                            
                                                                       --     , (select decode(quan_id, 3998, 'CanGio') from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (3998) and a.thuebao_id = dctb.thuebao_id) Khuvuc
                                                                            , dc.ten_quan khuvuc
                                                                            , case
                                                                                       -- when nhomtb_id is not null and a.loaitb_id in (61) then 0
                                                                                        ----thanh toan nhan cong + ap dung khu vuc Can Gio
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) < 3  
                                                                                                                -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                                                                  and dc.ten_quan is not null
                                                                                                                            then 8700*0.5
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6  
                                                                                                                -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                                                                  and dc.ten_quan is not null
                                                                                                                  then 8700
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12  
                                                                                                                -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                                                                  and dc.ten_quan is not null
                                                                                                                  then 11700
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 12  
                                                                                                                -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                                                                  and dc.ten_quan is not null
                                                                                                                  then 16700
                                                                                       ----thanh toan nhan cong
                                                                                       when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) < 3 then 5700 * 0.5
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6 then 5700
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12 then 8700
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 12 then 13700
                                                                                        ----thanh toan online---
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) > 0 and max(so_thangdc) < 3 then 15700  * 0.5
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) > 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6 then 15700
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) > 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12 then 18700
                                                                                        when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                            when a.ht_tra_id in (2) then 1 end) > 0 and max(so_thangdc) >= 12 then 23700
                                                                            end 
                                                                                        ------tb thanh toan trong 30 ngay trong thang T het han
                                                                                        * case when max(ngay_tt) between add_months(trunc(sysdate, 'month'), -2) and add_months(trunc(sysdate, 'month'), -1) -1 then 1.05
                                                                                        ------tb thanh toan truoc  thang T het han
                                                                                                        when max(ngay_tt) < add_months(trunc(sysdate, 'month'), -2) then 1.1
                                                                                                else 1
                                                                                            end
                                                                            dongia
                                                                            
                                                                            , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                                                                when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0)
                                                                                                                                                                                            , 0)
                                                                                                                                                                            -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                                                                                                                * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                                                                                                        , 0)

                                                                                                ----Dich vu Mesh he so 0.2
                                                                                                when a.loaitb_id = 210 then 0.2  
                                                                                                ---MyTV he so 0.25 
                                                                                                when a.loaitb_id in (61, 171, 18) then 0.25  
                                                                                            else 0 
                                                                                end Heso
                                                                            ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, 0 KQ_TH_Dai, a.khdn, a.nhomtb_id, a.heso_giao
                                                                            , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                                                                from ttkd_bsc.ct_bsc_tratruoc_moi a 
                                                                                    left join dc on a.thuebao_id = dc.thuebao_id 
                                                                                    left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
                                                                where rkm_id is not null and a.thang = 202310 --and rownum = 1000--a.ma_tb = 'tanxuantan'
                                                                group by a.thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_giao, a.manv_thphuc, a.manv_cs, a.ma_to, a.ma_pb, a.mapb_thphuc
                                                                                    , a.thuebao_id, a.ma_tb, tien_dc_cu, a.khdn, a.nhomtb_id, a.loaitb_id, a.khachhang_id, a.goi_old_id, dc.ten_quan
                                                                                    , a.heso_giao, hs.khachhang_id
                                                ) a
                                        , ttkd_bsc.nhanvien_202310 nv
                        where a.manv_dongia = nv.ma_nv(+) and a.dthu > 0 and a.rnk =1
                                        and a.manv_dongia is not null
                                        and nv.ma_nv is null
                                        and nvl(nv.MA_PB, 'khongco') not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
                                        and not (nvl(nv.ma_to, 'khongco') in ('VNP0701402', 'VNP0702215', 'VNP0702216')
                                                                and nvl(nv.ma_vtcv, 'khongco') in ('VNP-HNHCM_BHKV_14', 'VNP-HNHCM_BHKV_18'))
                                        and nvl(nv.ma_vtcv, 'khongco') not in ('VNP-HNHCM_BHKV_18',
                                                                                                                            'VNP-HNHCM_BHKV_2.2',
                                                                                                                            'VNP-HNHCM_BHKV_14',
                                                                                                                            'VNP-HNHCM_BHKV_2',
                                                                                                                            'VNP-HNHCM_BHKV_12',
                                                                                                                            'VNP-HNHCM_BHKV_18.1')
                ---loai tru dongia 30ngay voi don gia > 0 thang n-1
                and not exists (select 1 from ttkd_bsc.ct_dongia_tratruoc where thang = to_char(add_months(trunc(sysdate, 'month'), -2), 'yyyymm') and loai_tinh = 'DONGIATRA30D' and DONGIA > 0 and tien_khop = 1 and ma_nv is not null and a.thuebao_id = thuebao_id)
            --    and not exists (select 1 from ttkd_bsc.ct_dongia_tratruoc where thang = to_char(add_months(trunc(sysdate, 'month'), -3), 'yyyymm') and loai_tinh = 'DONGIATRA_TR30D' and DONGIA > 0 and tien_khop = 1 and ma_nv is not null and a.thuebao_id = thuebao_id)

     ;

    --2---Don gia tru AS NV CSKH KHDN1---tinh tren tap giao ghtt--
       ---tinh lai thang 5, 6, va doi dk thang 7 tinh cho nhan vien AS to CSKH Viber 16/8/22, chua co van ban update 325
       --khong chay bsc T3, T4, T5 doi voi DN 1,2,3 theo vb 43 thay doi mo hinh
       --ap dung lai tu thang 9/23
       select * from ttkd_bsc.ct_dongia_tratruoc where thang = 202310 and ma_kpi like 'DONG%' and LOAI_TINH in ('DONGIATRU');
       
              insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, DTHU, NGAY_TT, khachhang_id, tien_khop)
                        select THANG, 'DONGIATRU' LOAI_TINH,  'DONGIA' ma_kpi, a.manv_giao, a.MA_TO, a.MA_PB
                                        , phong_cs, a.thuebao_id, a.ma_tb, DTHU, ngay_tt, khachhang_id, tien_khop
                        from (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.phong_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb
                                               , khachhang_id , sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                          from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                where a.thang = 202310                       ------------n------------
                                                group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, phong_cs, a.thuebao_id, a.ma_tb, khachhang_id
                                        ) a
                                        join ttkd_bsc.nhanvien_202310 nv on a.manv_giao = nv.ma_nv
                        where a.ma_pb in ('VNP0702300') --and nv.ma_vtcv = 'VNP-HNHCM_KHDN_6'
  
;
    --3---Don gia tru AS2 cskh KHDN2-3---tinh tren tap giao ghtt--
      
        insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, DTHU, NGAY_TT, khachhang_id, tien_khop)
                select THANG, 'DONGIATRU' LOAI_TINH,  'DONGIA' ma_kpi, a.manv_giao, a.MA_TO, a.MA_PB
                                        , phong_cs, a.thuebao_id, a.ma_tb, DTHU, ngay_tt, khachhang_id, tien_khop
                from (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.phong_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb
                                            , khachhang_id, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                          from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                where a.thang = 202310                       ------------n------------
                                                group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.phong_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, khachhang_id
                                        ) a
                                        join ttkd_bsc.nhanvien_202310 nv on a.manv_giao = nv.ma_nv
                where nv.ma_pb in ('VNP0702400', 'VNP0702500') 
;
--4-khong thuc hien T3, T4, T5 KHDN, T3 QLDB CC, T3, T4 QLDB Can Gio--Dong luc ghtt---tinh tap giao ctr ghtt--va dung nguoi thuyet phuc----
  insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO
                                                                                                    , MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, DTHU, NGAY_TT, KQ_TH_DAI, KQ_POPUP)
                        select thang, 'DONGLUCTT' LOAI_TINH,  'DONGLUC' ma_kpi, nv.ma_nv, nv.ma_to, nv.ma_pb
                                              , phong_ql, thuebao_id, ma_tb, tien_dc_cu, manv_giao, ma_nv_cn, manv_thuyetphuc
                                              , SOTHANG_DC, HT_TRA_ONLINE, DTHU, NGAY_TT, KQ_TH_DAI, KQ_POPUP
                        from  (select thang, a.pbh_ql_id, a.phong_ql, a.pbh_giao_id, a.pbh_cn_id, a.manv_ql ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                                                    , a.manv_giao, a.ma_nv_cn, a.manv_thuyetphuc
                                                    , max(so_thangdc) sothang_dc, sum(decode(bo_dau(ten_ht_tra), 'Tien mat', 0, 1)) ht_tra_online
                                                    , 0 dongia
                                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                                                from ttkd_bsc.ct_bsc_tratruoc_moi a
                                                                where a.thang = 202301                  ------------n------------
                                                                group by thang, a.pbh_ql_id, a.phong_ql, a.pbh_giao_id, a.manv_giao, a.manv_thuyetphuc, a.manv_ql, a.ma_to, a.ma_pb
                                                                                    , a.thuebao_id, a.ma_tb, tien_dc_cu, a.kq_dvi_cn, a.kq_popup, a.ma_nv_cn, a.pbh_cn_id
                                                ) a
                                        , ttkd_bsc.nhanvien_202301 nv
                        where a.manv_giao = nv.ma_nv --and dthu > 0 --and nv.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
                                   --     and a.ma_tb = 'hcmba1017'
             */                               
;
commit;
rollback;
    ---Buoc 7---chot ds don gia Fiber tra sau chuyen tra truoc
    --5---Dong luc Fiber tra sau chuyen tra truoc--tinh tap giao ctr ghtt------va dung nguoi thuyet phuc----
    
    select a.ma_tb, a.manv_thuyetphuc, nv.ma_nv
    from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
                join css_hcm.hd_khachhang b on a.ma_gd = b.ma_gd
                                join ttkd_bsc.nhanvien_202308 nv on b.ctv_id = nv.nhanvien_id
    where rkm_id is not null and a.thang = 202308 and nvl(a.manv_thuyetphuc, 'a') <> nv.ma_nv
    ;

select * from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202309;
select *   from ttkd_bsc.ct_dongia_tratruoc where thang = 202309 and 'DONGLUCTS' = LOAI_TINH and  'DONGLUC' =ma_kpi ;
delete from ttkd_bsc.ct_dongia_tratruoc where thang = 202307 and 'DONGLUCTS' = LOAI_TINH and  'DONGLUC' =ma_kpi ;

insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, THUEBAO_ID, MA_TB, ma_nv_cn, manv_thuyetphuc
                                                                                                    , SOTHANG_DC, HT_TRA_ONLINE, dongia, DTHU, NGAY_TT, tien_khop)
                        select thang, 'DONGLUCTS' LOAI_TINH,  'DONGLUC' ma_kpi, manv_thuyetphuc ma_nv, ma_to, ma_pb
                                            , thuebao_id, ma_tb, manv_cn ma_nv_cn, manv_thuyetphuc
                                              , SOTHANG_DC, HT_TRA_ONLINE, dongia, DTHU, NGAY_TT, tien_khop
                                                  
                        from (select thang, manv_thuyetphuc, ma_to, ma_pb, thuebao_id, ma_tb, manv_cn
                                                , max(so_thangdc) sothang_dc
                                                --, sum(decode(bo_dau(ten_ht_tra), 'Tien mat', 0, 1)) ht_tra_online
                                                , sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                when a.ht_tra_id in (2) then 1 end) ht_tra_online
                                                , case
                                                            when max(so_thangdc) >= 3 and max(so_thangdc) < 6 
                                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                            when a.ht_tra_id in (2) then 1 end) > 0 then 35000
                                                            when max(so_thangdc) >= 6 and max(so_thangdc) < 12 
                                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                            when a.ht_tra_id in (2) then 1 end) > 0 then 45000
                                                            when max(so_thangdc) >=12 
                                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                            when a.ht_tra_id in (2) then 1 end) > 0 then 60000
                                                            ----thanh toan tien mat
                                                            when max(so_thangdc) >= 3 and max(so_thangdc) < 6 
                                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 then 25000
                                                            when max(so_thangdc) >= 6 and max(so_thangdc) < 12 
                                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 then 35000
                                                            when max(so_thangdc) >=12 
                                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                            when a.ht_tra_id in (2) then 1 end) = 0 then 50000
                                                end dongia
                                                ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                                from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
                                where rkm_id is not null and thang = 202310
                                group by thang, manv_thuyetphuc, ma_to, ma_pb, thuebao_id, ma_tb, manv_cn
                                ) 
                        where ma_pb is not null and dthu > 0 and rnk = 1
;
commit;
rollback;
select * from ttkd_bsc.tl_giahan_tratruoc  where ma_kpi like 'DONG%' and thang = 202310 and LOAI_TINH in ('DONGIATRA', 'DONGIATRU');174 -33017121
--delete from ttkd_bsc.tl_giahan_tratruoc  where ma_kpi like 'DONG%' and thang = 202309 and LOAI_TINH in ( 'DONGIATRU');
 select * from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202201;
 select * from ttkd_bsc.ct_dongia_tratruoc where thang = 202211;
 



    ---Buoc 8-----tinh tong don gia ghtt
--6---Don gia chi tra cho nhan vien thuyet phuc---
  insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
                        select thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
                                                , count(thuebao_id) tong
                                                  , sum(case when dthu > 0 then 1 else 0 end) da_giahan
                                                  , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                                  , sum(dthu) DTHU_thanhcong
                                                  , round(sum(dthu)*100/sum(tien_dc_cu/1.1), 2) tyle
                                                  , round(sum(dongia*heso *tien_khop),0) dongia
                        -- select * 
                        from ttkd_bsc.ct_dongia_tratruoc  a
                        where ma_kpi = 'DONGIA' 
                                        and loai_tinh in ('DONGIATRA') and thang = 202310
                          group by thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
                            order by 2
;
    ---7--huy 16/8/22 Don gia tru AS1.2 NV TCTN KHDN1---tinh tren tap quan ly o dia ban--
             --Update 16/08 Don gia tru AS NV CSKH KHDN1---tinh tren tap giao ghtt--
             --vb 411 eO 834390 tyle thay doi 80 --> 90 ds 60 ngay
  insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
            select THANG, LOAI_TINH,  ma_kpi, MA_NV, MA_TO, MA_PB
                         , count(thuebao_id) tong
                        , sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end) da_giahan
                        , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                        , sum(dthu) DTHU_thanhcong
                        , round(sum(case when dthu > 0 and tien_khop = 1 then 0 else 1 end) *100/count(thuebao_id), 2) tyle
                        , -1* case when round(sum(case when dthu > 0 and tien_khop = 1 then 0 else 1 end) *100/count(thuebao_id), 2) > 10 
                                            then round((count(thuebao_id) * 0.9)- sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end), 0) * 100000 
                                    else 0
                            end dongia
        --    select *
            from ttkd_bsc.ct_dongia_tratruoc 
            where ma_kpi = 'DONGIA' and ma_pb in ('VNP0702300')
                                        and loai_tinh = 'DONGIATRU' and thang = 202310
            group by THANG, LOAI_TINH,  ma_kpi, MA_NV, MA_TO, MA_PB
;
    --8---Don gia tru AS2 cskh KHDN2-3 ---tinh tren tap giao ghtt--
    insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
                select THANG, LOAI_TINH, ma_kpi, MA_NV, MA_TO, MA_PB
                                     , count(thuebao_id) tong
                                    , sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end) da_giahan_giao
                                    , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI_giao
                                    , sum(dthu) DTHU_thanhcong_giao
                                    , round(sum(case when dthu > 0 and tien_khop = 1 then 0 else 1 end) *100/count(thuebao_id), 2) tyle_ko_giahan
                                    , -1 * case when round(sum(case when dthu > 0 and tien_khop = 1 then 0 else 1 end) *100/count(thuebao_id), 2) > 10 
                                                        then round((count(thuebao_id) * 0.9)- sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end), 0) * 60000 
                                                else 0
                                        end dongia
                --    select *
            from ttkd_bsc.ct_dongia_tratruoc 
            where ma_kpi = 'DONGIA' and ma_pb in ('VNP0702400', 'VNP0702500')
                                        and loai_tinh = 'DONGIATRU' and thang = 202310
                group by THANG, LOAI_TINH, ma_kpi, MA_NV, MA_TO, MA_PB
    ;
---9--Dong luc Fiber tra sau chuyen tra truoc-----tinh tap giao ctr ghtt------va dung nguoi thuyet phuc----
select * from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202305;
select sum(tien) from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTS';
---delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202307 and ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTS';

;
commit;
-----Buoc 9---tong don gia Fiber tra sau chuyen tra truoc
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
                        select thang, LOAI_TINH, ma_kpi, ma_nv, ma_to, ma_pb
                                                , count(thuebao_id) tong
                                                  , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                                                  , 0 DTHU_DUYTRI
                                                  , sum(dthu) DTHU_thanhcong
                                                  , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                                                  , sum(dongia*tien_khop) dongia, null, null
                                                  
                            --    select *
                                from ttkd_bsc.ct_dongia_tratruoc 
                                where ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTS' and thang = 202310 --and ma_nv = manv_thuyetphuc 
                                 group by thang, LOAI_TINH, ma_kpi, ma_nv, ma_to, ma_pb

;

--Buoc 10----update bang luong don gia TTKD
update ttkd_bsc.bangluong_dongia_202206 a set dongluc_ghtt = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc 
                                                                            where thang = 202207 and ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTT' 
                                                                                            and ma_nv = a.MA_NV_HRM
                                                                            group by ma_nv 
                                                                            having  sum(tien)  <> 0)
                 , dongluc_ghts = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc 
                                                                            where thang = 202207 and ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTS' 
                                                                                            and ma_nv = a.MA_NV_HRM
                                                                            group by ma_nv 
                                                                            having  sum(tien)  <> 0)
          /*         , dongluc_ghts_hoito = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc 
                                                                            where thang like '2022032%' and ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTS' 
                                                                                            and ma_nv = a.MA_NV_HRM
                                                                            group by ma_nv 
                                                                            having  sum(tien)  <> 0)
       */             , luong_dongia_ghtt = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc 
                                                                            where thang = 202207 and ma_kpi = 'DONGIA' and ma_nv = a.MA_NV_HRM
                                                                            group by ma_nv 
                                                                            having  sum(tien)  <> 0)
;
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202208 and ma_kpi  like 'DONGIA%'; and loai_tinh = 'DONGLUCTS';380509000 + 109585000;
select sum(tien) from ttkd_bsc.tl_giahan_tratruoc where thang = 202204 and ma_kpi like 'DONGIA%' and loai_tinh = 'DONGIATRU' and ma_pb = 'VNP0702300';898,335,000 320 720 000
select sum(LUONG_DONGIA_GHTT), sum(DONGLUC_GHTT), sum(DONGLUC_GHTs), sum(LUONG_DONGIA_GHTT) +sum(DONGLUC_GHTT) + sum(DONGLUC_GHTs)--,  sum(DONGLUC_GHTS_hoito)
from ttkd_bsc.bangluong_dongia_202304; where  MA_DONVI = 'VNP0702300'; 


select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV
                , MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, LUONG_DONGIA_GHTT, DONGLUC_GHTT, DONGLUC_GHTs
from ttkd_bsc.bangluong_dongia_202304 where ma_nv = 'VNP017150';
select * from ttkd_bsc.bangluong_dongia_202208;
--  update ttkd_bsc.bangluong_dongia_202204 a set dongluc_ghtt = null, luong_dongia_ghtt = null, dongluc_ghts = null
;
commit;
rollback;

-- tong luong don gia:
update bangluong_dongia_202204 set tong_luong_dongia= null;
update bangluong_dongia_202204
    set tong_luong_dongia=nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)+nvl(luong_dongia_vnptt,0)
                                                +nvl(luong_dongia_ghtt,0)+nvl(dongluc_ghtt,0)+nvl(dongluc_ghts,0)
                                                +nvl(luong_dongia_shipper,0)+nvl(ctvxhh_qly_ptr_ctv,0)+nvl(dongluc_shop,0);
                                             --   +nvl(dongluc_vb128_032022,0)+nvl(dongluc_vb128_042022,0);

update bangluong_dongia_202204
    set tong_luong_dongia='' where tong_luong_dongia=0;

select sum(tong_luong_dongia) from bangluong_dongia_202204; 6222860267
commit;
rollback;





------Hoan tien thang 5, thang 6 DN1 trong thang 8
select THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, a, b, c, d, TYLE, sum(TIEN) TIEN
        from (
        select 202208 THANG,  'hoantien_thang56'  LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, 0 a, 0 b, 0 c, 0 d,0 tyle, - TIEN TIEN
        from ttkd_bsc.tl_giahan_tratruoc 
                                                                                                                                                                where thang in (202205, 202206) and ma_kpi = 'DONGIA' and ma_pb = 'VNP0702300' and tien < 0
                                      )
                                      group by THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, a, b, c, d, TYLE
                                                                                                                                                                ;
