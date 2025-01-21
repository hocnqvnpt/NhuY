create table tmp_ipcc as;  select count(1) --a.ob_id, ngay_tao, a.thuebao_id, ma_tb, LOAITB_ID, TOCDO_ID, a.KHACHHANG_ID, THANHTOAN_ID
--									, NGAY_BDDC, NGAY_KTDC, CUOC_DC, SO_THANGDC, CHITIETKM_ID, SL_DATCOC, MATB_PHU, TRANGTHAI_OB
--									, to_date(a.TD_TH, 'dd/mm/yyyy') td_th, TD_BD, TD_KT, TG_DC, TG_HD, TG_GM, TG_DT, a.ma_nd
--									, row_number() over (partition by thuebao_id, ma_nd order by ob_id desc) rnk
								from dhsx.v_ghtt_ipcc@dataguard a
								where a.trangthai_ob = 'SUCCESS' and a.tg_dt > 0 and 
                                to_char(to_date(a.TD_TH,'dd/mm/yyyy'),'yyyymm') in ('202409','202410','202408');
                                select* from TMP_IPCC