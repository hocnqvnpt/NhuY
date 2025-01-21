select* from ttkd_Bsc.ct_Bsc_giahan_Cntt where thang = 202404 and ma_pb = 'VNP0702500' and thang_ktdc_Cu = 202404 and loaitb_id not in (147,148);

select  count(distinct thuebao_id) from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202404 and ma_pb = 'VNP0702500' and tien_khop is not null;--and nvl(tien_khop,0) != 0;
create table bk_Ca as ;
delete  from ttkd_Bsc.ct_Bsc_giahan_cntt where thang =  202404 and loaitb_id not in (147,148) and thang_ktdc_cu = 202405 and  thuebao_id not in (
 select thuebao_id from ttkdhcm_ktnv.ghtt_chotngay_271 a where ngay_chot=to_date('20240503','yyyymmdd') and thang_kt = 202405 and  (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 )  
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) ));
commit;
delete FROM ttkd_bsc.tl_giahan_tratruoc where thang = 202404 and ma_kpi = 'HCM_TB_GIAHA_025';
