select* from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202411 and tien_khop = 0 and 
ma_gd in ;
commit;
(select * from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 where chungtu_id = 469744);
insert into ct_Bsc_tratruoc_moi_30day select* from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202411;
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi a
where thang = 202411 and thuebao_id in (;
commit;
select a.* , ten_kh from giao_oneb a
    join css_hcm.db_Thuebao b on a.thuebao_id =b.thuebao_id
    join css_hcm.db_khachhang c on b.khachhang_id = c.khachhang_id
where thang = 202411 and thang_kt = 202411 and a.thuebao_id not in (
select distinct (thuebao_id) from  ttkd_Bsc.ct_Bsc_tratruoc_moi_30day a
where thang = 202411);
insert into ttkd_Bsc.ct_bsc_tratruoc_moi 
select* from ct_bsc_tratruoc_moi where thang = 202411 and ma_Tb='mesh0202702';
delete from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day a
where thang = 202411 and tien_khop = -1 and exists (select 1 from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day 
                                                    where thang = 202411 and nvl(tien_khop,0) <> -1 and thuebao_id= a.thuebao_id);
                                                    group by thuebao_id;

select* from  ttkd_Bsc.ct_Bsc_tratruoc_moi_30day a
where thang = 202411 and thuebao_id in (;
select a.*,add_months(to_date(to_char(thang_bd_moi),'yyyymmdd') ,3) from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day a
where thang = 202411 and exists (select 1 from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang < 202411 and rkm_id = a.rkm_id)
and exists (select 1 from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang < 202411 and rkm_id = a.rkm_id);
and 
;
update ttkd_Bsc.ct_Bsc_tratruoc_moi_30day a set tien_khop = -1 where thang = 202411 and
 exists (select 1 from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang < 202411 and rkm_id = a.rkm_id)
and exists (select 1 from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang < 202411 and rkm_id = a.rkm_id);



select rkm_id from ttkd_Bsc.ct_bsc_tratruoc_moi_30day where thang = 202411 group by rkm_id having count(rkm_id)>1;
delete from ttkd_Bsc.ct_Bsc_tratruoc_moi a
where thang = 202411 and exists (select 1 from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang < 202411 and rkm_id = a.rkm_id)
and exists (select 1 from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang < 202411 and rkm_id = a.rkm_id);
commit;
group by thuebao_id;
update ttkd_Bsc.ct_Bsc_tratruoc_moi_30day
set tien_khop = 1
where thang = 202411 and tien_khop = 0 and 
ma_gd in (select ma from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1);

delete from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang = 202411 and khachhang_id in (8921108,2842255,9802705);
commit;
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202411 and ma_Tb='hcmkimhang5';

---

update ttkd_bsc.ct_Bsc_tratruoc_moi a 
set tien_khop = null, ngay_Tt = null, ma_Gd = null, rkm_id = null, THANG_BD_MOI = null, SO_THANGDC= null, AVG_THANG= null, TIEN_THANHTOAN= null, VAT= null,
SOSERI= null, SERI= null, KENHTHU= null, TEN_NGANHANG= null, TEN_HT_TRA= null
--select* from ttkd_bsc.ct_Bsc_tratruoc_moi a 
where thang = 202411 and rkm_id is not null and to_char(ngay_Tt ,'yyyymm') = '202408'
and not exists (Select 1 from  ttkd_bsc.ct_Bsc_tratruoc_moi where thang = 202411 and rkm_id is not null and
to_number(to_char(ngay_Tt ,'yyyymm')) >202408  and thuebao_id = a.thuebao_id)
and not exists (select 1 from tt where tt.thuebao_id = a.thuebao_id and to_number(to_char(ngay_Kt_mg,'yyyymm')) >202410) 
;and ma_Tb in ('hcm_tkhn',
'mesh0183450',
'thuylinh621',
'hoangoanh_ht3',
'fiber3_4c',
'dacloc_tv4',
'hcm_hoangoanh_tv',
'hcmphdacloc87',
'mesh0214202',
'camvan1535',
'xuanvu250122',
'duchnet2_2308',
'hcmduchnet2_2308',
'mesh0090945',
'mesh0090944',
'luuanap12023',
'hcm_tnluan_2',
'caoanh117',
'phuong-506',
'trunghiu93874990',
'xuong12102016');
select* from v_Thongtinkm_all where ma_tb='hcm_tkhn';
select* from tt where ma_tb='hcm_tkhn';
order by thang_bd_moi;