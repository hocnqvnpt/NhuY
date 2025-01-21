insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, MA_NV, MA_TO, MA_PB, LOAI_TINH, MA_KPI, TIEN)
--select sum(tien) from (
with tien as (
    select thang,manv_Thuyetphuc  ma_Nv,ma_to,ma_pb,  ma_Tb,tien_thuyetphuc tien, tien_Xuathd
    from thuhoi_bsc_dongia
    where thang = 202406 and ipcc =1 -- and loai_tinh = 'DONGIATRA_OB' and ipcc = 0 --and ghichu = 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
--    and ma_nv = 'VNP017247'
--    union all
--    select thang,nhanvien_xuathd,loai_tinh, ma_kpi,ma_tb,  heso_chuky*heso_dichvu*NVL(tien_xuathd,0) TIEN
--    from ttkd_Bsc.ct_dongia_tratruoc 
--    where thang = 202407 and loai_tinh = 'DONGIATRA_OB' --and ghichu= 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
)
select 202407 thang,a.ma_nv, b.ma_to, b.ma_pb, 'THUHOI_DONGIA_GHTT' loai_tinh, 'DONGIA' MA_KPI, sum(tien) tien--, ten_pb
from tien a
    join ttkd_bsc.nhanvien b on a.ma_nv = b.ma_nv and B.THANG = 202407
where 
--b.ma_pb  in ('VNP0701800','VNP0701500','VNP0701200','VNP0701300','VNP0701600','VNP0701400','VNP0701100','VNP0702200','VNP0702100') and 
b.donvi = 'TTKD' 
and b.ma_pb not in ('VNP0702300','VNP0702500','VNP0702400','VNP0700600') --and ma_vtcv = 'VNP-HNHCM_BHKV_49'
and a.thang = 202406
group by  a.thang,a.ma_nv, b.ma_to, b.ma_pb,b.ten_to, b.ten_pb, b.ma_Vtcv, b.ten_vtcv, ten_pb;
;
COMMIT;
select distinct loai_Tinh from ttkd_bsc.tl_giahan_tratruoc where thang = 202407;




















select donvi,ma_nv,ten_Nv,ten_to,ten_pb, sdt,user_ccbs, tr_thai
from ttkd_Bsc.nhanvien
where thang = 202407;
select * from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ma_Ct = 'VCB_20240716_246194';
select  a.chungtu_id, a.ma_ct,(select nhanvien_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
					    and ghilog_id in (select max(ghilog_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
                        where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
                        and timeinsert in (select max(timeinsert) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
                        where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)))) nhanvien_cn
from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a ;
                                           select* from ds_chungtu where ma_ct = 'VCB_20240624_205433';   
create table ds_chungtu as 
	select a.chungtu_id--,a.ma ma_gd
					,(select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id=a.chungtu_id) ma_ct--,a.tongtra tien_tt
--					,(select ma_nv || ' - ' || ten_nv from admin_hcm.nhanvien where nhanvien_id=a.nhanvien_id) nhanvien_tt
					,(select nhanvien_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
					    
					    and ghilog_id in (select max(ghilog_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
									  where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)
									    and timeinsert in (select max(timeinsert) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
													    where chungtu_id=a.chungtu_id and thaotac_id in (1,2,3)))) nhanvien_cn
--					,to_char(a.ngay_tt,'dd/mm/yyyy') ngay_tt
--					,decode(a.bosung,1,'nhancong','xuatfile') thuchien
from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a ;
select distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202407;
select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202407 and ma_tb in('hcm_ca_ivan_00017670',
'hcm_smartca_00047256',
'hcm_smartca_00047240',
'hcm_ca_ivan_00017237',
'hcm_ivan_00016446',
'hcm_ivan_00016885',
'hcm_smartca_00035351',
'hcm_ca_ivan_00017154');
VCB_20240704_224192
VCB_20240710_235021
VCB_20240719_251162
VCB_20240729_266089
VCB_20240602_161484

('hcm_ca_00036941',
'hcm_ca_00036952',
'hcm_ca_00036964',
'hcm_ca_00037000',
'hcm_ca_00037001',
'hcm_ca_00037002',
'hcm_ca_00037144',
'hcm_ca_00037639',
'hcm_ca_00037664',
'hcm_ca_00038006',
'hcm_ca_00038009',
'hcm_ca_00038016',
'hcm_ca_00038018',
'hcm_ca_00042692',
'hcm_ca_00042750',
'hcm_ca_00042796',
'hcm_ca_00042799',
'hcm_ca_00042806',
'hcm_ca_00042869',
'hcm_ca_00042947',
'hcm_ca_00043126',
'hcm_ca_00043291',
'hcm_ca_00043295',
'hcm_ca_00043334',
'hcm_ca_00043362',
'hcm_ca_00043371',
'hcm_ca_00043437',
'hcm_ca_00043456',
'hcm_ca_00043459',
'hcm_ca_00043487',
'hcm_ca_00043509',
'hcm_ca_00060097',
'hcm_ca_00060549',
'hcm_ca_00061361',
'hcm_ca_00061364',
'hcm_ca_00061367',
'hcm_ca_00061368',
'hcm_ca_00061369',
'hcm_ca_00061370',
'hcm_ca_00061928',
'hcm_ca_00061929',
'hcm_ca_00064785',
'hcm_ca_00080944',
'hcm_smartca_00069950',
'hcm_ca_00046454',
'hcm_ca_00046570',
'hcm_ca_00046586',
'hcm_ca_00046584',
'hcm_ca_00046566',
'hcm_ca_00046551',
'hcm_ca_00046573',
'hcm_ca_00046547',
'hcm_ca_00046564',
'hcm_ca_00046538',
'hcm_ca_00046569',
'hcm_ca_00046581',
'hcm_ca_00046593',
'hcm_ca_00046577',
'hcm_ca_00046565',
'hcm_ca_00046576',
'hcm_ca_00046536',
'hcm_ca_00046543',
'hcm_ca_00046592',
'hcm_ca_00046561',
'hcm_ca_00046546',
'hcm_ca_00046537',
'hcm_ca_00046588',
'hcm_ca_00046585',
'hcm_ca_00046562',
'hcm_ca_00046541',
'hcm_ca_00046589',
'hcm_ca_00046596',
'hcm_ca_00046594',
'hcm_smartca_00001455',
'hcm_ca_00081125',
'hcm_ca_00081129',
'hcm_ca_00081135',
'hcm_ca_00081167',
'hcm_ca_00081172',
'hcm_ca_00081178',
'hcm_ca_00081226',
'hcm_ca_00081231',
'hcm_ca_00081232',
'hcm_ca_00081234',
'hcm_ca_00081254',
'hcm_ca_00081279',
'hcm_ca_00082558',
'hcm_ca_00081117',
'hcm_ca_00081126',
'hcm_ca_00081138',
'hcm_ca_00081160',
'hcm_ca_00081162',
'hcm_ca_00081183',
'hcm_ca_00081199',
'hcm_ca_00081203',
'hcm_ca_00081207',
'hcm_ca_00081209',
'hcm_ca_00081225',
'hcm_ca_00081227',
'hcm_ca_00081235',
'hcm_ca_00081236',
'hcm_ca_00081238',
'hcm_ca_00081248',
'hcm_ca_00081267',
'hcm_ca_00081268',
'hcm_ca_00081271',
'hcm_ca_00081276',
'hcm_ca_00081277',
'hcm_ca_00081285',
'hcm_ca_00082545',
'hcm_ca_00040495',
'hcm_ca_00040497',
'hcm_smartca_00026916',
'hcm_smartca_00026986',
'hcm_smartca_00026928',
'hcm_smartca_00027044',
'hcm_smartca_00027028',
'hcm_smartca_00027052',
'hcm_smartca_00027088',
'hcm_smartca_00027065',
'hcm_smartca_00027069',
'hcm_smartca_00027043',
'hcm_smartca_00027092',
'hcm_smartca_00027033',
'hcm_smartca_00026972',
'hcm_smartca_00026949',
'hcm_smartca_00027078',
'hcm_smartca_00026978',
'hcm_smartca_00027019',
'hcm_smartca_00027037',
'hcm_smartca_00026953',
'hcm_smartca_00026983',
'hcm_smartca_00026932',
'hcm_smartca_00026922',
'hcm_smartca_00026925',
'hcm_smartca_00026929',
'hcm_smartca_00026930',
'hcm_smartca_00026931',
'hcm_smartca_00026940',
'hcm_smartca_00026937',
'hcm_smartca_00026938',
'hcm_smartca_00026959',
'hcm_smartca_00026942',
'hcm_smartca_00026943',
'hcm_smartca_00026945',
'hcm_smartca_00026944',
'hcm_smartca_00026946',
'hcm_smartca_00027029',
'hcm_smartca_00026950',
'hcm_smartca_00026971',
'hcm_smartca_00026954',
'hcm_smartca_00026960',
'hcm_smartca_00026961',
'hcm_smartca_00026970',
'hcm_smartca_00027035',
'hcm_smartca_00026973',
'hcm_smartca_00027040',
'hcm_smartca_00027027',
'hcm_smartca_00027010',
'hcm_smartca_00027002',
'hcm_smartca_00027003',
'hcm_smartca_00027008',
'hcm_smartca_00027009',
'hcm_smartca_00027032',
'hcm_smartca_00027011',
'hcm_smartca_00027018',
'hcm_smartca_00027024',
'hcm_smartca_00027034',
'hcm_smartca_00027045',
'hcm_smartca_00027048',
'hcm_smartca_00027049',
'hcm_smartca_00027050',
'hcm_smartca_00027051',
'hcm_smartca_00027059',
'hcm_smartca_00027062',
'hcm_smartca_00027063',
'hcm_smartca_00027064',
'hcm_smartca_00027066',
'hcm_smartca_00027067',
'hcm_smartca_00027076',
'hcm_smartca_00027083',
'hcm_smartca_00026995',
'hcm_smartca_00027094',
'hcm_smartca_00027089',
'hcm_smartca_00027077',
'hcm_smartca_00027079',
'hcm_smartca_00027080',
'hcm_smartca_00027093',
'hcm_smartca_00027075',
'hcm_ca_00059635',
'hcm_ca_00059636',
'hcm_ca_00059639',
'hcm_ca_00059640',
'hcm_ca_00059700',
'hcm_ca_00059646',
'hcm_ca_00059647',
'hcm_ca_00059650',
'hcm_ca_00059651',
'hcm_ca_00059664',
'hcm_ca_00059665',
'hcm_ca_00059666',
'hcm_ca_00059667',
'hcm_ca_00059668',
'hcm_ca_00059669',
'hcm_ca_00059670',
'hcm_ca_00059671',
'hcm_ca_00059682',
'hcm_ca_00059683',
'hcm_ca_00059685',
'hcm_ca_00059686',
'hcm_ca_00059687',
'hcm_ca_00059688',
'hcm_ca_00059689',
'hcm_ca_00059691',
'hcm_ca_00059693',
'hcm_ca_00059694',
'hcm_ca_00059695',
'hcm_ca_00059696',
'hcm_ca_00059697',
'hcm_ca_00059698',
'hcm_ca_00059699',
'hcm_ca_00059701',
'hcm_ca_00059702',
'hcm_ca_00059703',
'hcm_ca_00059704',
'hcm_ca_00059706',
'hcm_ca_00059707',
'hcm_ca_00059708',
'hcm_ca_00059709',
'hcm_ca_00059710',
'hcm_ca_00059711',
'hcm_ca_00059713',
'hcm_ca_00059714',
'hcm_ca_00059715',
'hcm_ca_00059716',
'hcm_ca_00059717',
'hcm_ca_00059720',
'hcm_ca_00059723',
'hcm_ca_00059725',
'hcm_ca_00059726',
'hcm_ca_00059727',
'hcm_ca_00059641',
'hcm_ca_00059729',
'hcm_ca_00059730',
'hcm_ca_00059731',
'hcm_ca_00059732',
'hcm_ca_00059733',
'hcm_ca_00059737',
'hcm_ca_00059738',
'hcm_ca_00059739',
'hcm_ca_00059740',
'hcm_ca_00059741',
'hcm_ca_00059742',
'hcm_ca_00059743',
'hcm_ca_00059744',
'hcm_ca_00059745',
'hcm_ca_00059747',
'hcm_ca_00059748',
'hcm_ca_00059754',
'hcm_ca_00059757',
'hcm_ca_00059758',
'hcm_ca_00059759',
'hcm_ca_00059760',
'hcm_ca_00059761',
'hcm_ca_00059762',
'hcm_ca_00059763',
'hcm_ca_00060555',
'hcm_ca_00060558',
'hcm_ca_00060557',
'hcm_ca_00060541',
'hcm_ca_00060559',
'hcm_ca_00060561',
'hcm_ca_00060556')