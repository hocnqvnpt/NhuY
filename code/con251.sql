





COMMIT;
;INSERT INTO TTKD_bSC.TL_GIAHAN_TRATRUOC (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TIEN)
with tp as 
(
    select ma_nv, SUM(NVL(tien_thuyetphuc,0)*heso_chuky*heso_dichvu) TIEN--, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and ipcc = 1
    GROUP BY MA_nV
)
,
XP AS 
(
    select NHANVIEN_XUATHD, SUM(NVL(TIEN_XUATHD,0)*heso_chuky*heso_dichvu) TIEN--, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB' --AND NHANVIEN_XUATHD IS NOT NULL
    GROUP BY NHANVIEN_XUATHD
)
, tien as (
select* from tp union all 
select* from xp)
select a.tien, b.ten_vtcv from tp a join ttkd_Bsc.nhanvien_202405 b on a.ma_nv = b.ma_nv where ma_Vtcv not in ('VNP-HNHCM_BHKV_47','VNP-HNHCM_BHKV_48','VNP-HNHCM_KDOL_3','VNP-HNHCM_BHKV_47')  ;

select 202405 thang, 'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI , a.ma_nv, b.ma_to, b.ma_pb, SUM(A.TIEN) TIEN
from tien a 
    join ttkd_Bsc.nhanvien_202405 b on a.ma_nv = b.ma_nv 
WHERE MA_PB NOT IN ('VNP0702500','VNP0702400','VNP0702300')
GROUP BY a.ma_nv, b.ma_to, b.ma_pb;








;select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202405 and ma_Tb in('hcm_ca_00055240','hcm_ca_00041448','hcm_ca_00041447','hcm_ca_00041449','hcm_ca_00057496','hcm_ca_00057502',
'hcm_ca_00057498','hcm_ca_00055981','hcm_ca_00055980','hcm_ca_00055971','hcm_ca_00055977','hcm_ca_00055964','hcm_ca_00055979','hcm_ca_00055965','hcm_ca_00055966','hcm_ca_00055968',
'hcm_ca_00055967','hcm_ca_00055974','hcm_ca_00055976','hcm_ca_00075881','hcm_ca_00077036');

delete from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202405 and ma_tb = 'hcm_ca_00055966';
update  ttkd_Bsc.ct_dongia_Tratruoc set dongia = 0, ghichu ='giam tru don gia do VB 188 TTr-NVC' where thang = 202405 and ma_tb = 'hcm_ca_00055966';
commit;

select* from ds_giahan_tratruoc2 where ma_Tb in ('hthanh04220','thanhthuybk');
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202405 and ma_kpi = 'HCM_TB_GIAHA_026';
update ttkd_bsc.tl_giahan_tratruoc set tien = 0 where thang = 202405 and ma_nv = 'VNP017793' and loai_tinh='DONGIATRU_CA';
	select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_024' and thang_kt is null and ma_vtcv = (select ma_to from ttkd_Bsc.nhanvien_202405 where ma_nv = 'VNP017793');
select* from ttkd_Bsc.nhanvien_202405 where ma_to = 'VNP0702302';

select* from ttkd

select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202405 and ma_To = 'VNP0702302' and ma_pb = 'VNP0702300' and ma_kpi = 'HCM_TB_GIAHA_024';
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202405 and ma_kpi = 'HCM_TB_GIAHA_024';

select* from ttkd_Bsc.nhanvien_202405 where sdt = '09190;';
select* from v_Thongtinkm_all where rkm_id in (6766448,6860131);

select A.LOAI_TINH,A.MA_NV,A.MA_TO, A.MA_TO,  B.TEN_PB,B.MA_vTCV, B.TEN_vTCV,B.TEN_TO,A.TIEN from ttkd_Bsc.TL_GIAHAN_TRATRUOC A
JOIN TTKD_BSC.NHANVIEN_202405 B ON A.MA_NV = B.MA_nV
where thang = 202405 and loai_tinh = 'DONGIATRA_OB' AND B.MA_PB NOT IN ('VNP0702300','VNP0702400','VNP0702500');
select* from css_Hcm.db_khachhang;
select owner , table_name from all_tab_columns where column_name = 'TEN_NHOM';

select* from ttkd_bsc.ct_dongia_tratruoc where ma_Tb in ('1705489',
'181tqc',
'1960196419881992',
'2011-nguyen',
'36pvc2',
'38606p25',
'39-30nt',
'714b1',
'and12a12a',
'anhkiet29623',
'anhtranghoang',
'baobei144',
'baominhhung',
'baotinh092016',
'bichphuong_tran',
'binh7-13e',
'binhan310396',
'bmduc206',
'bphuong1988',
'bthy_06',
'c2_25',
'caolinh172',
'caonha61',
'caovanhung1947',
'chau1122978',
'clinha1909',
'congsang10785',
'ctygiakhang68',
'ctytoanphat639',
'dailv1986',
'dangkhoabuianh',
'dangminhcuong2016',
'danhphuong93751042',
'dcvvien2022',
'ddnguyen180',
'dhtrieu_17',
'diemtrang5822160',
'dinhchinh_f30s',
'dinhngoc78',
'dknam72',
'doquoc2019',
'dtuanhai20',
'ducnhat255874.fb.hcm',
'ductai1988',
'dvt0522',
'fbvtiep672016',
'fiber1503102003',
'fiber15422',
'fibervnpt40',
'fier0',
'fishyvn',
'giahan93633157',
'ha-as',
'haphutranp83',
'hcm_caonha61',
'hcm_chi12866',
'hcm_danhphuong_2',
'hcm_kimlien_31',
'hcm_maib1011',
'hcm_mytv40',
'hcm_ngchong_5',
'hcm_ngocnghia_2',
'hcm_nhut',
'hcm_phuocthien2023',
'hcm_phuonghong175',
'hcm_quachthuvan',
'hcm_quocchi',
'hcm_tantien2911b',
'hcm_thngon104',
'hcm_thnhv',
'hcm_thphuc_5',
'hcm_tridung_10',
'hcm_trunghtv2db',
'hcm_tuandat_home2',
'hcm_tvd',
'hcm_vanhng_73',
'hcm_vanphuoc.dinh',
'hcm_vantng_39',
'hcm_vantrng_19',
'hcm_vantrung_21',
'hcm_vanvu_12',
'hcm_vnthong-q2',
'hcm_vuong052022',
'hcm1-2107',
'hcm1714b1',
'hcm19601964',
'hcm39-30nt',
'hcm45thanhapp',
'hcmanhtranghoang',
'hcmbaobei144',
'hcmbmduc206',
'hcmbthy_06',
'hcmclinha1909',
'hcmcuong139',
'hcmdhtrieu_17',
'hcmductai1988',
'hcmdvt0522',
'hcmfiber0',
'hcmfiber15422',
'hcmhaphutranp83',
'hcmhnqunhd71',
'hcmhoahoe7',
'hcmhoang9293',
'hcmhoangchau029',
'hcmhop340',
'hcmhotuandat',
'hcmhuy1981',
'hcmhuyenp21111',
'hcmhvy2339',
'hcmkyson1972',
'hcmlan999',
'hcmleduy6079635',
'hcmlttan2019',
'hcmlyviethoakn',
'hcmminh0522',
'hcmmyduyen164',
'hcmmytv2',
'hcmnbanhh',
'hcmnbduc2020',
'hcmngan0205-box',
'hcmngan-052022',
'hcmngocyen17',
'hcmntdta',
'hcmntmnang',
'hcmntn2209',
'hcmntn8c',
'hcmntthuat86',
'hcmnvnama83',
'hcmphongkn274',
'hcmphuocnhan_78',
'hcmphuongthao886',
'hcmquyenh3',
'hcmshop-phuonghoa',
'hcmtd41f',
'hcmthanhhai308',
'hcmthihuonge12',
'hcmthilam1005',
'hcmthinhi0906',
'hcmtkhien081117',
'hcmtlong15',
'hcmtoloan_70',
'hcmtong070121',
'hcmtranthinhung67',
'hcmtraphuong_hkn',
'hcmtrinhlinh37',
'hcmtt090819',
'hcmtuongvy2023',
'hcmtvu_2021',
'hcmvanlong-app',
'hcmvdhoan_188',
'hcmvnptnvs',
'hcmxuan3105',
'hcmxuanhip3082735',
'hien18a',
'hiep0166858793',
'hnganf30',
'hnqunhd71',
'ho_tnhan',
'hoahoe7',
'hoangchau029',
'hoangchuong2023',
'hoangmai77',
'hop3105',
'hotuandat',
'hson2401',
'htdo2018',
'htkieuloan2020',
'htnh2019',
'huong-fiber30',
'huonglan_133',
'huuhoa2018',
'huy1981',
'hvthanh_cd',
'hvy2339',
'hyenp21111',
'i1_07.01',
'kimlien7247626',
'kyson1972',
'lactran57',
'lanchi12866',
'lanhuongtran2015',
'ldtuan52',
'leduy6079635',
'lehuyen0110',
'liem_nh',
'lmchauvnn10',
'longrinb19',
'ltbt45',
'lttan2019',
'lttthuy_h3',
'ltyv31',
'lvnhan86',
'lyviethoa',
'maib1011',
'manhcuong_139',
'mesh_phuonga0418',
'mesh0006625',
'mesh0006626',
'mesh0006627',
'mesh0007901',
'mesh0048260',
'mesh0048261',
'mesh0059908',
'mesh0061406',
'mesh0061407',
'mesh0065890',
'mesh0066102',
'mesh0127570',
'mesh0128565',
'mesh0128570',
'mesh0128576',
'mesh0129379',
'mesh0129380',
'mesh0131369',
'mesh0131397',
'mesh0131402',
'mesh0131747',
'mesh0132616',
'mesh0136203',
'mesh0188773',
'mesh0188775',
'mesh0188972',
'meshlananhc1',
'meshlananhc2',
'meshlananhc3',
'meshlananhc4',
'meshlananhc5',
'meshlananhc6',
'meshlananhc7',
'meshlananhc8',
'minhhi5771621',
'minhhoang9292',
'minhyen1023',
'mthu92',
'mvan1',
'myduyen164',
'nat1219',
'nbanhh',
'nbduc2020',
'ndtai0322',
'ndthanh040121',
'ngan-052022',
'ngan1209',
'nghiem552c',
'ngminhhieu2023',
'ngngochiep74',
'ngocanhd936',
'ngocb0509',
'ngoclinh153',
'ngocyen14b',
'nguyenthiphuc54',
'nguyet_2019',
'nh996',
'nhb134',
'nhokhoa',
'nhut_2022',
'nhuya704',
'nnd-x91',
'nnga279',
'nphuonghong',
'nqhien130622',
'nqminh_0121',
'nqquyen727',
'ntcuong10kb',
'ntdta',
'nth1b',
'nthdiepa804',
'ntmh1219',
'ntmnang',
'ntn2209',
'ntn8c',
'ntronghieu_fm',
'nttam_f26',
'nttc160223',
'ntthuat86',
'nvphuc2013',
'pbphuongf30',
'pdk66',
'phammylinh789',
'phamvan12',
'phongkn274',
'phuocnhan_78',
'phuocthien2023',
'phuonga0418',
'phuongthao886',
'phuongtoan36',
'ptcuong130916',
'pvt1609',
'qtn474',
'quachthuvan',
'quang0921',
'quangthi937658',
'quocchi2021',
'quocquoc7151635',
'quyenh3',
'sanh091023',
'shop-phuonghoa',
'tam93782043',
'tanphuocthinh66a',
'tantien2911b',
'tcmanh115',
'td41',
'thai-112021',
'thai604',
'thaitaitu',
'thanhha_h1',
'thanhhai308',
'thanhhiep.hcm',
'thanhhong1994',
'thanhlan999',
'thanhthuy42',
'thanhvuong052022',
'thehien_fiber30',
'thienky1986',
'thihuonge12',
'thilam1005',
'thile072023',
'thinhi0906',
'thngon104',
'thphuc823',
'thuem7238172',
'thup6a4007',
'thuy-521a',
'thuy9078',
'thuytrang-090819',
'tindung217',
'tkhien081117',
'tlong15',
'tolan_70',
'tong070121',
'tphonggds_2019',
'tranphuong2018',
'tranthinhung67',
'traphuong_hkn',
'tridung130',
'triet281019',
'trinhlinh37',
'trung-htv2db',
'trungngha93627854',
'trungtin2311',
'truongsinh713',
'tthientan6879',
'ttthao1993',
'tt-tung3',
'tuananh55_18',
'tuandat_home2',
'tuongvy2023',
'tuyen-net3',
'tvd_1723',
'tvu_2021',
'vanho3724928',
'vanloc44',
'vanlong180522',
'vanphuoc.dinh',
'vant78924',
'vantairachla',
'vanteo122017',
'vanthang_2209',
'vantrng93700969',
'vantrung1908',
'vanvu5313316',
'vdhoan_188',
'viet_home2',
'vinhthanh80203',
'vlpxuan21',
'vnn121121',
'vnpt_2103',
'vnpt150487',
'vnpt290323',
'vnpt-3a26ccfh',
'vnpt-cckgvinhsy',
'vnpt-gm3-1103',
'vnptnvs260522',
'vnptonline_1616627',
'vnthong-q2',
'vnyn62',
'vps98',
'vudaivnpt2017',
'xuan3105',
'xuanhip3082735') and thang = 202405 and loai_tinh ='DONGIATRA_OB_BS';
COMMIT;
CREATE TABLE CT_dONGIA_TRATRUOC_OB AS;
select* from TTKD_bSC.ct_dongia_Tratruoc where thang = 202405 and thuebao_id =9272248	;
DELETE
--SELECT* 
FROM TTKD_bSC.CT_DONGIA_TRATRUOC A WHERE THANG = 202405 AND LOAI_TINH = 'DONGIATRA_OB' 
AND EXISTS (SELECT 1 FROM TTKD_BSC.CT_DONGIA_tRATRUOC WHERE THANG = 202405 AND LOAI_tINH = 'DONGIATRA_OB_BS' AND A.THUEBAO_ID = THUEBAO_ID AND A.MA_NV= MA_nV AND TIEN_KHOP > 0 );
select thuebao_id, ma_nv from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202405 and loai_tinh in ('DONGIATRA_OB','DONGIATRA_OB_BS') and tien_khop >1 group by thuebao_id, ma_nv having count(Thuebao_id) > 1;
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202405 and thuebao_id =9001872	;
select to_Date(ngay_tt,'dd-mm-yyyy') from nv_capnhat;

select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a where thang = 202405 and not exists (select 1 from ttkd_Bsc.ct_Dongia_tratruoc where thang >= 202404 and loai_tinh != 'DONGIATRA30D'
and thuebao_id = a.thuebao_id);
select count (distinct thuebao_id)  from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB';
select*  from ttkd_Bsc.nhuy_Ct_bsc_ipcc_obghtt  where thang = 202405 and tien_khop > 1 
and thuebao_id not in (select thuebao_id from ttkd_Bsc.CT_dONGIA_TRATRUOC where thang = 202405 and loai_tinh = 'DONGIATRA_OB')
and thuebao_id not in (select thuebao_id from ttkd_Bsc.ct_dongia_tratruoc where thang = 202404 and loai_tinh = 'DONGIATRA_OB' and tien_khop > 0)
and thuebao_id not in (select thuebao_id from ttkd_Bsc.ct_dongia_tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB_BS' and tien_khop > 0)
and thuebao_id not in (select thuebao_id from ttkd_Bsc.ct_dongia_tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB_NSG_H' and tien_khop > 0);
delete from ttkd_bsc.tl_giahan_tratruoc where thang =202405 and loai_tinh = 'DONGIATRA_OB';
select* from ttkd_Bsc.CT_dONGIA_TRATRUOC where thang = 202405 and loai_tinh = 'DONGIATRA_OB';