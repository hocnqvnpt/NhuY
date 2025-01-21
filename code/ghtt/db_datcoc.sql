with hdtb as (select hdkh_id, hdtb_id, thuebao_id, ma_tb, loaitb_id, kieuld_id, tthd_id, ngay_ht
                                            , row_number() over (partition by thuebao_id order by KIEULD_ID) rkn
                                    from css_hcm.hd_thuebao
                                    where kieuld_id in (51, 280, 13102)
                            )
SELECT hdkh.hdkh_id, hdtb.hdtb_id,hdkh.ngay_yc,hdtb.thuebao_id, hdtb.ma_tb, kld.TEN_KIEULD, kld.kieuld_id
from css_hcm.hd_khachhang hdkh
                left join hdtb on hdtb.rkn = 1 and hdkh.hdkh_id = hdtb.hdkh_id
                left join css_hcm.kieu_ld kld on hdtb.kieuld_id = kld.kieuld_id
where hdtb.ngay_ht between '01/09/2022' and '30/09/2022' and hdtb.loaitb_id = 58 
                and hdkh.loaihd_id in (1, 27) and hdtb.tthd_id = 6 --and thuebao_id = 9415816--and hdkh.hdkh_id = 16334063
                and hdtb.kieuld_id  in (51, 280,13102);
select thuebao_id from (
select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, cuoc_dc, ngay_huy, ngay_thoai
from css_hcm.db_datcoc 
where cuoc_dc > 0 and ngay_bddc < sysdate and  sysdate < least(nvl(ngay_thoai - 1, sysdate + interval '50' year ),ngay_ktdc) 
    and sysdate < least(nvl(ngay_huy - 1, sysdate + interval '50' year ),ngay_ktdc) and nhom_datcoc_id = 1-- and tien_thoai is not null  )
    
 ) 
 group by thuebao_id
having count(thuebao_id) > 1
----------------------
select thuebao_id from (
with dbdc as (select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, cuoc_dc, ngay_huy, ngay_thoai
from css_hcm.db_datcoc 
where cuoc_dc > 0 and ngay_bddc < sysdate and  sysdate < least(nvl(ngay_thoai - 1, sysdate + interval '50' year ),ngay_ktdc) 
    and sysdate < least(nvl(ngay_huy - 1, sysdate + interval '50' year ),ngay_ktdc) and nhom_datcoc_id = 1-- and tien_thoai is not null 
union all
select thuebao_id, rkm_id, ngay_bddc, ngay_ktdc, datcoc_csd, ngay_huy, ngay_thoai 
from css_hcm.khuyenmai_dbtb 
where  datcoc_csd > 0 and  sysdate < least(nvl(ngay_thoai - 1, sysdate + interval '50' year ),ngay_ktdc) 
    and sysdate < least(nvl(ngay_huy - 1, sysdate + interval '50' year ),ngay_ktdc)--  and tien_thoai is null 
),
 hdtb as (select hdkh_id, hdtb_id, thuebao_id, ma_tb, loaitb_id, kieuld_id, tthd_id, ngay_ht
                                            , row_number() over (partition by thuebao_id order by KIEULD_ID) rkn
                                    from css_hcm.hd_thuebao
                                    where kieuld_id in (51, 280, 13102)
)
SELECT hdkh.hdkh_id, hdtb.hdtb_id,hdkh.ngay_yc,hdtb.thuebao_id, hdtb.ma_tb, kld.TEN_KIEULD, kld.kieuld_id,dbdc.cuoc_dc, dbdc.ngay_bddc, mc.muccuoc, td.tocdo,td.thuonghieu
from  css_hcm.hd_khachhang hdkh
    left join hdtb on hdtb.rkn = 1 and hdkh.hdkh_id = hdtb.hdkh_id
    left join css_hcm.kieu_ld kld on hdtb.kieuld_id = kld.kieuld_id
    join css_hcm.db_thuebao dbtb on dbtb.thuebao_id = hdtb.thuebao_id
    left join dbdc on dbdc.thuebao_id = dbtb.thuebao_id
    left join css_hcm.muccuoc_tb mc on mc.mucuoctb_id = dbtb.mucuoctb_id
    left join css_hcm.tocdo_adsl td on mc.tocdo_id = td.tocdo_id
where hdtb.ngay_ht between '01/09/2022' and '30/09/2022' and hdtb.loaitb_id = 58 
                and hdkh.loaihd_id in (1, 27) and hdtb.tthd_id = 6 --and dbtb.trangthaitb_id = 1 --and thuebao_id = 9415816--and hdkh.hdkh_id = 16334063
                and hdtb.kieuld_id  in (51, 280,13102)
)
 group by thuebao_id
having count(thuebao_id) > 1
select* from css_hcm.muccuoc_Tb
select * from css_hcm.tocdo_adsl 