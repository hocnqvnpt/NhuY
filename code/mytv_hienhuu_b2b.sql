select * from nhuy.v_Thongtinkm_all@ttkddb where nhom_datcoc_id=1 and cuoc_dc > 0 and 202411 ------ Doi thang n
between thang_bddc and least(thang_ktdc, nvl(thang_huy, 99999999), nvl(thang_kt_dc, 999999999)) 
union all
select * from nhuy.v_Thongtinkm_all@ttkddb where nhom_datcoc_id=1 and (tyle_sd=100 or tyle_tb=100) and 202411  -- Doi thang n
between thang_bd_mg and least(thang_kt_mg, nvl(thang_huy, 99999999), nvl(thang_kt_dc, 999999999)) ;

with skm as(select thuebao_id, loaitb_id, nvl(tyle_sd, tyle_tb) tyle_tb, nvl(cuoc_sd, cuoc_tb) cuoc_tb, congvan_id from v_thongtinkm_all
                                                where ((tyle_sd <> 100 and tyle_sd + cuoc_sd > 0) or (tyle_tb <> 100 and tyle_tb + cuoc_tb > 0)) --and thang_bddc >= 202002
                                                        and 202411 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
                                                ),
 km as (select distinct km.thuebao_id, km.loaitb_id
        from v_thongtinkm_all km
        where --Thay doi thang
                    202411 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
 ) ,
ldm as (
    select distinct a.donvi_id, thuebao_id
    from css.v_hd_khachhang@dataguard a
        left join css.v_hd_thuebao@dataguard b on a.hdkh_id = b.hdkh_id
    where loaihd_id = 1 and tthd_id = 6
)
select a.thuebao_id, a.ma_tb, a.ngay_sd, e.thuonghieu, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tb) cuoc_tg
                , decode(skm.tyle_tb, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tb), 0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tb) - skm.cuoc_tb, (100 - skm.tyle_tb) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tb)/100) cuoc_saukm
              , nvl2(km.thuebao_id,1,0) tratruoc
            , nvl(ten_goi.ma_goi, 0) goi_tichhop, a.thuebao_cha_id, d.madoicap, d.tocdo_id, e.tocdothuc, e.iptinh, e.soluong_ip, d.muccuoc_id, d.trangbi_id, a.mucuoctb_id, a.doituong_id, g.khu_id khuvuc_ld
            , goi.nhomtb_id, tbi.tentrangbi, kh.ma_kh,kh.ten_kh, tt.trangthai_tb, a.ngay_Cat, q.tenchuquan, ldm.donvi_id, nv.ten_dvql,s.diachi_ld
            , pbh.ten_dv donvi_tiepthi--, hdtb.kieuld_id, hdkh.loaihd_id
from css_hcm.db_thuebao a
                join css_hcm.db_adsl d on a.thuebao_id = d.thuebao_id
                join css_hcm.trangbi tbi on d.trangbi_id = tbi.trangbi_id
                join css_hcm.tocdo_adsl e on d.tocdo_id = e.tocdo_id
                join css_hcm.muccuoc_tb f on a.mucuoctb_id = f.mucuoctb_id
                left join css_hcm.diachi_tb i on a.thuebao_id = i.thuebao_id
                left join css_hcm.diachi g on i.diachild_id = g.diachi_id
                left join css_hcm.bd_goi_dadv goi on a.thuebao_id = goi.thuebao_id and goi.trangthai = 1 and goi.goi_id not in (15414) and goi.goi_id < 100000
                left join css_hcm.goi_dadv ten_goi on goi.goi_id = ten_goi.goi_id
                left join css_hcm.goi_dadv_lhtb goi1 on goi.goi_id = goi1.goi_id and d.tocdo_id = goi1.tocdo_id and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1
                                                                                                   -- and goi1.goi_id not in (select goi_id from css_hcm.goi_dadv where goi_neo_id is not null)
                left join km on a.thuebao_id = km.thuebao_id and km.loaitb_id = 61
               left join  skm on a.thuebao_id = skm.thuebao_id and skm.loaitb_id = 61
               left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
               left join css_hcm.trangthai_tb tt on a.trangthaitb_id = tt.trangthaitb_id
               left join css_hcm.chuquan q on d.chuquan_id = q.chuquan_id 
               left join ldm on a.thuebao_id = ldm.thuebao_id
               left join admin_hcm.donvi nv on ldm.donvi_id = nv.donvi_id
               left join css_hcm.db_thuebao_sub s on a.thuebao_id = s.thuebao_id
               left join admin_hcm.donvi  pbh on nv.donvi_cha_id = pbh.donvi_id
               -- left join (select thuebao_id, count(thang_tru_cuoi) times from v_thongtinkm_all where thang_tru_cuoi >= thang_bddc and cuoc_dc > 0 group by thuebao_id) tt on a.thuebao_id = tt.thuebao_id 
where thuonghieu like 'B2B%'
--     kh.ma_Kh in ('HCM014465780','HCM014822516','HCM002508784','HCM002604557','HCM015035303','HCM015423736','HCM015332638','HCM015726182')
and a.loaitb_id in ( 61,171) and  a.trangthaitb_id not in (7, 8, 9)