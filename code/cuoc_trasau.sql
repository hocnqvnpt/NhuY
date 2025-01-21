select a.thuebao_id, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) cuoc_tg
  , decode(skm.tyle_sd, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg), 0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) - skm.cuoc_sd, (100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg)/100) cuoc_saukm
 from ...
left join css_hcm.bd_goi_dadv goi on a.thuebao_id = goi.thuebao_id and goi.trangthai = 1 and goi_id not between 1715 and 1726
  left join css_hcm.goi_dadv_lhtb goi1 on goi.goi_id = goi1.goi_id and d.tocdo_id = goi1.tocdo_id and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1

left join (select thuebao_id, loaitb_id, tyle_sd, cuoc_sd, congvan_id from v_thongtinkm_all
                                                            where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11)
                                                            and 202309 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
                                                            ) skm on a.thuebao_id = skm.thuebao_id and skm.loaitb_id in (58, 59)