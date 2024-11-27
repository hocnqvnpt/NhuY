insert into phithu_tainha(LOAIDV, PHIEU_ID, MA_TT, MA_TB, DICHVUVT_ID, GHICHU, NGAY_CN, CHUKYNO, MANV_TC, TENNV_TC, MA_TN, TEN_TN, MA_ND, TEN_ND, TENTO, TEN_DV, TRAGOC, 
--TRATHUE, TONGTRA, NGAY_LAYSL, THANG_TAMTHU, TENDV_CS, MANV_CS, TENNV_CS, MANV_TCTN, TENNV_TCTN, THANG)
TRATHUE, TONGTRA, NGAY_LAYSL, THANG_TAMTHU, THANG)
--update phithu_tainha set thang = 202404;
--select* from phithu_tainha;
--alter table phithu_tainha add(thang number(6));
select decode(b.dichvuvt_id,2,'VNP','CDH')loaidv, a.phieu_id, a.ma_tt, b.ma_tb, b.dichvuvt_id, a.ghichu, to_char(TRUNC(a.ngay_cn),'dd/mm/yyyy') ngay_cn, b.chukyno
  , a.manv_tc, j.ten_nv tennv_tc
  , a.ma_tn, d.ten_nv ten_tn
  , e.ma_nd, e.ten_nd
  , c.ten_dv tento, h.ten_dv
  , sum(tragoc)tragoc, sum(trathue)trathue, sum(tragoc+trathue)tongtra
  , TO_CHAR(sysdate, 'DD-MM-YYYY HH:MI:SS') as ngay_laysl, to_number(to_char(sysdate,'yyyymm')) thang_tamthu
--  , k.tendv_cs, k.manv_cs, k.tennv_cs, k.manv_tctn, k.tennv_tctn
  , 202410 thang
from qltn.v_bangphieutra@dataguard a, qltn.v_ct_tra@dataguard b
    ,admin_hcm.donvi c
    ,admin_hcm.nhanvien d
    ,admin_hcm.nguoidung e
    ,qltn_hcm.hinhthuc_tt f
    ,admin_hcm.donvi h
    ,admin_hcm.nhanvien j
    ,qlc_hcm.cs_online k
where a.phieu_id=b.phieu_id and a.phanvung_id=28 and a.phanvung_id=b.phanvung_id
    and a.ky_cuoc=20240901 and a.ky_cuoc=b.ky_cuoc
    and (b.tragoc+b.trathue-b.chihoahong)<>0
    and a.quaythu_id=c.donvi_id(+)
    and a.ma_tn=d.ma_nv(+)
    and a.nguoidung_id=e.nguoidung_id(+)
    and a.httt_id=f.httt_id(+)
    and c.donvi_cha_id=h.donvi_id(+)
    and a.manv_tc=j.ma_nv(+)
    and b.khoanmuctt_id=3130 and a.httt_id=127
    and (b.tragoc+b.trathue) in (20000,22000,24000)
    and a.ghichu not like '%KHYCTL%'
    and NOT (b.tragoc<>8000 and b.dichvuvt_id not in (4,11))
    and a.ma_tt=k.ma_tt(+) and b.ma_tb=k.ma_tb(+) and b.dichvuvt_id=k.dichvuvt_id(+)
group by decode(b.dichvuvt_id,2,'VNP','CDH'), a.phieu_id, a.ma_tt, b.ma_tb, b.dichvuvt_id, a.ghichu, a.ngay_cn, b.chukyno
  , a.manv_tc, j.ten_nv, a.ma_tn, d.ten_nv, e.ma_nd, e.ten_nd, c.ten_dv, h.ten_dv
--  , k.tendv_cs, k.manv_cs, k.tennv_cs, k.manv_tctn, k.tennv_tctn
order by a.ma_tt,a.ngay_cn;
commit;
select* from phithu_tainha where thang = 202406;