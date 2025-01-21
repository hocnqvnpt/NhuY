with trangthaiHD as (select a.TRANGTHAITB_ID, a.TRANGTHAI_TB, f.NGAY_SD
from css_hcm.trangthai_tb a
    join css_hcm.db_thuebao f on a.trangthaitb_id = f.trangthaitb_id 
    where a.trangthaitb_id NOT IN (7, 8, 9)
    )
SELECT a.ma_kh, b.ma_tt, c.ma_tb, c.thuebao_id, c.ngay_sd,c.diachi_tb,d.trangthaitb_id,count(d.trangthai_tb)
FROM css_hcm.db_khachhang a 
JOIN css_hcm.db_thanhtoan b ON a.khachhang_id = b.KHACHHANG_ID
JOIN css_hcm.db_thuebao c ON c.thanhtoan_id = b.thanhtoan_id
join trangthaiHD d on c.ngay_sd = d.ngay_sd
WHERE c.ngay_sd BETWEEN TO_DATE('2024-02-01', 'YYYY-MM-DD') AND TO_DATE('2024-02-29', 'YYYY-MM-DD')
group by a.ma_kh, b.ma_tt, c.ma_tb, c.thuebao_id, c.ngay_sd,c.diachi_tb,d.trangthaitb_id

;
—---------------------+
    select thuebao_id from (
    with trangthaiHD as (select f.thuebao_id,a.TRANGTHAITB_ID, a.TRANGTHAI_TB, f.NGAY_SD
    from css_hcm.trangthai_tb a
        join css_hcm.db_thuebao f on a.trangthaitb_id = f.trangthaitb_id 
        where a.trangthaitb_id NOT IN (7, 8, 9)
        )
    SELECT a.ma_kh, b.ma_tt, c.ma_tb, d.thuebao_id, c.ngay_sd,c.diachi_tb,d.trangthaitb_id
    FROM css_hcm.db_khachhang a 
    JOIN css_hcm.db_thanhtoan b ON a.khachhang_id = b.KHACHHANG_ID
    JOIN css_hcm.db_thuebao c ON c.thanhtoan_id = b.thanhtoan_id
    join trangthaiHD d on c.thuebao_id = d.thuebao_id
    WHERE d.ngay_sd BETWEEN TO_DATE('2024-02-01', 'YYYY-MM-DD') AND TO_DATE('2024-02-29', 'YYYY-MM-DD')) group by thuebao_id having count(thuebao_id) >1
;

select* from css_hcm.duan

update ghep_duan a
set ten_du_an = (select ten_duan from css.v_duan@dataguard where a.ma_du_an = ma_duan)
commit;
select * from ttkdhcm_ktnv.DM_MVIEW where MVIEW_NAME= upper('diachi')
update ghep_duan
set duong = (select 
            from 
            select* from css_hcm.pho where ten_pho like '%??ng%'

select a.TRANGTHAITB_ID, a.TRANGTHAI_TB, b.NGAY_SD from css_hcm.trangthai_tb a
    join css_hcm.db_thuebao b  using (trangthaitb_id) 
    where trangthaitb_id = 1
    
    select* from  css_hcm.phuong;
SELECT phanvung_id, duan_id, ma_duan, ten_duan, diachi,
       chudtu_id, phuong_id, quan_id, ghichu, ngay_cn,
       may_cn, nguoi_cn, ip_cn, ttda_id, ngay_tk, ngay_ht,
       ttvt_id, nguoi_cctt, htht_id, sohieu_ct, ma_ct,
       soqd_dt, soqd_qt, ngay_nt, diachi_id, loaihinhkd_id
  FROM css.v_duan@dataguard
  WHERE phanvung_id = 28 and duan_id >760;
select* from css_hcm.khuvuc_dv 54169	4004;
with kvttvt as
(select kv.KHUVUC_ID, kv.MA_KV, kv.TEN_KV khuvuc, il.ten_kv to_vt, ik.ten_kv ten_ttvt, kvpx.quan_id, kvpx.phuong_id--, ik.*
                                , row_number() over (partition by kvpx.quan_id, kvpx.phuong_id order by ik.khuvuc_id) rnk
    from css_hcm.khuvuc kv
             left join css_hcm.KHUVUC_LKV kl on kl.loaikv_id = 4 and kv.khuvuc_id = kl.khuvuc_id
             left join css_hcm.khuvuc il on kv.khuvuc_cha_id = il.khuvuc_id
             left join css_hcm.khuvuc ik on il.khuvuc_cha_id = ik.khuvuc_id
             left join css_hcm.khuvuc_px kvpx on kvpx.khuvuc_id = kv.khuvuc_id
    where ik.khuvuc_id is not null
) 
select b.duan_id, a.ma_du_an,b.diachi,b.chudtu_id, b.ten_duan, q.ten_quan, p.ten_phuong, d.ten_pho, LOAIHINHKD_ID, e.khuvuc_id, dv.donvi_id, p.phuong_id, q.quan_id
      ,  b.ttvt_id
from ghep_duan a
    left join css.v_duan@dataguard b on a.ma_du_an = b.ma_duan
    left join css_hcm.diachi c on b.diachi_id = c.diachi_id
    left join css_hcm.quan q on c.quan_id = q.quan_id
    left join css_hcm.phuong p on c.phuong_id = p.phuong_id
    left join css_hcm.pho d on c.pho_id = d.pho_id
    left join kvttvt e on q.quan_id = e.quan_id and p.phuong_id = e.phuong_id and rnk = 1
    left join css_hcm.khuvuc_dv dv on e.khuvuc_id = dv.khuvuc_id and dv.loaidv_id = 5
    left join admin_hcm.donvi ttvt on dv.donvi_id = ttvt.donvi_id
    