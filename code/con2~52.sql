DELETE FROM ttkd_Bsc.dongia_Vttp WHERE LOAI_tINH = 'DONGIA_BOCAU';
select a.ma_nv, a.ten_nv, a.so_dt,b.ten_dv,c.ten_Dv donvi_cha, a.email, a.ngay_login, a.gioitinh, a.ngay_sn , d.ma_nd, d.trangthai , e.hinhthuc_hd
from admin.v_nhanvien@dataguard a
    left join admin.v_donvi@dataguard b on a.donvi_id = b.donvi_id
    left join admin.v_donvi@dataguard c on b.donvi_cha_id = c.donvi_id
    left join admin.v_nguoidung@dataguard d on a.nhanvien_id = d.nhanvien_id
    left join hrm.hinhthuc_Hd@dataguard e on a.hthd_id = e.hthd_id
where c.donvi_Cha_id IN (893845,283425,283413) and d.trangthai = 1;
COMMIT;
insert into ttkd_Bsc.dongia_Vttp(THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, 
TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, TIEN_XHDON)
with ds as (
    select* from ttkd_bsc.ct_dongia_tratruoc a where thang in   (202406,202407,202408,202409,202410) and loai_tinh = 'DONGIATRA_OB' and 
        ma_nv in  ('daily_bocau','daily_bocau3','daily_bocau2')
)
    select ds.THANG,'DONGIA_BOCAU' LOAI_TINH, MA_KPI, ds.MA_NV, THUEBAO_ID, MA_TB,ds.MA_NV MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO_dichvu, TIEN_KHOP,
            round(nvl(tien_thuyetphuc*heso_chuky*heso_dichvu,0)) tien_tp
            , b.ten_nv, dv1.ten_dv dv_cap1, dv1.ten_dvql dv_cap2, dv1.donvi_cha_id, heso_chuky, null,  null tien_xp
    from ds
        left join ttkd_bsc.nhanvien b on ds.ma_nv= b.ma_Nv and ds.thang = b.thang and donvi = 'VTTP'
        left join admin_hcm.nhanvien_onebss nv on ds.ma_nv = nv.ma_nv
        left join admin_hcm.donvi dv1 on nv.donvi_id = dv1.donvi_id
;
COMMIT;
with tien as (
    select thang,ma_Nv,TEN_nV,DV_CAP1 TEN_PB, tien_dongia
    from ttkd_Bsc.dongia_vttp 
    where loai_tinh in ( 'DONGIA_BOCAU')  

)
,sl as (
    select ma_tb, ma_nv,dthu, thang
    from  ttkd_Bsc.dongia_vttp 
    where loai_tinh in ( 'DONGIA_BOCAU')
)
,abc as (
    select ma_nv, count(distinct ma_tb) sltb, sum(dthu) dthu , THANG from sl group by ma_nv, THANG
) 

select  a.ma_nv,A.ten_nv, A.ten_pb,  abc.sltb, abc.dthu,a.thang,sum(tien_dongia) tien
from tien a
--      join ttkd_Bsc.nhanvien_vttp_potmasco b on a.ma_nv = b.ma_Nv and a.thang = b.thang 
      left join abc on a.ma_nv = abc.ma_nv AND A.THANG = ABC.THANG
group by a.ma_nv, A.ten_nv, A.ten_pb,abc.sltb, abc.dthu, a.thang
having sum(tien_dongia)>  0;