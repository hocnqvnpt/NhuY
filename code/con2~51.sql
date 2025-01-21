
-- BC NKT hóa ??n chi ti?t C?-BR
select* f
PROCEDURE BSS_96734
(in_phanvung_id number
,i_tungay IN VARCHAR2
,i_denngay IN VARCHAR2
,i_donvitt_id IN NUMBER
,i_httt IN NUMBER
  ,i_da_ttoan IN NUMBER --1:dã thanh toán; 2:MTB còn n?; 3:MTB có phi?u h?y; 4:MTT ti?n du d?u k?
   ,p_username IN VARCHAR2
  ,p_baocao OUT REFCURSOR)
IS    
   c INT;
   v_sql VARCHAR2(32000);    
   v_sqlwith VARCHAR2(32000); 
   v_donvi VARCHAR2(1000);
   v_tenbc VARCHAR2(200);
   t_data VARCHAR2(5000);   
   v_ddmmyyyy VARCHAR2(50):='01'||to_char(add_months(to_date(i_denngay,'dd/mm/yyyy'),-1),'mmyyyy');--l?y tháng-1 c?a i_denngay
   v_yyyymmdd VARCHAR2(50):=to_char(add_months(to_date(i_denngay,'dd/mm/yyyy'),-1),'yyyymm')||'01';--l?y tháng-1 c?a i_denngay
   v_dk VARCHAR2(200);
   v_dk1 VARCHAR2(200);
   v_dk2 VARCHAR2(200);
   p_nguoidung number;
BEGIN
  select nvl(nguoidung_id_goc,0) into p_nguoidung from admin.nguoidung where ma_nd = p_username;
   --v_tenbc:='Bao cao nhat ky thu hoa don chi tiet CDBR test(ctrkm)';
   --Xet nguoidung_id cap=2(Xu ly nguoidung chi xem duoc DULIEU_HCM cua PBH cua minh)    
   CASE
   WHEN p_nguoidung IN(5078) THEN 
      v_donvi:='(11441)';       --ToSBH NSG
   WHEN p_nguoidung=5997 THEN   --user cap PBH DN2 thay các don vi thuoc PBH DN2
      v_donvi:='(SELECT dv.donvi_id  --,dv.ten_dv to_cuahang,dvc.donvi_id phong_id,dvc.ten_dv phong
                 FROM admin.donvi dv,admin.donvi dvc
                 WHERE dvc.donvi_id=dv.donvi_cha_id AND dvc.donvi_id_goc=11563   --PBH DN2
                )';
   WHEN p_nguoidung IN(5149,2670) THEN  --user cap PBH DN3 thay các don vi thuoc PBH DN3
      v_donvi:='(SELECT dv.donvi_id  --,dv.ten_dv to_cuahang,dvc.donvi_id phong_id,dvc.ten_dv phong
                 FROM admin.donvi dv,admin.donvi dvc
                 WHERE dvc.donvi_id=dv.donvi_cha_id AND dvc.donvi_id_goc=11564   --PBH DN3
                 )';
   ELSE v_donvi:='('||i_donvitt_id||')'; --Cho user th?y các don v? c?p t?, nhu các toSBH
   END CASE;          
   
   IF i_da_ttoan=1 THEN --1:Da thu tien; 0: Chua thu tien(còn n? ???); 2:không bi?t(có phi?u h?y ???) 
     v_dk:='  AND b2.trangthai=1 AND DECODE('||i_httt||',0,0,b2.ht_tra_id )='||i_httt||' ';
     v_dk1:=' AND a3.trangthai=1 AND DECODE('||i_httt||',0,0,a3.ht_tra_id )='||i_httt||' ';
     v_dk2:=' AND b5.trangthai=1 AND DECODE('||i_httt||',0,0,b5.ht_tra_id )='||i_httt||' ';
   ELSE --không xét trangthai(có th? có tru?ng h?p dã thu ti?n MTB nhung MTB dó v?n còn n?)
     v_dk:='  AND DECODE('||i_httt||',0,0,b2.ht_tra_id )='||i_httt||' ';
     v_dk1:=' AND DECODE('||i_httt||',0,0,a3.ht_tra_id )='||i_httt||' ';
     v_dk2:=' AND DECODE('||i_httt||',0,0,b5.ht_tra_id )='||i_httt||' ';
   END IF;
   
   v_sqlwith:='
   with  phieutt_hd_ngaytt as (select  /*result cache*/ phieutt_id,hdkh_id ,donvi_tt_id ,ngay_tt , ht_tra_id ,kenhthu_id 
                           , trangthai , ma_gd , ghichu , sophieu , ngay_hd , soseri , seri , MST , diachi_kh , ten_kh , nguoi_cn , so_ct
                from css.phieutt_hd partition for ('||in_phanvung_id||') a3 
                  where  a3.ngay_tt >= TO_DATE('''||i_tungay||''',''DD/MM/YYYY'')
                  AND a3.ngay_tt <= TO_DATE('''||i_denngay||''',''DD/MM/YYYY'') +0.999   
                  AND DECODE('|| i_donvitt_id ||',0,0, a3.donvi_tt_id) IN'||v_donvi||' )
  , ct_phieutt_temp as (select  /*result cache*/ hdtb_id,khoanmuctt_id,tien, vat , phieutt_id 
            from css.ct_phieutt partition for ('||in_phanvung_id||') b3 
              where EXISTS (select 1 from phieutt_hd_ngaytt a where a.phieutt_id = b3.phieutt_id))
  , hd_thuebao_temp as (select  /*result cache*/ ma_tb,goi_id,nhomtb_id,loaitb_id ,hdkh_id , thuebao_id ,kieuld_id ,tthd_id ,hdtb_id
          from css.hd_thuebao partition for ('||in_phanvung_id||') d3
          where d3.dichvuvt_id NOT IN(14,15,16,13) 
          and EXISTS (select 1 from ct_phieutt_temp a where a.hdtb_id = d3.hdtb_id))
   ,nkt_cv1688_temp as (
   SELECT /*result cache*/ t_2.*
          ,CASE WHEN t_2.loaihd_id IN (1,3,6,8) THEN kmdbtb.rkm_id
           ELSE hdtbdc.rkm_id END AS rkm_id              
          ,CASE WHEN t_2.loaihd_id IN (1,3,6,8) THEN DECODE(kmdbtb.thang_bddc,NULL,kmdbtb.thang_bd,0,kmdbtb.thang_bd,kmdbtb.thang_bddc)
           ELSE hdtbdc.thang_bd END AS ky_batdau
          ,CASE WHEN t_2.loaihd_id IN (1,3,6,8) THEN DECODE(kmdbtb.thang_ktdc,NULL,kmdbtb.thang_kt,0,kmdbtb.thang_kt,kmdbtb.thang_ktdc)
           ELSE hdtbdc.thang_kt END AS ky_ketthuc , ctkm.huong_dc
       ,CASE WHEN t_2.loaihd_id IN (1,3,6,8) THEN kmdbtb.ngay_bddc
           ELSE hdtbdc.ngay_bddc END AS ngay_bddc 
       ,CASE WHEN t_2.loaihd_id IN (1,3,6,8) THEN kmdbtb.ngay_ktdc
           ELSE hdtbdc.ngay_ktdc END AS ngay_ktdc 
    FROM (--t_2
          SELECT b2.donvi_tt_id,dvi.ten_dv,dvc.ten_dv phong,b2.nguoi_cn nguoi_tt,b2.ten_kh,b2.diachi_kh,b2.mst,b2.ngay_tt,b2.seri,LPAD(b2.soseri,7,''0'') hoadon,b2.ngay_hd
                ,b2.sophieu,b2.ma_gd
                ,CASE b2.ht_tra_id 
                       WHEN 1 THEN ''TM''
                       WHEN 2 THEN ''CK''
                       WHEN 3 THEN ''TM''
                       WHEN 4 THEN ''UNC''
                       WHEN 5 THEN ''UNC''
                       WHEN 6 THEN ''TT''
                       WHEN 7 THEN ''PAY''
                       WHEN 8 THEN ''TGDD''
                       WHEN 9 THEN ''SHOP''
                       ELSE '''' 
                 END AS httt 
                ,kmtt.ma_kmtt,a2.loaihd_id,a2.hdtb_id,a2.thuebao_id,a2.ma_tb,a2.tienthu,a2.tien,a2.vat,ht.kenhthu,ma_tt,ma_nvtc,ten_nvtc
                ,(SELECT dm.ten_km 
                  FROM css.khuyenmai_hdtb partition for ('||in_phanvung_id||') km
          ,css.khuyenmai dm
                  WHERE km.hdtb_id = a2.hdtb_id AND dm.phanvung_id = km.phanvung_id  AND dm.khuyenmai_id = km.khuyenmai_id AND ROWNUM = 1)ghichu
                ,b2.ghichu ghichu_tt,lh.loaihinh_tb,b2.ma_gd magd_vnpt_pay,b2.phieutt_id,a2.ma_tiepthi,a2.ten_tiepthi,a2.goi_id,a2.nhomtb_id,a2.SO_CT
          FROM (--a2
                SELECT hdkh_id,loaihd_id,hdtb_id,thuebao_id,ma_tb,khoanmuctt_id,SUM(tienthu)tienthu,SUM(tien)tien,SUM(vat)vat
                      ,ma_tt,ma_nvtc,ten_nvtc,loaitb_id,ma_tiepthi,ten_tiepthi,goi_id,nhomtb_id , SO_CT
                FROM (SELECT a3.phieutt_id,a3.hdkh_id,k3.loaihd_id,b3.hdtb_id,d3.thuebao_id,d3.ma_tb,b3.khoanmuctt_id,d3.goi_id,d3.nhomtb_id ,a3.SO_CT
                            ,(b3.tien + b3.vat) tienthu,b3.tien,b3.vat,p3.ma_tt,r3.ma_nv ma_nvtc,r3.ten_nv ten_nvtc,d3.loaitb_id
                            ,(case when k3.ctv_id>0 then (select ma_nv from admin.nhanvien where nhanvien_id=k3.ctv_id and rownum=1) else null end) ma_tiepthi
                            ,(case when k3.ctv_id>0 then (select ten_nv from admin.nhanvien where nhanvien_id=k3.ctv_id and rownum=1) else null end) ten_tiepthi
                      FROM phieutt_hd_ngaytt a3
                          , ct_phieutt_temp b3
                          ,hd_thuebao_temp d3
                          ,css.db_thuebao partition for ('||in_phanvung_id||') e3
                          ,css.db_thanhtoan partition for ('||in_phanvung_id||') p3
                          ,css.tuyenthu q3
                          ,admin.nhanvien r3
                          ,css.hd_khachhang partition for ('||in_phanvung_id||') k3                                
                      WHERE  a3.phieutt_id = b3.phieutt_id
                        AND a3.hdkh_id = d3.hdkh_id
                        AND b3.hdtb_id = d3.hdtb_id
                        AND d3.hdkh_id = k3.hdkh_id
                        AND k3.loaihd_id <> 17
                        AND d3.thuebao_id = e3.thuebao_id(+)
                        AND e3.thanhtoan_id = p3.thanhtoan_id(+)
                        AND p3.tuyenthu_id = q3.tuyenthu_id(+) AND p3.phanvung_ID = q3.phanvung_ID(+)
                        AND q3.nhanvien_id = r3.nhanvien_id(+)
                        AND d3.kieuld_id <> 553   ';
                        ---AND a3.trangthai='||i_da_ttoan||'  ---1:Da thu tien; 0: Chua thu tien(còn n? ???); 2:không bi?t(có phi?u h?y ???) 
                        ---AND DECODE('||i_httt||',0,0, a3.ht_tra_id )='||i_httt||'
      v_sqlwith:=v_sqlwith||v_dk1;
      v_sqlwith:=v_sqlwith||'                         
                                             
                      UNION ALL 
                      --row thoai tra truoc khi KH tra tien: ton tai 1 row da thu tien; ngay_tt sau ngay thoai tra
                      SELECT a4.phieutt_id,a4.hdkh_id,k4.loaihd_id,b4.hdtb_id,c4.thuebao_id,c4.ma_tb,b4.khoanmuctt_id,c4.goi_id,c4.nhomtb_id ,a4.SO_CT
                            ,(b4.tien+b4.vat)tienthu,b4.tien,b4.vat,p4.ma_tt,r4.ma_nv ma_nvtc,r4.ten_nv ten_nvtc,c4.loaitb_id
                            ,(case when k4.ctv_id>0 then (select ma_nv from admin.nhanvien where nhanvien_id=k4.ctv_id and rownum=1) else null end) ma_tiepthi
                            ,(case when k4.ctv_id>0 then (select ten_nv from admin.nhanvien where nhanvien_id=k4.ctv_id and rownum=1) else null end) ten_tiepthi
                      FROM phieutt_hd_ngaytt a4
                          ,ct_phieutt_temp b4
                          ,hd_thuebao_temp c4
                          ,css.hd_khachhang partition for ('||in_phanvung_id||') k4
                          ,css.db_thuebao partition for ('||in_phanvung_id||') e4
                          ,css.db_thanhtoan partition for ('||in_phanvung_id||') p4
                          ,css.tuyenthu q4
                          ,admin.nhanvien r4                                       
                      WHERE a4.phieutt_id = b4.phieutt_id
                        AND b4.hdtb_id = c4.hdtb_id
                        AND a4.hdkh_id = k4.hdkh_id
                        AND k4.loaihd_id <> 17
                        AND c4.thuebao_id = e4.thuebao_id(+)
                        AND e4.thanhtoan_id = p4.thanhtoan_id(+)
                        AND p4.tuyenthu_id = q4.tuyenthu_id(+) AND p4.phanvung_id = q4.phanvung_id(+)
                        AND q4.nhanvien_id = r4.nhanvien_id(+)
                        AND c4.tthd_id = 7 
                        AND ht_tra_id = 6   
                        AND EXISTS (SELECT 1 
                                    FROM phieutt_hd_ngaytt b5  -- thanh toan sau khi thoai tra
                                    WHERE b5.hdkh_id = a4.hdkh_id
                                      AND b5.ht_tra_id <> 6                                      
                        ';
                                      ---AND b5.trangthai=1  ---1:dã thu ti?n; 0:chua thu ti?n
                                      ---AND DECODE('||i_httt||',0,0,b5.ht_tra_id)='||i_httt||'  
                    v_sqlwith:=v_sqlwith||v_dk2;
                    v_sqlwith:=v_sqlwith||'                                     
                                      --AND b5.ngay_tt >= TO_DATE('''||i_tungay||''',''DD/MM/YYYY'')
                                      --AND b5.ngay_tt <= TO_DATE('''||i_denngay||''',''DD/MM/YYYY'')+0.999
                                      AND b5.ngay_tt > a4.ngay_tt
                                   )                                          
                     )
                GROUP BY hdkh_id,loaihd_id,hdtb_id,thuebao_id,ma_tb,khoanmuctt_id,goi_id,nhomtb_id,ma_tt,ma_nvtc,ten_nvtc,loaitb_id,ma_tiepthi,ten_tiepthi ,SO_CT
               )a2
               ,phieutt_hd_ngaytt b2
               ,css.khoanmuc_tt  kmtt
               ,admin.donvi dvi
               ,admin.donvi dvc
               ,css.kenhthu ht
               ,css.loaihinh_tb lh
          WHERE a2.hdkh_id = b2.hdkh_id
            AND a2.khoanmuctt_id = kmtt.khoanmuctt_id and kmtt.phanvung_ID = '||in_phanvung_id||'
            AND b2.donvi_tt_id = dvi.donvi_id  
            AND dvi.donvi_cha_id = dvc.donvi_id
            --AND b2.donvi_tt_id = dvi.donvi_id(+)   ---lay luon donvi_tt_id NULL
            --AND dvi.donvi_cha_id = dvc.donvi_id(+) ---lay luon donvi_tt_id NULL
            AND b2.kenhthu_id = ht.kenhthu_id(+)
            --AND b2.ngay_tt >= TO_DATE('''||i_tungay||''',''DD/MM/YYYY'')
            --AND b2.ngay_tt <= TO_DATE('''||i_denngay||''',''DD/MM/YYYY'')+1
            ';
            ---AND b2.trangthai='||i_da_ttoan||' ---1:Da thu tien; 0: Chua thu tien(còn n? ???); 2:không bi?t(có phi?u h?y ???) 
            ---AND DECODE('||i_httt||',0,0,b2.ht_tra_id)='||i_httt||'
   v_sqlwith:=v_sqlwith||v_dk;
   v_sqlwith:=v_sqlwith||'
            --AND DECODE('||i_donvitt_id||',0,0,b2.donvi_tt_id) IN'||v_donvi||'            
            AND a2.tienthu <> 0
            AND b2.ht_tra_id <> 6
            AND a2.khoanmuctt_id NOT IN(27,36) ---10/9/2021:Tách ti?n Chi_ThayDoi_DC(khoanmuctt_id=36)qua Nh?t ký thu hóa don thoái tr?-CDBR(yc c?a P.KTNV)     
                                               ---T?i sao lo?i khoanmuctt_id <> 27 ?!(27:thu ti?n gói da DV)
            AND a2.loaitb_id = lh.loaitb_id(+)            
         )t_2
    LEFT JOIN css.hdtb_datcoc  partition for ('||in_phanvung_id||') hdtbdc ON hdtbdc.hdtb_id=t_2.hdtb_id
    left join css.ct_khuyenmai  partition for ('||in_phanvung_id||') ctkm ON hdtbdc.chitietkm_id=ctkm.chitietkm_id and hdtbdc.phanvung_id=ctkm.phanvung_id
    LEFT JOIN (
         SELECT hdtb_id,thuebao_id,thang_bd,thang_kt,thang_bddc,thang_ktdc,rkm_id, ngay_bddc , ngay_ktdc
         FROM ( 
            SELECT hdtb_id,thuebao_id,thang_bd,thang_kt,thang_bddc,thang_ktdc,rkm_id , ngay_bddc , ngay_ktdc
                  ,MAX(rkm_id) OVER (PARTITION BY thuebao_id) AS max_rkm_id
            FROM css.khuyenmai_dbtb   partition for ('||in_phanvung_id||')
            WHERE datcoc_csd>0 AND hieuluc=1
      union 
            select hdc.hdtb_id,ddc.thuebao_id,ddc.thang_bd,ddc.thang_kt,ddc.thang_bd thang_bddc,ddc.thang_kt thang_ktdc,hdc.rkm_id , ddc.ngay_bddc , ddc.ngay_ktdc
            ,MAX(ddc.rkm_id) OVER (PARTITION BY ddc.thuebao_id) AS max_rkm_id 
            from css.hdtb_datcoc partition for ('||in_phanvung_id||') hdc, css.db_datcoc  partition for ('||in_phanvung_id||') ddc
            where ddc.rkm_id = hdc.rkm_id
            and ddc.cuoc_dc>0 AND ddc.hieuluc=1
            )
         WHERE rkm_id=max_rkm_id   
    and EXISTS (select 1 from hd_thuebao_temp k where k.hdtb_id = k.hdtb_id)
      )kmdbtb ON kmdbtb.hdtb_id=t_2.hdtb_id AND kmdbtb.thuebao_id=t_2.thuebao_id           
   )';   
   
   v_sqlwith:=v_sqlwith||' , nkt_cv1688_temp2 as (select t.* 
        , case when t.rkm_id IS not  NULL then t.rkm_id else (SELECT MAX(rkm_id) FROM css.khuyenmai_dbtb partition for ('||in_phanvung_id||') WHERE thuebao_id=t.thuebao_id AND hdtb_id=t.hdtb_id) END rkm_id2
        , case when t.rkm_id IS not  NULL then  t.ky_batdau else(SELECT DECODE(thang_bddc,NULL,thang_bd,0,thang_bd,thang_bddc) FROM css.khuyenmai_dbtb partition for ('||in_phanvung_id||') WHERE thuebao_id=t.thuebao_id AND hdtb_id=t.hdtb_id AND rownum=1) END ky_batdau2
        , case when t.rkm_id IS not  NULL then  t.ky_ketthuc else (SELECT DECODE(thang_ktdc,NULL,thang_kt,0,thang_kt,thang_ktdc) FROM css.khuyenmai_dbtb partition for ('||in_phanvung_id||') WHERE thuebao_id=t.thuebao_id AND hdtb_id=t.hdtb_id AND rownum=1) end ky_ketthuc2
         from nkt_cv1688_temp t 
         ) ';
     

   v_sqlwith:=v_sqlwith||' , nkt_cv1688_temp3 as (select t.* 
        , case when  t.huong_dc is not null then  t.huong_dc else (SELECT c.HUONG_DC FROM css.khuyenmai_dbtb  partition for ('||in_phanvung_id||') a 
                                  , css.ct_khuyenmai  partition for ('||in_phanvung_id||') c  
                                  WHERE a.chitietkm_id = c.chitietkm_id AND a.rkm_id=t.rkm_id AND rownum=1) end HUONG_KM2
         from nkt_cv1688_temp2 t 
         ) ';

   --DBMS_OUTPUT.ENABLE(1000000000);
  -- DBMS_OUTPUT.PUT_LINE( v_sql);
  -- return;
   /*
   
   EXECUTE IMMEDIATE 'CREATE TABLE baocao_pttb.nkt_cv1688_temp AS SELECT * FROM('||v_sql||')';
   COMMIT;    
   update cac truong hop con rkm_id null 
   EXECUTE IMMEDIATE '
    UPDATE baocao_pttb.nkt_cv1688_temp 
    SET rkm_id=(SELECT MAX(rkm_id) FROM css.khuyenmai_dbtb WHERE thuebao_id=nkt_cv1688_temp.thuebao_id AND hdtb_id=nkt_cv1688_temp.hdtb_id)
       ,ky_batdau=(SELECT DECODE(thang_bddc,NULL,thang_bd,0,thang_bd,thang_bddc) FROM css.khuyenmai_dbtb WHERE thuebao_id=nkt_cv1688_temp.thuebao_id AND hdtb_id=nkt_cv1688_temp.hdtb_id AND rownum=1)
       ,ky_ketthuc=(SELECT DECODE(thang_ktdc,NULL,thang_kt,0,thang_kt,thang_ktdc) FROM css.khuyenmai_dbtb WHERE thuebao_id=nkt_cv1688_temp.thuebao_id AND hdtb_id=nkt_cv1688_temp.hdtb_id AND rownum=1)
    WHERE rkm_id IS NULL ';
   COMMIT;
   
   */
   v_sql:='
   
   SELECT DISTINCT donvi_tt_id,t.ten_dv --,phong.ten_dv phong
                ,t.nguoi_tt,p.phong_bh,dvi.ttvt_pbh donvi_nguoitt
                ,t.ten_kh,t.diachi_kh,t.mst,TO_CHAR(t.ngay_tt,''DD/MM/YYYY HH24:MI:SS'') ngay_tt,t.seri,t.hoadon,TO_CHAR(t.ngay_hd,''DD/MM/YYYY'') ngay_hd,t.sophieu,t.ma_gd,t.magd_vnpt_pay,t.httt
                ,CASE WHEN hd.httt_id IN(2,5) THEN ''H'' ELSE ''G'' END ma_gh ---Note:css.hinhthuc_tt;maH(httt_id=2,5): T?i d?a ch? thanh toán; maG(httt_id<>2,5): Còn l?i
                ,logpay.transactionid diemcham_tt,t.ma_kmtt,t.loaihd_id,t.hdtb_id,t.thuebao_id,t.ma_tb
                ,t.tienthu,t.tien,t.vat,t.kenhthu,t.ma_tt,t.ma_nvtc,t.ten_nvtc,t.ghichu,t.ghichu_tt,t.loaihinh_tb
                ,t.rkm_id2 rkm_id,TO_CHAR(t.ky_batdau2) ky_batdau,TO_CHAR(t.ky_ketthuc2) ky_ketthuc
                --,MONTHS_BETWEEN(TO_DATE(t.ky_ketthuc2,''yyyymm''),TO_DATE(t.ky_batdau2,''yyyymm''))+1 so_thang
        ,t.HUONG_KM2 so_thang
                ,t.ma_tiepthi,t.ten_tiepthi,dvtt.ten_dv donvi_tiepthi,dvtt.phong_tiepthi
                ,gddv.ten_ctkm --,gddv.ten_goi,gddv.nhomtb_id nhom
                ---ghép chu?i 2 c?t:ten_goi và nhomtb_id khi trùng các c?t còn l?i ngo?i tr? ten_goi và nhomtb_id
                --,LISTAGG(gddv.ten_goi,'' ; '') WITHIN GROUP(ORDER BY t.ma_gd,t.hdtb_id,t.thuebao_id,t.ma_tb,t.loaihinh_tb,gddv.ten_ctkm) AS ten_goi
                --,LISTAGG(gddv.nhomtb_id,'' ; '') WITHIN GROUP(ORDER BY t.ma_gd,t.hdtb_id,t.thuebao_id,t.ma_tb,t.loaihinh_tb,gddv.ten_ctkm) AS nhom
        ,t.SO_CT , t.ngay_bddc , t.ngay_ktdc
         FROM nkt_cv1688_temp3 t
           LEFT JOIN(select ma_tt,phieu_id,kyhoadon,to_char(ngay_cn,''dd/mm/yyyy hh24:mi:ss'') ngay_cn,transactionid
                     from qltn.logs_vnptpay  partition for ('||in_phanvung_id||')                           
            )logpay ON logpay.ma_tt=t.ma_tt AND logpay.phieu_id=t.phieutt_id  
           ---23/5/2022 bs thông tin gói da dv: Ten-CTKM(ten chtrinh km),Ten goi,Nhom(NHOMTB_ID) 
           LEFT JOIN(SELECT  a.hdtb_id,hd.thuebao_id,hd.ma_tb,a.goi_id,a.nhomtb_id,a.chunhom,k.khuyenmai_id
                           ,c.ten_km ten_ctkm,d.ten_goi,hd.ghichu
                     FROM css.hdtb_goi_dadv  partition for ('||in_phanvung_id||') a --hdtb_id,goi_id,nhomtb_id,chunhom 
                         ,css.hd_thuebao  partition for ('||in_phanvung_id||') hd   --hdtb_id,thuebao_id,ma_tb
                         ,css.bd_goi_dadv  partition for ('||in_phanvung_id||') bd  --ma_tb,thuebao_id,goi_id,nhomtb_id,chunhom,thang_bd,thang_kt 
                         --,css.km_goi_dadv b   --khuyenmai_id,goi_id 
                         ,css.khuyenmai c     --khuyenmai_id,ten_km
                         ,css.goi_dadv  d      --goi_id,ten_goi    
                         ,css.khuyenmai_hdtb  partition for ('||in_phanvung_id||') k --hdtb_id,khuyenmai_id                     
                     WHERE hd.hdtb_id=a.hdtb_id  and hd.phanvung_id=a.phanvung_id 
                       AND a.hdtb_id=k.hdtb_id(+) AND a.phanvung_id=k.phanvung_id(+)
                       AND hd.thuebao_id=bd.thuebao_id(+) AND hd.phanvung_id=bd.phanvung_id(+) --and hd.ma_tb=bd.ma_tb(+) 
                       AND k.khuyenmai_id=c.khuyenmai_id AND k.phanvung_id=c.phanvung_id
                       AND a.goi_id=d.goi_id AND a.phanvung_id=d.phanvung_id 
             AND k.datcoc_csd>0 ---30/6/2022 thêm dk cùng HDTB_ID n?u có nhi?u KHUYENMAI_ID thì ch? l?y các dòng KHUYENMAI_ID có DATCOC_CSD >0
             and EXISTS (select 1 from hd_thuebao_temp k where k.hdtb_id = a.hdtb_id)
             and EXISTS (select 1 from hd_thuebao_temp k where k.hdtb_id = k.hdtb_id)
             group by  a.hdtb_id,hd.thuebao_id,hd.ma_tb,a.goi_id,a.nhomtb_id,a.chunhom,k.khuyenmai_id
                           ,c.ten_km,d.ten_goi,hd.ghichu
           )gddv ON gddv.hdtb_id=t.hdtb_id         
           ---11/7/2022 bs c?t ttvt_pbh can c? c?t nguoi_tt(ma_nd)
           LEFT JOIN(SELECT a.ma_nd,b.donvi_id,c.donvi_cha_id,d.ten_dv ttvt_pbh
                     FROM admin.nguoidung a
                      LEFT JOIN admin.nhanvien b ON b.nhanvien_id=a.nhanvien_id
                      LEFT JOIN admin.donvi c ON c.donvi_id=b.donvi_id
                      LEFT JOIN admin.donvi d ON d.donvi_id=c.donvi_cha_id     
            where a.phanvung_ID = '||in_phanvung_id||'            
           )dvi ON dvi.ma_nd=t.nguoi_tt   
           /* --ch?nh không l?y d? li?u Ð?i lý chung chung 
           LEFT JOIN(SELECT a.ma_nd,b.donvi_id,c.donvi_cha_id,d.ten_dv ttvt_pbh
                     FROM admin.nguoidung a
                      LEFT JOIN admin.nhanvien b ON b.nhanvien_id=a.nhanvien_id
                      LEFT JOIN admin.donvi c ON c.donvi_id=b.donvi_id
                      LEFT JOIN admin.donvi d ON d.donvi_id=c.donvi_cha_id
                     WHERE a.nhanvien_id IN(SELECT nhanvien_id FROM admin.nhanvien_lnv WHERE loainv_id = 40) 
           )dvi ON dvi.ma_nd=t.nguoi_tt
           */
           ---L?y don v? theo ma_nd
           LEFT JOIN(SELECT b.ma_nd,c.donvi_id to_id,d.ten_dv ten_to,d.donvi_cha_id phong_id,ph.ten_dv phong_bh 
              FROM admin.nguoidung_dl a,admin.nguoidung b,admin.nhanvien c,admin.donvi d,admin.donvi ph
              WHERE a.nguoidung_id = b.nguoidung_id
                AND b.nhanvien_id = c.nhanvien_id
                AND b.nhanvien_id IN(SELECT nhanvien_id FROM admin.nhanvien_lnv WHERE loainv_id = 40)
                AND c.donvi_id=d.donvi_id AND ph.donvi_id=d.donvi_cha_id
        and c.phanvung_ID = '||in_phanvung_id||'
           )p ON p.ma_nd=t.nguoi_tt
           ---1/8/2022 BS c?t ma_gh
           LEFT JOIN qltn.hoadon hd ON hd.ma_tt=t.ma_tt
           ---5/8/2022 bs c?t donvi_tiepthi
           LEFT JOIN(SELECT a.ma_nv,b.ten_dv,c.ten_dv phong_tiepthi 
                     FROM admin.nhanvien a 
                      LEFT JOIN admin.donvi b ON b.donvi_id=a.donvi_id
                      LEFT JOIN admin.donvi c ON c.donvi_id=b.donvi_cha_id
            where a.phanvung_ID = '||in_phanvung_id||'
                    )dvtt ON dvtt.ma_nv=t.ma_tiepthi
           
           /*---Can c? ma_dl(c?t nguoi_tt) l?y tên phòng         
           LEFT JOIN(SELECT b.ma_nd,a.daily_id,c.donvi_id tosbh_id,d.ten_dv 
              FROM admin.nguoidung_dl a,admin.nguoidung b,admin.nhanvien c,admin.donvi d
              WHERE a.nguoidung_id=b.nguoidung_id AND b.nhanvien_id=c.nhanvien_id AND c.donvi_id=d.donvi_id
                AND c.nhanvien_id IN(SELECT nhanvien_id FROM admin.nhanvien_lnv WHERE loainv_id = 40)
           )phong ON phong.ma_nd=t.nguoi_tt
           */
        -- WHERE T.ky_ketthuc2 <> 0 AND T.ky_ketthuc2 IS NOT NULL
        --   AND T.ky_batdau2 <> 0 AND T.ky_batdau2 IS NOT NULL
         GROUP BY t.donvi_tt_id,t.ten_dv,t.phong,t.nguoi_tt,p.phong_bh,dvi.ttvt_pbh,t.ten_kh,t.diachi_kh,t.mst,t.ngay_tt,t.seri,t.hoadon,t.ngay_hd,t.sophieu,t.ma_gd,t.magd_vnpt_pay,t.httt,logpay.transactionid  
                 ,t.ma_kmtt,t.loaihd_id,t.hdtb_id,t.thuebao_id,t.ma_tb,t.tienthu,t.tien,t.vat,t.kenhthu,t.ma_tt,t.ma_nvtc,t.ten_nvtc,t.ghichu,t.ghichu_tt,t.loaihinh_tb
                 ,t.rkm_id2,t.ky_batdau2,t.ky_ketthuc2,t.ma_tiepthi,t.ten_tiepthi,dvtt.ten_dv,dvtt.phong_tiepthi,gddv.ten_ctkm,hd.httt_id, t.HUONG_KM2,t.SO_CT, t.ngay_bddc , t.ngay_ktdc';                 
   
   --26/7/2022 bs thêm tru?ng h?p MTB còn n? và MTB có phi?u h?y;MTT có du d?u k?
   CASE i_da_ttoan     
     WHEN 2 THEN --MTB còn n?: thêm c?t ti?n n?
      --L?y d? li?u MTB còn n?(thamkhao:qltn.baocao8.bc_ct_conno_capnhat_xuly_kq_kothuduoc_cuoc)
      t_data:='select a.ma_tt,a.ma_tb,a.dichvuvt_id,nvl(a.tongps,0) phatsinh,nvl(b.tongno,0) conno,a.thanhtoan_id
      from (select thanhtoan_id,ma_tt,ma_tb,dichvuvt_id, sum(nogoc+thue) tongps
            from bcss.ct_no  subpartition for ('||in_phanvung_id||','||v_yyyymmdd||') where dichvuvt_id <> 2
            group by thanhtoan_id,ma_tt,ma_tb,dichvuvt_id)a         
          ,(select thanhtoan_id,ma_tt,ma_tb,dichvuvt_id,sum(nogoc+thue) tongno
            from qltn.ct_no  subpartition for ('||in_phanvung_id||','||v_yyyymmdd||') 
            where dichvuvt_id <> 2 and chukyno='||v_yyyymmdd||'              
            group by thanhtoan_id,ma_tt,ma_tb,dichvuvt_id
            having sum(nogoc+thue)>0)b
      where a.ma_tt = b.ma_tt and a.ma_tb = b.ma_tb and a.dichvuvt_id = b.dichvuvt_id';

      v_sql:='SELECT t.*,a.conno tienno FROM('||v_sql||')t LEFT JOIN('||t_data||')a ON a.ma_tb=t.ma_tb WHERE a.conno>0';
     WHEN 3 THEN --MTB có phi?u h?y: thêm các c?t thông tin h?y
      --L?y d? li?u MTB có phi?u h?y
      t_data:='SELECT b.ma_tb,b.ma_tt,b.nguoihuy user_huy,nd.ten_nd ten_userhuy,dv.ten_dv donvi_userhuy,dvc.ten_dv donvi_cha,b.sotienhuy,b.ngayhuy
                  ---,b.kyhoadon  --(yyyymm01)
            FROM qltn.bienlaihuy partition for ('||in_phanvung_id||') b
             LEFT JOIN admin.nguoidung nd ON nd.ma_nd=b.nguoihuy
             LEFT JOIN admin.nhanvien nv ON nv.nhanvien_id=nd.nhanvien_id
             LEFT JOIN admin.donvi dv ON dv.donvi_id=nv.donvi_id
             LEFT JOIN admin.donvi dvc ON dvc.donvi_id=dv.donvi_cha_id
            WHERE b.kyhoadon='||v_yyyymmdd||' 
           ';
      v_sql:='SELECT t.*,a.user_huy,a.ten_userhuy,a.donvi_userhuy,a.sotienhuy,a.ngayhuy
              FROM('||v_sql||')t 
              LEFT JOIN('||t_data||')a ON a.ma_tb=t.ma_tb AND a.ma_tt=t.ma_tt';
     WHEN 4 THEN --MTT ti?n du d?u k?
      t_data:='SELECT DISTINCT ma_tt,ma_tb,dichvuvt_id
               ,SUM(no_dk) no_dk,SUM(no_ps) no_ps,SUM(tra_dk) tra_dk,SUM(tra_ps) tra_ps
               ,SUM(tra_dk+tra_ps) tongtra,SUM(no_dk-tra_dk) conno_dk,SUM(no_ps-tra_ps) conno_ps
               ,SUM(no_dk-tra_dk+no_ps-tra_ps) conno
         FROM(SELECT ma_tt,ma_tb,dichvuvt_id,SUM(tien_dk) no_dk,SUM(tien_ps) no_ps,0 tra_dk,0 tra_ps
              FROM(SELECT ma_tt,ma_tb,dichvuvt_id
                         ,SUM(DECODE(kieuno,0,nogoc+thue-hoahong,0)) tien_dk
                         ,SUM(DECODE(kieuno,5,nogoc+thue-hoahong,0)) tien_ps      
                   FROM qltn.ct_no  subpartition for ('||in_phanvung_id||','||v_yyyymmdd||')   --01102020  
                   WHERE ngay_cn>=TO_DATE('''||i_tungay||''',''dd/MM/yyyy'')
                     AND ngay_cn<=TO_DATE('''||i_denngay||''',''dd/MM/yyyy'') +0.999
                   GROUP BY ma_tt,ma_tb,dichvuvt_id 
                   UNION ALL
                   SELECT b.ma_tt,a.ma_tb,a.dichvuvt_id
                         ,SUM(DECODE(a.kieutra,0,a.tragoc+a.trathue-a.chihoahong,0)) tien_dk
                         ,SUM(DECODE(a.kieutra,5,a.tragoc+a.trathue-a.chihoahong,0)) tien_ps
                   FROM qltn.ct_tra  subpartition for ('||in_phanvung_id||','||v_yyyymmdd||') a        --01102020 a 
                       ,qltn.bangphieutra  subpartition for ('||in_phanvung_id||','||v_yyyymmdd||') b  --01102020 b 
                   WHERE a.phieu_id = b.phieu_id
                     AND b.ngay_tt>=TO_DATE('''||i_tungay||''',''dd/MM/yyyy'')
                     AND b.ngay_tt<=TO_DATE('''||i_denngay||''',''dd/MM/yyyy'')+0.999
                   GROUP BY b.ma_tt,a.ma_tb,a.dichvuvt_id)
              GROUP BY ma_tt,ma_tb,dichvuvt_id
              UNION ALL
              SELECT ma_tt,ma_tb,dichvuvt_id,0 no_dk,0 no_ps,SUM(tra_dk) tra_dk,SUM(tra_ps) tra_ps
              FROM(SELECT b.ma_tt,a.ma_tb,a.dichvuvt_id
                         ,DECODE(a.kieutra,0,SUM(NVL(a.tragoc,0)+NVL(a.trathue,0)-NVL(a.chihoahong,0)),0) tra_dk
                         ,DECODE(a.kieutra,5,SUM(NVL(a.tragoc,0)+NVL(a.trathue,0)-NVL(a.chihoahong,0)),0) tra_ps 
                   FROM qltn.ct_tra  subpartition for ('||in_phanvung_id||','||v_yyyymmdd||') a        --01102020 a 
                       ,qltn.bangphieutra  subpartition for ('||in_phanvung_id||','||v_yyyymmdd||') b  --01102020 b 
                   WHERE a.phieu_id=b.phieu_id
                     AND b.ngay_tt>=TO_DATE('''||i_tungay||''',''dd/MM/yyyy'')
                     AND b.ngay_tt<=TO_DATE('''||i_denngay||''',''dd/MM/yyyy'') +0.999
                   GROUP BY b.ma_tt,a.ma_tb,a.dichvuvt_id,a.kieutra)
              GROUP BY ma_tt,ma_tb,dichvuvt_id)
         GROUP BY ma_tt,ma_tb,dichvuvt_id';
      v_sql:='SELECT t.*,a.conno tienno FROM('||v_sql||')t LEFT JOIN('||t_data||')a ON a.ma_tb=t.ma_tb WHERE a.conno<0';         
     ELSE ---Ðã thu ti?n: xét v_dk ? trên
      v_sql:=v_sql;
   END CASE;  
   v_sql:=v_sqlwith|| '
                      SELECT rownum stt,t_sql.* FROM('||v_sql||')t_sql';    
   DBMS_OUTPUT.PUT_LINE(v_sql);
   --/*
   --insert into baocao_pttb.a_thu(lenh,caulenh,ngay_cn) values (v_sql,v_tenbc,sysdate); 
   --commit;
   --*/
   --Ghi log 
   --INSERT INTO baocao_pttb.log_xembaocao(ngay, nguoidung_id,ten_bc,thamso)
   --VALUES(sysdate,p_nguoidung,v_tenbc,'Tu ngay: '||i_tungay||' - Den ngay: '||i_denngay||' - Don vi:'||i_donvitt_id ||' - HTTT: '||i_httt||' - Trangthai: '||i_da_ttoan);
   --COMMIT;   
   OPEN p_baocao FOR v_sql;      
END BSS_96734; 