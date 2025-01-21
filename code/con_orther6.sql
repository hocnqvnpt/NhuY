(select thuebao_id
                from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202401
                group by thuebao_id having count(thuebao_id) > 1)
                
                
                select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202403 and rkm_id is not null
                ;
                
select* from css_hcm.db_Thuebao where ten_Tb = 'Lê Hoàng Th?nh Nh? Ý';
select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv where thang_bd = 202404;