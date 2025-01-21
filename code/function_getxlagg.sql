create or replace FUNCTION            GETXMLAGG_TABLE 
(
    vtable_name in varchar2,
    vcolumn_name in varchar2,
    vdieukien in varchar2 ,
   -- vcolumn_order in varchar2,
    vcolumn_name_alias in varchar2:='-1'
)RETURN  varchar2 IS
    vkq varchar2(32000);
    valias  varchar2(20);

BEGIN
    if(vcolumn_name_alias='-1')then
        valias:=vcolumn_name;
    else
        valias:=vcolumn_name_alias;
    end if;
    execute immediate
    'select RTRIM(XMLAGG(XMLELEMENT(R,'|| valias ||','', '').EXTRACT(''//text()'')),'', '')
     vkq
    from (
           select '||vcolumn_name || ' ' || valias  ||' from '||vtable_name  ||'
           where '||vdieukien  ||'
    )a' into vkq;
    vkq:=replace(vkq,';','>');
    return  vkq;
     exception
       when OTHERS then
      return  '' ;
end getxmlagg_table;