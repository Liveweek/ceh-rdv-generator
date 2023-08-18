import pandas as pd
import re
from dataclasses import dataclass, KW_ONLY


from .context import SourceContext, TargetContext, MappingContext, DAPPSourceContext, DRPSourceContext
from .exceptions import IncorrectMappingReadException


class MappingMeta:
    mapping_df: pd.DataFrame
    src_cd:  str
    
    def __init__(self, file_obj):
        self.mapping_df = self._generate_mapping_df(file_obj)
        self.src_cd = re.search(r'[A-Z]+', self.mapping_df['Expression'].dropna().unique()[0]).group(0)
        
        
    def _generate_mapping_df(self, file_obj) -> pd.DataFrame:
        try:
            mapping = pd.read_excel(
                file_obj,
                sheet_name='Детали загрузок Src-RDV',
                header=1,
            )[
                [
                    'Mapping_ver_to',
                    'Algorithm_UID',
                    'SubAlgorithm_UID',
                    'Src_table',
                    'Src_attr',
                    'Src_attr_datatype',
                    'Src_PK',
                    'Expression',
                    'Tgt_table',
                    'Tgt_PK',
                    'Tgt_attribute',
                    'Tgt_attr_datatype',
                    'Tgt_attr_mandatory',
                    'Attr:Conversion_type',
                    'Attr:BK_Schema',
                    'Attr:BK_Object',
                    'Attr:nulldefault',
                ]
            ]
        except:
            raise IncorrectMappingReadException
        return mapping[mapping["Mapping_ver_to"].isnull()]
    
    
    def get_tgt_tables_list(self) -> list[str]:
        return self.mapping_df['Tgt_table'].dropna().unique().tolist()
    
    
    def get_mapping_by_table(self, table_name) -> pd.DataFrame:
        return self.mapping_df.where(self.mapping_df.Tgt_table == table_name).dropna(how="all")
        
    
@dataclass
class MartMapping:
    mart_name:         str
    mart_mapping:      pd.DataFrame
    src_cd:            str
    data_capture_mode: str
    source_system:     str
    _: KW_ONLY
    
    src_ctx:           SourceContext  | None = None
    tgt_ctx:           TargetContext  | None = None
    mapping_ctx:       MappingContext | None = None
    
        
    def _get_tgt_table_fields(self) -> list:
        return self.mart_mapping[['Tgt_attribute', 'Tgt_attr_datatype', 'Tgt_attr_mandatory']] \
            .fillna("null")\
            .to_numpy()\
            .tolist()
                              
                              
    def _get_tgt_hub_fields(self) -> list:
        return self.mart_mapping \
            .where(self.mart_mapping['Attr:Conversion_type'] == 'hub')[
                [
                    'Tgt_attribute', 
                    'Attr:BK_Schema',
                    'Attr:BK_Object',
                    'Attr:nulldefault'
                ]]\
            .dropna()\
            .to_numpy()\
            .tolist()
        
        
    def _get_src_table_fields(self) -> list:
        return self.mart_mapping[['Src_attr', 'Src_attr_datatype']]\
            .dropna(how="any")\
            .to_numpy()\
            .tolist()    

    
    def _get_field_map(self) -> list:
        return self.mart_mapping\
            .where(self.mart_mapping['Attr:Conversion_type'] != 'hub')[
                [
                    'Src_attr', 
                    'Tgt_attribute', 
                    'Tgt_attr_datatype'
                ]]\
            .dropna()\
            .to_numpy()\
            .tolist()
            
            
    def _src_ctx_post_init(self):
        src_table_name = self.mart_mapping['Src_table'].dropna().unique()[0].lower()
        src_field_ctx = self._get_src_table_fields()
        __src_ctx_cls = {
            "DAPP" : DAPPSourceContext,
            "DRP"  : DRPSourceContext
        }
        
        self.src_ctx = __src_ctx_cls[self.source_system](
            name=src_table_name,
            src_cd=self.src_cd,
            field_context=src_field_ctx,
            data_capture_mode=self.data_capture_mode
        )
        
        
    def _tgt_ctx_post_init(self):
        tgt_field_ctx = self._get_tgt_table_fields()
        tgt_hub_field_ctx = self._get_tgt_hub_fields()
        self.tgt_ctx = TargetContext(
            name=self.mart_name, 
            src_cd=self.src_cd, 
            field_context=tgt_field_ctx,
            hub_context=tgt_hub_field_ctx
        )
        
        
    def _map_ctx_post_init(self):
        fld_map_ctx = self._get_field_map()
        algo = self.mart_mapping['Algorithm_UID'].unique()[0]
        algo_sub = self.mart_mapping['SubAlgorithm_UID'].unique()[0]
        self.mapping_ctx = MappingContext(
            field_map_context=fld_map_ctx,
            src_cd=self.src_cd,
            src_name=self.src_ctx.name,
            src_schema=self.src_ctx.schema,
            tgt_name=self.tgt_ctx.name,
            algo=algo,
            algo_sub=algo_sub,
            data_capture_mode=self.data_capture_mode,
            hub_pool=self.tgt_ctx.hub_pool,
            hub_ctx_list=self.tgt_ctx.hub_ctx_list,
            source_system=self.source_system
        )

            
    def __post_init__(self):
        # Подготовка контекста источника
        self._src_ctx_post_init()
        
        # Подготовка контекста таргета
        self._tgt_ctx_post_init()
        
        # Подготовка контекста маппинга
        self._map_ctx_post_init()        