from pathlib import Path

from jinja2 import Environment
from core.exporters import MartPackExporter
from core.mapping import MappingMeta, MartMapping


def mapping_generator(
        file_path: str,
        src_cd: str,
        load_mode: str,
        sys: str,
        env: Environment,
        author: str = "pkonakov (VTB70171591)"
    ) -> None:
    """Функция генератора маппинга, вызывает функционал по генерации
       файлов

    Args:
        file_path (str): Полный путь к файлу маппинга РДВ
        src_cd (str): Код источника (на основании него будет создана папка)
        load_mode (str): Режим загрузки (increment, snapshot)
        sys (str): Система источника данных для загрузки
        env (Environment): Окружение шаблонов jinja2
        author (str): Наименование автора потоков для заполнения в шаблоне
    """
    
    with open(file_path, 'rb') as f:
        mapping_meta = MappingMeta(f.read())
        map_objects = mapping_meta.get_tgt_tables_list()

        for obj_num, map_obj in enumerate(map_objects):
            mapping = mapping_meta.get_mapping_by_table(map_obj)
            mm = MartMapping(
                mart_name=map_obj,
                mart_mapping=mapping,
                src_cd=mapping_meta.src_cd,
                data_capture_mode=load_mode,
                source_system=sys
            )
            
            mp_exporter = MartPackExporter(
                exp_obj=mm,
                path=str(Path(__file__).parent) + "\\{src_cd}\\wf_{obj_num}\\".format(
                    src_cd=src_cd,
                    obj_num=obj_num+1
                ), 
                env=env,
                author=author)
            mp_exporter.load()