import argparse
from jinja2 import Environment, FileSystemLoader
from pathlib import Path


from core.mapping import MappingMeta, MartMapping
from core.exporters import MartPackExporter
from ui import MainWindow


parser = argparse.ArgumentParser(prog="Конвертер маппинга")

parser.add_argument(
    "--ui", 
    action='store_true',
    help="Вызов графического интерфейса маппинга"
)
parser.add_argument(
    "--src_cd", 
    help="Код источника маппинга, используется для создания папки с соответствующим наименованием"
)
parser.add_argument(
    "--path",
    help="Полный путь к файлу маппинга"
)
parser.add_argument(
    "--load",
    help="Режим загрузки"
)
parser.add_argument("--sys", default="DAPP")



if __name__ == "__main__":
    args = parser.parse_args()
    env = Environment(loader=FileSystemLoader('templates'))
    
    if args.ui:
        win = MainWindow(env=env)
        win.mainloop()
        
    else:
        file_path = args.path
          
        # TODO : Заменить этот код на вызов функции mapping_generator 
          
        with open(file_path, 'rb') as f:
            mapping_meta = MappingMeta(f.read())
            map_objects = mapping_meta.get_tgt_tables_list()

            for obj_num, map_obj in enumerate(map_objects):
                mapping = mapping_meta.get_mapping_by_table(map_obj)
                mm = MartMapping(
                    mart_name=map_obj,
                    mart_mapping=mapping,
                    src_cd=mapping_meta.src_cd,
                    data_capture_mode=args.load,
                    source_system=args.sys
                )
                
                mp_exporter = MartPackExporter(
                    exp_obj=mm,
                    path=str(Path(__file__).parent) + "\\{src_cd}\\wf_{obj_num}\\".format(
                        src_cd=args.src_cd,
                        obj_num=obj_num+1
                    ), 
                    env=env)
                mp_exporter.load()