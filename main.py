import argparse
from jinja2 import Environment, FileSystemLoader


from map_gen import mapping_generator
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
parser.add_argument(
    "--author",
    help="Название автора потоков"
)




if __name__ == "__main__":
    args = parser.parse_args()
    env = Environment(loader=FileSystemLoader('templates'))
    
    if args.ui:
        win = MainWindow(env=env)
        win.mainloop()
        
    else:
        mapping_generator(
            file_path=args.path,
            src_cd=args.src_cd,
            load_mode=args.load,
            sys=args.sys,
            env=env
        )