import os
import yaml
from jinja2 import Environment, FileSystemLoader
import logging
from pathlib import Path
import config as conf
from ui import MainWindow


def main() -> int:

    # Файл настройки программы.
    config_name: str = 'generator.yaml'
    if not os.path.exists(config_name):
        print('Не найден файл конфигурации программы "{config_name}" в каталоге основного модуля')
        return 1

    with open(config_name, 'r', encoding='utf-8') as f:
        conf.config = yaml.safe_load(f)

    conf.tags = conf.config.get('tags', dict())
    conf.setting_up_field_lists = conf.config.get('setting_up_field_lists', dict())
    conf.field_type_list = conf.config.get('field_type_list', dict())
    conf.excel_data_definition = conf.config.get('excel_data_definition', dict())

    # "Загрузка" файлов-шаблонов
    templates_path: str = os.path.abspath(conf.config.get('templates', 'templates'))
    if not os.path.exists(templates_path):
        print(f'Не найден каталог с шаблонами "{templates_path}"')
        return 1

    env = Environment(loader=FileSystemLoader(templates_path))

    # Файл журнала
    log_file = os.path.join(Path(__file__).parent, 'generator.log')
    if os.path.exists(log_file):
        os.remove(log_file)

    logging.basicConfig(level=logging.INFO, filename=log_file, filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")

    # Сохраняем "для потомков"
    conf.log_file = log_file

    print(f"log_file={log_file}")
    logging.info('START')
    logging.info(f'templates_path="{templates_path}"')

    win = MainWindow(env=env)
    win.mainloop()

    logging.info('STOP')
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
