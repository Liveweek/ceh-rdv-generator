import os
import io
from pathlib import Path
from jinja2 import Environment
from pandas import DataFrame

from core.exceptions import IncorrectMappingException
from core.exporters import MartPackExporter
from core.mapping import MappingMeta, MartMapping
import logging


def mapping_generator(
        file_path: str,
        out_path: str,
        load_mode: str,
        source_system: str,
        env: Environment,
        author: str
) -> None:
    """Функция генератора маппинга, вызывает функционал по генерации
       файлов

    Args:
        file_path (str): Полный путь к файлу маппинга РДВ
        out_path (str): Каталог, в котором будут сформированы подкаталоги с описанием потоков
        load_mode (str): Режим загрузки (increment, snapshot)
        source_system (str): Система источника данных для загрузки
        env (Environment): Окружение шаблонов jinja2
        author (str): Наименование автора потоков для заполнения в шаблоне
    """

    logging.info(f"file_path: {file_path}")
    logging.info(f"out_path: {out_path}")
    logging.info(f"load_mode: {load_mode}")
    logging.info(f"source_system: {source_system}")
    logging.info(f"author: {author}")

    logging.info(f'Чтение данных из файла "{file_path}"')

    # Чтение данных их EXCEL
    try:
        with open(file_path, 'rb') as f:
            byte_data = io.BytesIO(f.read())

    except Exception as err:
        msg = f"Ошибка чтения данных из файла {file_path}"
        logging.error(msg)
        raise IncorrectMappingException(msg)

    # Данные EXCEL
    mapping_meta = MappingMeta(byte_data)

    # Список целевых таблиц
    map_objects: list[str] = mapping_meta.get_tgt_tables_list()

    # Цикл по списку целевых таблиц
    for tbl_index, tbl_name in enumerate(map_objects):
        logging.info(f"{tbl_index} {tbl_name}")

        # Данные для заданной целевой таблицы
        mapping: DataFrame = mapping_meta.get_mapping_by_table(tbl_name)

        # Возвращает наименование (логическое) "источника" для заданной целевой таблицы
        src_cd: str | None = mapping_meta.get_src_cd_by_table(tbl_name)
        if not src_cd:
            logging.error(f'Для таблицы {tbl_name} неверно задано/не задано имя источника')
            logging.error('Имя источника задается в колонке "Expression" для поля "src_cd"')
            raise Exception("Имя источника не определено")

        # Имя потока без wf_/cf_
        work_flow_name: str | None = mapping_meta.get_flow_name_by_table(tbl_name)

        if not work_flow_name:
            logging.error(f'Для таблицы {tbl_name} неверно задано/не задано поле "Название потока"/Flow_name ')
            raise Exception("Имя потока не определено")

        # Название схемы источника (берется из названия таблицы)
        source_system_schema: str | None = mapping_meta.get_source_system_schema_by_table(tbl_name)
        if not source_system_schema:
            logging.error(f'Для таблицы {tbl_name} неверно задано/не задано имя схемы источника')
            raise Exception("Имя схемы источника не определено")

        # Подготовка данных для файлов для одной таблицы
        exp_obj = MartMapping(
            mart_name=tbl_name,
            mart_mapping=mapping,
            src_cd=src_cd,
            data_capture_mode=load_mode,
            source_system=source_system,
            work_flow_name=work_flow_name,
            source_system_schema=source_system_schema
        )

        # Каталог для файлов
        # out_path_tbl = os.path.join(Path(__file__).parent, out_path, tbl_name)
        out_path_tbl = os.path.join(out_path, tbl_name)
        logging.info(f'Каталог потока {work_flow_name}: {out_path_tbl}')

        # Объект для формирования данных для вывода в файлы
        mp_exporter = MartPackExporter(
            exp_obj=exp_obj,
            path=out_path_tbl,
            env=env,
            author=author)

        # Вывод данных в файлы
        mp_exporter.load()
        logging.info(f'Файлы потока {work_flow_name} сформированы')
