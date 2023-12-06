import random
import string

import numpy
import pandas as pd
import re
from dataclasses import dataclass

from pandas import DataFrame
import logging

import config
from .context import SourceContext, TargetContext, MappingContext, DAPPSourceContext, DRPSourceContext, UniContext
from .exceptions import IncorrectMappingException

# columns - Список колонок, которые должны присутствовать в листах -
# устанавливается в файле generator.yaml

# tgt_attr_datatype - Список возможных значений колонки "Tgt_attr_datatype"
# задается в файле generator.yaml

# tgt_attr_predefined_datatype - Список предопределенных "связок" поле/тип поля
# задается в файле generator.yaml


def _generate_mapping_df(file_data: bytes, sheet_name: str):
    """
    Трансформирует полученные данные EXCEL в тип DataFrame.
    Обрабатываются только данные листа из sheet_name.
    Проверяет в данных наличие колонок из списка.

    Параметры:
        file_data: bytes
            Данные в виде "строки", считанные их EXCEL-файла
        sheet_name: str
            Название листа в книге EXCEL

    Возвращаемое значение:
        Объект с типом DataFrame.
    """

    columns = config.excel_data_definition.get('columns', dict())
    columns_list: list[str] = columns[sheet_name]

    # Преобразование данных в DataFrame
    mapping = pd.read_excel(file_data,
                            sheet_name=sheet_name,
                            header=1,
                            )

    # Проверка полученных данных
    error: bool = False
    for col_name in columns_list:
        if not (col_name in mapping.columns.values):
            logging.error(f"Колонка '{col_name}' не найдена на листе '{sheet_name}'")
            error = True

    if error:
        raise IncorrectMappingException("Ошибка в структуре данных EXCEL")

    # Трансформация данных: оставляем в наборе только колонки из списка и не пустые строки
    mapping = mapping[columns_list].dropna(how='all')

    return mapping


class MappingMeta:
    # Данные листа 'Детали загрузок Src-RDV'
    mapping_df: pd.DataFrame
    # Данные листа 'Перечень загрузок Src-RDV'
    mapping_list: pd.DataFrame

    def __init__(self, byte_data):
        # Проверка, очистка данных, преобразование в DataFrame
        # Детали загрузок Src-RDV
        self.mapping_df = _generate_mapping_df(file_data=byte_data, sheet_name='Детали загрузок Src-RDV')

        # Преобразуем значения в "нужный" регистр
        self.mapping_df['Src_attr'] = self.mapping_df['Src_attr'].str.lower()
        self.mapping_df['Src_attr_datatype'] = self.mapping_df['Src_attr_datatype'].str.lower()
        self.mapping_df['Tgt_PK'] = self.mapping_df['Tgt_PK'].str.lower()
        self.mapping_df['Tgt_attribute'] = self.mapping_df['Tgt_attribute'].str.lower()
        self.mapping_df['Tgt_attr_datatype'] = self.mapping_df['Tgt_attr_datatype'].str.lower()

        # Перечень загрузок Src-RDV
        self.mapping_list = _generate_mapping_df(file_data=byte_data, sheet_name='Перечень загрузок Src-RDV')

        # Список целевых таблиц. Проверяем наличие дубликатов в списке
        self.tgt_tables_list: list[str] = self.mapping_list['Tgt_table'].dropna().tolist()
        visited: set = set()
        error: bool = False
        for tbl in self.tgt_tables_list:
            if tbl in visited:
                logging.error(f"В таблице 'Перечень загрузок Src-RDV' "
                              f"присутствуют повторяющиеся названия таблиц: {tbl}")
                error: bool = True
            else:
                visited.add(tbl)

        if error:
            raise IncorrectMappingException("Ошибка в структуре данных")

    def get_tgt_tables_list(self) -> list[str]:
        """
        Возвращает список целевых таблиц (из колонки 'Tgt_table')
        """
        return self.tgt_tables_list

    def get_mapping_by_table(self, table_name: str) -> pd.DataFrame:
        """
        Возвращает список (DataFrame) строк для заданной целевой таблицы
        """
        return self.mapping_df.where(self.mapping_df.Tgt_table == table_name).dropna(how="all")

    def get_flow_name_by_table(self, table_name: str) -> str | None:
        """
        Возвращает наименование потока для заданной целевой таблицы. Если None, то поток не найден
        """
        flow_name: str = self.get_parameter_by_table(table_name=table_name, parameter_name='Flow_name')

        if not flow_name:
            return None

        # Удаляем пробельные символы
        flow_name = re.sub(r"\s", '', flow_name)
        flow_name = re.sub(r"^wf_", '', flow_name)
        return flow_name

    def get_source_system_schema_by_table(self, table_name: str) -> str | None:
        """
        Args:
            table_name: Имя целевой таблицы

        Returns:
            Имя схемы источника
        """
        pattern: str = r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$"

        src_table: str = self.get_parameter_by_table(table_name=table_name, parameter_name='Src_table')

        if not src_table:
            return None

        if not re.match(pattern, src_table):
            logging.error("Значение поля 'Src_table' не соответствует шаблону '{pattern}'")
            return None

        source_system_schema = src_table.split('.')[0]

        return source_system_schema.lower()

    def get_parameter_by_table(self, table_name: str, parameter_name: str) -> str | None:
        """
        Возвращает значение параметра (поля) для заданной целевой таблицы с листа 'Перечень загрузок Src-RDV'.
        Если None, то поток не найден
        """
        flow_name_obj = self.mapping_list.query(f'Tgt_table == "{table_name}"')[parameter_name]
        if len(flow_name_obj) == 0:
            logging.error(f"Не найдено имя потока для целевой таблицы '{table_name}' "
                          f"на листе 'Перечень загрузок Src-RDV'")
            return None

        if len(flow_name_obj) > 1:
            logging.error("Найдено несколько строк для целевой таблицы '{table_name}' на листе "
                          "'Перечень загрузок Src-RDV'")
            return None

        parameter_value: str = flow_name_obj.to_numpy()[0]
        # Удаляем пробельные символы
        parameter_value = re.sub(r"\s", '', parameter_value)
        return parameter_value

    def get_src_cd_by_table(self, table_name: str) -> str | None:
        """
        Возвращает наименование источника для заданной целевой таблицы. Если None, то источник не найден
        """
        src_cd_obj = self.mapping_df.query(f'Tgt_table == "{table_name}" and Tgt_attribute == "src_cd"')['Expression']
        if len(src_cd_obj) == 0:
            logging.error(f"Не найдено поле 'src_cd' в таблице '{table_name}'")
            return None

        if len(src_cd_obj) > 1:
            logging.error(f"Найдено несколько описаний для поля 'src_cd' в таблице '{table_name}'")
            return None

        src_cd: str = src_cd_obj.to_numpy()[0]
        # Удаляем пробельные символы
        src_cd = re.sub(r"\s", '', src_cd)
        # Выделяем имя источника
        pattern = r"='([A-Z]+)'$"
        result = re.match(pattern, src_cd)

        if result is None:
            logging.error(f"Не найдено имя источника для таблицы '{table_name}' по шаблону '{pattern}'")
            return None

        src_cd = result.groups()[0]
        return src_cd


@dataclass
class MartMapping:
    """
    Переменные класса напрямую используются при заполнении шаблона
    """
    mart_name: str
    mart_mapping: pd.DataFrame
    # Код источника
    src_cd: str
    # Режим захвата данных: snapshot/increment
    data_capture_mode: str
    # Имя источника (для uni-провайдера).
    source_system: str
    # Имя схемы источника (для uni-провайдера)
    source_system_schema: str
    # Имя потока без префиксов wf_/cf_
    work_flow_name: str
    # Имя схемы источника (для uni-провайдера)

    src_ctx: SourceContext | None = None
    tgt_ctx: TargetContext | None = None
    mapping_ctx: MappingContext | None = None
    uni_ctx: UniContext | None = None
    # Режим загрузки "дельты" - значение параметра в файле описания рабочего потока
    delta_mode: str = 'new'

    # Инициализация данных
    def __post_init__(self):
        # Подготовка контекста источника
        self._src_ctx_post_init()

        # Подготовка контекста целевых таблиц
        self._tgt_ctx_post_init()

        # Подготовка контекста
        self._map_ctx_post_init()

        self.delta_mode = 'new'

        # Формирование контекста для шаблона uni_res
        # Сделано отдельно от src_ctx, что-бы не "ломать" мозги
        self.uni_ctx = UniContext(source=self.source_system,
                                  schema=self.source_system_schema,
                                  table_name=self.src_ctx.name,
                                  src_cd=self.src_cd)

    def _get_tgt_table_fields(self) -> list:
        """
        Возвращает список полей целевой таблицы с типами данных и признаком "null"/"not null"
        Выполняет проверку типов и обязательных полей
        """
        is_error: bool = False

        tgt = self.mart_mapping[['Tgt_attribute', 'Tgt_attr_datatype', 'Tgt_attr_mandatory', 'Tgt_PK', 'Comment']]

        # Проверяем типы данных. Читаем данный из настроек программы
        tgt_attr_datatype: dict = config.field_type_list.get('tgt_attr_datatype', dict())
        err_rows = tgt[~tgt['Tgt_attr_datatype'].isin(tgt_attr_datatype)]
        if len(err_rows) > 0:
            logging.error(f"Неверно указаны типы данных в строках для целевой таблицы '{self.mart_name}':")
            for line in str(err_rows).splitlines():
                logging.error(line)
            is_error = True

        # Заполняем признак 'Tgt_attr_mandatory'.
        # При чтении данных Панда заменяет строку 'null' на значение 'nan'
        # Поэтому производим "обратную" замену ...
        # И заодно "давим" предупреждение, которое "выскакивает" ...
        chained_assignment = pd.options.mode.chained_assignment
        pd.options.mode.chained_assignment = None
        tgt['Tgt_attr_mandatory'].fillna(value="null", inplace=True)
        tgt['Comment'].fillna(value='', inplace=True)
        pd.options.mode.chained_assignment = chained_assignment

        err_rows = tgt[~tgt['Tgt_attr_mandatory'].isin(['null', 'not null'])]
        if len(err_rows) > 0:
            logging.error(f"Неверно указан признак null/not null для целевой таблицы '{self.mart_name}':")
            for line in str(err_rows).splitlines():
                logging.error(line)
            is_error = True

        # Проверка признака первичного ключа
        err_rows = tgt[~tgt['Tgt_PK'].isin(['pk', numpy.nan, 'fk_rk'])]
        if len(err_rows) > 0:
            logging.error(f"Неверно указан признак 'Tgt_PK' для целевой таблицы '{self.mart_name}':")
            logging.error("Допустимые значения: 'fk_rk'/'pk'/'пустая ячейка'")
            for line in str(err_rows).splitlines():
                logging.error(line)
            is_error = True

        # Проверка полей, тип которых фиксирован
        tgt_attr_predefined_datatype: dict = config.field_type_list.get('tgt_attr_predefined_datatype', dict())
        for fld_name in tgt_attr_predefined_datatype.keys():
            err_rows = tgt.query(f"Tgt_attribute == '{fld_name}'")
            if len(err_rows) == 0:
                logging.error(f"Не найден обязательный атрибут '{fld_name}' для целевой таблицы '{self.mart_name}'")
                is_error = True

            elif len(err_rows) > 1:
                logging.error(f"Обязательный атрибут '{fld_name}' для целевой таблицы '{self.mart_name}'"
                              f" указан более одного раза")
                for line in str(err_rows).splitlines():
                    logging.error(line)
                is_error = True

            else:
                if (err_rows.iloc[0]['Tgt_attr_datatype'] != tgt_attr_predefined_datatype[fld_name][0] or
                        err_rows.iloc[0]['Tgt_attr_mandatory'] != tgt_attr_predefined_datatype[fld_name][1]):
                    logging.error(
                        f"Параметры обязательного атрибута '{fld_name}' для целевой таблицы '{self.mart_name}'"
                        f" указаны неверно")
                    for line in str(err_rows).splitlines():
                        logging.error(line)
                    is_error = True

        if is_error:
            raise IncorrectMappingException(f"Неверно указаны атрибуты для целевой таблицы '{self.mart_name}'")

        return tgt.to_numpy().tolist()

    def _get_tgt_hub_fields(self) -> list:
        """
        Возвращает список атрибутов для hub-таблицы
        ["Имя_поля_в_источнике", "Имя_BK_Schema", "Имя_hub_таблицы",
         "признак_null", "поле_в_источнике",
         "имя_схемы", "имя_hub_таблицы_без_схемы", "short_name",
         "имя_поля_в_hub"]
        """
        hub: pd.DataFrame = self.mart_mapping.where(self.mart_mapping['Attr:Conversion_type'] == 'hub')
        hub = hub[['Tgt_attribute', 'Attr:BK_Schema', 'Attr:BK_Object', 'Attr:nulldefault', 'Src_attr']]
        hub_list = hub.dropna().to_numpy().tolist()

        # Проверяем корректность имен
        # Шаблон формирования short_name в wf.yaml
        pattern = r"^[a-z][a-z0-9_]{2,22}$"
        bk_object_pattern = r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$"

        # Проверяем соответствие имени таблицы правилам формирования поля short_name
        for hh in hub_list:

            if not re.match(bk_object_pattern, hh[2]):
                logging.error(f"Значение в поле 'Attr:BK_Object' не соответствует шаблону '{bk_object_pattern}'")
                h_schema, h_name = ['error_hub_schema', 'error_hun_table']
            else:
                h_schema, h_name = hh[2].split('.')

            if not re.match(pattern, h_name):
                h_short_name = 'hub_' + ''.join(random.choice(string.ascii_lowercase) for i in range(10))
            else:
                h_short_name = h_name

            hh.append(h_schema)
            hh.append(h_name)
            hh.append(h_short_name)
            name_rk: str = hh[0]
            if name_rk.endswith('_rk'):
                name_id = re.sub(r'_rk$', r'_id', name_rk)
            else:
                name_id = name_rk + '_id'
            hh.append(name_id)

        return hub_list

    def _get_src_table_fields(self) -> list:
        """
        Возвращает список полей источника с типами данных
        """
        src_attr: DataFrame = self.mart_mapping[['Src_attr', 'Src_attr_datatype']] \
            .dropna(how="any")

        # Удаление дубликатов в списке полей
        src_attr = src_attr.drop_duplicates(subset=['Src_attr'])

        # Преобразуем к виду python list
        return src_attr.to_numpy().tolist()

    def _get_field_map(self) -> list:
        """
        Returns: Список атрибутов для заполнения секции field_map в шаблоне wf.yaml
        """
        mart_map = self.mart_mapping.where(self.mart_mapping['Attr:Conversion_type'] != 'hub')[
            ['Src_attr', 'Tgt_attribute', 'Tgt_attr_datatype', 'Src_attr_datatype']
        ].dropna().to_numpy().tolist()

        # Добавляем пустое поле
        for lst in mart_map:
            lst.append(None)

        # Список полей, которые рассчитываются
        mart_map_exp = self.mart_mapping.where(self.mart_mapping['Attr:Conversion_type'] != 'hub')[
            ['Expression', 'Tgt_attribute', 'Tgt_attr_datatype']
        ].dropna().to_numpy().tolist()

        for lst in mart_map_exp:
            if not lst[0].startswith('='):
                logging.warning(f'Значение в колонке "Expression" должно начинаться со знака равно. Src_attr={lst[1]}')
            else:
                mart_map.append([None, lst[1], lst[2], None, lst[0]])

        return mart_map

    def _src_ctx_post_init(self):
        # Имя таблицы-источника
        src_table_name = self.mart_mapping['Src_table'].dropna().unique()[0].lower()
        # Список полей таблицы - источника
        src_field_ctx = self._get_src_table_fields()
        __src_ctx_cls = {
            "DAPP": DAPPSourceContext,
            "DRP": DRPSourceContext
        }

        self.src_ctx = __src_ctx_cls[self.source_system](
            name=src_table_name,
            src_cd=self.src_cd,
            field_context=src_field_ctx,
            data_capture_mode=self.data_capture_mode
        )
        # Небольшой "допил"
        self.src_ctx.schema = self.source_system_schema

    def _tgt_ctx_post_init(self):
        tgt_field_ctx = self._get_tgt_table_fields()
        tgt_hub_field_ctx = self._get_tgt_hub_fields()
        self.tgt_ctx = TargetContext(
            name=self.mart_name,
            src_cd=self.src_cd,
            field_context=tgt_field_ctx,
            hub_context=tgt_hub_field_ctx,
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
            work_flow_name=self.work_flow_name,
            hub_ctx_list=self.tgt_ctx.hub_ctx_list,
            source_system=self.source_system,

        )
