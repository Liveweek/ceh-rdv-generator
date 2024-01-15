import random
import string

import numpy
import pandas
import pandas as pd
import re
from dataclasses import dataclass

from pandas import DataFrame
import logging

from config import Config
from .context import SourceContext, TargetContext, MappingContext, DAPPSourceContext, DRPSourceContext, UniContext, HubFieldContext
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

    columns = Config.excel_data_definition.get('columns', dict())
    columns_list: list[str] = columns[sheet_name]

    # Преобразование данных в DataFrame
    mapping: DataFrame = DataFrame()
    try:
        mapping = pd.read_excel(file_data,
                                sheet_name=sheet_name,
                                header=1,
                                )
    except Exception:
        logging.exception("Ошибка преобразования данных в DataFrame")
        raise

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
        self.mapping_df['Src_attr'] = self.mapping_df['Src_attr'].str.strip()

        self.mapping_df['Src_attr_datatype'] = self.mapping_df['Src_attr_datatype'].str.lower()
        self.mapping_df['Src_attr_datatype'] = self.mapping_df['Src_attr_datatype'].str.strip()

        self.mapping_df['Tgt_PK'] = self.mapping_df['Tgt_PK'].str.lower()
        self.mapping_df['Tgt_PK'] = self.mapping_df['Tgt_PK'].str.strip()

        self.mapping_df['Tgt_attribute'] = self.mapping_df['Tgt_attribute'].str.lower()
        self.mapping_df['Tgt_attribute'] = self.mapping_df['Tgt_attribute'].str.strip()

        self.mapping_df['Tgt_attr_datatype'] = self.mapping_df['Tgt_attr_datatype'].str.lower()
        self.mapping_df['Tgt_attr_datatype'] = self.mapping_df['Tgt_attr_datatype'].str.strip()

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
        df: DataFrame = self.mapping_df[self.mapping_df['Tgt_table'] == table_name].dropna(how="all")
        return df

    def get_flow_name_by_table(self, table_name: str) -> str | None:
        """
        Возвращает наименование потока для заданной целевой таблицы. Если None, то поток не найден
        """
        flow_name: str = self.get_parameter_by_table(table_name=table_name, parameter_name='Flow_name')

        if not flow_name:
            return None

        # Удаляем пробельные символы
        flow_name = re.sub(r"\s", '', flow_name)
        return flow_name

    def get_source_system_schema_by_table(self, table_name: str) -> str | None:
        """
        Args:
            table_name: Имя целевой таблицы

        Returns:
            Имя схемы источника
        """
        pattern: str = r"^[a-z][a-z0-9_]*\.[a-zA-Z][a-zA-Z0-9_]*$"

        src_table: str = self.get_parameter_by_table(table_name=table_name, parameter_name='Src_table')

        if not src_table:
            return None

        if not re.match(pattern, src_table):
            logging.error(f"Значение поля 'Src_table' '{src_table}' не соответствует шаблону '{pattern}'")
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

        src = self.mart_mapping[['Src_table', 'Src_attr', 'Src_attr_datatype']].dropna(how='all')
        tgt = self.mart_mapping[['Tgt_attribute', 'Tgt_attr_datatype', 'Tgt_attr_mandatory', 'Tgt_PK', 'Comment']]

        # Проверяем типы данных, заданные для источника. Читаем данные из настроек программы
        src_attr_datatype: dict = Config.field_type_list.get('src_attr_datatype', dict())
        err_rows = src[~src['Src_attr_datatype'].isin(src_attr_datatype)]
        if len(err_rows) > 0:
            logging.error(f"Неверно указаны типы данных источника '{src.iloc[0].at['Src_table']}':")
            for line in str(err_rows).splitlines():
                logging.error(line)
            logging.error(f'Допустимые типы данных: {src_attr_datatype}')
            is_error = True

        # Проверяем типы данных для целевой таблицы. Читаем данные из настроек программы
        tgt_attr_datatype: dict = Config.field_type_list.get('tgt_attr_datatype', dict())
        err_rows = tgt[~tgt['Tgt_attr_datatype'].isin(tgt_attr_datatype)]
        if len(err_rows) > 0:
            logging.error(f"Неверно указаны типы данных в строках для целевой таблицы '{self.mart_name}':")
            for line in str(err_rows).splitlines():
                logging.error(line)

            logging.error(f'Допустимые типы данных: {tgt_attr_datatype}')
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
        err_rows = tgt[~tgt['Tgt_PK'].isin(['pk', numpy.nan, 'fk'])]
        if len(err_rows) > 0:
            logging.error(f"Неверно указан признак 'Tgt_PK' для целевой таблицы '{self.mart_name}':")
            logging.error("Допустимые значения: 'fk'/'pk'/'пустая ячейка'")
            for line in str(err_rows).splitlines():
                logging.error(line)
            is_error = True

        # Проверка: Поля 'pk' должны быть not null
        err_rows = tgt.query('Tgt_PK in ["pk"] and Tgt_attr_mandatory != "not null"')
        if len(err_rows) > 0:
            logging.error(f"Неверно указан признак 'Tgt_attr_mandatory' для целевой таблицы '{self.mart_name}':")
            logging.error("Поля отмеченные как 'pk' должны быть 'not null'")
            for line in str(err_rows).splitlines():
                logging.error(line)
            is_error = True

        # Проверка полей, тип которых фиксирован
        tgt_attr_predefined_datatype: dict = Config.field_type_list.get('tgt_attr_predefined_datatype', dict())
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

        # Проверяем соответствие названия полей целевой таблицы шаблону
        pattern: str = Config.field_type_list.get('tgt_arrt_name_regexp', r"^[a-zA-Z][a-zA-Z0-9_]*$")
        err_rows = tgt[~tgt.Tgt_attribute.str.match(pattern)]
        if len(err_rows) > 0:
            logging.error(f"Названия полей целевой таблицы '{self.mart_name}' не соответствуют шаблону '{pattern}'")
            for index, fld_name in err_rows['Tgt_attribute'].items():
                logging.error(fld_name)
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

        # Ф-ия "where" в библиотеке pandas не фильтрует записи, а меняет значения полей по условию !!!
        # hub: pd.DataFrame = self.mart_mapping.where(cond=self.mart_mapping['Attr:Conversion_type'] == 'hub')
        # Не используйте разные "знаки" в именах полей ...
        # hub: pd.DataFrame = self.mart_mapping.query("Attr:Conversion_type == 'hub'")
        hub: pd.DataFrame = self.mart_mapping[self.mart_mapping['Attr:Conversion_type'] == 'hub']
        hub = hub[['Tgt_attribute', 'Attr:BK_Schema', 'Attr:BK_Object', 'Attr:nulldefault', 'Src_attr',
                   'Expression', 'Tgt_PK', 'Tgt_attr_datatype']]
        hub_list = hub.to_numpy().tolist()

        # Проверяем корректность имен
        # Шаблон формирования short_name в wf.yaml
        pattern = r"^[a-z][a-z0-9_]{2,22}$"
        bk_object_pattern = r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$"

        ret_list: list = list()
        # Проверяем соответствие имени таблицы правилам формирования поля short_name
        for hh in hub_list:

            # Значение NaN, которое так "любит" pandas "плохо" воспринимается другими библиотеками
            src_attr: str | None = hh[4] if not pandas.isna(hh[4]) else None
            expression: str | None = None
            if not pandas.isna(hh[5]):
                expression = hh[5]
                # Удаляем знак "="
                expression = expression.strip().removeprefix('=')

            hub_ctx: HubFieldContext = HubFieldContext(name=hh[0],
                                                       bk_schema_name=hh[1],
                                                       hub_name=hh[2],
                                                       on_full_null=hh[3],
                                                       src_attr=src_attr,
                                                       expression=expression,
                                                       hub_schema=None,
                                                       hub_name_only=None,
                                                       hub_short_name=None,
                                                       hub_field=None,
                                                       is_bk='true' if hh[6] == 'pk' else 'false',
                                                       tgt_type=hh[7])

            h_schema: str
            h_name: str

            if not re.match(bk_object_pattern, hh[2]):
                logging.error(f"Значение hub_name '{hh[2]}' в поле 'Attr:BK_Object' не соответствует шаблону "
                              f"'{bk_object_pattern}'")
                raise IncorrectMappingException("Ошибка в структуре данных EXCEL")
            else:
                h_schema, h_name = hh[2].split('.')

            # Длина short_name должна быть от 2 до 22 символов
            if not re.match(pattern, h_name):
                h_short_name = ('hub_' + h_name.removeprefix('hub_')[0:12] + '_' +
                                ''.join(random.choice(string.ascii_lowercase) for i in range(5)))
            else:
                h_short_name = h_name

            hub_ctx.hub_schema = h_schema
            hub_ctx.hub_name_only = h_name
            hub_ctx.hub_short_name = h_short_name
            name_rk: str = hh[0]
            if name_rk.endswith('_rk'):
                name_id = re.sub(r'_rk$', r'_id', name_rk)
            else:
                name_id = name_rk + '_id'

            hub_ctx.hub_field = name_id

            ret_list.append(hub_ctx)

        return ret_list

    def _get_src_table_fields(self) -> list:
        """
        Возвращает список полей источника с типами данных
        """
        src_attr: DataFrame = self.mart_mapping[['Src_attr', 'Src_attr_datatype', 'Src_table']] \
            .dropna(how="any")

        src_tbl_name: str = src_attr.iloc[0]['Src_table']

        # Удаление дубликатов в списке полей
        src_attr = src_attr.drop_duplicates(subset=['Src_attr'])

        is_error: bool = False
        # Проверяем соответствие названия полей источника шаблону
        pattern: str = Config.field_type_list.get('src_arrt_name_regexp', r"^[a-zA-Z][a-zA-Z0-9_]*$")
        err_rows = src_attr[~src_attr.Src_attr.str.match(pattern)]
        if len(err_rows) > 0:
            logging.error(f"Названия полей в таблице - источнике '{src_tbl_name}' не соответствуют шаблону '{pattern}'")
            for index, fld_name in err_rows['Src_attr'].items():
                logging.error(fld_name)
            is_error = True

        # Проверяем обязательные поля
        src_attr_predefined_datatype: dict = Config.field_type_list.get('src_attr_predefined_datatype', dict())
        for fld_name in src_attr_predefined_datatype.keys():
            err_rows = src_attr.query(f"Src_attr == '{fld_name}'")
            if len(err_rows) == 0:
                logging.error(f"Не найден обязательный атрибут '{fld_name}' таблицы - источника '{src_tbl_name}'")
                is_error = True

            elif len(err_rows) > 1:
                logging.error(f"Обязательный атрибут '{fld_name}' для таблицы - источника '{src_tbl_name}'"
                              f" указан более одного раза")
                for line in str(err_rows).splitlines():
                    logging.error(line)
                is_error = True

            else:
                if err_rows.iloc[0]['Src_attr_datatype'] != src_attr_predefined_datatype[fld_name][0]:
                    logging.error(
                        f"Параметры обязательного атрибута '{fld_name}' для целевой таблицы '{src_tbl_name}'"
                        f" указаны неверно")
                    for line in str(err_rows).splitlines():
                        logging.error(line)
                    is_error = True

        if is_error:
            raise IncorrectMappingException(f"Неверно указаны атрибуты таблицы - источника '{src_tbl_name}'")

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
        tgt_hub_field_ctx: list = self._get_tgt_hub_fields()
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
