# Генератор артефактов потоков RDV на основе маппинга

## Описание
Генератор на Python формирует на основе файла маппинга артефакты потоков RDV 
(yaml файлы метаданных, py файл WF, json-ресурсов ЦЕХ) на основании 
установленных разработчиком параметров (системы источника DAPP или 
DRP и режима захвата данных) в соответствии с их расположением в 
директориях для работы фреймворка на стендах и для формирования 
поставки в Bitbucket. Потоки формируются для проведения загрузки 
1к1 (1 источник грузит в 1 приёмник).

## Установка
Для работы с генератором требуется **Python 3.12+** версии.

1. Клонировать репозиторий на локальный ресурс (диск)
2. Открыть терминал и создать в папке виртуальное окружение
```bash
py -m venv venv 
# или 
python -m venv venv
```
3. Перейти в режим виртуального окружения при помощи запуска скрипта (важно, чтобы запуск скрипта был именно через **cmd**, на не через PowerShell). Если в начале командной строки появилась надпись **(venv)**, то переход в виртуальное оркужение произошёл успешно.
```bash
.\venv\Scripts\activate.bat
```
4. Установить необходимые зависимотси для виртуального окружения с помощью команды
```bash
py -m pip install -r requirements.txt 
# или
python -m pip install -r requirements.txt
```
5. Для проверки запуска программы необходимо выполнить команду:
```bash
py main.py 
```

Если все пункты выполнены успешно - **Вы великолепны!**

## Изменения

 * Удалена возможность выбора способа загрузки "snapshot"
 * Удалена возможность выбора источника
 * Весь вывод перенаправлен из консоли в журнальный файл, который можно открыть во внешнем редакторе вручную 
или через "кнопку" графического интерфейса  
 * Добавлена настройка через использование файла конфигурации generator.yaml.
Файл содержит возможность сделать следующие настройки:
   * Имя автора
   * Название команды проекта
   * Название СУБО
   * Название Предметной области
   * Каталог с файлами шаблонов (по умолчанию "templates")
   * Программа для просмотра журнального файла
   * Каталог для сохранения сформированных фалов
   * Набор строк для формирования секции tags в файлах wf_\*.yaml, cf_\*.yaml
   * Список названий полей, которые НЕ будут использоваться для формирования hash
   * Список полей, которые НЕ включаются в опцию distributed_by / multi_fields
   * Список полей целевой таблицы, которые НЕ будут добавлены в секцию field_map шаблона wf_\*.yaml
   * Список полей с описанием, которые БУДУТ добавлены в секцию field_map шаблона wf_\*.yaml
   * Список предопределенных "связок" поле - тип поля для целевой таблицы, 
   для выполнения проверки типов "технических" полей
   * Список возможных значений колонки "Tgt_attr_datatype", для выполнения проверки данных в EXCEL
   * Список имен колонок на листах файла EXCEL, которые должны присутствовать, для выполнения проверки данных в EXCEL   * 

____________________

## Целевые файлы (результат)
Программа помещает результат работы в каталог заданный в диалоге (поле "Каталог"). 
Каталог располагается "рядом" с файлом main.py. Внутри каталога располагаются подкаталоги потоков, названные по имени целевой таблицы потока.
Структура каталогов с файлами потока аналогична структуре каталогов репозитория через который происходит публикация наработок (adgp, etl-scale):

```
+---adgp 
¦   L---extensions
¦       L---ripper
¦           L---.data                         # Скрипт создания целевой таблицы 
+---etl-scale
¦   +---general_ledger
¦   ¦   L---src_rdv
¦   ¦       +---dags                          # wf_/*.py 
¦   ¦       +---flow_dumps                    # Файл описания управляющего потока
¦   ¦       L---schema
¦   ¦           +---ceh
¦   ¦           ¦   L---rdv                   # Файл описания целевой иаблицы
¦   ¦           +---db_tables                 # Файл описания таблицы - источника
¦   ¦           L---work_flows                # Файл описания рабочего потока
¦   L---_resources
¦       +---ceh
¦       ¦   L---rdv                           # Ресурс целевой таблицы 
¦       L---uni
¦           L---dapp
¦               L---prod_repl_subo_nobankserv # UNI - ресурс источника
L---src                                       # Скрипт для формирования скрипта создания акцессоров 
```
