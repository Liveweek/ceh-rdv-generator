import os
import tkinter as tk
from tkinter import ttk, SOLID, LEFT
from tkinter.messagebox import showinfo, showerror
from tkinter import filedialog
import logging

from jinja2 import TemplateNotFound

from map_gen import mapping_generator
import core.exceptions as exp
import config as conf


class MainWindow(tk.Tk):
    def __init__(self, env):
        super().__init__()

        author: str = conf.config.get('author', 'Unknown Author')
        out_path: str = os.path.abspath(conf.config.get('out_path', '999'))

        self.out_path = tk.StringVar(value=out_path)
        self.file_path = tk.StringVar()
        self.source_system = tk.StringVar(value="DAPP")
        self.load_mode = tk.StringVar(value="increment")
        self.author = tk.StringVar(value=author)

        self.env = env

        self.wm_title("Генератор файлов описания потока")
        self.geometry("450x470")

        frame = tk.Frame(
            self,       # Обязательный параметр, который указывает окно для размещения Frame.
            padx=5,     # Задаём отступ по горизонтали.
            pady=5,     # Задаём отступ по вертикали.
            borderwidth=1,
            relief=SOLID
        )
        frame.pack(anchor='nw', fill='both', padx=5, pady=10)

        open_file_dialog_button = ttk.Button(
            frame,
            text="Выбрать EXCEL-файл с описанием данных",
            command=self._setup_file_path
        )
        open_file_dialog_button.pack(fill=tk.X, padx=25, pady=15)

        file_path_text = ttk.Entry(
            frame,
            textvariable=self.file_path,
            font=("Arial", 10),
            state='readonly')
        file_path_text.pack(fill=tk.X, padx=25)

        label_src_cd = ttk.Label(frame, text="Каталог", font=("Arial", 10))
        label_src_cd.pack(pady=10)

        src_cd_entry = ttk.Entry(frame, textvariable=self.out_path, font=("Arial", 10))
        src_cd_entry.pack(fill=tk.X, padx=25)

        label_sys = ttk.Label(frame, text="Режим загрузки", font=("Arial", 10))
        label_sys.pack(pady=10)

        sys_code = ttk.Combobox(frame, textvariable=self.source_system, values=['DAPP'])
        sys_code.pack(fill=tk.X, padx=25)

        label_load_mode = ttk.Label(frame, text="Принцип загрузки", font=("Arial", 10))
        label_load_mode.pack(pady=10)

        load_mode_name = ttk.Combobox(frame, textvariable=self.load_mode, values=['increment'])
        load_mode_name.pack(fill=tk.X, padx=25)

        label_author = ttk.Label(frame, text="Автор потока", font=("Arial", 10))
        label_author.pack(pady=10)

        author_entry = ttk.Entry(frame, textvariable=self.author, font=("Arial", 10))
        author_entry.pack(fill=tk.X, padx=25)

        start_export_button = tk.Button(
            frame,
            text="ЭКСПОРТИРОВАТЬ",
            command=self._export_mapping
        )
        start_export_button.pack(pady=10, anchor='e', padx=25)

        frame_log = tk.Frame(frame,     # Обязательный параметр, который указывает окно для размещения Frame.
                             padx=5,    # Задаём отступ по горизонтали.
                             pady=5,    # Задаём отступ по вертикали.
                             borderwidth=0,
                             relief=SOLID
                             )
        frame_log.pack(anchor='nw', fill='both', padx=5, pady=5)
        frame_log.columnconfigure(0, weight=5)
        frame_log.columnconfigure(1, weight=1)

        view_log_button = tk.Button(
            frame_log,
            text="Журнал",
            command=self._view_log
        )
        view_log_button.grid(row=0, column=1, sticky=tk.E, padx=10)

        label_log = ttk.Label(frame_log, text=conf.log_file, font=("Arial", 10))
        label_log.grid(row=0, column=0, sticky=tk.W)

    def _setup_file_path(self):
        self.file_path.set(filedialog.askopenfilename())

    @staticmethod
    def _view_log():
        log_file: str = conf.log_file
        log_viewer: str = conf.config.get('log_viewer')
        log_file_cmd: str = conf.config.get('log_file_cmd').replace('{log_file}', f'{log_file}')
        log_cmd: str = f'"{log_viewer} {log_file_cmd}"'
        print(log_cmd)
        os.system(log_cmd)

    def _export_mapping(self):
        msg: str

        if not all((
                self.file_path.get(),
                self.out_path.get(),
                self.source_system.get(),
                self.load_mode.get(),
                self.author.get(),
                )):

            showerror("Ошибка",
                      "Проверьте заполнение полей формы")
        else:
            try:

                logging.info('Формирование файлов описания потоков ...')

                mapping_generator(
                    file_path=self.file_path.get(),
                    out_path=os.path.abspath(self.out_path.get()),
                    source_system=self.source_system.get(),
                    load_mode=self.load_mode.get(),
                    env=self.env,
                    author=self.author.get()
                )

                msg = "Файлы потоков сформированы"
                showinfo("Успешно", msg)
                logging.info(msg)

            except (exp.IncorrectMappingException, ValueError) as err:
                logging.error(err)
                msg = f"Ошибка: {err}.\nПроверьте журнал работы программы."
                showerror(title="Ошибка", message=msg)

            except TemplateNotFound:
                msg = "Ошибка чтения шаблона.\nПроверьте журнал работы программы."
                logging.exception("Ошибка чтения шаблона")
                showerror(title="Ошибка", message=msg)

            except Exception:
                msg = "Неизвестная ошибка.\nПроверьте журнал работы программы."
                logging.exception("Неизвестная ошибка")
                showerror(title="Ошибка", message=msg)
