import tkinter as tk
from tkinter import ttk
from tkinter.messagebox import showinfo, showerror
from tkinter import filedialog

from map_gen import mapping_generator
import core.exceptions as exp



class MainWindow(tk.Tk):
    def __init__(self, env,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.src_cd = tk.StringVar()
        self.file_path = tk.StringVar()
        self.sys = tk.StringVar(value="DAPP")
        self.load_mode = tk.StringVar(value="snapshot")
        
        self.env = env
        
        self.wm_title("Генератор маппинга SRC-RDV")
        self.geometry("400x500")
        
        label = ttk.Label(text="Генератор маппинга SRC-RDV", font=('Arial', 14))
        label.pack(pady=20)

        open_file_dialog_button = ttk.Button(
            text="Выбрать файл...",
            command=self._setup_file_path
        )
        open_file_dialog_button.pack(fill=tk.X, padx=25)
        
        file_path_text = ttk.Entry(
            textvariable=self.file_path,
            font=("Arial", 10),
            state='readonly')
        file_path_text.pack(fill=tk.X, padx=25)
        
        label_src_cd = ttk.Label(text="Введите код источника (Числовое значение)", font=("Arial", 10))
        label_src_cd.pack(pady=15)
        src_cd_entry = ttk.Entry(textvariable=self.src_cd, font=("Arial", 10))
        src_cd_entry.pack(fill=tk.X, padx=25)
        
        label_sys = ttk.Label(text="Выберите систему источника", font=("Arial", 10))
        label_sys.pack(pady=15)
        sys_code = ttk.Combobox(textvariable=self.sys, values=['DRP', 'DAPP'])
        sys_code.pack(fill=tk.X, padx=25)
        
        label_load_mode = ttk.Label(text="Выберите принцип загрузки", font=("Arial", 10))
        label_load_mode.pack(pady=15)
        load_mode_name = ttk.Combobox(textvariable=self.load_mode, values=['snapshot', 'increment'])
        load_mode_name.pack(fill=tk.X, padx=25)
        
        start_export_button = tk.Button(
            text="ЭКСПОРТИРОВАТЬ",
            command=self._export_mapping,
            bg="#ffadaf"
        )
        start_export_button.pack(pady=10)
        
        
    def _setup_file_path(self):
        self.file_path.set(filedialog.askopenfilename())
        
        
    def _export_mapping(self):
        if all(
          self.file_path.get(),
          self.src_cd.get(),
          self.sys.get(),
          self.load_mode.get() 
        ):
            try:
                mapping_generator(
                    file_path=self.file_path.get(),
                    src_cd=self.src_cd.get(),
                    sys=self.sys.get(),
                    load_mode=self.load_mode.get(),
                    env=self.env
                )
                showinfo("Успешно", "Файлы для маппинга сгенерированы успешно!")
            except exp.IncorrectMappingReadException:
                showerror("ОШИБКА", "Ошибка чтения маппинга. Проверьте, соответствует ли он необходимому для генерации формату")
        else:
            raise exp.IncorrectSetupException