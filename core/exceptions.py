class IncorrectMappingReadException(Exception):
    def __str__(self):
        return 'Некорректно сформирован excel-файл маппинга'
    
    
class IncorrectSetupException(Exception):
    def __str__(self):
        return 'Заполните все файлы программы для генерации маппинга'