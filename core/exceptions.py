class IncorrectMappingException(Exception):
    message: str = ''

    def __init__(self, message="Ошибка формирования файлов описания потока"):
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message
