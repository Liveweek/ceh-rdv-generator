import logging
from config import Config
from ui import MainWindow


def main() -> int:

    # Файл настройки программы.
    config_name: str = 'generator.yaml'
    Config.load_config(config_name=config_name)

    logging.basicConfig(level=logging.INFO, filename=Config.log_file, filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s",
                        encoding='utf-8')

    print(f"log_file={Config.log_file}")
    logging.info('START')
    logging.info(f'templates_path="{Config.templates_path}"')

    win = MainWindow()
    win.mainloop()

    logging.info('STOP')
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
else:
    exit(100)
