from pathlib import Path
from ceh_core_rdv.app import dag_builder

DAG = 'Нужна переменная DAG, что бы airflow подгрузил файл'
dag = dag_builder.build(Path(__file__).stem)

if __name__ == '__main__':
    print(f'{dag.dag_id} loaded.')
