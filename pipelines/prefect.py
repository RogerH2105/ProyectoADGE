from prefect import flow, task
from subprocess import run

@task
def parse_raw():
    run(["python", "parse_raw.py"], check=True)

@task
def preprocess():
    run(["python", "preprocesar.py"], check=True)

@task
def train_model():
    run(["python", "train_als.py"], check=True)

@flow(name="Pipeline Recomender ALS")
def pipeline_als():
    print("=== INICIANDO PIPELINE ===")
    parse_raw()
    preprocess()
    train_model()
    print("=== PIPELINE COMPLETADO ===")

if __name__ == "__main__":
    pipeline_als()
