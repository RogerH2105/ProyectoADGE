from prefect import flow, task
from subprocess import run

@task
def parse_raw():
    run(["spark-submit", "parse_raw.py"], check=True)

@task
def preprocess():
    run(["spark-submit", "preprocesar.py"], check=True)
@task
def train_model():
    run(["spark-submit", "train_als.py"], check=True)

@flow(name="Pipeline Recomender ALS")
def pipeline_als():
    print("INICIANDO PIPELINE")
    parse_raw()
    preprocess()
    train_model()
    print("PIPELINE COMPLETADO")

if __name__ == "__main__":
    pipeline_als()
