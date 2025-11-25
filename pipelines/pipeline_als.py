from prefect import flow, task
from subprocess import run

@task
def parse_raw():
    run([
        "spark-submit", 
        "--master", "local[*]", 
        "../scripts/tratamiento.py"
    ], check=True)

@task
def preprocess():
    run([
        "spark-submit", 
        "--master", "local[*]", 
        "../scripts/preprocesamiento.py"
    ], check=True)

@task
def train_model():
    run([
        "spark-submit", 
        "--master", "local[*]", 
        "../modelo/als_model.py"
    ], check=True)


@flow(name="Pipeline Recommender ALS")
def pipeline_als():
    print("INICIANDO PIPELINE ALS")
    parse_raw()
    preprocess()
    train_model()
    print("PIPELINE COMPLETADO")

if __name__ == "__main__":
    pipeline_als()
