import json
import logging
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

project_id = "dev-project"
subscription = "dev-subscription"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_element(msg):
    return json.loads(msg.decode("utf-8"))  # Fazer o parsing do JSON


def process_element(element):
    # Adicionar um valor calculado ao dicionário
    n = element["n"]
    element["n_squared"] = n * n
    # Retornar o elemento modificado
    return element


def process_batch(windown_batch):
    model_id, batch = windown_batch
    nums = [element["i"] for element in batch]
    logging.info(f"Processing model {model_id} with size {len(batch)}: {nums}")
    result = []
    ele_sum = 0
    for element in batch:
        result.append(process_element(element))
        ele_sum += element["n"]
    for element in result:
        element["sum"] = ele_sum
    time.sleep(0.1)
    yield result


if __name__ == "__main__":
    # Definir as opções do pipeline
    options = PipelineOptions(streaming=True)

    # Criar um pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Ler os dados do Google PubSub
        p_collection = (
                pipeline
                | beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{subscription}")
                | beam.Map(parse_element)  # Aplicar função de processamento aos elementos
                | beam.Map(lambda element: (element["model_id"], element))  # Mapear o id como chave
                | beam.WindowInto(FixedWindows(0.5))  # Definir janela de n segundos
                | beam.GroupIntoBatches(50)  # Agrupar elementos pelo id em no max n
                | beam.ParDo(process_batch)
        )

        # # Imprimir os elementos transformados
        # p_collection | beam.Map(print)
