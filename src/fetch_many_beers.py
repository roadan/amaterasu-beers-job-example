from amaterasu.pyspark.runtime import AmaContext
import requests
from typing import List, Dict, Any

ama_context: AmaContext = AmaContext\
    .builder()\
    .setMaster("local[*]")\
    .build()

brewery_db_conf = ama_context.dataset_manager.get_dataset_configuration("brewerydb-beers")
api_key = ama_context.env['configurations']['breweryDBApiKey']
brewery_db_uri = brewery_db_conf['uri']


def get_random_beers(num_beers_to_get: int) -> List[Dict[str, Any]]:
    num_calls = num_beers_to_get // 10
    if num_beers_to_get % 10 != 0:
        num_calls += 1

    all_beers = []

    for _ in range(num_calls):
        r = requests.get(brewery_db_uri, params={'key': api_key, 'order': 'random', 'randomCount': 10})
        beers = r.json()['data']
        all_beers.extend(beers)
    return all_beers


random_beers = get_random_beers(200)
df = ama_context.spark.createDataFrame(random_beers)
ama_context.persist("random-beers", df)


