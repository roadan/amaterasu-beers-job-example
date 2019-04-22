from amaterasu.pyspark.runtime import AmaContext
import requests

ama_context = AmaContext.builder().build()
brewery_db_conf = ama_context.dataset_manager.get_dataset_configuration("brewerydb-styles")
r = requests.get(brewery_db_conf['uri'], params={'key': ama_context.env['configuration']['breweryDBApiKey']})
styles_df = ama_context.spark.createDataFrame(r.json()['data']).select('id', 'shortName')
beers_df = ama_context.get_dataset("random-beers").select('abv', 'styleId', 'name')
joined_df = beers_df.join(styles_df, on=beers_df.styleId == styles_df.id).drop('id', 'styleId')
saturday_beers = joined_df.where(joined_df.abv > 8)
ama_context.persist("saturday-beers", saturday_beers)
