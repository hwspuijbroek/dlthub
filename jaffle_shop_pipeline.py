import dlt
import os
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# Extract
os.environ["EXTRACT__WORKERS"] = "6" # Earlier 3.
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "100000" # File Rotation, instead of 1 big intermediary file, for Every 100,000 items, a new intermediary file will be created
os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000'  # Verhogen van 5000, Bufersize

os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '20000'  # Verhogen van 10000
# Normalize
os.environ['NORMALIZE__WORKERS'] = '3'
os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '20000'  # Verhogen van 10000
os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = '100000'

# Load
os.environ["LOAD__WORKERS"] = "4"

client = RESTClient(
    base_url="https://jaffle-shop.scalevector.ai/api/v1",
    paginator=HeaderLinkPaginator(links_next_key="next")
)
# Orders resource - full load
@dlt.resource(
    name="orders",
    write_disposition="replace",  # Gebruik replace voor volledige vervanging bij elke run
    parallelized=True
)
def get_orders():
    params = {"pageSize": 1000} # From 100 to 1000
    
    for page in client.paginate(
        path="orders",
        params=params
    ):
        yield page # Yield **chunks/pages**, not single rows

# Customers resource - full load
@dlt.resource(
    name="customers",
    write_disposition="replace",  # Volledige vervanging bij elke run
    parallelized=True
)
def get_customers():
    params = {"pageSize": 1000} # From 100 to 1000
    
    for page in client.paginate(
        path="customers",
        params=params
    ):
        yield page # Yield **chunks/pages**, not single rows

# Products resource - full load
@dlt.resource(
    name="products",
    write_disposition="replace",  # Volledige vervanging bij elke run
    parallelized=True
)
def get_products():
    params = {"pageSize": 1000} # From 100 to 1000
    
    for page in client.paginate(
        path="products",
        params=params
    ):
        yield page # Yield **chunks/pages**, not single rows

# Step 1 Group Resources into Sources
@dlt.source
def source():
    return get_products, get_customers, get_orders

# Pipeline definitie met dev_mode=True voor een volledige lading
pipeline = dlt.pipeline(
    pipeline_name="jaffleshop_pipeline_full_optimized",
    destination="duckdb",
    dataset_name="jaffle_shop",
    dev_mode=True  # Zorgt voor het resetten van de state bij elke run
)

# Run de pipeline met alle drie de resources
load_info = pipeline.run(source())
print(pipeline.last_trace)
