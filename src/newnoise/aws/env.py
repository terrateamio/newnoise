import os

# Default location for this module's data
NOISES_ROOT = "./noises/aws"

# Default location for data cache, eg. the sqlite database
NOISES_DB = os.path.join(NOISES_ROOT, "cache.db")

# Default location for CSV produced at end of this module's processes
NOISES_CSV = os.path.join(NOISES_ROOT, "products.csv")

# Path to AWS host that supplies the pricing data
PRICE_API = "https://pricing.us-east-1.amazonaws.com"

# HTTP path to file that stores the root of the pricing tree
PRICE_ROOT = f"{PRICE_API}/offers/v1.0/aws/index.json"
