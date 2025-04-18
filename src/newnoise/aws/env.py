import os

NOISES_ROOT = "./noises/aws"
NOISES_DB = os.path.join(NOISES_ROOT, "cache.db")
NOISES_CSV = os.path.join(NOISES_ROOT, "pro_ducks.csv")

PRICE_API = "https://pricing.us-east-1.amazonaws.com"
PRICE_ROOT = "/offers/v1.0/aws/index.json"

# NO IDEA what this is or where it comes from
SPOT_API = "https://website.spot.ec2.aws.a2z.com/spot.js"
