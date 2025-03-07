# New Noise

It loads noisy data from one place, mutates it, and writes beautiful noise to some other place.


## Usage

This is probably what you want:

```
newnoise -o "./oiqdata" /path/to/price_data.csv
```

The help menu:

```
usage: newnoise [-h] [-o OUTPUT] input

NewNoise CLI

positional arguments:
  input                Path to the input CSV

options:
  -h, --help           show this help message and exit
  -o, --output OUTPUT  Path to the output directory
```


## Installing

```
git clone git@github.com:terrateamio/newnoise
cd newnoise/src
python -mvenv venv
pip install -e .
```
