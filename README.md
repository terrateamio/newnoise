![Still frame of Dennis Lyxzen from a music video for the band Refused's song New Noise, the namesake for this project. There is a speech bubble to show Dennis screaming the words, "I've got a bone to pick with pricing my infrastructure", referencing the opening lyrics of the same song.](docs/bone_to_pick.png)

A tool for gathering and processing infrastructure pricing data from cloud providers and for creating new representations useful for rapid cost estimation. It converts multiple formats of noisy data into new noise with a consistent structure.


## Overview

There are two main subcommands:
- `aws`: for working with AWS pricing data
- `sheet`: for generating an OIQ price sheet

```
newnoise aws fetch
newnoise aws load
newnoise aws dump
newnoise sheet "noises/aws/products.csv"
```

The last command writes the OIQ sheet to "oiqdata/prices.csv"


## Installing

```
git clone git@github.com:terrateamio/newnoise
cd newnoise/src
python -mvenv venv
pip install -e .
```
