import os
from main import gen_stage
from pathlib import Path


if __name__ == "__main__":
    datasets = os.listdir(Path('./data'))
    for d in datasets:
        name = d.split('.')
        print(name)