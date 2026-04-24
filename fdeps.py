from itertools import combinations
import pandas as pd
from extract import load_dtypes

def find_partial_dependencies(df, primary_key):
    partial_deps = {}

    # Check each subset of the primary key (size < full key)
    for i in range(1, len(primary_key)):
        for subset in combinations(primary_key, i):
            subset = list(subset)

            for col in df.columns:
                if col in primary_key:
                    continue

                unique_counts = df.groupby(subset)[col].nunique()

                if (unique_counts <= 1).all():
                    partial_deps.setdefault(tuple(subset), []).append(col)

    return partial_deps

def find_candidate_keys_fast(df, max_key_size=None):
    cols = sorted(df.columns, key=lambda c: df[c].nunique(), reverse=True)
    
    candidate_keys = []

    for r in range(1, len(cols) + 1):
        if max_key_size and r > max_key_size:
            break

        for combo in combinations(cols, r):
            combo = list(combo)

            if any(set(key).issubset(combo) for key in candidate_keys):
                continue

            if df.duplicated(subset=combo).sum() == 0:
                candidate_keys.append(combo)

    return candidate_keys

def normalize_to_2nf(df, primary_key):
    partial_deps = find_partial_dependencies(df, primary_key)
    tables = {}

    used_columns = set()

    # Create tables for partial dependencies
    for subset, cols in partial_deps.items():
        table_cols = list(subset) + cols
        tables[subset] = df[table_cols].drop_duplicates()
        used_columns.update(cols)

    # Main table (remaining columns)
    remaining_cols = [
        col for col in df.columns
        if col not in used_columns
    ]

    tables['main'] = df[remaining_cols]

    return tables

if __name__ == "__main__":
    df = load_dtypes("CONVOCATORIAS")
    keys = find_candidate_keys_fast(df)