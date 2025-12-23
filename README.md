# Human Rights Defenders Analysis

Estimating the historical population of human rights defenders using Wikidata and ecological species richness methods.

## Overview

This project applies the **Chao1 estimator** (a non-parametric species richness method from ecology) to estimate the total number of human rights defenders throughout history, including those not captured in historical records.

We combine:

- **Wikidata**: Database of human rights defenders with associated works
- **V-Dem**: Varieties of Democracy dataset for democracy indicators
- **Chao1 Estimator**: Statistical method to estimate unobserved population

## Project Structure

```
human_rights/
├── notebooks/
│   ├── 01_data_extraction.ipynb    # Extract data from Wikidata
│   ├── 02_chao1_analysis.ipynb     # Apply Chao1 estimator
│   └── 03_vdem_analysis.ipynb      # Analyze V-Dem democracy data
├── data/
│   ├── human_rights_defender.csv           # Raw Wikidata export
│   ├── human_rights_defender_clean.csv     # Cleaned data
│   ├── chao1_results_world.csv             # Chao1 analysis results
│   └── V-Dem-CY-FullOthers-v15_csv/        # V-Dem dataset
├── figures/                         # Generated visualizations
├── requirements.txt                 # Python dependencies
└── README.md
```

## Installation

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

Run the notebooks in order:

1. **01_data_extraction.ipynb**: Fetches works counts from Wikidata API
2. **02_chao1_analysis.ipynb**: Applies Chao1 estimator to estimate total population
3. **03_vdem_analysis.ipynb**: Correlates with V-Dem democracy indicators

## Methodology

### Chao1 Estimator

The Chao1 estimator uses the frequency of rare observations to estimate total population:

```
S_chao1 = S_obs + (f1²) / (2 × f2)
```

Where:

- `S_obs` = observed number of individuals
- `f1` = singletons (individuals with 1 work)
- `f2` = doubletons (individuals with 2 works)

### Data Sources

- **Wikidata**: Human rights defenders identified via occupation/category properties
- **V-Dem v15**: Democracy indicators from 1789-2024 for 200+ countries

## Key Findings

1. **Low capture rates**: Historical records capture only a small fraction of human rights defenders
2. **Temporal variation**: Capture rates vary significantly across time periods
3. **Democracy correlation**: Strong correlation between democracy levels and documented defenders

## Requirements

- Python 3.9+
- pandas
- numpy
- matplotlib
- scipy
- jupyter
- requests
- tqdm

## License

MIT License

## Citation

If you use this analysis, please cite:

- V-Dem Dataset: Coppedge et al. (2024). V-Dem Dataset v15
- Wikidata: <https://www.wikidata.org>
