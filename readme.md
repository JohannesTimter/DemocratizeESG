# 🌍 DemocratizeESG 🌱📊

DemocratizeESG is an open-source set of tools designed to evaluate the true environmental performance of corporations. By extracting environmental key performance indicators (eKPIs) from company reports, we aim to objectively evaluate and compare the environmental impact of large companies.

## 📑 Table of Contents
- [About](#about)
- [Project Overview](#project-overview)
  - [Document Corpus](#document-corpus)
  - [Environmental KPIs](#environmental-kpis)
  - [Information Extraction Pipeline](#information-extraction-pipeline)
- [Pipeline Usage](#pipeline-usage)
- [Roadmap](#roadmap)

## 💡 About
Our core contributions to the open-source community include:
1. **Report Collection:** A dataset of 1,350 annual company reports [(link)](https://drive.google.com/drive/folders/1ysF7PHBu29_0LGV-c22iwBoPx8X_NS9N?usp=sharing).
2. **eKPI Definitions:** 50 industry-agnostic eKPIs and over 70 industry-specific eKPIs [(link)](https://docs.google.com/spreadsheets/d/1Ezxv7jIAin8RrFseVCTXWd1V5L2W7QsHtAlhYRuzpvM/edit?usp=sharing).
3. **Extraction Pipeline:** An automated, LLM-based information extraction pipeline to parse eKPIs from the reports (contained in this repository).
4. **Training/Test Data:** 62 manually annotated reports used to train and test the pipeline [(SQL file)](https://drive.google.com/file/d/1VOFs1NNrqzmk__R-wX3_0Pd19xB_qPe1/view?usp=sharing).
5. **Extracted Database:** A complete database of all the information extracted from the report corpus [(SQL file)](https://drive.google.com/file/d/1VOFs1NNrqzmk__R-wX3_0Pd19xB_qPe1/view?usp=sharing).

## 🔎 Project Overview

### Document Corpus
[Climate Action 100+](https://www.climateaction100.org/about/) is an investor-led initiative to ensure the world’s largest corporate greenhouse gas emitters take necessary action on climate change to mitigate financial risk and maximize the long-term value of assets.

We manually collected 1,350 Annual Reports from 170 CA100+ companies spanning the years 2020-2024. This collection is publicly available on our [Google Drive](https://drive.google.com/drive/folders/1ysF7PHBu29_0LGV-c22iwBoPx8X_NS9N?usp=sharing). *(Note: Reports were collected in mid-2024 and may not reflect the absolute current state of CA100+).*

### Environmental KPIs
To determine what information to extract, we synthesized the most commonly used metrics across the three pillars of ESG from:
* **Governmental and NGO frameworks:** TCFD, GRI, SASB, ESRS
* **ESG rating agencies:** CDP, LSEG, EcoVadis
* **Scientific Literature:** Colesanti Senni et al. (2024); Zou et al. (2025)

The resulting list of 49 industry-agnostic and 70+ industry-specific eKPIs can be found in this [Google Sheet](https://docs.google.com/spreadsheets/d/1Ezxv7jIAin8RrFseVCTXWd1V5L2W7QsHtAlhYRuzpvM/edit?usp=sharing). We specifically focused on outcome- and impact-oriented metrics that are straightforward to visualize.

### Information Extraction Pipeline
The pipeline loads the target documents and the desired eKPIs. The reports are loaded entirely into the context window of an LLM (currently Gemini 2.5 Flash), where highly engineered prompts are used to extract the target data. The extracted information is then stored in a local MySQL database for further analysis and visualization. 

**🚀 Current Performance:** The pipeline achieves an **F1-Score of 93%** on our test set.

## 🛠️ Pipeline Usage

### Installation and Preparation
1. Install Python 3.12.6
2. Create and Store a Gemini API key, following the [Gemini documentation](https://ai.google.dev/gemini-api/docs/api-key)
3. Install MySQL '8.0.43' and use [democratize_esg.sql](democratize_esg.sql) to generate all 
   required tables
4. Use [createGoogleAccessToken.py](createGoogleAccessToken.py) to create a Google access token, 
   enabling script access to the required Google-drive.

To run the full extraction process, follow this workflow:

1. **Prepare Prompts:** Run `Fullcontext_main.py`. This contains the most effective pipeline version ("Divide and Conquer"). Reports are downloaded from the Drive, uploaded to the Google Files API, and prompts are built and saved to a large `.jsonl` file.
2. **Submit Batch Job:** Open `gemini_batch_management.ipynb` to submit the `.jsonl` file to the Gemini 2.5 Flash Batch API. You can query the status of the job here (expected runtime is ~20-30 minutes).
3. **Parse Results:** Once the batch job finishes, use `gemini_batch_output_parsing.ipynb` to download the results and store them in your local MySQL database.
4. **Clean Data:** Run `ConsolidateBatchResults.py` to access the local database and resolve any conflicting extracted information.
5. **Standardize Units:** Finally, run `UnitConversion.py` to convert numerical values to a common format and standard units, enabling historical and cross-company comparisons.

The resulting database is now ready for data analysis! Check out the `Data_Analysis` folder for usage examples.

## 🚀 Roadmap
We are currently working on releasing the pipeline and database as an interactive website. The site will feature easy access to data visualization methods for deeper insights. Users will also be able to upload new reports into the pipeline, and the resulting extracted information will be continuously added to the database!