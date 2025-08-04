# dbt-colibri

**dbt-colibri** is a lightweight, developer-friendly CLI tool and self-hostable dashboard that extracts and visualizes **full column-level lineage** from your `dbt-core` project — no cloud syncs, agents, or vendor lock-in required.

It’s ideal for data teams who want a transparent, flexible, and open approach to lineage tracking without relying on complex enterprise tooling.

---

## ✨ Features

- ✅ **Column-level lineage graph**: Understand how every column is derived across models  
- 🔍 **Model + column metadata**: Parse directly from `dbt`'s manifest and catalog  
- 🧠 **Smart parsing**: Uses the structure of your SQL to extract relationships  
- 📦 **Self-hostable**: Generate static HTML or JSON reports (no server required)  
- 💡 Built for `dbt-core` users working locally or in CI pipelines  

---

## 🚀 Quickstart

### 📦 Installation

For local development:

```
git clone https://github.com/your-org/dbt-colibri.git
cd dbt-colibri
pip install -e .
```

### ⚙️ Usage
Generate a lineage report from your dbt project directory:

```
colibri generate
```

By default, this will:

    Look for `target/manifest.json` and `target/catalog.json`

    Output the results to the `dist/` folder:

        `colibri-manifest.json`: human-readable lineage data

        `index.html`: interactive visualization

### Compatibility:
- dbt-core == 1.10.6
- python = 3.16
- snowflake dialect


### 🧰 Built on Open Source

This project is based on a fork of [`dbt-column-lineage-extractor`](https://github.com/canva-public/dbt-column-lineage-extractor), originally created under the MIT license.

Some core logic is adapted and modified for enhanced usability and reporting.



