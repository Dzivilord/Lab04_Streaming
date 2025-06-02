# Lab 03: Machine Learning on Spark

## Description
As datasets continue to grow in volume and complexity, leveraging distributed computing frameworks like Apache Spark becomes essential. Modern machine learning on big data relies on Spark’s scalable architecture and optimized execution engine (Catalyst and Tungsten) to handle large-scale model training and prediction efficiently. In this lab, you will explore three implementation approaches:

---
- **Structured API Implementation**: Use the high-level DataFrame-based API (spark.ml) to rapidly build and evaluate models.
- **MLlib RDD-Based Implementation**: Leverage Spark’s MLlib built-in functionalities on RDD data (spark.mllib) to perform machine learning tasks, deepening your understanding of Spark’s handling of RDDs.
- **Low-Level Operations Implementation**: Manually decompose ML algorithms into fundamental RDD operations (without relying on MLlib), enhancing your parallelism mindset and grasp of distributed computation.

## Getting started

### Prerequisites

- Python > 3.10
- Spark : 3.5.5
- Hadoop: 3.5.5
- Java 11+ (11 is recommended)

### Installation

Create and activate a virtual environment (optional but recommended)

```cmd
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

Install dependencies

```cmd
pip install -r requirements.txt
```

## Project Structure
```plain
<22120017>/
├── docs/
│   └── Report.pdf
├── src/
│   ├── Classification/
│   │   ├── Structured_API/
│   │   │   └── Structured_API_Fraud.ipynb
│   │   ├── MLlib_RDD_Based/
│   │   │   └── MLlib_RDD_Fraud.ipynb
│   │   └── Low_Level/
│   │       └── Low_Level_Fraud.ipynb
│   └── Regression/
│       ├── Structured_API/
│       │   └── Structured_API.ipynb
│       ├── MLlib_RDD_Based/
│       │   └── MLlib_RDD_Based.ipynb
│       └── Low_Level/
│           └── Low_Level.ipynb
└── README.md
```

