from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.onprem.database import PostgreSQL
from diagrams.programming.framework import Flask
import os

ICONS = "/Users/chiwoong.moon/.claude/plugins/cache/fe-vibe/fe-workflows/1.2.9/skills/fe-architecture-diagram/resources/icons"

with Diagram(
    "Sentinel Fraud Defense Platform",
    show=False,
    filename="/tmp/fraud-repo/docs/architecture",
    outformat="png",
    direction="TB",
    graph_attr={
        "splines": "ortho",
        "nodesep": "1.2",
        "ranksep": "1.4",
        "pad": "0.8",
        "fontsize": "16",
        "fontname": "Helvetica Bold",
        "bgcolor": "white",
        "dpi": "150",
        "labeljust": "c",
    },
    node_attr={
        "fontsize": "11",
        "fontname": "Helvetica",
    },
    edge_attr={
        "fontsize": "9",
        "fontname": "Helvetica",
        "color": "#444444",
    },
):

    # Data Sources
    with Cluster("Data Sources\n(Unity Catalog Volume)", graph_attr={"bgcolor": "#E8F0FE", "style": "rounded", "fontsize": "13"}):
        csv_txn = Custom("transactions.csv\n10K records", f"{ICONS}/databricks/delta_lake.png")
        csv_login = Custom("login_logs.csv\n8K records", f"{ICONS}/databricks/delta_lake.png")
        csv_users = Custom("users.csv\n500 records", f"{ICONS}/databricks/delta_lake.png")

    # DLT Pipeline
    with Cluster("Lakeflow DLT Pipeline", graph_attr={"bgcolor": "#FFF3E0", "style": "rounded", "fontsize": "13"}):
        with Cluster("Bronze Layer"):
            bronze = Custom("Raw Ingestion\n(Auto Loader)", f"{ICONS}/databricks/lakehouse.png")

        with Cluster("Silver Layer"):
            silver = Custom("Enriched +\nJoined Tables", f"{ICONS}/databricks/lakehouse.png")

        with Cluster("Gold Layer\n5 Fraud Detection Rules"):
            gold = Custom("Flagged\nTransactions", f"{ICONS}/databricks/lakehouse.png")

    # Processing outputs
    with Cluster("Unity Catalog Governance", graph_attr={"bgcolor": "#E8F5E9", "style": "rounded", "fontsize": "13"}):
        uc = Custom("PII Masking\n(Secure Views)", f"{ICONS}/databricks/unity_catalog.png")

    with Cluster("AI Risk Scoring", graph_attr={"bgcolor": "#F3E5F5", "style": "rounded", "fontsize": "13"}):
        ai_agent = Custom("Foundation Model API\n(Claude Sonnet 4)", f"{ICONS}/databricks/model_serving.png")

    with Cluster("Natural Language Investigation", graph_attr={"bgcolor": "#E0F7FA", "style": "rounded", "fontsize": "13"}):
        genie = Custom("Genie Space\n(Fraud Investigation)", f"{ICONS}/databricks/sql_warehouse.png")

    # Lakebase
    with Cluster("Real-Time Serving", graph_attr={"bgcolor": "#FFF8E1", "style": "rounded", "fontsize": "13"}):
        lakebase = PostgreSQL("Lakebase\n(Serverless Postgres)")

    # Databricks App
    with Cluster("Databricks App — Sentinel", graph_attr={"bgcolor": "#FCE4EC", "style": "rounded", "fontsize": "13"}):
        app = Flask("Flask App")

    # Data flow
    csv_txn >> bronze
    csv_login >> bronze
    csv_users >> bronze
    bronze >> Edge(label="  clean + validate  ") >> silver
    silver >> Edge(label="  apply rules  ") >> gold

    gold >> Edge(label="  mask PII  ") >> uc
    gold >> Edge(label="  explain risk  ") >> ai_agent
    gold >> Edge(label="  query  ") >> genie

    ai_agent >> Edge(label="  upsert triage  ") >> lakebase
    lakebase >> Edge(label="  Fraud Queue tab  ") >> app
    genie >> Edge(label="  Investigator tab  ") >> app
