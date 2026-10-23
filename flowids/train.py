"""Train the binary (benign vs attack) Random Forest and save a Spark pipeline.

By default the training data is read from a CSV that contains the 41 feature
columns plus `Label`. Pass --source cassandra to read the ingested table
instead (only the columns stored there are then available as features).

    spark-submit flowids/train.py --csv /app/data/sample/netflow_sample.csv
"""
import argparse
import json

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from flowids.config import settings
from flowids.features import FEATURE_COLUMNS, LABEL_COLUMN
from flowids.spark_utils import build_session


def load_training_frame(spark, source: str, csv_path: str, max_rows: int):
    if source == "cassandra":
        from flowids import storage
        df = storage.read(spark, settings.flows_table)
        excluded = {"flow_id", "src_ip", "dst_ip", "src_ip_int", "dst_ip_int",
                    "flow_start_time", "attack_type", "label"}
        columns = [c for c in df.columns if c not in excluded]
        return df.select(*columns, F.col("label").alias("target")), columns
    df = spark.read.option("header", "true").csv(csv_path)
    df = df.select(*[F.col(c).cast("double") for c in FEATURE_COLUMNS],
                   F.col(LABEL_COLUMN).cast("int").alias("target"))
    if max_rows:
        df = df.limit(max_rows)
    return df, list(FEATURE_COLUMNS)


def run(args) -> dict:
    spark = build_session("flowids-train", cassandra=args.source == "cassandra")
    df, features = load_training_frame(spark, args.source, args.csv, args.max_rows)
    df = df.dropna().cache()

    print("Class balance:")
    df.groupBy("target").count().show()

    train, test = df.randomSplit([0.8, 0.2], seed=args.seed)
    assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid="skip")
    forest = RandomForestClassifier(labelCol="target", featuresCol="features",
                                    numTrees=args.trees, maxDepth=args.depth,
                                    subsamplingRate=0.8, seed=args.seed)
    model = Pipeline(stages=[assembler, forest]).fit(train)
    scored = model.transform(test)

    metrics = {
        "accuracy": MulticlassClassificationEvaluator(
            labelCol="target", metricName="accuracy").evaluate(scored),
        "f1": MulticlassClassificationEvaluator(
            labelCol="target", metricName="f1").evaluate(scored),
        "auc_roc": BinaryClassificationEvaluator(
            labelCol="target", metricName="areaUnderROC").evaluate(scored),
        "train_rows": train.count(), "test_rows": test.count(),
        "trees": args.trees, "max_depth": args.depth, "features": len(features),
    }
    importances = model.stages[-1].featureImportances.toArray()
    metrics["top_features"] = [
        {"feature": f, "importance": round(float(i), 4)}
        for f, i in sorted(zip(features, importances), key=lambda p: p[1], reverse=True)[:10]
    ]
    print(json.dumps(metrics, indent=2))

    settings.model_dir.mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(str(settings.model_path))
    (settings.model_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(f"Model saved to {settings.model_path}")
    spark.stop()
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["csv", "cassandra"], default="csv")
    parser.add_argument("--csv", default=str(settings.dataset_csv))
    parser.add_argument("--max-rows", type=int, default=0, help="0 = use everything")
    parser.add_argument("--trees", type=int, default=80)
    parser.add_argument("--depth", type=int, default=12)
    parser.add_argument("--seed", type=int, default=42)
    run(parser.parse_args())
