from pipelines.benchmarks.baselines.baseline_spark_glue import run_spark_glue_baseline


def run_fragmented_serialized_baseline(**kwargs):
    return run_spark_glue_baseline(**kwargs)


if __name__ == "__main__":
    run_fragmented_serialized_baseline()
