import json

import great_expectations as gx
from great_expectations.checkpoint import UpdateDataDocsAction

from pipeline.config import ROOT
from pipeline.extract import quarantine
from pipeline.observability import event


def validate(frame, work, name, suite_name):
    context = gx.get_context(mode="file", project_root_dir=str(work / "quality"))
    source = context.data_sources.add_pandas(name=f"source_{name}")
    asset = source.add_dataframe_asset(name=name)
    batch = asset.add_batch_definition_whole_dataframe("batch")
    suite = gx.ExpectationSuite(name=name)
    rules = json.loads((ROOT / f"great_expectations/expectations/{suite_name}.json").read_text())
    for rule in rules:
        suite.add_expectation(getattr(gx.expectations, rule["type"])(**rule["kwargs"]))
    suite = context.suites.add(suite)
    definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=name, data=batch, suite=suite)
    )
    config = json.loads((ROOT / "great_expectations/checkpoints/quality.json").read_text())
    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name=name, validation_definitions=[definition],
        actions=[UpdateDataDocsAction(name="html")], result_format=config["result_format"],
    ))
    result = checkpoint.run(batch_parameters={"dataframe": frame})
    validation = next(iter(result.run_results.values()))
    score = validation.statistics["successful_expectations"] / len(rules)
    event(work, "quality", suite=name, success=result.success, score=score)
    if not result.success:
        quarantine(frame, work, f"gx_failed:{suite_name}")
        event(work, "alert", reason="data_quality", suite=name)
        raise ValueError(f"Great Expectations: {name} failed")
    return score
