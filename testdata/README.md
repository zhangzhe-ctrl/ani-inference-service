# Accelerator quantity fixtures

`accelerator-quantity-plans.json` contains Whole1, Whole2, Shared1 and WholeProfileMax plans emitted
by Accelerator's real `BuildPlan` and checked by `ValidatePlan` in
`TestQuantityInferenceBuilderVectors`. Whole1/Whole2 share the same max=2 profile;
WholeProfileMax is the single maximum-above-two boundary fixture (max=4, N=4);
all requests use one `kserve-container` Pod. This is isolated software input,
not hardware evidence, a published supply group, or a successful GPU allocation.

Regenerate through the Accelerator test and retain its complete JSON output;
do not edit quantities, totals, runtime or digests by hand. The Inference tests
check public/managed acceptance, both protobuf adapters, actual PostgreSQL
roundtrip, CurrentRuntime, rendering, and original-charge deletion context.
