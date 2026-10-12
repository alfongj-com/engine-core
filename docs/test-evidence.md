# Test evidence archive

Generated benchmark reports, load-test notes, raw captures and formal execution
logs live on the [load-tests branch](https://github.com/alfongj-com/engine-core/tree/load-tests).
The archive preserves the original successful, failed and incomplete experiments.
Runtime code, regression tests, formal models and operational design docs remain
in this branch.

- [Capacity and chaos results](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/capacity-2026-09-26/RESULTS.md)
- [Concise findings and next priorities](https://github.com/alfongj-com/engine-core/blob/load-tests/TO_ALFONSO.md)
- [Historical verification record](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/verification.md)
- [Complete archive index](https://github.com/alfongj-com/engine-core/blob/load-tests/LOAD_TESTS.md)

The split removes 1,795 evidence/report files from the implementation PR. One
supervisor script needed by a regression test is retained byte-for-byte as
`scripts/fixtures/capacity_supervisor.py`. Reports describe their original source
commits; they do not automatically qualify later changes.
