# Paper

`sparkquest.tex` — the technical paper, *"SparkQuest: A Gamified, Browser-Based
System for Teaching Python, PySpark, and Spark Structured Streaming."* A compiled
`sparkquest.pdf` (4 pages, two-column) is included.

All numbers, tables, and figures come from the real benchmark runs in
[`../benchmarks/`](../benchmarks/); regenerate the figures with
`python ../benchmarks/run_benchmarks.py --phase figures`.

## Build

```bash
pdflatex sparkquest.tex && pdflatex sparkquest.tex   # twice for references
```

The document uses the `article` class styled to emulate the IEEE two-column look,
because `IEEEtran.cls` was not on the build host. **For a conference/journal
submission**, switch to the official class: replace the preamble's
`\documentclass`/styling block with `\documentclass[conference]{IEEEtran}` (drop
the `titlesec`/`geometry` overrides), put the author block in `\IEEEauthorblockN`/
`\IEEEauthorblockA`, and compile on Overleaf. The body, tables, figures, and
bibliography carry over unchanged.
