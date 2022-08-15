.. _reproducibility_rmodes:

R-Mode Standards
================

Each drop's provenance information defines what a workflow signature claims.
Inspired and extending current workflow literature, we define seven R-modes.
R-mode selection occurs when submitting a workflow to |daliuge| for initial filling and unrolling;
|daliuge| handles everything else automatically.

Additionally, the ALL mode will generate a signature structure containing separate hash graphs for
all supported modes, which is a good choice when experimenting with new workflow concepts or
certifying a particular workflow version.

Conversely, the NOTHING option avoids all provenance collection and processing, which may be of performance interest.
For now, this is also the default option if no rmode is specified.

Rerunning
---------
A workflow reruns another if they execute the same logical workflow; their logical components and
dependencies match.
At this standard, the runtime information is simply an execution status flag; the translate-time
information is logical template data excluding physical drops structurally.

When scaling up an in-development workflow or deploying to a new facility asserting that executions
rerun the original workflow build confidence in the workflow tools.
Rerunning is also useful where data scale and contents change, like an ingest pipeline.

Repeating
---------
A workflow repeats another if they execute the same logical workflow and a principally identical
physical workflow; their logical components, dependencies, and physical tasks match.
At this standard, the runtime information is still only an execution flag, and translate-time
information includes component parameters (in addition to rerunning information) and includes all physical drops structurally.

Workflows with stochastic results need statistical power to make scientific claims.
Asserting workflow repetitions allows using results in concert.

Recomputing
-----------
A workflow recomputes another if they execute the same physical workflow; their physical tasks and
dependencies match precisely.
In addition to repetition information, a maximal amount of detail for computing drops is stored
at this standard.

Recomputation is a meticulous approach that is helpful when debugging workflow deployments.

Reproducing
-----------
A workflow reproduces another if their scientific information match. In other words, the terminal
data drops of two workflows match in content.
The precise mechanism of establishing comparable data need not be a naive copy but is a
domain-specific decision.
At this standard, runtime and translate-time data only include data-drops structurally. At runtime,
data drops are expected to provide a characteristic summary of their contents

Reproductions are practical in asserting whether a given result can be independently reported or to
test an alternate methodology.
An alternate methodology could mean an incremental change to a single component
(somewhat akin to regression testing) or testing a vastly different workflow approach.

Replicating - Scientifically
----------------------------
A scientific replica reruns and reproduces a workflow execution.

Scientific replicas establish a workflow design as a gold standard for a given set of results.

Replicating - Computationally
-----------------------------
A computational replica recomputes and reproduces a workflow execution.

Computational replicas are useful if performing science on workflows directly
(performance claims etc.)

Replicating - Totally
---------------------
A total replica repeats and reproduces a workflow execution.

Total replicas allow for independent verification of results, adding direct credibility to
results coming from a workflow.
Moreover, if a workflow's original deployment environment is unavailable, a total replica is
the most robust assertion possibly placed on a workflow.
