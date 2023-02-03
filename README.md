# Overview

Skytether is an extension of SkyhookDM that introduces capabilities for computational storage.

This is the main repository for the Skytether project.


# Dependent Libraries

The Skytether project will be split into a few repositories to help encourage decoupling of subsystems.

This repository will integrate the various libraries as needed into a single library implementing the design of Skytether. The integrated subsystems are:
1. [Mohair](https://github.com/drin/mohair) -- The query processing layer for decomposable queries.
1. <Logical Storage Layer> -- This (placeholder) will be the logical storage layer. For now this is a Ceph-like interface (based on OSDs and SkyhookDM), but whether this continues to be prototype research code or a full library is yet to be determined.
1. <Physical Storage Layer> -- This (placeholder) will be the interfaces, or adapters, to services implementing the physical storage layer. Initially this will include adapters to [Kinetic](https://github.com/kinetic/kinetic-protocol) and [DuckDB](https://github.com/duckdblabs).

NOTE: as more subsystems are fleshed out, I will add them here.


# Usage

It is expected that Skytether will be a library that can be used by domain-specific applications or data management layers to leverage computational storage systems. One such example is <MSG Express>; this (placeholder) is a data management layer designed for single-cell gene expression data.


# Plans

For now, the first thing I plan to do is build up Mohair and migrate code from [Skytether-SingleCell](https://gitlab.com/skyhookdm/skytether-singlecell/), which is a research prototype that combines both Skytether and MSG Express. Decomposing `Skytether-SingleCell` will allow me to decouple skytether from gene expression specific semantics (allow for generalization) as well as developing MSG Express to support various [scverse](https://github.com/scverse) libraries. Supporting scverse libraries will hopefully allow us to more quickly support a wide variety of scientists and scientific applications transparently (one of our guiding principles).
