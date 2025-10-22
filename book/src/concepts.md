# Concepts

## Jobs and queues

This automation works similarly to a job scheduler like SLURM or PBS that you might find on a high performance cluster.
Each request for priors is referred to as a "job" and represents one call to `ginput` to generate priors for a certain time period and set of locations.
These jobs can be added several ways:

1. user request through an input file uploaded to the server,
2. automatically to generate standard sites' priors, or
3. by an administrator using the command line interface.

The jobs are also divided into queues.
A queue represents a pool of computing resources to be shared among jobs in that queue.
Within each queue, the jobs are processed by default in the order that they were submitted.
There is also a fair-share option which can be assigned to a given queue that adjusts individual jobs' priority based on how heavily that user has been using the system.

Jobs can also be assigned a custom base priority that allows administrators to prioritize certain jobs.
This is used by the automation when generating standard sites to ensure that jobs generating new priors (for the most recent days) take priority over any backfilling of old priors for a new site recently added to the system.

The automation expects at least two queues: one for user submitted requests and one for standard sites.
This division of resources ensures that standard site processing is not delayed when large user requests are being processed.

## Standard sites

A standard site is a location where priors are routinely needed.
All established TCCON sites are added as standard sites, and other locations (such as quasi-permanent EM27/SUN sites) can ask to be added.
The automation will routinely add jobs to the standard site queue for all sites in its list.
If a site moves or is taken permanently offline, that can be noted in the database to avoid generating unnecessary priors.

For sites that need priors routinely, generating them as a standard site has benefits for both the automation and the user.
For the user, this alleviates the need for them to submit requests and helps ensure that the priors are ready as early as possible.
For the automation, this allows the jobs to be batched in such a way that certain calculations in `ginput` can be shared across all of the sites, reducing the compute time compared to what would be needed if each site submitted a separate request.
