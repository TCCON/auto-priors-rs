# Configuration

## Execution

### Queues

## Data

## Default options

## Email

## Timing

## Blacklist

The configuration can include a blacklist of users who are no longer permitted
to request priors.
This is primarily for users that abuse the system and request a large volume of
priors without a good reason to do so.
(For example, a COCCON maintainer who needs to reprocess many sites' data with a
new PROFFAST version is expected to request a large volume of priors, but someone
attempting to produce a grid of priors for comparison with a model for their own
research can be blacklisted.)

In the code, a blacklist entry is modeled by the `orm::config::BlacklistEntry` struct.
In the configuration file, a blacklist entry will look like so:

```toml
[[blacklist]]
identifier = { type = "SubmitterEmail", submitter = "did_not_read_the_wiki@sorry.com" }
silent = true
reason = "ignored the restrictions on use given at https://tccon-wiki.caltech.edu/Main/ObtainingGinputData"
```

Each entry must contain all three fields:

- `identifier` (more details below) specifies the user to which this entry applies.
- `silent` specifies whether the user receives an email (`false`) or not (`true`) indicating
that their request has been blocked.
- `reason` is a string recording why the user has been blocked. When `silent` is `false`, it will
be included in the email sent to the user. Otherwise, it is useful as a record of why this user was
blocked.

The identifier is modeled in code by the `orm::config::BlacklistIdentifier` enum.
It was written as an enum in case other variants are required in the future.
Currently, the "SubmitterEmail" type is the only one available, which will reject any
input files with the email specified by the `submitter` field included in the input file.
This is rather weak, as the user can specify any email.
(The system does not check the input file email against the allowed list of FTP users.)


```admonish info
The blacklist is expected to be deprecated in the future, as it is essentially defining
permissions for jobs submitted via the FTP site. Work is underway to replace this with
a more modern and robust web form and API system, which will include a proper permission
system.
```
