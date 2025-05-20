# Contributing to Kafka Delta Ingest

Welcome, and thank you for your interest in contributing to **Kafka Delta Ingest**! We appreciate all contributions, whether it's reporting bugs, suggesting features, improving documentation, or submitting code changes.

This guide outlines how to get started and how to structure your contributions.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Reporting Issues](#reporting-issues)
- [Submitting Changes](#submitting-changes)
- [Development Setup](#development-setup)
- [Style Guidelines](#style-guidelines)
- [Major Contributions](#major-contributions)
- [Sign Your Work](#sign-your-work)
- [Contributor Community and Decision-Making](#contributor-community-and-decision-making)
- [Questions?](#questions)

---

## Code of Conduct

We follow the [Delta Lake Code of Conduct](https://github.com/delta-io/delta/blob/master/CODE_OF_CONDUCT.md). Please be respectful and constructive in all interactions.

---

## Reporting Issues

Please use GitHub Issues to report bugs, request features, or propose protocol changes.

When opening an issue, include the following:

- **Type**: Bug, Feature request, Protocol change, Other  
- **Connector**: Kafka Delta Ingest  
- **Overview**: A short description of the issue or request  
- **Steps to reproduce** (for bugs): A clear way to reproduce the behavior  
- **Expected/Observed behavior**  
- **Environment Info**: Versions of Kafka Delta Ingest, Delta Lake, etc.  
- **Willingness to Contribute**: Let us know if you're able to help implement a fix or feature  

> You can refer to [these Delta Lake issue templates](https://github.com/delta-io/delta/issues/new/choose) as a guideline.

---

## Submitting Changes

When submitting a pull request:

1. Fork the repo and create your branch from `main`.
2. Keep PRs focused and small. One feature/fix per PR.
3. Add unit and/or integration tests if applicable.
4. Use a consistent and descriptive title.
5. In the PR description, include:
   - **Connector**: Kafka Delta Ingest
   - **Description**: What this PR does and why
   - **Testing**: How was this tested?
   - **User-facing changes**: Any new behavior, configuration, or backwards-incompatible changes?
   - **Signed-off-by**: Include a DCO sign-off line in each commit (see below).
6. Link related issues if applicable.

**Example PR Description:**
```
Which Delta project/connector is this regarding?

* Kafka Delta Ingest

Description
Fix bug in offset tracking logic during stream resume.

How was this patch tested?
Added unit tests and verified with a local Kafka + Delta Lake setup.

Does this PR introduce any user-facing changes?
Yes, fixes incorrect stream offset replay on recovery.

Signed-off-by: Jane Smith [jane.smith@email.com](mailto:jane.smith@email.com)

````

---

## Development Setup

To get started locally:

1. Clone the repo:
   ```bash
   git clone https://github.com/<your-username>/kafka-delta-ingest.git
   cd kafka-delta-ingest
    ````

2. Refer to the [README](README.adoc) for build and usage instructions.

---

## Style Guidelines

* Follow standard Rust coding conventions.
* Keep code clean and well-commented.
* Use meaningful commit messages.
* Format code using the project's standard tools (e.g., `cargo fmt`).

---

## Major Contributions

### Communication

Before starting work on a major feature, please reach out via GitHub (issue or discussion), Slack, or email. This helps us:

* Ensure no one else is already working on it.
* Clarify scope and expectations.
* Avoid duplicate or conflicting work.

A **major feature** is any change that:

* Modifies more than 100 lines of code (excluding tests), or
* Introduces new user-facing functionality or behavior changes.

Please create a GitHub issue to track the proposal. If needed, include a design document in the issue or link to it (hosted in a public location).

If your contribution is an extension or plugin, please review the extension policy if one exists or consult maintainers first.

**Small patches and bug fixes do not require prior discussion.** You're welcome to directly open a PR or issue.

If you have example code demonstrating a use case or feature, feel free to contribute it under an `examples/` directory via PR.

---

## Sign Your Work

We require contributors to sign their work using the [Developer Certificate of Origin (DCO)](https://developercertificate.org/).

### What does it mean?

By signing your commits, you confirm that:

* You wrote the code or have permission to contribute it under the project's license.
* You agree your contribution is public and recorded permanently.

### How to sign commits

To add the required signature to each commit message, use:

```bash
git commit -s -m "your commit message"
```

This appends a `Signed-off-by` line like this:

```
Signed-off-by: Jane Smith <jane.smith@email.com>
```

Make sure your Git name and email are properly configured:

```bash
git config --global user.name "Jane Smith"
git config --global user.email "jane.smith@email.com"
```

We do not accept anonymous or pseudonymous contributions.

---

## Contributor Community and Decision-Making

Kafka Delta Ingest is part of the broader Delta Lake ecosystem, which is supported by contributors from over 50 organizations. Since 2019, more than 190 developers have contributed to Delta Lake across its repositories.

We encourage all contributors to engage with the Delta Lake community:

- Join the [Delta Users Slack](https://delta-users.slack.com/join/shared_invite/zt-35g79pi1c-ybZkUVGGh65blPsGwFIp2Q) to connect with other developers, ask questions, and discuss ideas.
- Review the [Delta Lake Technical Charter](https://delta.io/pdfs/delta-charter.pdf) to understand how technical decisions are made and how you can get involved.

---

## Questions?

For anything not covered here, feel free to open an issue or discussion.

Thanks again for helping improve **Kafka Delta Ingest**!