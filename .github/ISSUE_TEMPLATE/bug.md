name: Bug report
description: Report a bug in the dbt-colibri package
title: "[BUG] "
labels: [Bug]
assignees: []

body:
  - type: markdown
    attributes:
      value: |
        Thanks for reporting a bug in **dbt-colibri**! Please fill out the form below to help us reproduce and fix the issue efficiently.

  - type: textarea
    id: description
    attributes:
      label: Describe the bug
      description: A clear and concise description of the problem you're experiencing.
      placeholder: What did you expect to happen, and what actually happened?
    validations:
      required: true

  - type: textarea
    id: reproduction
    attributes:
      label: To Reproduce
      description: Steps to reproduce the behavior.
      placeholder: |
        1. Run command '...'
        2. Use configuration '...'
        3. Observe output or error
      render: shell
    validations:
      required: true

  - type: textarea
    id: expected
    attributes:
      label: Expected behavior
      description: What was the expected behavior?

  - type: input
    id: screenshots
    attributes:
      label: Screenshots (optional)
      description: If applicable, link or drag-and-drop screenshots after submitting the issue.

  - type: input
    id: colibri-version
    attributes:
      label: dbt-colibri version
      description: You can find this by running `colibri --version` or checking your `pyproject.toml`
      placeholder: e.g. 0.2.0b3
    validations:
      required: true

  - type: input
    id: dbt-version
    attributes:
      label: dbt version
      placeholder: e.g. 1.8.1
    validations:
      required: true

  - type: input
    id: python-version
    attributes:
      label: Python version
      placeholder: e.g. 3.11.7

  - type: input
    id: operating-system
    attributes:
      label: Operating system
      placeholder: e.g. macOS 14.4, Ubuntu 24.04, Windows 11

  - type: textarea
    id: logs
    attributes:
      label: Logs or traceback
      description: Paste relevant logs or traceback messages here, if available.
      render: shell

  - type: textarea
    id: context
    attributes:
      label: Additional context
      description: Any other information you think is relevant.

  - type: dropdown
    id: willing-to-contribute
    attributes:
      label: Would you be willing to contribute a fix?
      description: Let us know if you'd like to help fix this issue.
      options:
        - "Yes"
        - "No"
        - "Not sure – I need guidance"
