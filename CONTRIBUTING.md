# Contributing to Opteryx

We're excited your considering contributing to Opteryx and look forward to reviewing your contribution. 

Opteryx is open source software, we value your feedback and want to make contributing to this project as easy and transparent as possible.

We use GitHub to host code, and to track issues and feature requests. 

**Quick Links**   
[Submitting Code Changes](#submitting-code-changes)     
[Reporting a Bug](#reporting-a-bug)    
[Contibutor Documentation](https://opteryx.dev/latest/contributing/contributing/)

## Submitting Code Changes

We actively welcome your improvements to the Opteryx project. If you are addressing an existing issue or a minor change, you can just raise a Pull Request with the details of the changes you are making and why, new Pull Requestss have a template to help prompt you for the information we'd like to see.

If you're unfamiliar with the Pull Request process, see the [GitHub Help Pages](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests) for more information on using pull requests.

If you are considering a significant change to the code, or adding a new feature not discussed previously, we ask that you start a [discussion](https://github.com/mabel-dev/opteryx/discussions) or raise an [issue](https://github.com/mabel-dev/opteryx/issues) before starting.

### All Changes must meet Quality Bars

We have included a number of tests which run automatically when code is submitted using GitHub Actions. These tests must pass before a change is elligible to be merged.

These tests include a regression test suite, security tests and other quality checks.

New contributors may need approval before these run.

#### All contributions will be under Apache 2.0 Licence

The project is licensed under [Apache 2.0](https://github.com/mabel-dev/opteryx/blob/main/LICENSE), your submissions will be under this same licence.

Code which is licensed under a compatible or more open license (e.g. Public Domain) will be considered.

#### Format and Style Guidance

For consistent style, code should look like:

- Imports on separate lines (`import`s then `from`s)
- Variables should be in `snake_case`
- Classes should be in `PascalCase`
- Constants should be in `UPPER_SNAKE_CASE`
- Methods should have docstrings
- [Black](https://github.com/psf/black) formatted
- Self-explanatory method, class and variable names
- Type hints, especially in function definitions

Code should have:  

- Corresponding unit/regression tests
- Attributed external sources - even if there is no explicit license requirement

A note about comments:  

- Computers will interpret anything, humans need help interpreting code
- Spend time writing readable code rather than verbose comments
- Humans struggle with threading, recursion, parallelization, variables called `x` and more than 10... of anything
- Comments should usually be more than just the code in other words
- Well-written code doesn't always need comments

## Reporting a Bug

We would love to hear how we can make Opteryx better. We use GitHub [issues](https://github.com/mabel-dev/opteryx/issues) to track bugs and improvement requests.

Great Bug and Feature requests usually have:

- A summary of the problem
- Sample code, if possible
- What you expected to happen
- What actually happened
- Any other context (did you try other things to get it to work, did it stop working after something happened)

We provide a template for [Bug Reports](https://github.com/mabel-dev/opteryx/issues/new?labels=Bug+%F0%9F%AA%B2&template=bug_report.md&title=%F0%9F%AA%B2) to help prompt you for information to help us act on your report.