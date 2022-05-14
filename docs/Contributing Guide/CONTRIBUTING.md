# Contribution Guide

We'd love to accept your patches and contributions to this project. There are just a few small guidelines you need to follow.

## We Develop with GitHub

All submissions, including submissions by core project members, require review. We use GitHub pull requests for this purpose. Consult [GitHub Help](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests) for more information on using pull requests.

## All Change must meet Quality Bars

We have included a number of tests which run automatically when code is submitted using GitHub Actions. These tests must pass before a change is elligible to be merged.

These tests include a regression test suite, security tests and other quality checks.

## All contributions will be under Apache 2.0 Licence

The project is licensed under [Apache 2.0](https://github.com/mabel-dev/opteryx/blob/main/LICENSE), your submissions will be under this same licence.

## Format and Style Guidance

### Code Style

For consistent style, code should look like:

- Imports on separate lines (`imports` then `froms`)
- Variables should be in `snake_case`
- Classes should be in `PascalCase`
- Constants should be in `UPPER_CASE`
- Methods have docstrings
- [Black](https://github.com/psf/black) formatted
- Self-explanatory method, class and variable names
- Type hints, especially in function definitions

Code should have:  

- Corresponding unit/regression tests
- Attributed external sources - even if there is no explicit license requirement

A note about comments:  

- Computers will interpret anything, humans need help interpreting code
- Prefer readable code over verbose comments
- Humans struggle with threading, recursion, parallelization, variables called `x` and more than 10... of anything
- Comments should usually be more than just the code in other words
- Good variable names and well-written code doesn't always need comments

Code pushes should have prefixes:  

- `FIX\#nn` merges which fix [GitHub issues](https://github.com/mabel-dev/opteryx/issues) (#nn represents the GitHub issue number)
- `FEATURE\#nn` merges which implement [GitHub feature requests](https://github.com/mabel-dev/opteryx/issues) (#nn represents the GitHub issue number) 
- `[DOCS]` improvements to documentation

Docstrings should look like this for non-trivial methods:
~~~python
def sample_method(
        param_1: str,
        param_2: Optional[int] = None) -> bool:
"""
A short description of what the method does.

Parameters:
    param_1: string
        describe this parameter
    param_2: integer (optional)
        describe this parameter, if it's optional, what is the default

Returns:
    boolean
"""
~~~