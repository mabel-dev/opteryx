# How Opteryx is Tested

Opteryx utilizes a number of test approaches to help ensure the system is performant, secure and correct. The key test harnesses which are used to test Opteryx are listed here.

## Unit Testing

**Frequency**: CI  
**Maturity**: Medium

Part of the CI process. Tests specific aspects of the internals.

Combined with the SQL Battery test, the aim is for 95% coverage (with explicit exceptions). Whilst 95% coverage does not ensure the tests are 'good', it does help ensures any material changes to the function of the system are captured early.

## SQL Battery

**Frequency**: CI  
**Maturity**: Medium

Part of the CI process. Executes hundreds of hand-crafted SQL statements against the engine.

The SQL Battery helps to ensure the entire system performs as expected and when used in tandem with Unit Testing, which primarily focuses on ensuring parts work as they should, this provides a level of confidence that the system continues to perform as expected.

## Performance Testing

**Frequency**: Ad hoc  
**Maturity**: Low

To measure impact of changes.

## SQL Logic Test

**Frequency**: Ad hoc  
**Maturity**: Low

Runs SQL statements against Operyx and SQLite (which performs [similar testing](https://www.sqlite.org/testing.html) against other database engines) to verify Opteryx returns the same answer as SQLite.

## Security and Code Quality Testing

**Frequency**: CI  
**Maturity**: Medium

Bandit, Semgrep, Black, MyPy, PyLint, PerfLint, Fides, SonarCloud