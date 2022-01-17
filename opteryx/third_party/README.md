## Third Party Libraries

These are third-party modules which we include into the Opteryx codebase.

- [**accumulation_tree**](https://github.com/tkluck/accumulation_tree)
- [**distogram**](https://github.com/maki-nage/distogram)
- [**hyperloglog**](https://github.com/svpcom/hyperloglog)
- [**pyarrow_ops**](https://github.com/TomScheffers/pyarrow_ops)
- [**pyudorandom**](https://github.com/mewwts/pyudorandom)
- [**tdigest**](https://github.com/CamDavidsonPilon/tdigest)
- [**uintset**](https://github.com/standupdev/uintset/)

Being in the Opteryx codebase means they are likely to have some deviations from the
original source due to the following reasons:

- Formatting with Black
- Resolving errors from Security Testing
- Resolving errors from Quality Testing

These modules are excluded from maintainability checks.

Other changes may have been made to improve performance, readability or to reuse
existing imports (for example using CityHash as per other parts of Opteryx instead of
a new hash algorithm for the included library).
