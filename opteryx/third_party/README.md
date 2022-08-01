## Third Party Libraries

These are third-party modules which we include into the Opteryx codebase.

- [**datetime_truncate**](https://github.com/mediapop/datetime_truncate)
- [**distogram**](https://github.com/maki-nage/distogram)
- [**hyperloglog**](https://github.com/ekzhu/datasketch)
- [**mbleven**](https://github.com/fujimotos/mbleven)
- [**pyarrow_ops**](https://github.com/TomScheffers/pyarrow_ops)

These modules have been removed from the codebase

- [**bintrees**](https://github.com/mozman/bintrees)
- [**accumulation_tree**](https://github.com/tkluck/accumulation_tree)
- [**pyudorandom**](https://github.com/mewwts/pyudorandom)
- [**sketch**](https://github.com/dnbaker/sketch)
- [**tdigest**](https://github.com/CamDavidsonPilon/tdigest)
- [**uintset**](https://github.com/standupdev/uintset/)

Being in the Opteryx codebase means they are likely to have some non-annotated deviations from the original source due to the following reasons:

- Formatting with Black
- Resolving errors from Security Testing
- Resolving errors from Quality Testing

These modules may be excluded from some quality checks.

Other changes may have been made to improve performance, readability or to reuse existing imports (for example, using CityHash as per other parts of Opteryx instead of a new hash algorithm for the included library).

Where changes have been made to extend or alter functionality, these have been noted inline in the code. 
