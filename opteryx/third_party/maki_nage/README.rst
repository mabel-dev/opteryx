==========
DistoGram
==========


.. image:: https://badge.fury.io/py/distogram.svg
    :target: https://badge.fury.io/py/distogram

.. image:: https://github.com/maki-nage/distogram/workflows/Python%20package/badge.svg
    :target: https://github.com/maki-nage/distogram/actions?query=workflow%3A%22Python+package%22
    :alt: Github WorkFlows

.. image:: https://img.shields.io/codecov/c/github/maki-nage/distogram?style=plastic&color=brightgreen&logo=codecov&style=for-the-badge
    :target: https://codecov.io/gh/maki-nage/distogram
    :alt: Coverage

.. image:: https://readthedocs.org/projects/distogram/badge/?version=latest
    :target: https://distogram.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://mybinder.org/badge_logo.svg
    :target: https://mybinder.org/v2/gh/maki-nage/distogram/master?urlpath=notebooks%2Fexamples%2Fdistogram.ipynb


DistoGram is a library that allows to compute histogram on streaming data, in
distributed environments. The implementation follows the algorithms described in
Ben-Haim's `Streaming Parallel Decision Trees
<http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf>`__

Get Started
============

First create a compressed representation of a distribution:

.. code:: python

    import numpy as np
    import distogram

    distribution = np.random.normal(size=10000)

    # Create and feed distogram from distribution
    # on a real usage, data comes from an event stream
    h = distogram.Distogram()
    for i in distribution:
        h = distogram.update(h, i)


Compute statistics on the distribution:

.. code:: python

    nmin, nmax = distogram.bounds(h)
    print("count: {}".format(distogram.count(h)))
    print("mean: {}".format(distogram.mean(h)))
    print("stddev: {}".format(distogram.stddev(h)))
    print("min: {}".format(nmin))
    print("5%: {}".format(distogram.quantile(h, 0.05)))
    print("25%: {}".format(distogram.quantile(h, 0.25)))
    print("50%: {}".format(distogram.quantile(h, 0.50)))
    print("75%: {}".format(distogram.quantile(h, 0.75)))
    print("95%: {}".format(distogram.quantile(h, 0.95)))
    print("max: {}".format(nmax))


.. code:: console

    count: 10000
    mean: -0.005082954640481095
    stddev: 1.0028524290149186
    min: -3.5691130319855047
    5%: -1.6597242392338374
    25%: -0.6785107421744653
    50%: -0.008672960012168916
    75%: 0.6720718926935414
    95%: 1.6476822301131866
    max: 3.8800560034877427

Compute and display the histogram of the distribution:

.. code:: python

    hist = distogram.histogram(h)
    df_hist = pd.DataFrame(np.array(hist), columns=["bin", "count"])
    fig = px.bar(df_hist, x="bin", y="count", title="distogram")
    fig.update_layout(height=300)
    fig.show()

.. image:: docs/normal_histogram.png
  :scale: 60%
  :align: center

Install
========

DistoGram is available on PyPi and can be installed with pip:

.. code:: console

    pip install distogram


Play With Me
============

You can test this library directly on this
`live notebook <https://mybinder.org/v2/gh/maki-nage/distogram/master?urlpath=notebooks%2Fexamples%2Fdistogram.ipynb>`__.


Performances
=============

Distogram is design for fast updates when using python types. The following
numbers show the results of the benchmark program located in the examples. 

On a i7-9800X Intel CPU, performances are:

============  ==========  =======  ==========
Interpreter   Operation   Numpy         Req/s
============  ==========  =======  ==========
pypy 7.3      update      no          6563311
pypy 7.3      update      yes          111318
CPython 3.7   update      no           436709
CPython 3.7   update      yes          251603
============  ==========  =======  ==========

On a modest 2014 13" macbook pro, performances are:

============  ==========  =======  ==========
Interpreter   Operation   Numpy         Req/s
============  ==========  =======  ==========
pypy 7.3      update      no          3572436
pypy 7.3      update      yes           37630
CPython 3.7   update      no           112749
CPython 3.7   update      yes           81005
============  ==========  =======  ==========

As you can see, your are encouraged to use pypy with python native types. Pypy's
jit is penalised by numpy native types, causing a huge performance hit. Moreover
the streaming phylosophy of Distogram is more adapted to python native types
while numpy is optimized for batch computations, even with CPython.


Credits
========

Although this code has been written by following the aforementioned research
paper, some parts are also inspired by the implementation from
`Carson Farmer <https://github.com/carsonfarmer/streamhist>`__.

Thanks to `John Belmonte <https://github.com/belm0>`_ for his help on
performances and accuracy improvements.