"""The tests that every Stream Store must pass.

A package runs them on its own Store. Its ``tests/conftest.py`` defines the fixture
``new_store``, a function that gives a new Store with its own Streams and takes the
options of the Store as keyword arguments. The fixture ``client`` is in ``python/conftest.py``.
The test module of a package imports the modules of this package, so that pytest collects
their tests.
"""
