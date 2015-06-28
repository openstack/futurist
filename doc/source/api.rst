===
API
===

---------
Executors
---------

.. autoclass:: futurist.GreenThreadPoolExecutor
    :members:

.. autoclass:: futurist.ProcessPoolExecutor
    :members:

.. autoclass:: futurist.SynchronousExecutor
    :members:
    :special-members: __init__

.. autoclass:: futurist.ThreadPoolExecutor
    :members:

-------
Futures
-------

.. autoclass:: futurist.Future
    :members:

.. autoclass:: futurist.GreenFuture
    :members:

---------
Periodics
---------

.. autoclass:: futurist.periodics.PeriodicWorker
    :members:
    :special-members: __init__, __len__

.. autofunction:: futurist.periodics.periodic

.. autoclass:: futurist.periodics.Watcher
    :members:

-------------
Miscellaneous
-------------

.. autoclass:: futurist.ExecutorStatistics
    :members:
