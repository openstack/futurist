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
    :special-members: __init__

.. autofunction:: futurist.periodics.periodic

-------------
Miscellaneous
-------------

.. autoclass:: futurist.ExecutorStatistics
    :members:

-------
Waiters
-------

.. autofunction:: futurist.waiters.wait_for_any
.. autofunction:: futurist.waiters.wait_for_all
.. autoclass:: futurist.waiters.DoneAndNotDoneFutures
