========
Features
========

* A :py:class:`.futurist.GreenThreadPoolExecutor` using `eventlet`_ green thread
  pools. It provides a standard `executor`_ API/interface and it also gathers
  execution statistics. It returns instances of
  :py:class:`.futurist.GreenFuture` objects.
* A :py:class:`.futurist.ProcessPoolExecutor` derivative that gathers execution
  statistics. It returns instances of :py:class:`.futurist.Future` objects.
* A :py:class:`.futurist.SynchronousExecutor` that **doesn't** run
  concurrently. It has the same `executor`_ API/interface and it also
  gathers execution statistics. It returns instances
  of :py:class:`.futurist.Future` objects.
* A :py:class:`.futurist.ThreadPoolExecutor` derivative that gathers
  execution statistics. It returns instances
  of :py:class:`.futurist.Future` objects.

.. _eventlet: http://eventlet.net/
.. _executor: https://docs.python.org/dev/library/concurrent.futures.html#executor-objects
