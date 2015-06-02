========
Futurist
========

Code from the future, delivered to you in the **now**.

* Free software: Apache license
* Documentation: http://docs.openstack.org/developer/futurist
* Source: http://git.openstack.org/cgit/openstack/futurist
* Bugs: http://bugs.launchpad.net/futurist

Features
--------

* A ``futurist.GreenThreadPoolExecutor`` using `eventlet`_ green thread
  pools. It provides a standard `executor`_ API/interface and it also gathers
  execution statistics.
* A ``futurist.ProcessPoolExecutor`` derivative that gathers execution
  statistics.
* A ``futurist.SynchronousExecutor`` that **doesn't** run concurrently. It
  has the same `executor`_ API/interface and it also gathers execution
  statistics.
* A ``futurist.ThreadPoolExecutor`` derivative that gathers execution
  statistics.

.. _eventlet: http://eventlet.net/
.. _executor: https://docs.python.org/dev/library/concurrent.futures.html#executor-objects
