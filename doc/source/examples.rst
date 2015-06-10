========
Examples
========

-----------------------------------------
Creating and using a synchronous executor
-----------------------------------------

.. testcode::

    import time

    import futurist

    def delayed_func():
        time.sleep(0.1)
        return "hello"

    e = futurist.SynchronousExecutor()
    fut = e.submit(delayed_func)
    print(fut.result())
    e.shutdown()

**Expected output:**

.. testoutput::

    hello


------------------------------------------
Creating and using a thread-based executor
------------------------------------------

.. testcode::

    import time

    import futurist

    def delayed_func():
        time.sleep(0.1)
        return "hello"

    e = futurist.ThreadPoolExecutor()
    fut = e.submit(delayed_func)
    print(fut.result())
    e.shutdown()

**Expected output:**

.. testoutput::

    hello

------------------------------------------------
Creating and using a green thread-based executor
------------------------------------------------

.. testcode::

    import time

    import futurist

    def delayed_func():
        time.sleep(0.1)
        return "hello"

    e = futurist.GreenThreadPoolExecutor()
    fut = e.submit(delayed_func)
    print(fut.result())
    e.shutdown()

**Expected output:**

.. testoutput::

    hello


-------------------------------------------
Creating and using a process-based executor
-------------------------------------------

.. testcode::

    import time

    import futurist

    def delayed_func():
        time.sleep(0.1)
        return "hello"

    e = futurist.ProcessPoolExecutor()
    fut = e.submit(delayed_func)
    print(fut.result())
    e.shutdown()

**Expected output:**

.. testoutput::

    hello
