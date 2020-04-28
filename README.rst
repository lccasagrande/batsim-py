Batsim-py
=========
`Batsim <https://batsim.readthedocs.io/en/latest/>`_ is a scientific simulator commonly used to 
**evaluate Resource and Job Management System (RJMS) policies. Batsim-py allows using Batsim from Python 3** following 
an event-based approach or a time-based approach.

Policies that adopts an event-based approach acts in response of specific events while policies 
following a time-based approach acts periodically. Both approaches can coexist in the same policy,
read the API documentation to understand how this can be done and what events are dispatched.

Main Features
-------------
- Developed on Python 3.7
- Simple API for evaluating Scheduling/Shutdown/DVFS policies
- Event-based design
- Simple to be extended
- Fully documented
- Different monitors to collect simulation statistics.

Simulation Inputs
-----------------
To evaluate the behavior of a RJMS policy you must provide:

    - A `platform <https://batsim.readthedocs.io/en/latest/input-platform.html>`_ description (XML): describes the type of resources and how they are distributed within a computing platform (grid, cluster, ...).
    - A `workload <https://batsim.readthedocs.io/en/latest/input-workload.html>`_ (JSON): defines the jobs to be submitted to the system and their profiles. The profiles will determine the job behavior.

The expected format of both files are the same adopted in Batsim. 
Check their great `documentation <https://batsim.readthedocs.io/en/latest/>`_ to get further information on 
how to define platforms and workloads.

Quickstart 
------------

1. Clone the repository and navigate to the downloaded folder:

.. code-block:: bash

    git clone https://github.com/lccasagrande/batsim-py.git
    cd batsim-py

2. Install required packages: 

.. code-block:: bash

    pip install -e .

3. Go to the docs/source/tutorials directory and run an example.

.. code-block:: bash

    cd docs/source/tutorials/scheduling
    mkdir results
    python scheduling.py -o results

4. Check the simulation outputs in the results directory.


For further information, check the `API Documentation <https://lccasagrande.github.io/batsim-py/#api-documentation>`_ 
section to understand how to customize the simulation behavior and the 
`Tutorials <https://lccasagrande.github.io/batsim-py/#tutorials>`_  section to see different simulation scenarios.


Tutorials
---------
The tutorials section provides examples about different simulation scenarios:

- `Scheduling`_
- `Shutdown`_
- `DVFS`_

.. _`Scheduling`: https://lccasagrande.github.io/batsim-py/tutorials/scheduling.html
.. _`Shutdown`: https://lccasagrande.github.io/batsim-py/tutorials/shutdown.html
.. _`DVFS`: https://lccasagrande.github.io/batsim-py/tutorials/dvfs.html

API Documentation
-----------------
The API documentation provides information on classes and modules in the Batsim-py package.

- `Simulator`_
- `Events`_
- `Monitors`_
- `Jobs`_
- `Resources`_
- `Dispatcher`_

.. _`Simulator`: https://lccasagrande.github.io/batsim-py/api_doc/simulator.html
.. _`Events`: https://lccasagrande.github.io/batsim-py/api_doc/events.html
.. _`Monitors`: https://lccasagrande.github.io/batsim-py/api_doc/monitors.html
.. _`Resources`: https://lccasagrande.github.io/batsim-py/api_doc/resources.html
.. _`Jobs`: https://lccasagrande.github.io/batsim-py/api_doc/jobs.html
.. _`Dispatcher`: https://lccasagrande.github.io/batsim-py/api_doc/dispatcher.html