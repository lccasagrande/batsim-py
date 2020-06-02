Batsim-py |build| |coverage| |doc| |license| |last-release|
===========================================================

`Batsim <https://batsim.readthedocs.io/en/latest/>`_ is a scientific simulator commonly used to 
evaluate Resource and Job Management System (RJMS) policies. **Batsim-py allows using Batsim from Python 3**.

Main Features
-------------
- Developed on Python 3.8
- Simple API for evaluating the performance of Scheduling/Shutdown/DVFS policies
- Time/Event-based design
- Simple to be extended
- Fully documented

Installation
------------

1. Make sure you have Batsim v3.1.0 installed and working. Otherwise, you must follow `Batsim installation <https://batsim.readthedocs.io/en/latest/installation.html>`_ instructions. Check the version of Batsim with:

.. code-block:: bash

    batsim --version

2. Install batsim-py from PyPI:

.. code-block:: bash

    pip install batsim-py

3. [Optional] If you prefer, you can install batsim-py and run tests with:

.. code-block:: bash

    git clone https://github.com/lccasagrande/batsim-py.git
    cd batsim-py
    pip install -e .
    python setup.py test

4. [Optional] Go to the `Tutorials`_  section to find examples.

For further information, check the `API Documentation`_ to understand how to customize the simulation behavior.

Simulation Inputs
-----------------
To evaluate the behavior of a RJMS policy, you must provide:

- A `platform <https://batsim.readthedocs.io/en/latest/input-platform.html>`_ description (XML): describes the network and resource characteristics. 
- A `workload <https://batsim.readthedocs.io/en/latest/input-workload.html>`_ (JSON): defines the jobs to be submitted to the system and their execution profiles. 

The expected format of both files is the same adopted in Batsim. Check `Batsim docs <https://batsim.readthedocs.io/en/latest/>`_ to get further information on how to define platforms and workloads.

Tutorials
---------
The tutorials section provides examples of different simulation scenarios:

- `Scheduling`_
- `Shutdown`_
- `DVFS`_

API Documentation
-----------------
The API documentation provides information on classes and modules in the Batsim-py package.

- `Simulator`_
- `Events`_
- `Monitors`_
- `Jobs`_
- `Resources`_

.. _`Scheduling`: https://lccasagrande.github.io/batsim-py/tutorials/scheduling.html
.. _`Shutdown`: https://lccasagrande.github.io/batsim-py/tutorials/shutdown.html
.. _`DVFS`: https://lccasagrande.github.io/batsim-py/tutorials/dvfs.html

.. _`Simulator`: https://lccasagrande.github.io/batsim-py/api_doc/simulator.html
.. _`Events`: https://lccasagrande.github.io/batsim-py/api_doc/events.html
.. _`Monitors`: https://lccasagrande.github.io/batsim-py/api_doc/monitors.html
.. _`Resources`: https://lccasagrande.github.io/batsim-py/api_doc/resources.html
.. _`Jobs`: https://lccasagrande.github.io/batsim-py/api_doc/jobs.html

.. |build| image:: https://travis-ci.org/lccasagrande/batsim-py.svg?branch=master
    :alt: coverage
    :target: https://travis-ci.org/lccasagrande/batsim-py

.. |coverage| image:: https://coveralls.io/repos/github/lccasagrande/batsim-py/badge.svg?branch=master&kill_cache=1
    :alt: coverage
    :target: https://coveralls.io/github/lccasagrande/batsim-py?branch=master&kill_cache=1

.. |doc| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat
    :alt: doc
    :target: https://lccasagrande.github.io/batsim-py/index.html

.. |license| image:: https://img.shields.io/github/license/lccasagrande/batsim-py
    :alt: GitHub
    :target: https://github.com/lccasagrande/batsim-py/blob/master/LICENSE


.. |last-release| image:: https://img.shields.io/github/v/release/lccasagrande/batsim-py
    :alt: GitHub release (latest by date)
    :target: https://github.com/lccasagrande/batsim-py/releases/latest
