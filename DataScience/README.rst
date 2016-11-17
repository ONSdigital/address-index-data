ONS AI - Data Science
=====================

Content
-------

This package contains prototype codes written for ONS Address Index project.
Note that these codes are not production ready. Also note that not all binaries
are held in the repository. Please contact the ONS AI team to receive requires files.


Prerequisites
-------------

* Python 3.5 or higher
* Repository DataScience folder in PYTHONPATH (e.g. '''export PYTHONPATH="~/ONSAI/DataScience/:$PYTHONPATH"''')


Dependencies
------------

See requirements.txt for packages installed from Python Package Index and
spec-file.txt for Anaconda Python environment specification.


Running
-------

Probabilistic Parser
....................

After placing the Conditional Random Fields model file to the DataScience/data folder import
the probabilistic parser:

.. code-block:: python

    from ProbabilisticParser import parser

Assuming that the import executes without errors, run the parser or tagger using
methods parser.parser or parser.tag:

.. code-block:: python

    parser.tag('FLAT ABC 7-9 TEDWORTH SQUARE LONDON SW3 4DU')

Which returns::

    OrderedDict([('SubBuildingName', 'FLAT ABC'),
                 ('BuildingName', '7-9'),
                 ('StreetName', 'TEDWORTH SQUARE'),
                 ('TownName', 'LONDON'),
                 ('Postcode', 'SW3 4DU')])



Address Linking Prototype
.........................

The various linking prototypes can be found Analytics/prototype folder.
These can be run from command line simply using cPython interpreter::

    python edgeCasesTest.py

Note however that one needs AddressBase and the test files to run the
prototype linking codes.


Last Updated
------------

:date: 17th of November 2016