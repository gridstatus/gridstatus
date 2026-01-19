.. _api_ref:

API Reference
=============

.. currentmodule:: gridstatus

Supported Independent System Operators (ISOs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoapisummary::

    gridstatus.base.ISOBase
    gridstatus.AESO
    gridstatus.CAISO
    gridstatus.Ercot
    gridstatus.IESO
    gridstatus.ISONE
    gridstatus.MISO
    gridstatus.NYISO
    gridstatus.PJM
    gridstatus.SPP

ISO API Clients
~~~~~~~~~~~~~~~

Some ISOs provide official APIs that offer additional data or functionality beyond
what's available through web scraping. These API clients require authentication
and are alternatives to the standard ISO classes above.

.. autoapisummary::

    gridstatus.ercot_api.ercot_api.ErcotAPI
    isone_api.ISONEAPI
    gridstatus.miso_api.MISOAPI

Other Data Sources
~~~~~~~~~~~~~~~~~~

.. autoapisummary::

    gridstatus.eia.EIA

Utilities
~~~~~~~~~

.. autoapisummary::

    gridstatus.get_iso
    gridstatus.list_isos
    gridstatus.utils.make_availability_table
    gridstatus.utils.filter_lmp_locations
    gridstatus.load_folder

Enums and Data Classes
~~~~~~~~~~~~~~~~~~~~~~

.. autoapisummary::

    gridstatus.Markets
    gridstatus.base.InterconnectionQueueStatus
    gridstatus.base.GridStatus

Exceptions
~~~~~~~~~~

.. autoapisummary::

    gridstatus.NotSupported
    gridstatus.NoDataFoundException

Visualization
~~~~~~~~~~~~~

.. autoapisummary::

    gridstatus.viz.dam_heat_map
    gridstatus.viz.load_over_time
