ocp-build-data-validator
========================

.. image:: https://badge.fury.io/py/rh-ocp-build-data-validator.svg
  :target: https://badge.fury.io/py/rh-ocp-build-data-validator
.. image:: https://codecov.io/gh/openshift/ocp-build-data-validator/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/openshift/ocp-build-data-validator

Validation of `ocp-build-data <https://gitlab.cee.redhat.com/openshift-art/ocp-build-data>`__
Image & RPM declarations

Installation
------------

::

    $ pip install rh-ocp-build-data-validator

Usage
-----

Validating a single file:

::

    $ validate-ocp-build-data path/to/ocp-build-data/images/or/rpms.yml

Validating the whole `ocp-build-data <https://gitlab.cee.redhat.com/openshift-art/ocp-build-data>`__
repository:

::

    $ validate-ocp-build-data path/to/ocp-build/data/{images,rpms}/*

Validations
-----------

-  YAML Format
-  YAML Schema (supported schemas: **Image** and **RPM**)
-  Presence of corresponding `DistGit <http://pkgs.devel.redhat.com>`__ repository
   (needs to match YAML filename)

   -  Presence of corresponding **branch** on `DistGit <http://pkgs.devel.redhat.com>`__

-  Presence of `GitHub <https://github.com>`__ repository (if declared)

   -  Presence of **branch** on `GitHub <https://github.com>`__ (if declared)
   -  Presence of **dockerfile** on `GitHub <https://github.com>`__ (if declared)
   -  Presence of **manifests-dir** on `GitHub <https://github.com>`__ (if declared)

-  Stream is supported (if declared, value must match one of the entries in ``streams.yml``)
-  Member exists (if declared, member must be another existing declaration under ``images/``)
