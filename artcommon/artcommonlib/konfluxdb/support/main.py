import logging

from artcommonlib.konfluxdb.support.konflux_build import generate_random_build
from artcommonlib.konfluxdb.support.konflux_db import SupportKonfluxDb
from artcommonlib.logutil import setup_logging

PROJECT = 'openshift-gce-devel'
DATASET_ID = 'art_test'
TABLE_ID = 'builds'


def init_db(k_db):
    k_db.init_db(TABLE_ID, delete=True)
    k_db.add_builds([generate_random_build().to_dict() for _ in range(1000)], TABLE_ID)


def search_builds(k_db):
    builds = k_db.search_builds_by_component(TABLE_ID, 'openshift-enterprise-egress-dns-proxy')
    for _ in range(3):
        build = next(builds)
        print(build)

    build = k_db.search_builds_by_id_or_nvr(TABLE_ID, build_id='5510ead8-532d-1e97-411c-43f93bdba1f4')
    print(build)

    build = k_db.search_builds_by_id_or_nvr(
        TABLE_ID, nvr='vmware-vsphere-syncer-v4.13.0.20240802122441.assembly.stream')
    print(build)


if __name__ == '__main__':
    setup_logging(log_level=logging.INFO, debug_log_path='/tmp/info.log')
    db = SupportKonfluxDb(project_id=PROJECT, dataset_id=DATASET_ID)
    # init_db(db)
    # search_builds(db)
