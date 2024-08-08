import inspect
import typing
from datetime import datetime, timedelta
import hashlib
import random

from google.cloud.bigquery import SchemaField

from artcommonlib.konfluxdb.konflux_build import KonfluxBuildOutcome, KonfluxBuild, ArtifactType, Engine

NAMES = [
    'openshift-enterprise-egress-dns-proxy',
    'openshift-enterprise-hyperkube',
    'ci-openshift-base',
    'dpu-cni',
    'ose-azure-cluster-api-controllers',
    'egress-router-cni',
    'telemeter',
    'openshift-enterprise-ansible-operator',
    'clusterresourceoverride',
    'ose-ibm-vpc-block-csi-driver-operator',
    'ose-machine-api-provider-gcp',
    'vmware-vsphere-syncer',
    'ose-ovirt-csi-driver',
    'networking-console-plugin',
    'ovn-kubernetes-microshift',
    'ingress-node-firewall-operator',
    'ose-csi-driver-shared-resource-webhook',
    'ose-cluster-control-plane-machine-set-operator',
    'openshift-enterprise-cluster-capacity',
]

ASSEMBLIES = [
    'stream',
    'test',
    'art1234'
]

EL_TARGETS = [
    '',
    'el8',
    'el9'
]


def generate_random_build():
    major_minor = f'4.{random.randint(10, 17)}'  # e.g. 4.16
    now = datetime.now()

    # to avoid colliding release strings
    delta = timedelta(minutes=random.randint(0,60), seconds=random.randint(0,60))

    name = random.choice(NAMES)
    group = f'openshift-{major_minor}'  # e.g. openshift-4.16
    version = f'v4.{random.randint(10, 17)}.0'  # e.g. v4.16.0
    release = (now + delta).strftime("%Y%m%d%H%M%S")
    assembly = random.choice(ASSEMBLIES)  # e.g. stream
    el_target = random.choice(EL_TARGETS)

    source_repo = f'https://github.com/openshift-priv/{name}'
    committish = hashlib.sha1(str(random.random()).encode()).hexdigest()[:7]  # e.g. 66909da

    embargoed = bool(random.getrandbits(1))

    artifact_type = random.choice(list(ArtifactType)).value
    engine = random.choice(list(Engine)).value

    start_time = now + timedelta(minutes=5)
    end_time = start_time + timedelta(minutes=5)

    image_tag = hashlib.sha1(str(random.random()).encode()).hexdigest()[:16]
    outcome = random.choice(list(KonfluxBuildOutcome)).value
    job_url = f'https://jenkins.com/job/aos-cd-builds/job/build%252Focp4/{random.randint(1000,10000)}'

    return KonfluxBuild(
        name=name,
        group=group,
        version=version,
        release=release,
        assembly=assembly,
        el_target=el_target,
        source_repo=source_repo,
        commitish=committish,
        embargoed=embargoed,
        artifact_type=artifact_type,
        engine=engine,
        start_time=start_time,
        end_time=end_time,
        image_tag=image_tag,
        outcome=outcome,
        job_url=job_url
    )


def generate_build_schema(cls):
    """
    Generate a schema that can be fed into the create_table() function,
    starting from the representation of a Konflux build object
    """

    fields = []
    annotations = typing.get_type_hints(cls.__init__)
    parameters = inspect.signature(cls.__init__).parameters

    for param_name, param in parameters.items():
        if param_name == 'self':
            continue

        field_type = annotations.get(param_name, str)  # Default to string if type is not provided
        mode = 'NULLABLE' if param.default is param.empty else 'REQUIRED'

        if field_type == int:
            field_type_str = 'INTEGER'
        elif field_type == float:
            field_type_str = 'FLOAT'
        elif field_type == bool:
            field_type_str = 'BOOLEAN'
        elif field_type == datetime:
            field_type_str = 'TIMESTAMP'
        else:
            field_type_str = 'STRING'

        fields.append(SchemaField(param_name, field_type_str, mode=mode))

    return fields
