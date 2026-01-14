import asyncio
import base64
from typing import Dict, Iterable, List, Optional, Set, Union, cast
from urllib.parse import quote, urlparse

import aiohttp
import gssapi
from aiohttp import ClientResponse, ClientResponseError, ClientTimeout
from artcommonlib import logutil
from artcommonlib.exectools import limit_concurrency
from artcommonlib.rpm_utils import parse_nvr

from elliottlib import constants, util

_LOGGER = logutil.get_logger(__name__)


class AsyncErrataAPI:
    def __init__(self, url: str = constants.errata_url):
        self._errata_url = urlparse(url).geturl()
        self._timeout = ClientTimeout(total=60 * 15)  # 900 seconds (15 min)
        self._errata_gssapi_name = gssapi.Name(
            f"HTTP@{urlparse(self._errata_url).hostname}", gssapi.NameType.hostbased_service
        )
        self._gssapi_flags = [gssapi.RequirementFlag.out_of_sequence_detection]
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=32, force_close=True), timeout=self._timeout
        )
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self._session.close()

    def _generate_auth_header(self):
        client_ctx = gssapi.SecurityContext(name=self._errata_gssapi_name, usage='initiate', flags=self._gssapi_flags)
        out_token = client_ctx.step(b"")
        return f'Negotiate {base64.b64encode(out_token).decode()}'

    async def _make_request(self, method: str, path: str, parse_json: bool = True, **kwargs) -> Union[Dict, bytes]:
        auth_header = self._generate_auth_header()
        headers = self._headers.copy()
        headers["Authorization"] = auth_header
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        async with self._session.request(method, self._errata_url + path, headers=headers, **kwargs) as resp:
            # resp.raise_for_status()
            await self._raise_for_status(resp)
            result = await (resp.json() if parse_json else resp.read())
        return result

    @staticmethod
    async def _raise_for_status(response: ClientResponse):
        if not response.ok:
            error_message = await response.json()
            response.release()
            raise ClientResponseError(
                response.request_info,
                response.history,
                status=response.status,
                message=f"{response.reason}: {error_message}",
                headers=response.headers,
            )

    async def get_advisory(self, advisory: Union[int, str]) -> Dict:
        path = f"/api/v1/erratum/{quote(str(advisory))}"
        return await self._make_request(aiohttp.hdrs.METH_GET, path)

    async def reserve_live_id(self) -> str:
        path = "/api/v1/advisory/reserve_live_id"
        result = await self._make_request(aiohttp.hdrs.METH_POST, path)
        live_id = result.get("live_id")
        return str(live_id) if live_id is not None else None

    async def get_builds(self, advisory: Union[int, str]):
        # As of May 25, 2023, /api/v1/erratum/{id}/builds_list doesn't return all builds.
        # Use /api/v1/erratum/{id}/builds instead.
        path = f"/api/v1/erratum/{quote(str(advisory))}/builds"
        return await self._make_request(aiohttp.hdrs.METH_GET, path)

    async def get_builds_flattened(self, advisory: Union[int, str]) -> Set[str]:
        pv_builds = await self.get_builds(advisory)
        return {nvr for pv in pv_builds.values() for pvb in pv["builds"] for nvr in pvb}

    async def get_cves(self, advisory: Union[int, str]) -> List[str]:
        # Errata API "/cve/show/{advisory}.json" doesn't return the correct CVEs for some RHSAs.
        # Not sure if it's an Errata bug. Use a different approach instead.
        return (await self.get_advisory(advisory))["content"]["content"]["cve"].split()

    async def get_cve_package_exclusions(self, advisory_id: int):
        path = "/api/v1/cve_package_exclusion"
        # This is a paginated API, we need to increment page[number] until an empty array is returned.
        params = {"filter[errata_id]": str(int(advisory_id)), "page[number]": 1, "page[size]": 1000}
        while True:
            result = await self._make_request(aiohttp.hdrs.METH_GET, path, params=params)
            data: List[Dict] = result.get('data', [])
            if not data:
                break
            for item in data:
                yield item
            params["page[number]"] += 1

    async def create_cve_package_exclusion(self, advisory_id: int, cve: str, package: str):
        path = "/api/v1/cve_package_exclusion"
        data = {
            "cve": cve,
            "errata": advisory_id,
            "package": package,
        }
        return await self._make_request(aiohttp.hdrs.METH_POST, path, json=data)

    async def delete_cve_package_exclusion(self, exclusion_id: int):
        path = f"/api/v1/cve_package_exclusion/{int(exclusion_id)}"
        await self._make_request(aiohttp.hdrs.METH_DELETE, path, parse_json=False)

    @limit_concurrency(limit=16)
    async def get_advisories_for_jira(self, jira_key: str, ignore_not_found=False):
        path = f"/jira_issues/{quote(jira_key)}/advisories.json"
        try:
            result = await self._make_request(aiohttp.hdrs.METH_GET, path)
        except ClientResponseError as e:
            # When newly created jira bugs are not sync'd to ET we get a 404,
            # assume that they are not attached to any advisory
            if ignore_not_found and e.status == 404:
                result = []
            else:
                raise
        return result

    @limit_concurrency(limit=16)
    async def get_advisories_for_bug(self, bz_key: str):
        path = f"/bugs/{bz_key}/advisories.json"
        return await self._make_request(aiohttp.hdrs.METH_GET, path)

    async def _paginated_request(
        self, method: str, path: str, params: Optional[Dict] = None, start_page_number: int = 1, page_size: int = 0
    ):
        if params is None:
            params = {}
        params["page[number]"] = start_page_number
        if page_size > 0:
            params["page[size]"] = page_size
        while True:
            result = cast(Dict, await self._make_request(method=method, path=path, params=params))
            data: List[Dict] = result.get('data', [])
            if not data:
                break
            for item in data:
                yield item
            params["page[number]"] += 1

    def get_batches(self, id: int = 0, name: str = ""):
        """Get details of all batches ordered by name.
        https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#batches

        :param id: Filter by batch ID
        :param name: Filter by batch name
        :return: a generator of batches
        """
        path = "/api/v1/batches"
        params = {}
        if id > 0:
            params["filter[id]"] = str(id)
        if name:
            params["filter[name]"] = name
        return self._paginated_request(aiohttp.hdrs.METH_GET, path, params=params)

    async def create_batch(
        self,
        name: str,
        release_name: str,
        release_date: str,
        description: str,
        is_active: bool = True,
        is_locked: bool = False,
    ):
        """Create a new batch.
        https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#batches

        :param name: Batch name
        :param release_name: Release name. Use "RHOSE ASYNC - AUTO" for an OCP release.
        :param release_date: Release date in "YYYY-MM-DD" format
        :param description: Batch description
        :param is_active: Whether the batch is active
        :param is_locked: Whether the batch is locked
        """
        path = "/api/v1/batches"
        data = {
            "name": name,
            "release_name": release_name,
            "release_date": release_date,
            "description": description,
            "is_active": is_active,
            "is_locked": is_locked,
        }
        return cast(Dict, await self._make_request(aiohttp.hdrs.METH_POST, path, json=data)).get("data", {})

    async def update_batch(
        self,
        batch_id: int,
        name: Optional[str] = None,
        release_name: Optional[str] = None,
        release_date: Optional[str] = None,
        description: Optional[str] = None,
        is_active: Optional[bool] = None,
        is_locked: Optional[bool] = None,
    ):
        """Update an existing batch.
        https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#batches

        :param batch_id: Batch ID
        :param name: The batch name to update if not None
        :param release_name: The release name to update if not None
        :param release_date: The release date to update if not None
        :param description: The description to update if not None
        :param is_active: Whether the batch is active to update if not None
        :param is_locked: Whether the batch is locked to update if not None
        """
        path = f"/api/v1/batches/{batch_id}"
        data = {}
        if name is not None:
            data["name"] = name
        if release_name is not None:
            data["release_name"] = release_name
        if release_date is not None:
            data["release_date"] = release_date
        if description is not None:
            data["description"] = description
        if is_active is not None:
            data["is_active"] = is_active
        if is_locked is not None:
            data["is_locked"] = is_locked
        return cast(Dict, await self._make_request(aiohttp.hdrs.METH_PUT, path, json=data)).get("data", {})

    async def change_batch_for_advisory(self, advisory: int, batch_id: Optional[int] = None):
        """Change the batch association for an advisory.

        :param advisory: Advisory ID
        :param batch_id: Batch ID to associate with the advisory. If None, clear the batch association.
        """
        path = f"/api/v1/erratum/{advisory}/change_batch"
        data = {}
        if batch_id is not None:
            data["batch_id"] = batch_id
        else:
            data["clear_batch"] = True
        return cast(Dict, await self._make_request(aiohttp.hdrs.METH_POST, path, json=data))

    async def create_advisory(
        self,
        product: str,
        release: str,
        errata_type: str,
        advisory_synopsis: str,
        advisory_topic: str,
        advisory_description: str,
        advisory_solution: str,
        advisory_publish_date_override: Optional[str] = None,
        advisory_text_only: bool = False,
        advisory_quality_responsibility_name: str = "Default",
        advisory_security_impact: Optional[str] = None,
        advisory_package_owner_email: Optional[str] = None,
        advisory_manager_email: Optional[str] = None,
        advisory_assigned_to_email: Optional[str] = None,
        idsfixed: Optional[List[Union[str, int]]] = None,
        batch_id: Optional[int] = None,
        batch_name: Optional[str] = None,
    ):
        """Create a new advisory.
        https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#advisories

        :param product: Product name
        :param release: Release name
        :param errata_type: Errata type (e.g. "RHSA")
        :param advisory_synopsis: Synopsis
        :param advisory_topic: Topic
        :param advisory_description: Description
        :param advisory_solution: Solution
        :param advisory_publish_date_override: Publish date override in "YYYY-MM-DD" format. This will only be set if batch is not provided
        :param advisory_text_only: Whether the advisory is text only
        :param advisory_quality_responsibility_name: Quality responsibility name
        :param advisory_security_impact: Security impact
        :param advisory_package_owner_email: Package owner email
        :param advisory_manager_email: Manager email
        :param advisory_assigned_to_email: Assigned to email
        :param idsfixed: List of IDs fixed
        :param batch_id: Batch ID to associate with the advisory
        :param batch_name: Batch name to associate with the advisory
        :return: a dict containing the new advisory
        """
        path = "/api/v1/erratum"
        data = {
            "product": product,
            "release": release,
            "advisory": {
                "errata_type": errata_type,
                "solution": advisory_solution,
                "description": advisory_description,
                "synopsis": advisory_synopsis,
                "topic": advisory_topic,
                "security_impact": advisory_security_impact,
                "quality_responsibility_name": advisory_quality_responsibility_name,
                "idsfixed": ' '.join(str(i) for i in idsfixed) if idsfixed else None,
                "package_owner_email": advisory_package_owner_email,
                "manager_email": advisory_manager_email,
                "assigned_to_email": advisory_assigned_to_email,
                "text_only": 1 if advisory_text_only else 0,
            },
            "batch": {},
        }
        if batch_id or batch_name:
            if batch_id:
                data["batch"]["id"] = batch_id
            if batch_name:
                data["batch"]["name"] = batch_name
        else:
            if not advisory_publish_date_override:
                raise ValueError("Either batch or advisory_publish_date_override must be provided")
            data["advisory"]["publish_date_override"] = advisory_publish_date_override
        advisory = cast(Dict, await self._make_request(aiohttp.hdrs.METH_POST, path, json=data))

        # if no batch is provided, make sure to clear the batch association of advisory after it is created
        # this is because ErrataTool will automatically associate the advisory with the latest unlocked batch
        # if it is present for the Errata release
        if not batch_id and not batch_name:
            advisory_info = next(iter(advisory["errata"].values()))
            advisory_id = advisory_info["id"]
            _LOGGER.info(f"Clearing batch association for advisory {advisory_id}")
            # clearing batch requires special permissions, so don't raise exception if it fails
            try:
                await self.change_batch_for_advisory(advisory_id)
            except Exception as e:
                _LOGGER.warning(f"Failed to clear batch association for advisory {advisory_id}: {e}")

        return advisory

    async def request_liveid(self, advisory_id: int):
        """Request a Live ID for an advisory.
        https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#advisories

        :param advisory_id: Advisory ID
        """
        path = f"/api/v1/erratum/{advisory_id}/set_live_advisory_name"
        return await self._make_request(aiohttp.hdrs.METH_POST, path)


class AsyncErrataUtils:
    @classmethod
    async def get_advisory_cve_exclusions(cls, api: AsyncErrataAPI, advisory_id: int):
        """This is a wrapper around `AsyncErrataAPI.get_cve_package_exclusions`.
        The result value of original Errata API call `get_cve_package_exclusions` is not user-friendly.
        This method converts the result value into a better data structure.
        :return: a dict that key is CVE name, value is another dict with package name as key and exclusion_id as value
        """
        current_exclusions: Dict[str, Dict[str, int]] = {}
        async for ex in api.get_cve_package_exclusions(advisory_id):
            current_exclusions.setdefault(ex["relationships"]["cve"]["name"], {})[
                ex["relationships"]["package"]["name"]
            ] = ex["id"]
        return current_exclusions

    @classmethod
    def compute_cve_exclusions(cls, attached_builds: Iterable[str], expected_cve_components: Dict[str, Set[str]]):
        """Compute cve package exclusions from a list of attached builds and CVE-components mapping.
        :param attached_builds: list of NVRs
        :param expected_cve_components: a dict mapping each CVE to a list of brew components
        :return: a dict that key is CVE name, value is another dict with package name as key and 0 as value
        """
        attached_brew_components = {parse_nvr(nvr)["name"] for nvr in attached_builds}

        # separate out golang CVEs and non-golang CVEs
        # expected_brew_components contain non-golang CVEs components for regular analysis
        # All golang CVEs will have component as constants.GOLANG_BUILDER_CVE_COMPONENT
        # which we do not attach to our advisories since it's a builder image
        # It requires special treatment
        expected_brew_components = set()
        golang_cve_names = set()
        for cve_name, components in expected_cve_components.items():
            if constants.GOLANG_BUILDER_CVE_COMPONENT not in components:
                expected_brew_components.update(components)
            else:
                golang_cve_names.add(cve_name)

        missing_brew_components = expected_brew_components - attached_brew_components
        if missing_brew_components:
            raise ValueError(f"Missing builds for brew component(s): {missing_brew_components}")

        if golang_cve_names:
            expected_cve_components = cls.populate_golang_cve_components(
                golang_cve_names, expected_cve_components, attached_builds
            )

        cve_exclusions = {
            cve_name: {pkg: 0 for pkg in attached_brew_components - components}
            for cve_name, components in expected_cve_components.items()
        }
        return cve_exclusions

    @classmethod
    def populate_golang_cve_components(cls, golang_cve_names, expected_cve_components, attached_builds):
        # Get go builder images for all attached image builds
        parsed_nvrs = [(n['name'], n['version'], n['release']) for n in [parse_nvr(n) for n in attached_builds]]
        go_nvr_map = util.get_golang_container_nvrs(parsed_nvrs, _LOGGER)

        etcd_golang_builder, base_golang_builders = None, []
        for builder_nvr_string in go_nvr_map.keys():
            builder_nvr = parse_nvr(builder_nvr_string)

            # Make sure they are go builder nvrs (this should never happen)
            if builder_nvr['name'] != constants.GOLANG_BUILDER_CVE_COMPONENT:
                raise ValueError(
                    f"Unexpected `name` value for nvr {builder_nvr}. Expected "
                    f"{constants.GOLANG_BUILDER_CVE_COMPONENT}. Please investigate."
                )

            if 'etcd' in list(go_nvr_map[builder_nvr_string])[0]:
                if etcd_golang_builder:
                    raise ValueError(
                        f"Multiple etcd builds found in advisory {go_nvr_map[etcd_golang_builder][0]}, "
                        f"with builders: {etcd_golang_builder} and {builder_nvr_string}. "
                        "Please investigate"
                    )
                etcd_golang_builder = builder_nvr_string
            else:
                base_golang_builders.append(builder_nvr_string)

        # Now try to map CVEs to {etcd, rhelX, rhelX+1} builders
        # TODO: Figure out how to find out if a golang CVE only affects etcd
        # For now we map CVEs to base_golang build images

        if etcd_golang_builder:
            _LOGGER.warning(
                f'etcd build found in advisory {go_nvr_map[etcd_golang_builder][0]}, with builder: '
                f'{etcd_golang_builder}. If an attached CVE affects etcd please manually associate CVE '
                'with etcd build'
            )

        for cve_name in golang_cve_names:
            nvrs = set()
            for base_golang_builder in base_golang_builders:
                nvrs.update({nvr[0] for nvr in go_nvr_map[base_golang_builder]})
            _LOGGER.info(f"Associating golang {cve_name} with golang images ({len(nvrs)})")
            expected_cve_components[cve_name] = nvrs

        return expected_cve_components

    @classmethod
    def diff_cve_exclusions(
        cls, current_exclusions: Dict[str, Dict[str, int]], expected_exclusions: Dict[str, Dict[str, int]]
    ):
        """Given 2 cve_package_exclusions dicts, return the difference.
        :return: (extra_exclusions, missing_exclusions)
        """
        extra_exclusions = {
            cve: {pkg: exclusions[pkg] for pkg in exclusions.keys() - expected_exclusions.get(cve, {}).keys()}
            for cve, exclusions in current_exclusions.items()
        }
        missing_exclusions = {
            cve: {pkg: exclusions[pkg] for pkg in exclusions.keys() - current_exclusions.get(cve, {}).keys()}
            for cve, exclusions in expected_exclusions.items()
        }
        return extra_exclusions, missing_exclusions

    @classmethod
    async def validate_cves_and_get_exclusions_diff(
        cls, api: AsyncErrataAPI, advisory_id: int, attached_builds: List[str], cve_components_mapping: Dict[str, Dict]
    ):
        _LOGGER.info("Getting associated CVEs for advisory %s", advisory_id)
        advisory_cves = await api.get_cves(advisory_id)

        extra_cves = cve_components_mapping.keys() - advisory_cves
        if extra_cves:
            raise ValueError(
                f"The following CVEs does not seem to be associated with advisory {advisory_id}: "
                f"{', '.join(sorted(extra_cves))}. Make sure CVE names field in advisory"
                f"is consistent with CVEs that are attached (`elliott attach-cve-flaws` is your friend)"
            )

        missing_cves = advisory_cves - cve_components_mapping.keys()
        if missing_cves:
            raise ValueError(
                f"Tracker bugs for the following CVEs associated with advisory {advisory_id} "
                f"are not attached: {', '.join(sorted(missing_cves))}. Either attach trackers or remove "
                f"associated flaw bug (`elliott verify-attached-bugs` is your friend) and remove {missing_cves}"
                " from the CVE names field in advisory"
            )

        _LOGGER.info("Getting current CVE package exclusions for advisory %s", advisory_id)
        current_exclusions = await cls.get_advisory_cve_exclusions(api, advisory_id)
        _LOGGER.info("Comparing current CVE package exclusions with expected ones for advisory %s", advisory_id)

        try:
            expected_exclusions = cls.compute_cve_exclusions(attached_builds, cve_components_mapping)
        except ValueError as e:
            raise ValueError(f"Error computing CVE package exclusions for advisory {advisory_id}: {e}")

        extra_exclusions, missing_exclusions = cls.diff_cve_exclusions(current_exclusions, expected_exclusions)
        return extra_exclusions, missing_exclusions

    @classmethod
    async def associate_builds_with_cves(
        cls,
        api: AsyncErrataAPI,
        advisory_id: int,
        attached_builds: List[str],
        cve_components_mapping: Dict[str, Dict],
        dry_run=False,
    ):
        """Request Errata to associate CVEs to attached Brew builds
        :param api: Errata API
        :param advisory_id: advisory id
        :param attached_builds: list of attached Brew build NVRs
        :param cve_components_mapping: a dict mapping each CVE to a dict containing flaw bug and brew components
        """

        extra_exclusions, missing_exclusions = await cls.validate_cves_and_get_exclusions_diff(
            api, advisory_id, attached_builds, cve_components_mapping
        )
        _LOGGER.info("Reconciling CVE package exclusions for advisory %s", advisory_id)
        if dry_run:
            _LOGGER.warning("[DRY RUN] Would have Reconciled CVE package exclusions for advisory %s", advisory_id)
            return
        futures = []
        for cve, exclusions in extra_exclusions.items():
            for package, exclusion_id in exclusions.items():
                futures.append(api.delete_cve_package_exclusion(exclusion_id))

        for cve, exclusions in missing_exclusions.items():
            for package in exclusions:
                futures.append(api.create_cve_package_exclusion(advisory_id, cve, package))

        await asyncio.gather(*futures)
        _LOGGER.info("Reconciled CVE package exclusions for advisory %s", advisory_id)
