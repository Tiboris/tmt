import dataclasses
import datetime
import sys
from typing import Any, Dict, List, Optional, cast

import click
import random
import logging


import tmt
import tmt.options
import tmt.steps
import tmt.steps.provision
import tmt.utils
from tmt.utils import ProvisionError, updatable_message

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


import mrack

from mrack.utils import async_run

from mrack.providers import providers
from mrack.providers.beaker import PROVISIONER_KEY as BEAKER
from mrack.providers.beaker import BeakerProvider
from mrack.transformers.beaker import BeakerTransformer

providers.register(BEAKER, BeakerProvider)

DEFAULT_USER = 'root'
DEFAULT_ARCH = 'x86_64'
DEFAULT_KEYNAME = 'default'
DEFAULT_PROVISION_TIMEOUT = 3600  # 1 hour timeout at least
DEFAULT_PROVISION_TICK = 60  # poll job each minute


# Type annotation for "data" package describing a guest instance. Passed
# between load() and save() calls
GuestInspectType = TypedDict(
    'GuestInspectType', {
        "status": str,
        'address': Optional[str]
        }
    )

class TmtBeakerTransformer(BeakerTransformer):
    def _get_distro_and_variant(self, environment):
        """Get distribution and its variant for the host system to requirement."""
        compose = environment["os"].get("compose")
        required_distro = self._find_value(
            environment, "distro", "distros", compose, default=compose
        )
        distro_variants = self.config.get("distro_variants")

        if "beaker_variant" in environment["os"]:
            variant = environment["beaker_variant"]
        elif distro_variants:
            variant = distro_variants.get(
                required_distro, distro_variants.get("default")
            )
        else:  # Default to Server for RHEL7 and Fedora systems
            variant = "Server"

        return (required_distro, variant)

    def create_host_requirement(self, host):
        """Create single input for Beaker provisioner."""
        environment = host.get("environment")
        distro, variant = self._get_distro_and_variant(environment)
        return {
            "name": environment.get("name"),
            "distro": distro,
            "os": environment.get("os"),
            "group": environment.get("group"),
            "meta_distro": "distro" in environment,
            "arch": environment["hw"].get("arch", "x86_64"),
            "variant": variant,
            f"mrack_{BEAKER}": host.get(BEAKER, {}),
        }

class BeakerAPI:
    @async_run
    async def __init__(self, guest: 'GuestBeaker') -> None:
        # HAX remove mrack stdout
        mrack.logger.removeHandler(mrack.console_handler)

        self._guest = guest # FIXME

        # use global context class
        global_context = mrack.context.global_context

        # init global context with paths to files
        mrack_config = "mrack.conf"
        provisioning_config = "provisioning-config.yaml"
        db_file = "mrackdb.json"
        global_context.init(mrack_config, provisioning_config, db_file)

        self._mrack_transformer = TmtBeakerTransformer()
        await self._mrack_transformer.init(global_context.PROV_CONFIG, {})


    @async_run
    async def create(
            self,
            data: Dict[str, Any],
            ) -> Dict:
        """
        Create - or request creation of - a resource using mrack up.

        :param data: optional key/value data to send with the request.

        """
        req = self._mrack_transformer.create_host_requirement(data)
        self._bkr_job_id, self._req = await self._mrack_transformer._provider.create_server(req)
        return self._mrack_transformer._provider._get_recipe_info(self._bkr_job_id)


    @async_run
    async def inspect(
            self,
            ) -> Dict:
        """
        Inspect a resource.  # kinda wait till provisioned

        """
        return self._mrack_transformer._provider._get_recipe_info(self._bkr_job_id)


    @async_run
    async def delete(  # destroy
            self,
            ) -> Dict:
        """
        Delete - or request removal of - a resource.

        """
        return await self._mrack_transformer._provider.delete_host(self._bkr_job_id, None)



@dataclasses.dataclass
class BeakerGuestData(tmt.steps.provision.GuestSshData):
    # Override parent class with our defaults
    user: str = DEFAULT_USER

    # Guest request properties
    arch: str = DEFAULT_ARCH
    image: Optional[str] = None
    hardware: Optional[Any] = None
    pool: Optional[str] = None
    keyname: str = DEFAULT_KEYNAME
    user_data: Dict[str, str] = dataclasses.field(default_factory=dict)

    # Provided in Beaker job
    guestname: Optional[str] = None

    # Timeouts and deadlines
    provision_timeout: int = DEFAULT_PROVISION_TIMEOUT
    provision_tick: int = DEFAULT_PROVISION_TICK


GUEST_STATE_COLOR_DEFAULT = 'green'

GUEST_STATE_COLORS = {
    "Reserved": "green",
    "New": "blue",
    "Scheduled": "blue",
    "Queued": "magenta",
    "Processed": "magenta",
    'Waiting': 'magenta',
    'Installing': 'cyan',
    "Running": "cyan",
    "Cancelled": "yellow",
    "Aborted": "yellow",
    "Completed": "green",
}


@tmt.steps.provides_method('beaker')
class ProvisionBeaker(tmt.steps.provision.ProvisionPlugin):
    """
    Provision guest on Beaker system using mrack

    Minimal configuration could look like this:

        provision:
            how: beaker
            image: Fedora

    Full configuration example:

    """

    _data_class = BeakerGuestData

    # Guest instance
    _guest = None

    _keys = [
        'arch',
        'image',
        'hardware',
        'pool',
        'priority-group',
        'keyname',
        'user-data',
        'provision-timeout',
        'provision-tick',
    ]

    @classmethod
    def options(cls, how: Any = None) -> List[tmt.options.ClickOptionDecoratorType]:
        """ Prepare command line options for Artemis """
        return cast(List[tmt.options.ClickOptionDecoratorType], [
            click.option(
                '--arch', metavar='ARCH',
                help='Architecture to provision.'
                ),
            click.option(
                '--image', metavar='COMPOSE',
                help='Image (distro or "compose" in Beaker terminology) '
                     'to provision.'
                ),
            click.option(
                '--keyname', metavar='NAME',
                help='SSH key name.'
                ),
            click.option(
                '--provision-timeout', metavar='SECONDS',
                help=f'How long to wait for provisioning to complete, '
                     f'{DEFAULT_PROVISION_TIMEOUT} seconds by default.'
                ),
            click.option(
                '--provision-tick', metavar='SECONDS',
                help=f'How often check Beaker for provisioning status, '
                     f'{DEFAULT_PROVISION_TICK} seconds by default.',
                ),
            ]) + super().options(how)

    def default(self, option: str, default: Optional[Any] = None) -> Any:
        """ Return default data for given option """

        return getattr(BeakerGuestData(), option.replace('-', '_'), default)

    def wake(self, data: Optional[BeakerGuestData] = None) -> None:  # type: ignore[override]
        """ Wake up the plugin, process data, apply options """

        super().wake(data=data)

        if data:
            self._guest = GuestBeaker(data, name=self.name, parent=self.step)

    def go(self) -> None:
        """ Provision the guest """
        super().go()

        try:
            user_data = {
                key.strip(): value.strip()
                for key, value in (
                    pair.split('=', 1)
                    for pair in self.get('user-data')
                    )
                }

        except ValueError:
            raise ProvisionError('Cannot parse user-data.')

        data = BeakerGuestData(
                arch=self.get('arch'),
                image=self.get('image'),
                hardware=self.get('hardware'),
                pool=self.get('pool'),
                keyname=self.get('keyname'),
                user_data=user_data,
                user=self.get('user'),
                provision_timeout=self.get('provision-timeout'),
                provision_tick=self.get('provision-tick'),
            )

        self._guest = GuestBeaker(data, name=self.name, parent=self.step)
        self._guest.start()

    def guest(self) -> Optional['GuestBeaker']:
        """ Return the provisioned guest """
        return self._guest


class GuestBeaker(tmt.GuestSsh):
    """
    Beaker guest instance

    The following keys are expected in the 'data' dictionary:
    """
    _data_class = BeakerGuestData  # type: ignore[assignment]

    # Guest request properties
    arch: str
    image: str
    hardware: Optional[Any]
    pool: Optional[str]
    keyname: str
    user_data: Dict[str, str]

    # Provided in Beaker response
    guestname: Optional[str]

    # Timeouts and deadlines
    provision_timeout: int
    provision_tick: int
    _api: Optional[BeakerAPI] = None


    @property
    def api(self) -> BeakerAPI:
        if self._api is None:
            self._api = BeakerAPI(self)

        return self._api

    def _create(self) -> None:
        environment: Dict[str, Any] = {
            'hw': {
                'arch': self.arch
                },
            'os': {
                'compose': self.image
                }
            }

        data: Dict[str, Any] = {
            'environment': environment,
            'keyname': self.keyname,
            'user_data': self.user_data
            }

        if self.pool:
            environment['pool'] = self.pool

        if self.hardware is not None:
            assert isinstance(self.hardware, dict)

            environment['hw']['constraints'] = self.hardware

        response = self.api.create(data)

        if response["status"] == "New":
            self.info('guest', 'has been requested', 'green')

        else:
            raise ProvisionError(
                f"Failed to create, response status: '{response['status']}'.")

        self.guestname = response["rid"]
        self.info('guestname', self.guestname, 'green')

        with updatable_message(
                "status", indent_level=self._level()) as progress_message:

            def get_new_state() -> GuestInspectType:
                response = self.api.inspect()

                if response["status"] == "Aborted":
                    raise ProvisionError(
                        f"Failed to create, "
                        f"unhandled API response '{response['status']}'.")

                current = cast(GuestInspectType, response)
                state = current["status"]
                state_color = GUEST_STATE_COLORS.get(
                    state, GUEST_STATE_COLOR_DEFAULT
                )

                progress_message.update(state, color=state_color)

                if state in {"Error, Aborted"}:
                    raise ProvisionError(
                        'Failed to create, provisioning failed.')

                if state == 'Reserved':
                    return current

                raise tmt.utils.WaitingIncomplete()

            try:
                guest_info = tmt.utils.wait(
                    self, get_new_state, datetime.timedelta(
                        seconds=self.provision_timeout), tick=self.provision_tick)

            except tmt.utils.WaitingTimedOutError:
                response = self.api.delete()
                raise ProvisionError(
                    f'Failed to provision in the given amount '
                    f'of time (--provision-timeout={self.provision_timeout}).')

        self.guest = guest_info['system']
        self.info('address', self.guest, 'green')

    def start(self) -> None:
        """
        Start the guest

        Get a new guest instance running. This should include preparing
        any configuration necessary to get it started. Called after
        load() is completed so all guest data should be available.
        """

        if self.guestname is None or self.guest is None:
            self._create()

    def remove(self) -> None:
        """ Remove the guest """

        if self.guestname is None:
            return

        self.api.delete()
