import dataclasses
import datetime
import sys
from typing import Any, Dict, List, Optional, cast

import click
import random

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


DEFAULT_API_URL = 'http://127.0.0.1:8001'
DEFAULT_USER = 'root'
DEFAULT_ARCH = 'x86_64'
DEFAULT_KEYNAME = 'default'
DEFAULT_PROVISION_TIMEOUT = 60
DEFAULT_PROVISION_TICK = 6


# Type annotation for "data" package describing a guest instance. Passed
# between load() and save() calls
GuestInspectType = TypedDict(
    'GuestInspectType', {
        'state': str,
        'address': Optional[str]
        }
    )

@dataclasses.dataclass
class BeakerGuestData(tmt.steps.provision.GuestSshData):
    # Override parent class with our defaults
    user: str = DEFAULT_USER

    # API
    api_url: str = DEFAULT_API_URL

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
    'routing': 'yellow',
    'provisioning': 'magenta',
    'promised': 'blue',
    'preparing': 'cyan',
    'cancelled': 'red',
    'error': 'red'
}


class BeakerAPI:
    def __init__(self, guest: 'GuestBeaker') -> None:
        self._guest = guest

    def create(
            self,
            data: Dict[str, Any],
            ) -> Dict:
        """
        Create - or request creation of - a resource using mrack up.

        :param data: optional key/value data to send with the request.

        """


        return {
            "state": "provisioning",
            "guestname": "beakerHOST",
        }

    def inspect(
            self,
            ) -> Dict:
        """
        Inspect a resource.

        """
        state = random.choice(list(set(GUEST_STATE_COLORS) - {"cancelled", "error"} ))

        if random.randint(0,999) % 11 == 0:
            state = "ready"

        import time
        time.sleep(3)
        return {
            "state": state,
            "guestname": "beakerHOST",
            "address": "Brno",
        }

    def delete(
            self,
            path: str,
            request_kwargs: Optional[Dict[str, Any]] = None
            ) -> Dict:
        """
        Delete - or request removal of - a resource.

        """

        return {}


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

        if response:
            self.info('guest', 'has been requested', 'green')

        else:
            raise ProvisionError(
                f"Failed to create, '{response['state']}'.")

        self.guestname = response['guestname']
        self.info('guestname', self.guestname, 'green')

        with updatable_message(
                'state', indent_level=self._level()) as progress_message:

            def get_new_state() -> GuestInspectType:
                response = self.api.inspect()

                if response['state'] in {"cancelled", "error"}:
                    raise ProvisionError(
                        f"Failed to create, "
                        f"unhandled API response '{response['state']}'.")

                current = cast(GuestInspectType, response)
                state = current['state']
                state_color = GUEST_STATE_COLORS.get(
                    state, GUEST_STATE_COLOR_DEFAULT)

                progress_message.update(state, color=state_color)

                if state == 'error':
                    raise ProvisionError(
                        'Failed to create, provisioning failed.')

                if state == 'ready':
                    return current

                raise tmt.utils.WaitingIncomplete()

            try:
                guest_info = tmt.utils.wait(
                    self, get_new_state, datetime.timedelta(
                        seconds=self.provision_timeout), tick=self.provision_tick)

            except tmt.utils.WaitingTimedOutError:
                raise ProvisionError(
                    f'Failed to provision in the given amount '
                    f'of time (--provision-timeout={self.provision_timeout}).')

        self.guest = guest_info['address']
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

        response = self.api.delete(f'/guests/{self.guestname}')

        if response['state'] == 404:
            self.info('guest', 'no longer exists', 'red')

        elif response['state'] == 409:
            self.info('guest', 'has existing snapshots', 'red')

        elif response.ok:
            self.info('guest', 'has been removed', 'green')

        else:
            self.info(
                'guest',
                f"Failed to remove, "
                f"unhandled API response '{response['state']}'.")
