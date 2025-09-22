import logging
import re
from typing import Optional

from slack_sdk.web.async_client import AsyncWebClient

_LOGGER = logging.getLogger(__name__)


class SlackClient:
    """A SlackClient allows pipelines to send Slack messages."""

    DEFAULT_CHANNEL = "#art-release"

    def __init__(
        self,
        token: str,
        job_name: Optional[str],
        build_id: Optional[str],
        build_url: Optional[str],
        dry_run: bool = False,
    ) -> None:
        self.token = token
        self.channel = self.DEFAULT_CHANNEL
        self.job_name = job_name
        self.build_id = build_id
        self.build_url = build_url
        self.dry_run = dry_run
        self.as_user = "art-release-bot"
        self.icon_emoji = ":robot_face:"
        self._thread_ts = None
        self._client = AsyncWebClient(token=token)

    def bind_channel(self, channel_or_release: Optional[str]):
        """Bind this SlackClient to a specified Slack channel. Future messages will be sent to that channel.
        :param channel_or_release: An explicit channel name ('#art-team') or a string that contains a prefix of the
        release the jobs is associated with (e.g. '4.5.2-something' => '4.5'). If a release is specified, the
        slack channel will be #art-release-MAJOR-MINOR. If None or empty string is specified, the slack channel will be the default channel.
        """
        if not channel_or_release:
            self.channel = self.DEFAULT_CHANNEL
            return
        if channel_or_release.startswith("#"):
            self.channel = channel_or_release
            return
        match = re.compile(r"(\d+)\.(\d+)").search(channel_or_release)
        if match:
            self.channel = f"#art-release-{match[1]}-{match[2]}"
        else:
            raise ValueError(f"Invalid channel_or_release value: {channel_or_release}")

    async def say_in_thread(self, message: str, reaction: Optional[str] = None, broadcast: bool = False):
        if not self._thread_ts:
            response_data = await self.say(message, thread_ts=None, reaction=reaction)
            self._thread_ts = response_data["ts"]
            return response_data
        else:
            return await self.say(message, thread_ts=self._thread_ts, reaction=reaction, broadcast=broadcast)

    async def say(
        self,
        message: str,
        thread_ts: Optional[str] = None,
        reaction: Optional[str] = None,
        broadcast: bool = False,
        link_build_url: bool = True,
    ):
        attachments = []
        if self.build_url and link_build_url:
            attachments.append(
                {
                    "title": f"Job: {self.job_name} <{self.build_url}/consoleFull|{self.build_id}>",
                    "color": "#439FE0",
                }
            )
        if self.dry_run:
            _LOGGER.warning("[DRY RUN] Would have sent slack message to %s: %s %s", self.channel, message, attachments)
            return {"message": {"ts": "fake"}, "ts": "fake"}
        response = await self._client.chat_postMessage(
            channel=self.channel,
            text=message,
            thread_ts=thread_ts,
            username=self.as_user,
            link_names=True,
            attachments=attachments,
            icon_emoji=self.icon_emoji,
            reply_broadcast=broadcast,
        )
        # https://api.slack.com/methods/reactions.add
        if reaction:
            await self._client.reactions_add(
                channel=response.data["channel"],
                name=reaction,
                timestamp=response.data["ts"],
            )

        return response.data

    async def upload_file(
        self, file=None, content=None, filename=None, initial_comment=None, thread_ts: Optional[str] = None
    ):
        response = await self._client.files_upload_v2(
            file=file,
            content=content,
            filename=filename,
            initial_comment=initial_comment,
            channel=self.channel,
            thread_ts=thread_ts,
        )
        return response.data

    async def upload_content(self, content, intro=None, filename=None, thread_ts: Optional[str] = None):
        """
        Similar to upload_file but can upload from a variable instead of a file
        """
        response = await self._client.files_upload_v2(
            initial_comment=intro, channel=self.channel, content=content, filename=filename, thread_ts=thread_ts
        )
        return response.data
