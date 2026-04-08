"""
Channel service for managing channel discovery, online status checks, and priority sorting.

This service handles all channel-related operations including fetching live streams
from directories, bulk online status verification, and channel priority determination.
"""

from __future__ import annotations

import asyncio
import logging
from collections import abc
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.config import GQL_OPERATIONS, MAX_INT
from src.exceptions import GQLException, MinerException
from src.models.channel import Channel
from src.utils import chunk


if TYPE_CHECKING:
    from src.config import GQLOperation, JsonType
    from src.core.client import Twitch
    from src.models.game import Game


logger = logging.getLogger("TwitchDrops")


class ChannelService:
    """
    Service responsible for channel management operations.

    Handles:
    - Channel priority calculation
    - Live stream discovery from game directories
    - Bulk online status checks for ACL channels
    - Channel sorting by viewers and priority
    """

    def __init__(self, twitch: Twitch) -> None:
        """
        Initialize the channel service.

        Args:
            twitch: The Twitch client instance
        """
        self._twitch = twitch

    def get_priority(self, channel: Channel) -> int:
        """
        Return a priority number for a given channel based on the selected mining priority mode.

        Priority modes:
        - PRIORITY_LIST: Position in wanted_games list (default)
        - TIME_TO_END: Time remaining until the campaign ends (earlier ends first)
        - TIME_RATIO: Progression ratio of the campaign (higher ratio first)

        Args:
            channel: The channel to evaluate

        Returns:
            Priority number where:
            - Lower numbers indicate higher priority
            - MAX_INT signifies the lowest possible priority
        """
        if (
            (game := channel.game) is None  # None when OFFLINE or no game set
            or game not in self._twitch.wanted_games  # we don't care about the played game
        ):
            return MAX_INT

        mode = getattr(self._twitch.settings, "mining_priority", "PRIORITY_LIST")

        if mode == "PRIORITY_LIST":
            return self._twitch.wanted_games.index(game)

        # Find active campaign for this game
        now = datetime.now(timezone.utc)
        campaign = next(
            (c for c in self._twitch.inventory if c.game == game and c.active), None
        )

        if campaign is None:
            return MAX_INT

        if mode == "TIME_TO_END":
            return int(campaign.ends_at.timestamp())

        if mode == "TIME_RATIO":
            total_duration = (campaign.ends_at - campaign.starts_at).total_seconds()
            if total_duration <= 0:
                return MAX_INT
            elapsed = (now - campaign.starts_at).total_seconds()
            ratio = max(0.0, min(1.0, elapsed / total_duration))
            return int((1.0 - ratio) * 1000000)

        return self._twitch.wanted_games.index(game)

    @staticmethod
    def get_viewers_key(channel: Channel) -> int:
        """
        Sort key for channels by viewer count (descending).

        Args:
            channel: The channel to evaluate

        Returns:
            Viewer count, or -1 if not available (offline channels)
        """
        if (viewers := channel.viewers) is not None:
            return viewers
        return -1

    async def get_live_streams(
        self, game: Game, *, limit: int = 20, drops_enabled: bool = True
    ) -> list[Channel]:
        """
        Fetch live streams for a specific game from Twitch directory.

        Args:
            game: The game to fetch streams for
            limit: Maximum number of streams to return (default: 20)
            drops_enabled: Only return channels with drops enabled (default: True)

        Returns:
            List of Channel objects representing live streams

        Raises:
            MinerException: If the GQL request fails
        """
        filters: list[str] = []
        if drops_enabled:
            filters.append("DROPS_ENABLED")

        try:
            response = await self._twitch.gql_request(
                GQL_OPERATIONS["GameDirectory"].with_variables(
                    {
                        "limit": limit,
                        "slug": game.slug,
                        "options": {
                            "includeRestricted": ["SUB_ONLY_LIVE"],
                            "systemFilters": filters,
                        },
                    }
                )
            )
        except GQLException as exc:
            raise MinerException(f"Game: {game.slug}") from exc

        if "game" in response["data"]:
            return [
                Channel.from_directory(
                    self._twitch, stream_channel_data["node"], drops_enabled=drops_enabled
                )
                for stream_channel_data in response["data"]["game"]["streams"]["edges"]
                if stream_channel_data["node"]["broadcaster"] is not None
            ]
        return []

    async def bulk_check_online(self, channels: abc.Iterable[Channel]) -> None:
        """
        Utilize batch GQL requests to check ONLINE status for multiple channels at once.

        This method efficiently checks the online status and drops_enabled flag
        for a large number of channels by batching GraphQL requests.

        Args:
            channels: Iterable of Channel objects to check
        """
        acl_streams_map: dict[int, JsonType] = {}
        stream_gql_ops: list[GQLOperation] = [channel.stream_gql for channel in channels]

        if not stream_gql_ops:
            # shortcut for nothing to process
            # NOTE: Have to do this here, because "channels" can be any iterable
            return

        # gql_request may return either a single JsonType or a list[JsonType],
        # so accept the union in the Task type.
        stream_gql_tasks: list[asyncio.Task[JsonType | list[JsonType]]] = [
            asyncio.create_task(self._twitch.gql_request(stream_gql_chunk))
            for stream_gql_chunk in chunk(stream_gql_ops, 20)
        ]

        try:
            for coro in asyncio.as_completed(stream_gql_tasks):
                response = await coro
                # Normalize response to a list for uniform processing
                if isinstance(response, list):
                    response_list: list[JsonType] = response
                else:
                    response_list = [response]
                for response_json in response_list:
                    channel_data: JsonType = response_json["data"]["user"]
                    if channel_data is not None:
                        acl_streams_map[int(channel_data["id"])] = channel_data
        except Exception:
            # asyncio.as_completed doesn't cancel tasks on errors
            for task in stream_gql_tasks:
                task.cancel()
            raise

        # Update all channels with their stream data
        for channel in channels:
            channel_id = channel.id
            if channel_id not in acl_streams_map:
                continue
            channel_data = acl_streams_map[channel_id]
            if channel_data["stream"] is None:
                continue
            # Update channel with stream data (no available drops check)
            channel.external_update(channel_data, [])
