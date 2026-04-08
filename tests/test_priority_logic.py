import unittest
from unittest.mock import MagicMock
from datetime import datetime, timedelta, timezone
from src.services.channel_service import ChannelService
from src.models.channel import Channel
from src.models.game import Game
from src.models.campaign import DropsCampaign
from src.config import MAX_INT

class TestPriorityLogic(unittest.TestCase):
    def setUp(self):
        self.twitch = MagicMock()
        self.service = ChannelService(self.twitch)
        self.game = Game({"id": "1", "name": "Test Game", "boxArtURL": ""})
        self.channel = MagicMock(spec=Channel)
        self.channel.game = self.game
        self.twitch.wanted_games = [self.game]

    def test_priority_list_mode(self):
        self.twitch.settings.mining_priority = "PRIORITY_LIST"
        priority = self.service.get_priority(self.channel)
        self.assertEqual(priority, 0)

    def test_time_to_end_mode(self):
        self.twitch.settings.mining_priority = "TIME_TO_END"
        now = datetime.now(timezone.utc)
        ends_at = now + timedelta(hours=2)
        
        campaign = MagicMock(spec=DropsCampaign)
        campaign.game = self.game
        campaign.active = True
        campaign.ends_at = ends_at
        
        self.twitch.inventory = [campaign]
        
        priority = self.service.get_priority(self.channel)
        self.assertEqual(priority, int(ends_at.timestamp()))

    def test_time_ratio_mode(self):
        from unittest.mock import patch
        self.twitch.settings.mining_priority = "TIME_RATIO"
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        starts_at = now - timedelta(hours=1)
        ends_at = now + timedelta(hours=1) # 50% progressed
        
        campaign = MagicMock(spec=DropsCampaign)
        campaign.game = self.game
        campaign.active = True
        campaign.starts_at = starts_at
        campaign.ends_at = ends_at
        
        self.twitch.inventory = [campaign]
        
        with patch("src.services.channel_service.datetime") as mock_datetime:
            mock_datetime.now.return_value = now
            mock_datetime.timezone = timezone
            priority = self.service.get_priority(self.channel)
            
        # ratio = 1/2 = 0.5. priority = (1.0 - 0.5) * 1000000 = 500000
        self.assertEqual(priority, 500000)
