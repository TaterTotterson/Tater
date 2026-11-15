"""Rook hardware calls remain explicitly opt-in and balanced-wire compatible."""
import unittest
from unittest.mock import AsyncMock, patch
from tater_voice import sat_chat

class RookHardwareCallTests(unittest.IsolatedAsyncioTestCase):
    async def test_rook_requires_its_own_opt_in_and_both_balanced_receivers(self):
        for board in ("checkers", "cronos", "crown", "athena", "rook"):
            for reverse in (False, True):
                for own, peer_balanced in ((True,True),(False,True),(True,False)):
                    with self.subTest(board=board,reverse=reverse,own=own,peer_balanced=peer_balanced):
                        await sat_chat.reset_for_tests()
                        def client(target,balanced,explicit):
                            return {"connected":True,"board":target,"capabilities":{
                                "sat_chat_audio":True,"sat_chat_video":True,"sat_chat_h264":True,
                                "sat_chat_can_initiate":True,"sat_chat_can_end":True,"sat_chat_ui":True,
                                "sat_chat_h264_version":1,"sat_chat_h264_balanced":balanced,
                                "sat_chat_h264_balanced_only":explicit}}
                        rook=client("rook",True,own)
                        peer=client(board,peer_balanced,True)
                        a,b=(peer,rook) if reverse else (rook,peer)
                        sent=AsyncMock()
                        with patch.object(sat_chat,"_native_clients",new=AsyncMock(return_value={"native:a":a,"native:b":b})), patch.object(sat_chat,"_send",new=sent), patch.object(sat_chat,"refresh_directories",new=AsyncMock()):
                            await sat_chat.start("native:a","native:b",mode="video")
                        for message in sent.await_args_list:
                            receiver=b if message.args[0]=="native:a" else a
                            expected="echo-h264-balanced" if own and peer_balanced else ("echo-low" if receiver["board"]=="rook" else "echo-balanced")
                            self.assertEqual(message.args[2]["video_send_profile"],expected)
