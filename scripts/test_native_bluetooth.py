"""Discovery is explicit and bounded; proxy bonds are independent of scans."""
import asyncio
import base64
import json
from pathlib import Path
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tater_voice import native_bluetooth as bt, native_satellite

class BluetoothTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env=patch.dict(os.environ,{"TATER_BLUETOOTH_STATE_PATH":str(Path(self.temp.name)/"bonds.json")})
        self.env.start();self.addCleanup(self.env.stop)
        bt._observations.clear();bt._scan_until.clear();bt._status.clear();bt._bonds=None
        self.clients={"native:one":{"connected":True,"name":"Kitchen","capabilities":{"ble_gatt":True,"ble_on_demand":True}},"native:old":{"connected":True,"capabilities":{"ble_gatt":True}}}
        self.status=patch.object(native_satellite,"status_snapshot_sync",return_value={"clients":self.clients});self.status.start();self.addCleanup(self.status.stop)
        self.sender=AsyncMock(return_value={"ok":True})
        self.send=patch.object(native_satellite,"send_request",self.sender);self.send.start();self.addCleanup(self.send.stop)
    def advert(self):
        return {"adverts":[{"address":"AA:BB:CC:DD:EE:FF","rssi":-40,"address_type":1,"data":"0201061107fdea73e2ca5da89f1f46a81518b2a16b","name":"Meshtastic radio"}]}
    async def test_reading_workspace_never_starts_bluetooth(self):
        for _ in range(3): self.assertFalse(bt.snapshot()["scanning"])
        self.sender.assert_not_awaited()
        self.assertEqual(bt.ingest_advertisements("native:one",self.advert())["observations"],0)
    async def test_only_explicit_scan_accepts_advertisements(self):
        await bt.scan(selector="native:one",duration_s=15)
        self.assertEqual(self.sender.await_args.args[2]["t"],"scan")
        bt.ingest_advertisements("native:one",self.advert(),metadata={"device_name":"Kitchen"})
        snapshot=bt.snapshot()
        self.assertEqual(snapshot["devices"][0]["advertised_name"],"Meshtastic radio")
        self.assertIn("6ba1b218-15a8-461f-9fa8-5dcae273eafd",snapshot["devices"][0]["service_uuids"])
        self.assertNotIn("history",snapshot)
        self.assertNotIn("location_room",snapshot["devices"][0])
    async def test_expired_scan_drops_background_presence_packets(self):
        bt._scan_until["native:one"]=time.time()-1
        bt.ingest_advertisements("native:one",self.advert())
        self.assertEqual(bt.snapshot()["devices"],[])
    async def test_disconnect_clears_scan_lease_and_link_status_without_deleting_bonds(self):
        await bt.scan(selector="native:one")
        bt.ingest_event("native:one", {"t":"slots", "addrs":["aa:bb:cc:dd:ee:ff"]})
        record={"selector":"native:one","address":"aa:bb:cc:dd:ee:ff"}
        bt._save_bonds({"native:one|aa:bb:cc:dd:ee:ff":record})
        bt.disconnected("native:one")
        self.assertFalse(bt.snapshot()["scanning"])
        self.assertNotIn("native:one",bt._status)
        self.assertEqual(bt.snapshot()["paired"],[record])
    async def test_stopping_discovery_never_disconnects_active_links(self):
        await bt.scan(selector="native:one")
        self.sender.reset_mock()
        await bt.stop_scan("native:one")
        self.assertEqual(self.sender.await_args.args[2]["t"],"stop_scan")
        self.assertFalse(bt.snapshot()["scanning"])
        self.assertEqual(self.sender.await_count,1)
    async def test_old_offline_and_invalid_discovery_requests_are_rejected(self):
        for selector in ("native:old","native:missing"):
            with self.assertRaises(ValueError): await bt.scan(selector=selector)
        for duration in (0,61,-1,True,5.5,"15"):
            with self.assertRaises(ValueError): await bt.scan(duration_s=duration)
        self.sender.assert_not_awaited()
    async def test_malformed_advertisements_cannot_break_discovery(self):
        await bt.scan(selector="native:one")
        for payload in ({"adverts":5},{"adverts":{}},{"adverts":"invalid"}):
            bt.ingest_advertisements("native:one",payload)
        payload=self.advert()
        payload["adverts"][0]["service_uuids"]=[{},[],"invalid","6ba1b218-15a8-461f-9fa8-5dcae273eafd"]
        bt.ingest_advertisements("native:one",payload)
        self.assertEqual(bt.snapshot()["devices"][0]["service_uuids"],["6ba1b218-15a8-461f-9fa8-5dcae273eafd"])
    async def test_failed_scan_does_not_accept_unsolicited_observations(self):
        self.sender.side_effect=RuntimeError("offline")
        with self.assertRaises(RuntimeError): await bt.scan(selector="native:one")
        self.assertFalse(bt.snapshot()["scanning"])
        bt.ingest_advertisements("native:one",self.advert())
        self.assertEqual(bt.snapshot()["devices"],[])
    async def test_pair_preserves_preexisting_connection_and_never_saves_pin(self):
        await bt.scan(selector="native:one")
        bt.ingest_advertisements("native:one",self.advert())
        self.sender.reset_mock()
        self.sender.side_effect=[{"ok":False,"error":"already_connected"},{"ok":True,"bonded":True,"encrypted":True,"authenticated":True}]
        result=await bt.pair("native:one","aa:bb:cc:dd:ee:ff","012345")
        self.assertTrue(result["ok"])
        self.assertEqual(self.sender.await_count,2)
        saved=bt._bond_path().read_text()
        self.assertNotIn("012345",saved)
        self.assertNotIn("pin",saved)
        self.assertEqual(bt._bond_path().stat().st_mode & 0o777,0o600)
    async def test_failed_unpair_keeps_saved_metadata(self):
        record={"selector":"native:one","address":"aa:bb:cc:dd:ee:ff"}
        bt._save_bonds({"native:one|aa:bb:cc:dd:ee:ff":record})
        self.sender.side_effect=RuntimeError("offline")
        with self.assertRaises(RuntimeError): await bt.unpair("native:one",record["address"])
        self.assertEqual(bt.snapshot()["paired"],[record])

if __name__=='__main__':unittest.main()
