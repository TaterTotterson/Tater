import asyncio
import time
import unittest

from tater_voice import sendspin_playback


class SendspinStreamOutcomeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        sendspin_playback._active_live_streams.clear()
        sendspin_playback._live_target_owners.clear()
        sendspin_playback._stream_outcomes.clear()

    def state(self, stream_id="music-one"):
        return sendspin_playback._LiveStreamState(
            stream_id=stream_id,
            targets=[{"selector": "native:kitchen"}],
            started_event=asyncio.Event(),
            volume_percent={"native:kitchen": 75},
            airplay_targets=("airplay:office",),
            start_unix_ms=1_000,
            expected_duration_s=180.0,
            frames_sent=sendspin_playback.SENDSPIN_SAMPLE_RATE * 12,
        )

    async def finish(self, state, coroutine):
        task = asyncio.create_task(coroutine)
        state.task = task
        sendspin_playback._active_live_streams[state.stream_id] = state
        sendspin_playback._live_target_owners["native:kitchen"] = state.stream_id
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        sendspin_playback._finish_live_stream(
            state,
            ["native:kitchen"],
            task,
        )

    async def test_records_completion_and_returns_defensive_snapshot(self):
        state = self.state()

        async def complete():
            return {"duration_s": 42.25}

        await self.finish(state, complete())
        result = await sendspin_playback.stream_outcomes([state.stream_id])
        row = result["outcomes"][state.stream_id]
        self.assertEqual(row["status"], "completed")
        self.assertEqual(row["duration_s"], 42.25)
        self.assertEqual(row["expected_duration_s"], 180.0)
        self.assertEqual(row["members"], ["native:kitchen", "airplay:office"])
        self.assertNotIn(state.stream_id, sendspin_playback._active_live_streams)
        self.assertNotIn("native:kitchen", sendspin_playback._live_target_owners)

        row["members"].append("mutated")
        again = await sendspin_playback.stream_outcomes([state.stream_id])
        self.assertEqual(
            again["outcomes"][state.stream_id]["members"],
            ["native:kitchen", "airplay:office"],
        )

    async def test_classifies_source_transport_and_intentional_stops(self):
        async def fail(error):
            raise error

        source = self.state("source")
        await self.finish(
            source,
            fail(sendspin_playback.SendspinSourceError("ffmpeg failed")),
        )
        transport = self.state("transport")
        await self.finish(
            transport,
            fail(sendspin_playback.SendspinPlaybackError("satellite disconnected")),
        )

        stopped = self.state("stopped")
        stopped.stop_reason = "user_stop"
        stop_task = asyncio.create_task(asyncio.sleep(60))
        stop_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await stop_task
        sendspin_playback._finish_live_stream(stopped, [], stop_task)

        replaced = self.state("replaced")
        replaced.stop_reason = "replaced"
        replace_task = asyncio.create_task(asyncio.sleep(60))
        replace_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await replace_task
        sendspin_playback._finish_live_stream(replaced, [], replace_task)

        rows = (await sendspin_playback.stream_outcomes(
            ["source", "transport", "stopped", "replaced"]
        ))["outcomes"]
        self.assertEqual(rows["source"]["error_kind"], "source")
        self.assertEqual(rows["transport"]["error_kind"], "transport")
        self.assertEqual(rows["stopped"]["status"], "stopped")
        self.assertEqual(rows["stopped"]["stop_reason"], "user_stop")
        self.assertEqual(rows["replaced"]["status"], "replaced")

    async def test_query_filters_old_rows_and_registry_is_bounded(self):
        now_ms = time.time_ns() // 1_000_000
        sendspin_playback._stream_outcomes["expired"] = {
            "stream_id": "expired",
            "ended_unix_ms": now_ms
            - sendspin_playback.SENDSPIN_OUTCOME_RETENTION_MS
            - 1,
        }
        for index in range(sendspin_playback.SENDSPIN_OUTCOME_LIMIT + 4):
            sendspin_playback._stream_outcomes[f"stream-{index}"] = {
                "stream_id": f"stream-{index}",
                "ended_unix_ms": now_ms + index,
            }
        sendspin_playback._prune_stream_outcomes(now_ms)
        self.assertNotIn("expired", sendspin_playback._stream_outcomes)
        self.assertEqual(
            len(sendspin_playback._stream_outcomes),
            sendspin_playback.SENDSPIN_OUTCOME_LIMIT,
        )
        self.assertNotIn("stream-0", sendspin_playback._stream_outcomes)

    async def test_public_stop_records_an_intentional_reason(self):
        state = self.state("manual-stop")
        task = asyncio.create_task(asyncio.sleep(60))
        state.task = task
        sendspin_playback._active_live_streams[state.stream_id] = state
        sendspin_playback._live_target_owners["native:kitchen"] = state.stream_id
        task.add_done_callback(
            lambda finished: sendspin_playback._finish_live_stream(
                state,
                ["native:kitchen"],
                finished,
            )
        )

        result = await sendspin_playback.stop_live_stream(
            state.stream_id,
            reason="music_core_stop",
        )
        await asyncio.sleep(0)
        row = (await sendspin_playback.stream_outcomes([state.stream_id]))[
            "outcomes"
        ][state.stream_id]
        self.assertTrue(result["stopped"])
        self.assertEqual(row["status"], "stopped")
        self.assertEqual(row["stop_reason"], "music_core_stop")


if __name__ == "__main__":
    unittest.main()
