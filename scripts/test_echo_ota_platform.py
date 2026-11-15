from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'tater_voice/firmware.py'


class EchoOTAPlatformTests(unittest.TestCase):
    def helper(self):
        node = next(node for node in ast.parse(SOURCE.read_text()).body
                    if isinstance(node, ast.FunctionDef) and node.name == '_echo_usb_upgrade_reason')
        namespace = {'Dict': dict, 'Any': object, '_text': lambda value: str(value or '').strip(),
                     '_capability_version': lambda caps, key: int(caps.get(key) or 0)}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), 'exec'), namespace)
        return namespace[node.name]

    def test_old_firmware_cannot_take_app_system_or_bridge(self):
        for target in ('biscuit', 'radar', 'checkers', 'cronos', 'crown', 'athena', 'rook'):
            profile = f'echo-hardware-v1/{target}'
            device = {'ota_policy': {'package_schema': 2, 'platform': profile}}
            for caps in ({}, {'ota_package_schema': 2}, {'ota_package_schema': 2, 'ota_platform': 'wrong'}):
                self.assertIn('USB upgrade', self.helper()(device, caps))
            self.assertEqual('', self.helper()(device, {'ota_package_schema': 2, 'ota_platform': profile}))
        self.assertEqual('', self.helper()({}, {}))

    def test_platform_policy_survives_catalog_normalization(self):
        source = SOURCE.read_text()
        self.assertIn('"ota_policy": copy.deepcopy(raw_device.get("ota_policy"))', source)
        self.assertIn('ota_supported = ota_supported and not usb_upgrade_reason', source)
        self.assertIn('_text(context.get("usb_upgrade_reason")) or', source)


if __name__ == '__main__':
    unittest.main()
