# Copyright 2019 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import unittest.mock  # in this file, importing just 'patch' would be confusing

import ops


class TestJujuVersion(unittest.TestCase):

    def test_parsing(self):
        test_cases = [
            ("0.0.0", 0, 0, '', 0, 0),
            ("0.0.2", 0, 0, '', 2, 0),
            ("0.1.0", 0, 1, '', 0, 0),
            ("0.2.3", 0, 2, '', 3, 0),
            ("10.234.3456", 10, 234, '', 3456, 0),
            ("10.234.3456.1", 10, 234, '', 3456, 1),
            ("1.21-alpha12", 1, 21, 'alpha', 12, 0),
            ("1.21-alpha1.34", 1, 21, 'alpha', 1, 34),
            ("2.7", 2, 7, '', 0, 0)
        ]

        for vs, major, minor, tag, patch, build in test_cases:
            v = ops.JujuVersion(vs)
            self.assertEqual(v.major, major)
            self.assertEqual(v.minor, minor)
            self.assertEqual(v.tag, tag)
            self.assertEqual(v.patch, patch)
            self.assertEqual(v.build, build)

    @unittest.mock.patch('os.environ', new={})
    def test_from_environ(self):
        # JUJU_VERSION is not set
        v = ops.JujuVersion.from_environ()
        self.assertEqual(v, ops.JujuVersion('0.0.0'))

        os.environ['JUJU_VERSION'] = 'no'
        with self.assertRaisesRegex(RuntimeError, 'not a valid Juju version'):
            ops.JujuVersion.from_environ()

        os.environ['JUJU_VERSION'] = '2.8.0'
        v = ops.JujuVersion.from_environ()
        self.assertEqual(v, ops.JujuVersion('2.8.0'))

    def test_has_app_data(self):
        self.assertTrue(ops.JujuVersion('2.8.0').has_app_data())
        self.assertTrue(ops.JujuVersion('2.7.0').has_app_data())
        self.assertFalse(ops.JujuVersion('2.6.9').has_app_data())

    def test_is_dispatch_aware(self):
        self.assertTrue(ops.JujuVersion('2.8.0').is_dispatch_aware())
        self.assertFalse(ops.JujuVersion('2.7.9').is_dispatch_aware())

    def test_has_controller_storage(self):
        self.assertTrue(ops.JujuVersion('2.8.0').has_controller_storage())
        self.assertFalse(ops.JujuVersion('2.7.9').has_controller_storage())

    def test_has_secrets(self):
        self.assertTrue(ops.JujuVersion('3.0.3').has_secrets)
        self.assertTrue(ops.JujuVersion('3.1.0').has_secrets)
        self.assertFalse(ops.JujuVersion('3.0.2').has_secrets)
        self.assertFalse(ops.JujuVersion('2.9.30').has_secrets)

    def test_supports_open_port_on_k8s(self):
        self.assertTrue(ops.JujuVersion('3.0.3').supports_open_port_on_k8s)
        self.assertTrue(ops.JujuVersion('3.3.0').supports_open_port_on_k8s)
        self.assertFalse(ops.JujuVersion('3.0.2').supports_open_port_on_k8s)
        self.assertFalse(ops.JujuVersion('2.9.30').supports_open_port_on_k8s)

    def test_supports_exec_service_context(self):
        self.assertFalse(ops.JujuVersion('2.9.30').supports_exec_service_context)
        self.assertTrue(ops.JujuVersion('4.0.0').supports_exec_service_context)
        self.assertFalse(ops.JujuVersion('3.0.0').supports_exec_service_context)
        self.assertFalse(ops.JujuVersion('3.1.5').supports_exec_service_context)
        self.assertTrue(ops.JujuVersion('3.1.6').supports_exec_service_context)
        self.assertFalse(ops.JujuVersion('3.2.1').supports_exec_service_context)
        self.assertTrue(ops.JujuVersion('3.2.2').supports_exec_service_context)
        self.assertTrue(ops.JujuVersion('3.3.0').supports_exec_service_context)
        self.assertTrue(ops.JujuVersion('3.4.0').supports_exec_service_context)

    def test_parsing_errors(self):
        invalid_versions = [
            "xyz",
            "foo.bar",
            "foo.bar.baz",
            "dead.beef.ca.fe",
            "1234567890.2.1",     # The major version is too long.
            "0.2..1",             # Two periods next to each other.
            "1.21.alpha1",        # Tag comes after period.
            "1.21-alpha",         # No patch number but a tag is present.
            "1.21-alpha1beta",    # Non-numeric string after the patch number.
            "1.21-alpha-dev",     # Tag duplication.
            "1.21-alpha_dev3",    # Underscore in a tag.
            "1.21-alpha123dev3",  # Non-numeric string after the patch number.
        ]
        for v in invalid_versions:
            with self.assertRaises(RuntimeError):
                ops.JujuVersion(v)

    def test_equality(self):
        test_cases = [
            ("1.0.0", "1.0.0", True),
            ("01.0.0", "1.0.0", True),
            ("10.0.0", "9.0.0", False),
            ("1.0.0", "1.0.1", False),
            ("1.0.1", "1.0.0", False),
            ("1.0.0", "1.1.0", False),
            ("1.1.0", "1.0.0", False),
            ("1.0.0", "2.0.0", False),
            ("1.2-alpha1", "1.2.0", False),
            ("1.2-alpha2", "1.2-alpha1", False),
            ("1.2-alpha2.1", "1.2-alpha2", False),
            ("1.2-alpha2.2", "1.2-alpha2.1", False),
            ("1.2-beta1", "1.2-alpha1", False),
            ("1.2-beta1", "1.2-alpha2.1", False),
            ("1.2-beta1", "1.2.0", False),
            ("1.2.1", "1.2.0", False),
            ("2.0.0", "1.0.0", False),
            ("2.0.0.0", "2.0.0", True),
            ("2.0.0.0", "2.0.0.0", True),
            ("2.0.0.1", "2.0.0.0", False),
            ("2.0.1.10", "2.0.0.0", False),
        ]

        for a, b, expected in test_cases:
            self.assertEqual(ops.JujuVersion(a) == ops.JujuVersion(b), expected)
            self.assertEqual(ops.JujuVersion(a) == b, expected)

    def test_comparison(self):
        test_cases = [
            ("1.0.0", "1.0.0", False, True),
            ("01.0.0", "1.0.0", False, True),
            ("10.0.0", "9.0.0", False, False),
            ("1.0.0", "1.0.1", True, True),
            ("1.0.1", "1.0.0", False, False),
            ("1.0.0", "1.1.0", True, True),
            ("1.1.0", "1.0.0", False, False),
            ("1.0.0", "2.0.0", True, True),
            ("1.2-alpha1", "1.2.0", True, True),
            ("1.2-alpha2", "1.2-alpha1", False, False),
            ("1.2-alpha2.1", "1.2-alpha2", False, False),
            ("1.2-alpha2.2", "1.2-alpha2.1", False, False),
            ("1.2-beta1", "1.2-alpha1", False, False),
            ("1.2-beta1", "1.2-alpha2.1", False, False),
            ("1.2-beta1", "1.2.0", True, True),
            ("1.2.1", "1.2.0", False, False),
            ("2.0.0", "1.0.0", False, False),
            ("2.0.0.0", "2.0.0", False, True),
            ("2.0.0.0", "2.0.0.0", False, True),
            ("2.0.0.1", "2.0.0.0", False, False),
            ("2.0.1.10", "2.0.0.0", False, False),
            ("2.10.0", "2.8.0", False, False),
        ]

        for a, b, expected_strict, expected_weak in test_cases:
            with self.subTest(a=a, b=b):
                self.assertEqual(ops.JujuVersion(a) < ops.JujuVersion(b), expected_strict)
                self.assertEqual(ops.JujuVersion(a) <= ops.JujuVersion(b), expected_weak)
                self.assertEqual(ops.JujuVersion(b) > ops.JujuVersion(a), expected_strict)
                self.assertEqual(ops.JujuVersion(b) >= ops.JujuVersion(a), expected_weak)
                # Implicit conversion.
                self.assertEqual(ops.JujuVersion(a) < b, expected_strict)
                self.assertEqual(ops.JujuVersion(a) <= b, expected_weak)
                self.assertEqual(b > ops.JujuVersion(a), expected_strict)
                self.assertEqual(b >= ops.JujuVersion(a), expected_weak)
