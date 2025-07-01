"""
CertTools_t module provide unit tests for CertTools module
"""

# system modules
import os
import tempfile
import unittest

# WMCore modules
from Utils.CertTools import caBundle


class TestCaBundle(unittest.TestCase):
    """
    unittest for CertTools functions
    """
    def setUp(self):
        """
        setup certificates
        """
        self.tempDir = tempfile.TemporaryDirectory()
        self.pemGood = (
            "-----BEGIN CERTIFICATE-----\n"
            "MIIC+zCCAeOgAwIBAgIJAK8b...\n"
            "-----END CERTIFICATE-----\n"
        )
        self.pemBad = (
            "-----BEGIN FAKE-----\n"
            "INVALIDCERTDATA\n"
            "-----END FAKE-----\n"
        )

    def tearDown(self):
        """
        delete temp area with certificates
        """
        self.tempDir.cleanup()

    def testValidPEMFiles(self):
        """
        test valid pem files
        """
        for i in range(3):
            path = os.path.join(self.tempDir.name, f"cert{i}.pem")
            with open(path, "w") as f:
                f.write(self.pemGood)

        bundle = caBundle(self.tempDir.name)
        self.assertEqual(bundle.count("BEGIN CERTIFICATE"), 3)
        self.assertIn("END CERTIFICATE", bundle)

    def testMixedValidAndInvalid(self):
        """
        test valid and invalid files
        """
        valid_path = os.path.join(self.tempDir.name, "valid.pem")
        with open(valid_path, "w") as f:
            f.write(self.pemGood)

        badExtPath = os.path.join(self.tempDir.name, "invalid.txt")
        with open(badExtPath, "w") as f:
            f.write(self.pemGood)

        bad_content_path = os.path.join(self.tempDir.name, "bad.pem")
        with open(bad_content_path, "w") as f:
            f.write(self.pemBad)

        bundle = caBundle(self.tempDir.name)
        self.assertIn("BEGIN CERTIFICATE", bundle)
        self.assertNotIn("FAKE", bundle)
        self.assertEqual(bundle.count("BEGIN CERTIFICATE"), 1)

    def test_noPEMFiles(self):
        """
        test case without pem files
        """
        with open(os.path.join(self.tempDir.name, "file.txt"), "w") as f:
            f.write("no cert here")

        with self.assertRaises(ValueError) as ctx:
            caBundle(self.tempDir.name)
        self.assertIn("No PEM files found", str(ctx.exception))

    def testInvalidDirectory(self):
        """
        test invalid directory
        """
        with self.assertRaises(ValueError):
            caBundle("/non/existent/directory")


if __name__ == '__main__':
    unittest.main()
