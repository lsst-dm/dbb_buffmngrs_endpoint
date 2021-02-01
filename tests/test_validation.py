# This file is part of dbb_buffmngrs_endpoint.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import jsonschema
import os
import unittest
import yaml
from lsst.dbb.buffmngrs.endpoint import validation


TESTDIR = os.path.dirname(__file__)


class FinderConfigValidationTest(unittest.TestCase):

    def setUp(self):
        self.schema = yaml.safe_load(validation.FINDER)
        self.etc = os.path.join(TESTDIR, "../etc")

    def tearDown(self):
        pass

    def testSampleFile(self):
        """Test if sample configuration validates under the schema.
        """
        with open(os.path.join(self.etc, "finder.yaml")) as f:
            config = yaml.safe_load(f)
        jsonschema.validate(config, self.schema)


class BackfillConfigValidationTest(unittest.TestCase):

    def setUp(self):
        self.schema = yaml.safe_load(validation.BACKFILL)
        self.etc = os.path.join(TESTDIR, "../etc")

    def tearDown(self):
        pass

    def testSampleFile(self):
        """Test if sample configuration validates under the schema.
        """
        with open(os.path.join(self.etc, "backfill.yaml")) as f:
            config = yaml.safe_load(f)
        jsonschema.validate(config, self.schema)


class Gen2IngesterConfigValidationTest(unittest.TestCase):

    def setUp(self):
        self.schema = yaml.safe_load(validation.INGESTER)
        self.etc = os.path.join(TESTDIR, "../etc")

    def tearDown(self):
        pass

    def testSampleFile(self):
        """Test if sample configuration validates under the schema.
        """
        with open(os.path.join(self.etc, "gen2ingester.yaml")) as f:
            config = yaml.safe_load(f)
        jsonschema.validate(config, self.schema)


class Gen3IngesterConfigValidationTest(unittest.TestCase):

    def setUp(self):
        self.schema = yaml.safe_load(validation.INGESTER)
        self.etc = os.path.join(TESTDIR, "../etc")

    def tearDown(self):
        pass

    def testSampleFile(self):
        """Test if sample configuration validates under the schema.
        """
        with open(os.path.join(self.etc, "gen3ingester.yaml")) as f:
            config = yaml.safe_load(f)
        jsonschema.validate(config, self.schema)
