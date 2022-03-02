import os
import unittest
from contextlib import contextmanager
from unittest import mock

from project import Project

@mock.patch.dict(os.environ, {"INGEST_API": "MOCKED"})
class TestProject(unittest.TestCase):
    def setUp(self) -> None:
        self.project = Project('mock-uuid')

    def test_initialized(self):
        assert self.project.uuid == 'mock-uuid'

    @mock.patch.object(Project, '_get_azul_contributor_matrix_download_link', return_value='testing.tsv')
    @mock.patch('utils.download_file')
    def test_can_get_matrix(self, download_mock, project_mock):
        self.project.get_contributor_generated_matrix()

        download_mock.assert_called_with('testing.tsv')

    @mock.patch.object(Project, '_get_azul_metadata_download_link', return_value='testing.tsv')
    @mock.patch('utils.download_file')
    def test_can_get_metadata(self, download_mock, project_mock):
        self.project.get_azul_project_metadata_tsv()

        download_mock.assert_called_with('testing.tsv')


if __name__ == '__main__':
    unittest.main()
