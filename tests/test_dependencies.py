"""Tests for cluster_pack.dependencies module."""
import tempfile
import pytest

from cluster_pack import dependencies
from cluster_pack.settings import UV_AVAILABLE


class TestNormalizePackageName:
    def test_lowercase(self):
        assert dependencies.normalize_package_name("TensorFlow") == "tensorflow"

    def test_underscore_to_hyphen(self):
        assert dependencies.normalize_package_name("tf_yarn") == "tf-yarn"

    def test_dot_to_hyphen(self):
        assert dependencies.normalize_package_name("zope.interface") == "zope-interface"

    def test_mixed_normalization(self):
        assert dependencies.normalize_package_name("My_Package.Name") == "my-package-name"


class TestParseRequirement:
    def test_simple_requirement(self):
        req = dependencies.parse_requirement("numpy")
        assert req.name == "numpy"
        assert str(req.specifier) == ""

    def test_requirement_with_version(self):
        req = dependencies.parse_requirement("numpy==1.21.0")
        assert req.name == "numpy"
        assert "1.21.0" in str(req.specifier)

    def test_requirement_with_extras(self):
        req = dependencies.parse_requirement("requests[security]>=2.0")
        assert req.name == "requests"
        assert "security" in req.extras

    def test_requirement_with_complex_specifier(self):
        req = dependencies.parse_requirement("django>=3.0,<4.0")
        assert req.name == "django"
        assert req.specifier.contains("3.5")
        assert not req.specifier.contains("4.0")


class TestFormatRequirement:
    def test_with_version(self):
        assert dependencies.format_requirement("numpy", "1.21.0") == "numpy==1.21.0"

    def test_without_version(self):
        assert dependencies.format_requirement("numpy", None) == "numpy"

    def test_empty_version(self):
        assert dependencies.format_requirement("numpy", "") == "numpy"


class TestFormatRequirements:
    def test_basic(self):
        reqs = {"numpy": "1.21.0", "pandas": "1.3.0"}
        result = dependencies.format_requirements(reqs)
        assert "numpy==1.21.0" in result
        assert "pandas==1.3.0" in result

    def test_empty(self):
        assert dependencies.format_requirements({}) == []

    def test_none(self):
        assert dependencies.format_requirements(None) == []

    def test_without_version(self):
        reqs = {"numpy": "1.21.0", "pandas": ""}
        result = dependencies.format_requirements(reqs)
        assert "numpy==1.21.0" in result
        assert "pandas" in result


class TestNormalizeRequirements:
    def test_underscore_to_hyphen(self):
        result = dependencies.normalize_requirements(
            ["tf_yarn", "typing_extension", "to-to"]
        )
        assert result == ["tf-yarn", "typing-extension", "to-to"]


class TestSortRequirements:
    def test_case_insensitive_sort(self):
        reqs = ["Zebra", "apple", "Banana"]
        result = dependencies.sort_requirements(reqs)
        assert result == ["apple", "banana", "zebra"]

    def test_sorts_requirements(self):
        reqs = [
            "pipdeptree==2.0.0",
            "GitPython==3.1.14",
            "six==1.15.0",
            "Cython==0.29.22",
        ]
        expected = [
            "cython==0.29.22",
            "gitpython==3.1.14",
            "pipdeptree==2.0.0",
            "six==1.15.0",
        ]
        assert dependencies.sort_requirements(reqs) == expected


class TestFilterBuildRequirements:
    def test_filters_pip(self):
        reqs = ["numpy==1.0", "pip==21.0", "pandas==1.0"]
        result = dependencies.filter_build_requirements(reqs)
        assert "pip==21.0" not in result
        assert "numpy==1.0" in result
        assert "pandas==1.0" in result

    def test_filters_wheel(self):
        reqs = ["wheel==0.37.0", "numpy==1.0"]
        result = dependencies.filter_build_requirements(reqs)
        assert "wheel==0.37.0" not in result
        assert "numpy==1.0" in result

    def test_filters_setuptools(self):
        reqs = ["setuptools==50.0", "numpy==1.0"]
        result = dependencies.filter_build_requirements(reqs)
        assert "setuptools==50.0" not in result
        assert "numpy==1.0" in result

    def test_filters_all_build_packages(self):
        reqs = ["pip==21.0", "wheel==0.37.0", "setuptools==50.0", "numpy==1.0"]
        result = dependencies.filter_build_requirements(reqs)
        assert result == ["numpy==1.0"]


class TestGetInstalledPackages:
    def test_returns_dict(self):
        installed = dependencies.get_installed_packages()
        assert isinstance(installed, dict)

    def test_contains_known_packages(self):
        installed = dependencies.get_installed_packages()
        assert "pip" in installed or "Pip" in installed.keys()

    def test_normalized_names(self):
        installed = dependencies.get_installed_packages()
        for name in installed.keys():
            assert name == name.lower()
            assert "_" not in name


class TestCheckRequirementsSatisfied:
    def test_satisfied_requirements(self):
        result = dependencies.check_requirements_satisfied(["pip", "setuptools"])
        assert result is True

    def test_unsatisfied_requirement(self):
        result = dependencies.check_requirements_satisfied(["nonexistent_package_12345"])
        assert result is False

    def test_version_specifier_satisfied(self):
        result = dependencies.check_requirements_satisfied(["pip>=1.0"])
        assert result is True

    def test_version_specifier_not_satisfied(self):
        result = dependencies.check_requirements_satisfied(["pip>=9999.0"])
        assert result is False


class TestCheckVenvHasRequirements:
    def test_current_venv_has_pip(self):
        if not dependencies.is_running_in_venv():
            pytest.skip("Not running in a venv")
        result = dependencies.check_venv_has_requirements(None, ["pip"])
        assert result is True

    def test_current_venv_missing_package(self):
        if not dependencies.is_running_in_venv():
            pytest.skip("Not running in a venv")
        result = dependencies.check_venv_has_requirements(None, ["nonexistent_pkg_12345"])
        assert result is False


@pytest.mark.skipif(not UV_AVAILABLE, reason="uv not available")
class TestCreateUvVenv:
    def test_creates_venv_with_requirements(self):
        with tempfile.TemporaryDirectory() as tempdir:
            venv_path = f"{tempdir}/test_venv"
            dependencies.create_uv_venv(
                venv_path,
                requirements=["six==1.16.0"],
            )
            assert dependencies.check_venv_has_requirements(venv_path, ["six==1.16.0"])

    def test_creates_empty_venv(self):
        with tempfile.TemporaryDirectory() as tempdir:
            venv_path = f"{tempdir}/test_venv"
            dependencies.create_uv_venv(
                venv_path,
                requirements=[],
            )
            assert dependencies.check_venv_has_requirements(venv_path, ["pip"])
